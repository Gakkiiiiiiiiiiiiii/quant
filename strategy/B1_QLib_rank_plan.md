# B1 + Qlib 学习排序落地方案

## 1. 目标与边界

这套方案的目标不是让模型替代 B1，而是让模型在 **B1 候选内部做精排**。

策略职责拆分如下：

- **规则层**：保证样本“长得像 B1”
- **模型层**：判断哪些 B1 候选更接近“完美图形”
- **执行层**：根据模型分数决定 probe、confirm 和主仓档位

正确链路：

> B1 规则召回 → Qlib 模型排序 → 执行层按分数分配 probe / confirm / 主仓

不建议链路：

> 全市场裸选股 → 模型黑盒决定一切

---

## 2. 总体架构

### 2.1 三层结构

#### 第一层：规则层
保留当前 v5 规则定义：

- `eligible_entry`
- `env_a`，后续补 `env_b`
- `启动基础v5`
- `回调质量v5`
- `极致缩量v5`
- `K线约束v5`
- `禁入v5`
- `B1候选v5`
- `B1确认v5`
- `试探失效v5`

#### 第二层：模型层
只在 `B1候选v5 == 1` 的样本上训练一个排序模型或连续打分模型。

#### 第三层：执行层
模型不直接产生买卖信号，只影响：

- 同日多个候选谁先买
- `probe` 是否开仓
- `confirm` 后能否补主仓
- 主仓采用 `12% / 15% / 18%` 哪一档

---

## 3. 样本构造

### 3.1 样本单位

采用 **事件样本**，不是普通日频样本。

定义：

- 样本索引：`(instrument, t)`
- 条件：`B1候选v5[t] == 1`
- 含义：`t` 日收盘后观察到一个 B1 候选，若交易则在 `t+1` 开盘执行 `probe`

不要把所有股票每天都拿去训练。只训练规则已经筛出来的候选事件。

### 3.2 去重规则

B1 候选常会在相邻几天连续触发。为避免同一波回调被重复采样，建议做事件去重。

```python
# 候选事件去重
if instrument 在最近 8 个交易日内已经出现过 B1候选v5:
    若前一个候选尚未失效/确认，则跳过本次样本
```

建议参数：

- `dedup_window = 8`

### 3.3 样本纳入条件

训练样本纳入条件：

- `B1候选v5[t] == 1`
- `eligible_entry == 1`
- 当日非一字涨停，非停牌
- `t+1` 有可交易开盘价
- 后续至少有 10~15 个交易日行情可用于打标签

样本剔除条件：

- 重大停牌导致无法形成真实交易路径
- `t+1` 开盘缺失
- 上市不足 60 日或流动性不足

---

## 4. 标签设计

### 4.1 主标签：交易质量分数

不要直接预测未来 5 日收益率。建议直接预测一个与 B1 执行逻辑一致的连续分数。

```python
entry_px = open[t+1]

mfe_10 = max(high[t+1:t+10]) / entry_px - 1        # 10日最大顺行
mae_5  = min(low[t+1:t+5])  / entry_px - 1         # 5日最大逆行
confirm_hit = any(B1确认v5[t+1:t+5] == 1)
probe_fail  = first_hit(
    试探失效v5, LT硬止损, ST止损, probe_timeout
    in [t+1, t+5]
)

label_rank = (
    1.2 * clip(mfe_10, 0.00, 0.15)
    - 1.0 * clip(-mae_5, 0.00, 0.08)
    + 0.4 * int(confirm_hit)
    - 0.8 * int(probe_fail)
)
```

解释：

- 能快速走出顺行空间，加分
- 一买就逆行，扣分
- 能进入 `confirm`，加分
- 在 `probe` 阶段就失败，重扣分

### 4.2 辅助标签：是否为高质量候选

```python
label_cls = 1 if (
    confirm_hit
    and mfe_10 >= 0.10
    and not probe_fail
) else 0
```

含义：

- 候选后 5 日内能确认
- 10 日内能摸到 `+10%`
- 且不先被 `probe invalid / ST / LT / timeout` 打掉

这个标签适合做：

- SHAP 分析
- 分桶命中率分析
- score decile 单调性检查

---

## 5. 特征工程

建议采用 **Alpha 通用特征 + B1 专属结构特征** 的双层结构。

### 5.1 通用层：Alpha 基础特征

先用 qlib 常规价量特征做底座：

- `OPEN, HIGH, LOW, CLOSE, VWAP, VOLUME, AMOUNT`
- 1/3/5/10/20 日收益
- 1/3/5/10/20 日波动率
- 量比、换手、振幅、乖离
- rolling max/min、rank、zscore

### 5.2 B1 专属特征组

#### A. 结构强化特征

```python
f_lt_slope_3   = LT / REF(LT, 3) - 1
f_lt_slope_5   = LT / REF(LT, 5) - 1
f_st_over_lt   = ST / LT - 1
f_cnt_above_lt = COUNT(C > LT, 30) / 30
f_cnt_above_st = COUNT(C > ST, 20) / 20
f_close_lt_dev = C / LT - 1
f_close_st_dev = C / ST - 1
```

#### B. 启动段强度特征

```python
f_range_40         = HHV(H,40) / LLV(L,40) - 1
f_bull_bars_20     = COUNT(C > O and C/REF(C,1) > 1.03, 20)
f_big_up_cnt_30    = COUNT(C > O and C/REF(C,1) >= 1.05 and V > MA(V,20)*1.5, 30)
f_recent_high_days = BARSLAST(H >= HHV(H,20) * 0.995)
f_attack_quality   = SUM(max(C/REF(C,1)-1,0), 10) / (SUM(abs(C/REF(C,1)-1),10)+1e-6)
```

#### C. 回调质量特征

```python
f_j_value              = J
f_pullback_from_hh     = (REF(HHV(H,20),1) - C) / REF(HHV(H,20),1)
f_days_from_recent_high = BARSLAST(最近高点)
f_break_lt_cnt_10      = COUNT(C < LT, 10)
f_big_down_cnt_12      = COUNT(C < O and (O-C)/REF(C,1) >= 0.04, 12)
f_dist_down_cnt_12     = COUNT(C < O and V >= MA(V,20)*1.8, 12)
f_low_lt_dev           = L / LT - 1
f_pullback_range_5     = HHV(H,5)/LLV(L,5) - 1
f_down_vol_share_8     = SUM(IF(C<O,V,0),8) / (SUM(V,8)+1e-6)
```

#### D. 缩量衰竭特征

```python
f_vol_to_ma20      = V / MA(V,20)
f_vol_rank_10      = RANK(V, 10)
f_vol_rank_20      = RANK(V, 20)
f_turnover_to_ma20 = turnover / MA(turnover,20)
f_shrink_days_5    = COUNT(V < MA(V,20)*0.8, 5)
f_atr_5_to_20      = ATR(5) / ATR(20)
```

#### E. K 线形态特征

```python
f_body_pct   = abs(C/O - 1)
f_range_pct  = (H-L) / REF(C,1)
f_upper_wick = (H - max(C,O)) / REF(C,1)
f_lower_wick = (min(C,O) - L) / REF(C,1)
```

#### F. 相对强度 / 板块特征

```python
f_ret5_excess_idx   = ret_5_stock - ret_5_index
f_ret10_excess_idx  = ret_10_stock - ret_10_index
f_ret5_excess_ind   = ret_5_stock - ret_5_industry
f_ret10_excess_ind  = ret_10_stock - ret_10_industry
f_ind_rank_10       = industry_ret_10d_rank_pct
f_ind_rank_20       = industry_ret_20d_rank_pct
f_same_ind_up_cnt   = 同行业当日涨幅>3%的股票数
```

#### G. 风险状态特征

如果引入 IVIX，可加入：

```python
f_idx_above_ma20 = int(index_close > MA(index_close,20))
f_breadth_5      = up_stocks_5d_ratio
f_ivix_pct_120   = IVIX_120d_percentile
f_ivix_slope_5   = IVIX / MA(IVIX,5) - 1
```

---

## 6. 模型选择

### 6.1 第一阶段

优先使用 qlib 原生 `LGBModel` 回归：

- 模型：`LGBModel`
- 目标：预测 `label_rank`
- 用法：每天对 `B1候选v5` 打分并排序

理由：

- 事件样本量不会太大
- GBDT 对手工结构特征稳定
- 可做特征重要性和 SHAP 分析
- 调参与迭代成本低

### 6.2 第二阶段

等第一阶段稳定后，再拆成两阶段模型：

- **Model A**：预测 `probe` 成功概率
- **Model B**：在已 `probe` 的样本上预测 `confirm + 10% hit` 概率

---

## 7. Qlib 数据集落地

### 7.1 建议目录结构

```text
b1_qlib/
├─ conf/
│  ├─ train_b1_rank.yaml
│  ├─ backtest_b1_rank.yaml
├─ features/
│  ├─ b1_ops.py
│  ├─ industry_features.py
│  ├─ ivix_features.py
├─ signals/
│  ├─ b1_v5_rules.py
│  ├─ label_builder.py
├─ dataset/
│  ├─ handler_b1_event.py
│  ├─ processor.py
├─ model/
│  ├─ train_b1_rank.py
│  ├─ infer_b1_rank.py
├─ strategy/
│  ├─ b1_ranked_strategy.py
│  ├─ portfolio_manager.py
└─ research/
   ├─ analyze_feature_importance.py
   ├─ analyze_decile.py
```

### 7.2 Handler 设计

定义 `B1EventHandler`，输出两类内容：

- `feature_df`：只包含 `B1候选v5 == 1` 的事件样本
- `label_df`：对应事件标签

示意：

```python
class B1EventHandler(DataHandlerLP):
    def __init__(self, instruments, start_time, end_time, fit_start_time, fit_end_time):
        super().__init__(...)

    def get_feature_config(self):
        return [
            # Alpha 基础
            "$open", "$high", "$low", "$close", "$volume", "$vwap",
            "Ref($close,1)/$close - 1",
            # B1 custom
            "B1_LT_SLOPE_3",
            "B1_CNT_ABOVE_LT_30",
            "B1_PULLBACK_FROM_HH20",
            "B1_VOL_TO_MA20",
            "B1_BIG_DOWN_CNT_12",
            "B1_J_VALUE",
            "B1_IND_RANK_10",
        ]

    def get_label_config(self):
        return ["B1_LABEL_RANK"]
```

### 7.3 Processor 设计

建议处理器：

- 缺失值处理
- `winsorize / robust zscore`
- 按日期横截面标准化

```python
learn_processors = [
    DropnaLabel(),
    ProcessInf(),
    CSRankNorm(),
]

infer_processors = [
    ProcessInf(),
    CSRankNorm(),
]
```

---

## 8. 训练方式

### 8.1 时间切分

禁止随机切分。必须做滚动训练 / 滚动验证 / 滚动测试。

建议：

- 训练：24 个月
- 验证：6 个月
- 测试：6 个月
- 每 3 个月向前滚动一次

示例：

```text
train: 2022-01 ~ 2023-12
valid: 2024-01 ~ 2024-06
test : 2024-07 ~ 2024-12

然后滚动到：

train: 2022-04 ~ 2024-03
valid: 2024-04 ~ 2024-09
test : 2024-10 ~ 2025-03
```

### 8.2 泄漏控制

必须执行以下约束：

#### A. 事件窗口 embargo
由于标签使用未来 5~10 日窗口，相邻样本容易泄漏。建议对同一股票设置：

- `embargo = 10` 交易日

#### B. 特征只使用当日及过去信息
训练日 `t` 的特征只能使用 `t` 及以前信息，标签只能使用 `t+1` 以后。

#### C. 行业强度、指数特征也必须按当日可得口径构造

---

## 9. 回测接入方式

### 9.1 最稳妥方案：先替换排序，不改规则

先不动现有执行逻辑，只把现有 `priority_score` 替换成：

```python
final_score = 0.3 * quality_score + 0.7 * model_score
```

### 9.2 Probe 开仓

当日满足：

- `env_a == 1`
- `B1候选v5 == 1`
- `final_score` 进入当日前 `K`

则允许开 `probe`：

```python
if rank <= K_probe:
    open_probe()
```

建议初始参数：

- `K_probe = 3`
- `min_score_threshold = 当日候选分数中位数以上`

### 9.3 Confirm 加仓

仅当：

- 已有 `probe`
- `B1确认v5 == 1`
- `model_score >= confirm_threshold`

才允许补到主仓：

```python
if has_probe and B1确认v5 and model_score >= th_confirm:
    add_to_main_position()
```

### 9.4 仓位映射

根据分数分桶决定主仓档位：

```python
if model_score >= q80:
    main_weight = 0.18
elif model_score >= q50:
    main_weight = 0.15
else:
    main_weight = 0.12
```

### 9.5 卖出规则

卖出完全保持现有 `v5 + v4` 体系：

- `probe invalid`
- `LT / ST`
- `confirm` 后硬清仓
- 软卖点
- `30 / 30 / 30 / 10` 分批兑现

模型暂不参与卖出。

---

## 10. AB 测试矩阵

### A 组：现有基线
- 纯 `v5`
- 排序使用 `priority_score`

### B 组：模型替换排序
- 纯 `model_score`
- 其他执行不变

### C 组：混合排序
- `0.3 * quality_score + 0.7 * model_score`

### D 组：模型只控制 confirm
- `probe` 仍按原规则
- 仅高分样本允许 `confirm` 加仓

### E 组：模型 + env_b
- 在 C 组基础上补行业强度过滤

---

## 11. 评价指标

不要只看总收益，至少跟踪以下 8 项：

1. `probe_invalid_rate`
2. `probe_timeout_rate`
3. `confirm_conversion_rate`
4. `tp10_hit_rate`
5. `avg_mfe_10`
6. `avg_mae_5`
7. 分数分桶单调性
8. 最大回撤 / Calmar

其中最关键的是：

- **分数前 20% 样本的 `tp10_hit_rate` 是否显著高于后 20%**
- **高分样本的 `probe_invalid_rate` 是否显著更低**

如果没有单调性，模型就没有真正学到东西。

---

## 12. 最小可运行版本

### V0
- 保留 `v5`
- 不加行业强度
- 只做事件样本
- 标签用 `label_rank`
- 模型用 `LGBModel`
- 回测时只替换 `priority_score`

### V1
- 补行业强度特征
- 增加 `confirm_threshold`
- 高分样本才允许补主仓

### V2
- 引入 IVIX / 市场宽度
- 做 `probe` 仓位动态化
- 补两阶段模型

---

## 13. 关键伪代码

### 13.1 样本生成

```python
for date in trade_dates:
    for stock in universe:
        if env_a[date] != 1:
            continue
        if B1候选v5[stock, date] != 1:
            continue
        if duplicated_recent_event(stock, date, window=8):
            continue

        x = build_features(stock, date)
        y_rank = build_rank_label(stock, date)
        y_cls = build_cls_label(stock, date)

        save_sample(stock, date, x, y_rank, y_cls)
```

### 13.2 训练

```python
dataset = B1EventDataset(...)
model = LGBModel(...)
model.fit(dataset)
pred = model.predict(test_dataset)
analyze_decile(pred, labels)
```

### 13.3 回测

```python
for date in trade_dates:
    candidate_list = [
        s for s in universe
        if env_a[date] and B1候选v5[s, date]
    ]

    scored = rank_by_model(candidate_list, date)

    for s in topK(scored, k=3):
        if no_position(s):
            open_probe(s)

    for s in holdings:
        if is_probe(s) and B1确认v5[s, date] and model_score[s, date] >= th_confirm:
            add_to_main(s)

        execute_existing_exit_rules(s)
```

---

## 14. 实施顺序建议

建议按以下顺序推进：

### 第一步
保留 v5 规则不动，只在 `B1候选v5` 上训练一个 `LightGBM / LGBModel` 排序模型。

### 第二步
标签定义为：

- 未来 10 日内是否先达 `+10%`
- 且不先触发 `probe invalid / ST / LT / timeout`

### 第三步
先不改卖出，只做：

- 候选内排序
- 每天只买前 `3~5` 名

### 第四步
重点看三项：

- 第一档止盈触发率
- `probe_invalid` 占比
- 最大回撤

### 第五步
若上述指标改善，再把模型分数用于：

- `probe` 仓位大小
- 是否允许 `confirm` 加仓

---

## 15. 结论

这条路线本质上不是“让模型自动发明 B1”，而是：

> 先用 B1 规则把候选结构圈出来，再让 Qlib 模型在这些候选内部学习哪些量价关系更容易走成。

一句话概括：

**不是“模型替代 B1”，而是“模型给 B1 做精排和概率校准”。**

如果做对，这条路线通常会比继续手调阈值更有效。
