# B1 卖出侧 Qlib 建模方案 v3

> 版本目标：在原有 **B1 卖出侧 Qlib 建模方案** 基础上，把你补充的五类 **主力出货形态** 直接并入卖出端，形成一套可落地的 **硬性止损 + 阶段止盈 + 持仓状态机 + Qlib 训练样本** 的统一方案。
>
> 本版原则：
>
> 1. **硬规则优先于模型**。
> 2. **主力出货形态优先于一般软卖点**。
> 3. **Qlib 模型主要优化“卖多少、何时卖第二档/第三档、尾仓何时退出”**，而不是替代纪律性离场。
> 4. **probe 阶段仍然规则化**，主仓阶段再引入模型精细化。

---

## 1. 方案核心思想

B1 买入侧解决的是：

> 哪些候选值得上车。

B1 卖出侧真正要解决的是：

> 这趟车现在还值不值得继续坐，以及应该先下多少仓位。

因此，卖出端不能简单复制买入端“静态形态识别”的思路，而要拆成三层：

- **第一层：硬规则层** —— 纪律性离场、结构破坏、主力出货日
- **第二层：阶段止盈层** —— 30/30/30/10 的仓位切换
- **第三层：模型层** —— 在未触发硬规则时，优化分批卖出的节奏

---

## 2. 五类主力出货形态并入卖出体系

以下五类形态，按你的图示，统一视为 **顶部资金派发模板**。本方案不把它们当成“参考信号”，而是直接写进卖出规则优先级。

### 2.1 五类形态总表

1. **加速单日放天量大阴线**
2. **加速后次高点突然巨量长阴**
3. **新高后连续阶梯放量下跌**
4. **双头双放量巨阴**
5. **顶部绿长红短**

其中：

- 1、2、4 属于 **强硬卖点 / 准硬卖点**
- 3、5 属于 **阶段衰退卖点 / 分批兑现卖点**

---

## 3. 五类形态的交易解释与卖出动作映射

## 3.1 形态一：加速单日放天量大阴线

### 3.1.1 交易语义

这类形态通常出现在：

- 前面已经有一段连续加速上涨
- 当天成交量爆出阶段天量
- 当天 K 线实体很大，且收盘位置很差
- 该日往往是 **情绪高潮 + 筹码集中兑现** 的组合日

这里不仅包括 **标准大阴线**，也包括你特别强调的：

- **假阴真阳**
- **假阳真阴**
- **长上影 + 放巨量 + 收盘偏弱**

也就是说，本质不是盯“颜色”，而是盯：

> **加速后的天量衰竭日**

### 3.1.2 量化定义（建议版）

定义辅助量：

```python
ret_3 = close[t] / close[t-3] - 1
ret_5 = close[t] / close[t-5] - 1
vol_ma20 = mean(volume[t-19:t+1])
vol_ratio = volume[t] / max(vol_ma20, 1e-6)
vol_rank_60 = rank_pct(volume[t], volume[t-59:t+1])
body = abs(close[t] / open[t] - 1)
close_pos = (close[t] - low[t]) / max(high[t] - low[t], 1e-6)
upper_wick = (high[t] - max(open[t], close[t])) / max(prev_close[t], 1e-6)
turnover_ratio = amount[t] / max(amount_ma20[t], 1e-6)
```

触发条件建议：

```python
accel_exhaust_day = (
    (ret_3 >= 0.10 or ret_5 >= 0.16)
    and vol_ratio >= 2.2
    and vol_rank_60 >= 0.95
    and (
        # 标准大阴
        (close[t] < open[t] and body >= 0.05 and close_pos <= 0.35)
        # 假阴真阳 / 假阳真阴：颜色不重要，关键是天量+收盘弱
        or (body >= 0.04 and close_pos <= 0.40 and upper_wick >= 0.02)
        or (turnover_ratio >= 2.5 and close[t] < high[t] * 0.96 and upper_wick >= 0.025)
    )
)
```

### 3.1.3 卖出动作

- `stage == S0(probe)`：**直接清仓**
- `stage == S1(主仓满仓)`：**至少卖 50%**
- `stage == S2(剩70%)`：**再卖 40%**，即总剩余降到 30%
- `stage == S3(剩40%)`：**再卖 30%**，仅保留尾仓
- `stage == S4(尾仓)`：若次日不能反包或跌破 ST，**清尾仓**

### 3.1.4 升级为硬清仓的条件

若满足以下任一条件，则不再只卖部分，直接清仓：

```python
accel_exhaust_hard = (
    accel_exhaust_day
    and (
        close[t] < ST[t]
        or low[t] < LT[t] * 0.995
        or next_2d_rebound_fail
    )
)
```

其中：

```python
next_2d_rebound_fail = (
    max(high[t+1:t+3]) <= high[t] * 1.01
    and min(close[t+1:t+3]) < close[t]
)
```

---

## 3.2 形态二：加速后次高点突然巨量长阴

### 3.2.1 交易语义

这是典型的：

- 第一波主升已经完成
- 之后做了一个次高点或接近前高的冲顶
- 冲顶当天突然放出巨量长阴

它比“单次加速天量大阴”更偏向：

> **二次诱多后的集中兑现**

往往比形态一更危险，因为它说明：

- 市场已经尝试过再次上攻
- 但资金选择在接近前高区域大规模派发

### 3.2.2 量化定义（建议版）

```python
near_prior_peak = high[t] >= rolling_max(high, 20)[t-1] * 0.97
not_break_clean = close[t] <= rolling_max(high, 20)[t-1] * 1.01
pre_accel = (close[t-1] / close[t-6] - 1) >= 0.15
long_bear = close[t] < open[t] and abs(close[t] / open[t] - 1) >= 0.05
huge_vol = volume[t] / max(ma(volume, 20)[t], 1e-6) >= 2.0
close_bad = (close[t] - low[t]) / max(high[t] - low[t], 1e-6) <= 0.30

secondary_peak_distribution = (
    pre_accel
    and near_prior_peak
    and not_break_clean
    and huge_vol
    and long_bear
    and close_bad
)
```

### 3.2.3 卖出动作

- `S0`：直接清仓
- `S1`：**卖 70%**
- `S2`：**直接卖到只剩尾仓 10%**
- `S3/S4`：直接清仓

### 3.2.4 作为强硬规则的补充

如果同时出现：

- 次高点
- 巨量长阴
- 跌破 ST

则直接视为 **强制退出日**。

---

## 3.3 形态三：新高后连续阶梯放量下跌

### 3.3.1 交易语义

这类形态往往不是一天砸死，而是：

- 创新高之后
- 连续 2~4 天逐步走弱
- 下跌过程中量能不缩，甚至逐级放大

这说明顶部不是瞬间完成，而是：

> **边撤边派、接力资金承接不足**

这类形态特别适合并入 **阶段止盈模型**。

### 3.3.2 量化定义（建议版）

```python
new_high_recent = high[t-1] >= rolling_max(high, 20)[t-2] * 0.995
lower_high_3 = high[t] < high[t-1] < high[t-2]
lower_close_3 = close[t] < close[t-1] < close[t-2]
down_vol_expand = volume[t] >= volume[t-1] >= volume[t-2]
weak_close = close[t] <= high[t] - 0.6 * (high[t] - low[t])

stair_dist_3d = (
    new_high_recent
    and lower_high_3
    and lower_close_3
    and down_vol_expand
    and weak_close
)
```

增强版：

```python
stair_dist_4d = stair_dist_3d and close[t] < ST[t]
```

### 3.3.3 卖出动作

- `S1`：第一次触发 `stair_dist_3d`，**卖 30%**
- 若 1~2 天内继续弱、形成 `stair_dist_4d` 或跌破 ST，**再卖 30%**
- 若随后跌破 LT，**清仓**

对应执行可写成：

```python
if stair_dist_3d and stage == S1:
    sell_30()
elif stair_dist_4d and stage in [S1, S2]:
    sell_30()
elif close[t] < LT[t]:
    exit_all()
```

---

## 3.4 形态四：双头双放量巨阴

### 3.4.1 交易语义

这是五类形态里最接近 **终局性派发** 的一种。

特征是：

- 两个顶部高度接近
- 两次冲顶都伴随巨量大阴 / 巨量长上影弱收
- 第二个头部并未有效突破第一个头部

交易上它代表：

> **顶部区域反复放量兑现，主升段大概率结束**

### 3.4.2 量化定义（建议版）

```python
peak1 = local_peak_1[t]
peak2 = local_peak_2[t]
peaks_close = abs(peak2.price / peak1.price - 1) <= 0.04
head_gap = 3 <= (peak2.idx - peak1.idx) <= 20

bear_blow1 = peak1.vol_ratio >= 2.0 and peak1.close_pos <= 0.35 and peak1.body_abs >= 0.04
bear_blow2 = peak2.vol_ratio >= 2.0 and peak2.close_pos <= 0.35 and peak2.body_abs >= 0.04

double_top_distribution = (
    peaks_close
    and head_gap
    and bear_blow1
    and bear_blow2
    and peak2.price <= peak1.price * 1.02
)
```

### 3.4.3 卖出动作

这类形态直接归为 **硬规则**：

- `S0/S1/S2/S3/S4`：**全部清仓**

不建议对这类形态做“只减一点”的处理，因为它的交易含义已经不是“短线回撤”，而是“主升结束概率显著上升”。

---

## 3.5 形态五：顶部绿长红短

### 3.5.1 交易语义

“顶部绿长红短”的本质不是单根 K 线，而是一种 **上涨乏力、下跌主动、反弹弱修复** 的节奏特征。

可以理解为：

- 阴线实体偏长
- 阳线实体偏短
- 下跌时放量、反弹时缩量
- 价格虽然暂时未破位，但已经进入顶部派发阶段

它一般不适合第一时间当“硬清仓”，但非常适合并入 **阶段止盈 / 软卖点强化**。

### 3.5.2 量化定义（建议版）

统计最近 6 根 K：

```python
neg_body_mean = mean([abs(close[i] / open[i] - 1) for i in last_6 if close[i] < open[i]])
pos_body_mean = mean([abs(close[i] / open[i] - 1) for i in last_6 if close[i] > open[i]])
neg_vol_mean = mean([volume[i] for i in last_6 if close[i] < open[i]])
pos_vol_mean = mean([volume[i] for i in last_6 if close[i] > open[i]])
red_green_bias = neg_body_mean / max(pos_body_mean, 1e-6)
vol_bias = neg_vol_mean / max(pos_vol_mean, 1e-6)

weak_rebound_top = (
    red_green_bias >= 1.4
    and vol_bias >= 1.2
    and close[t] < rolling_max(close, 5)[t-1] * 1.01
)
```

### 3.5.3 卖出动作

- `S1`：第一次触发，**卖 20%~30%**
- 连续两次触发，或叠加 `close < ST`：**再卖 30%**
- 若叠加板块退潮 / 相对强度恶化：再进一步减仓
- 若最终跌破 LT：清仓

---

## 4. 五类形态的优先级划分

## 4.1 优先级矩阵

### A类：硬清仓

满足任一：

- `double_top_distribution == True`
- `secondary_peak_distribution == True and close[t] < ST[t]`
- `accel_exhaust_hard == True`
- `close[t] < LT[t]`
- `probe_invalid == True`

动作：

```python
exit_all()
```

### B类：强制大幅减仓

满足任一：

- `accel_exhaust_day == True`
- `secondary_peak_distribution == True`

动作建议：

- 主仓阶段至少减 50%
- 已分批阶段至少减到只剩尾仓

### C类：阶段止盈

满足任一：

- `stair_dist_3d == True`
- `weak_rebound_top == True`
- `hold_edge` 明显转弱
- 模型给出较高卖出概率

动作建议：

- 第一次：卖 30%
- 第二次：再卖 30%
- 第三次：卖到尾仓

---

## 5. 卖出侧总体架构

## 5.1 三层结构

### 第一层：硬规则层

保留并扩展以下规则：

- `probe invalid`
- `LT` 硬破位
- `ST` 短趋势破位后的强出货形态
- 形态一：加速天量衰竭日
- 形态二：次高点巨量长阴
- 形态四：双头双放量巨阴

### 第二层：阶段止盈层

处理以下形态：

- 形态三：新高后连续阶梯放量下跌
- 形态五：顶部绿长红短
- 一般性浮盈回吐、走势衰退、板块退潮

### 第三层：模型层

当未触发硬规则时，由 Qlib 模型决定：

- 是否卖第一档 30%
- 是否卖第二档 30%
- 是否卖第三档 30%
- 尾仓是否退出

---

## 6. 持仓事件样本定义

卖出侧样本单位定义为：

- 样本索引：`(instrument, t_hold, trade_id, stage_id)`
- 含义：在 `t_hold` 收盘后，决定 `t_hold+1` 开盘是否减仓 / 清仓

其中：

- `trade_id`：一次完整 B1 交易段的唯一标识
- `stage_id`：持仓阶段

### 6.1 stage_id 定义

```text
S0 = probe阶段，仅试探仓
S1 = confirm后满主仓，尚未分批卖出
S2 = 已卖第一档30%，剩余70%
S3 = 已卖第二档30%，剩余40%
S4 = 已卖第三档30%，剩余10%
S5 = flat，已清仓
```

### 6.2 样本纳入条件

仅纳入满足以下条件的持仓日：

- 当前 trade_id 来自真实 B1 入场路径
- `t+1` 有可交易开盘价
- 非停牌、非连续一字不可卖
- 当日收盘后仍有持仓
- 后续至少还有 5 个交易日可用于打标签

### 6.3 样本过滤

以下日期不纳入“模型训练样本”，因为它们直接由规则决定：

- `probe_invalid`
- `close < LT`
- `double_top_distribution`
- `accel_exhaust_hard`
- `secondary_peak_distribution and close < ST`

这些日子直接标记为：

```text
forced_exit = 1
```

模型不学它们的“是否卖”，只学习在其余日子里的“卖多少 / 何时卖下一档”。

---

## 7. 卖出标签设计

第一版建议采用：

- 一个连续标签：`hold_edge`
- 四个阶段二分类标签：`label_tp1 / label_tp2 / label_tp3 / label_tail_exit`

---

## 7.1 连续标签：继续持有价值 `hold_edge`

```python
exec_px = open[t+1]

mfe_5 = max(high[t+1:t+5]) / exec_px - 1
mae_5 = min(low[t+1:t+5]) / exec_px - 1

new_high_5 = int(max(high[t+1:t+5]) > peak_high_since_entry[t] * 1.01)
break_st_3 = int(any(close[t+1:t+3] < ST[t+1:t+3]))
break_lt_5 = int(any(close[t+1:t+5] < LT[t+1:t+5]))
rel_weak_5 = int((ret_stock_5 - ret_ind_5) <= -0.03)
dd_expand_5 = int(min(low[t+1:t+5]) < peak_high_since_entry[t] * (1 - dd_tol_t))

dist_flag_5 = int(
    accel_exhaust_day
    or secondary_peak_distribution
    or stair_dist_3d
    or weak_rebound_top
)

hold_edge = (
    1.0 * clip(mfe_5, 0.00, 0.12)
    - 1.2 * clip(-mae_5, 0.00, 0.08)
    + 0.25 * new_high_5
    - 0.30 * break_st_3
    - 0.55 * break_lt_5
    - 0.20 * rel_weak_5
    - 0.25 * dd_expand_5
    - 0.25 * dist_flag_5
)
```

说明：

- 若未来仍能走出新高和顺行空间，`hold_edge` 会高
- 若未来容易破 ST / LT，或明显派发，`hold_edge` 会低
- 新增 `dist_flag_5`，使五类出货形态直接进入标签体系

---

## 7.2 第一档卖出标签 `label_tp1`

适用于 `stage == S1`

```python
label_tp1 = 1 if (
    float_pnl_t >= 0.06
    and (
        drawdown_from_mfe_t >= 0.03
        or stair_dist_3d
        or weak_rebound_top
    )
    and hold_edge <= 0.00
) else 0
```

---

## 7.3 第二档卖出标签 `label_tp2`

适用于 `stage == S2`

```python
label_tp2 = 1 if (
    float_pnl_t >= 0.10
    and (
        drawdown_from_mfe_t >= 0.04
        or stair_dist_3d
        or weak_rebound_top
        or secondary_peak_distribution
    )
    and hold_edge <= -0.01
) else 0
```

---

## 7.4 第三档卖出标签 `label_tp3`

适用于 `stage == S3`

```python
label_tp3 = 1 if (
    hold_edge <= -0.02
    or break_st_3 == 1
    or days_from_peak_t >= 4
    or stair_dist_3d
    or weak_rebound_top
) else 0
```

---

## 7.5 尾仓退出标签 `label_tail_exit`

适用于 `stage == S4`

```python
label_tail_exit = 1 if (
    break_lt_5 == 1
    or hold_edge <= -0.03
    or rel_weak_5 == 1
    or hold_days_t >= max_hold_tail
    or secondary_peak_distribution
    or accel_exhaust_day
) else 0
```

---

## 8. 特征字段清单

## 8.1 持仓状态特征

```python
x_stage_id
x_days_in_pos
x_days_since_confirm
x_realized_stage
x_pos_left_ratio
x_ret_from_entry_close
x_ret_from_confirm_close
x_ret_from_last_sell
x_hold_cost_dev
x_float_pnl_t
x_drawdown_from_mfe_t
x_days_from_peak_t
```

---

## 8.2 路径质量特征

```python
x_mfe_since_entry
x_mae_since_entry
x_drawdown_from_mfe
x_peak_gain_decay
x_up_days_ratio_8
x_down_days_ratio_8
x_new_high_fail_cnt_5
x_lower_high_cnt_5
x_lower_close_cnt_5
```

---

## 8.3 结构衰退特征

```python
x_close_lt_dev
x_close_st_dev
x_low_lt_dev
x_break_lt_cnt_5
x_break_st_cnt_3
x_lt_slope_3
x_st_slope_3
x_high_fail_cnt_5
x_near_prior_peak
x_failed_breakout_flag
```

---

## 8.4 波动与量能衰退特征

```python
x_vol_to_ma20
x_vol_to_ma5
x_vol_rank_60
x_up_vol_share_5
x_down_vol_share_5
x_dist_down_cnt_5
x_atr5_to20
x_range_pct
x_gap_down_cnt_5
x_turnover_ratio
```

---

## 8.5 K线衰竭特征

```python
x_body_pct
x_upper_wick
x_lower_wick
x_upper_wick_mean_3
x_long_upper_cnt_5
x_bear_bar_cnt_5
x_close_pos
x_true_bear_exhaust_flag
x_false_bear_exhaust_flag
```

其中：

- `x_true_bear_exhaust_flag`：标准巨量大阴衰竭
- `x_false_bear_exhaust_flag`：假阴真阳 / 假阳真阴但本质弱收的衰竭日

---

## 8.6 相对强度 / 板块衰退特征

```python
x_ret3_excess_idx
x_ret5_excess_idx
x_ret3_excess_ind
x_ret5_excess_ind
x_ind_rank_5
x_ind_rank_drop_5
x_same_ind_up_cnt
x_same_ind_up_delta
x_sector_cooling_flag
```

---

## 8.7 五类出货形态特征

这一组是本版新增的核心特征。

```python
x_accel_exhaust_day
x_accel_exhaust_hard
x_secondary_peak_distribution
x_stair_dist_3d
x_stair_dist_4d
x_double_top_distribution
x_weak_rebound_top
x_distribution_score
```

建议把 `x_distribution_score` 定义为：

```python
x_distribution_score = (
    2.0 * x_double_top_distribution
    + 1.6 * x_secondary_peak_distribution
    + 1.3 * x_accel_exhaust_day
    + 1.0 * x_stair_dist_3d
    + 0.8 * x_weak_rebound_top
)
```

它不是硬规则的替代，而是给模型一个统一的顶部派发强度参考。

---

## 8.8 止盈几何特征

```python
x_tp1_reached
x_tp2_reached
x_tp3_reached
x_dist_to_tp1
x_dist_to_tp2
x_dist_to_tp3
x_profit_lock_ratio
x_gain_vs_atr_entry
```

默认阈值先采用：

```python
tp1_threshold = 0.06
tp2_threshold = 0.10
tp3_threshold = 0.14
```

---

## 9. 30/30/30/10 多阶段实现框架

## 9.1 状态机定义

```text
S0: probe
S1: confirm后满仓，未止盈
S2: 已卖30%，剩70%
S3: 已卖60%，剩40%
S4: 已卖90%，剩10%
S5: flat
```

---

## 9.2 执行优先级

每天对每个持仓按以下顺序执行：

### 第一步：硬规则

```python
if probe_invalid:
    exit_all()

elif close[t] < LT[t]:
    exit_all()

elif double_top_distribution:
    exit_all()

elif accel_exhaust_hard:
    exit_all()

elif secondary_peak_distribution and close[t] < ST[t]:
    exit_all()
```

### 第二步：强制大幅减仓

```python
elif accel_exhaust_day:
    if stage == S1:
        sell_to_ratio(0.50)
    elif stage == S2:
        sell_to_ratio(0.30)
    elif stage >= S3:
        sell_to_ratio(0.10)

elif secondary_peak_distribution:
    if stage == S1:
        sell_to_ratio(0.30)
    else:
        sell_to_ratio(0.10)
```

### 第三步：阶段止盈规则

```python
elif stair_dist_3d:
    if stage == S1:
        sell_30()
    elif stage == S2 and close[t] < ST[t]:
        sell_30()

elif weak_rebound_top:
    if stage == S1:
        sell_20_or_30()
    elif stage in [S2, S3] and weak_rebound_top_count >= 2:
        sell_30()
```

### 第四步：模型决策

仅在未触发以上规则时，交给模型：

```python
if stage == S1 and tp1_gate_passed and score_tp1 >= th1:
    sell_30()
elif stage == S2 and tp2_gate_passed and score_tp2 >= th2:
    sell_30()
elif stage == S3 and tp3_gate_passed and score_tp3 >= th3:
    sell_30()
elif stage == S4 and score_tail >= th4:
    exit_all()
```

---

## 9.3 gate 条件

```python
tp1_gate_passed = (
    stage == S1
    and days_since_confirm >= 1
    and mfe_since_entry >= 0.06
)

tp2_gate_passed = (
    stage == S2
    and bars_since_last_sell >= 1
    and mfe_since_entry >= 0.10
)

tp3_gate_passed = (
    stage == S3
    and bars_since_last_sell >= 1
    and mfe_since_entry >= 0.14
)

tail_gate_passed = (stage == S4)
```

---

## 9.4 初始阈值建议

```python
th1 = 0.70
th2 = 0.65
th3 = 0.60
th4 = 0.55
```

含义：

- 第一档卖出更谨慎，防止早卖
- 越往后越偏保护利润

---

## 10. 样本生成伪代码

```python
for trade in historical_b1_trades:
    for t in trade.holding_dates:
        if not tradable_next_open(trade.stock, t):
            continue

        stage = get_stage_id(trade, t)
        feat = build_exit_features(trade, t)

        # 先识别五类出货形态与硬规则
        dist_flags = detect_distribution_patterns(trade.stock, t)

        forced_exit = (
            probe_invalid(trade, t)
            or close[t] < LT[t]
            or dist_flags.double_top_distribution
            or dist_flags.accel_exhaust_hard
            or (dist_flags.secondary_peak_distribution and close[t] < ST[t])
        )

        if forced_exit:
            save_forced_exit_sample(trade, t, feat, dist_flags)
            continue

        y_hold = build_hold_edge_label(trade, t, dist_flags)

        if stage == S1:
            y_stage = build_tp1_label(trade, t, dist_flags)
        elif stage == S2:
            y_stage = build_tp2_label(trade, t, dist_flags)
        elif stage == S3:
            y_stage = build_tp3_label(trade, t, dist_flags)
        elif stage == S4:
            y_stage = build_tail_exit_label(trade, t, dist_flags)
        else:
            continue

        save_sample(
            stock=trade.stock,
            date=t,
            trade_id=trade.id,
            stage_id=stage,
            features=feat,
            label_hold=y_hold,
            label_stage=y_stage,
            forced_exit=0,
        )
```

---

## 11. 回测接入伪代码

```python
for date in trade_dates:

    # 1. 买入侧仍按原B1+Qlib买入框架执行
    handle_entry_and_confirm(date)

    # 2. 卖出侧
    for s in holdings:
        stage = get_stage_id(s, date)
        x = build_exit_features_live(s, date)
        flags = detect_distribution_patterns_live(s, date)

        # A. 硬规则
        if probe_invalid_live(s, date):
            exit_all(s)
            continue
        if close[s, date] < LT[s, date]:
            exit_all(s)
            continue
        if flags.double_top_distribution:
            exit_all(s)
            continue
        if flags.accel_exhaust_hard:
            exit_all(s)
            continue
        if flags.secondary_peak_distribution and close[s, date] < ST[s, date]:
            exit_all(s)
            continue

        # B. 强制减仓
        if flags.accel_exhaust_day:
            if stage == S1:
                sell_to_ratio(s, 0.50)
            elif stage == S2:
                sell_to_ratio(s, 0.30)
            else:
                sell_to_ratio(s, 0.10)
            continue

        if flags.secondary_peak_distribution:
            if stage == S1:
                sell_to_ratio(s, 0.30)
            else:
                sell_to_ratio(s, 0.10)
            continue

        # C. 阶段止盈规则
        if flags.stair_dist_3d:
            if stage == S1:
                sell_percent(s, 0.30)
                continue
            elif stage == S2 and close[s, date] < ST[s, date]:
                sell_percent(s, 0.30)
                continue

        if flags.weak_rebound_top:
            if stage == S1:
                sell_percent(s, 0.20)
                continue
            elif stage in [S2, S3] and weak_rebound_top_count(s) >= 2:
                sell_percent(s, 0.30)
                continue

        # D. 模型层
        if stage == S1 and tp1_gate(s, date):
            score = model_tp1.predict(x)
            if score >= th1:
                sell_percent(s, 0.30)
                continue

        if stage == S2 and tp2_gate(s, date):
            score = model_tp2.predict(x)
            if score >= th2:
                sell_percent(s, 0.30)
                continue

        if stage == S3 and tp3_gate(s, date):
            score = model_tp3.predict(x)
            if score >= th3:
                sell_percent(s, 0.30)
                continue

        if stage == S4:
            score = model_tail.predict(x)
            if score >= th4:
                exit_all(s)
                continue
```

---

## 12. 训练与验证建议

### 12.1 切分方式

必须使用滚动时间切分，不允许随机切分。

建议：

- 训练：24个月
- 验证：6个月
- 测试：6个月
- 每3个月滚动一次

### 12.2 泄漏控制

- 同一 `trade_id` 不跨训练/验证边界
- 标签窗口若为 5 日，则 `embargo >= 5`
- 所有五类形态特征只使用 `t` 及以前数据
- 严禁用未来高低点反推当天是否为“顶部”

### 12.3 建模顺序

建议按以下顺序做：

#### V0
- 先不上模型
- 只把五类形态写入规则层
- 验证硬规则后，是否显著减少大回撤和利润回吐

#### V1
- 只做 `TP1` 模型
- 解决“第一档 30% 何时卖”的问题

#### V2
- 加 `TP2`
- 引入 `x_distribution_score`
- 验证阶段止盈与主力出货形态的交互

#### V3
- 加 `TP3 + Tail`
- 形成完整状态机

---

## 13. 核心评价指标

不要只看总收益，重点看以下指标：

### 13.1 规则有效性

- `hard_exit_after_return_5d`：硬卖点触发后未来 5 日收益是否显著更差
- `distribution_hit_rate`：五类形态触发后未来 3~5 日继续下跌概率
- `false_positive_rate`：是否过多误砍强趋势票

### 13.2 阶段止盈效果

- `tp1_precision`
- `tp2_precision`
- `tp3_precision`
- `tail_exit_precision`
- `profit_lock_ratio`
- `max_trade_giveback`

### 13.3 过早卖飞指标

- `over_early_sell_rate`
- `post_sell_new_high_rate`
- `tail_capture_ratio`

### 13.4 分数有效性

- `hold_edge` 分桶单调性
- `distribution_score` 分桶单调性
- 模型高分卖出样本的后续 `hold_edge` 是否显著更差

---

## 14. 实战落地建议

### 14.1 先把五类形态写死到规则层

这是最重要的一步。因为这五类形态本质上不是一般性软信号，而是：

- 顶部派发的行为模板
- 纪律性保护利润的优先级信号

尤其是：

- 加速天量衰竭
- 次高点巨量长阴
- 双头双放量巨阴

这三类，优先级必须高于模型。

### 14.2 模型主要优化“卖多少、何时卖第二档”

你真正需要 Qlib 优化的，不是“有没有顶部派发”，而是：

- 形态三、五这种慢衰退下，先卖 20%、30% 还是 50%
- 第一档卖完后，第二档是否马上跟上
- 剩余尾仓还能不能博新高

### 14.3 probe 阶段继续规则化

probe 阶段只做：

- `probe invalid`
- `LT/ST`
- 异常放量反包失败

不要一开始就把 probe 也模型化，否则卖出侧会过度复杂。

---

## 15. 最终结论

这版 v3 的核心变化是：

1. 把你给出的五类主力出货形态直接并入卖出侧。
2. 明确区分：
   - **硬清仓形态**
   - **强制大减仓形态**
   - **阶段止盈形态**
3. 在 Qlib 层新增：
   - 五类形态特征
   - `distribution_score`
   - 将派发形态写入 `hold_edge` 标签
4. 保持总体路线不变：
   - **规则先兜底**
   - **模型后优化**
   - **30/30/30/10 分阶段执行**

一句话概括：

> **买入侧是识别 B1 完美图形，卖出侧则是识别“主升还在不在”，而这五类主力出货形态就是主升结束的高优先级证据。**

