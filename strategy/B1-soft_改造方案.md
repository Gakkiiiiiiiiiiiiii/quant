# B1-soft 改造方案

> 适用对象：`Gakkiiiiiiiiiiiiii/quant` 仓库中的 `strategy/strategy.py` 与 `strategy/B1_QLib_rank_plan.md`
>
> 目标：把当前 B1-v5 从“硬规则强筛选”升级为“硬结构约束 + 软特征排序”的 B1-soft 版本，用历史事件数据找到**赚钱概率更高**的 B1 图形，而不是仅靠单条布尔规则判定。

---

## 1. 方案目标

当前仓库中的 B1-v5 已经具备完整的：

- 候选事件抽取
- `probe -> confirm` 两段式入场
- `probe_invalid / ST / LT / timeout` 风险控制
- 事件标签 `label_rank / label_cls`
- B1 专属特征工程框架

因此，B1-soft **不是推倒重写**，而是在现有框架上做三件事：

1. 把 `ST > LT` 从**硬门槛**改为**软特征**。
2. 放宽少数容易误杀优质横盘样本的约束，但保留真正代表“结构健康”的条件。
3. 在 `B1候选` 内做模型排序，优先买入历史上更容易成功的图形。

---

## 2. 当前仓库里 B1-v5 的核心逻辑

### 2.1 当前 B1 的结构层

当前 `strategy.py` 中，`b1_structure` 主要由以下条件构成：

```python
b1_structure = (
    (g["lt"] > g["lt"].shift(3))
    & (g["st"] > g["lt"])
    & (count_above_lt_30 >= 20)
    & (count_above_st_20 >= 12)
)
```

这表示当前实现把以下四项都作为硬门槛：

- LT 抬升
- ST 在 LT 上方
- 近 30 日大多数时间收盘在 LT 上方
- 近 20 日较多时间收盘在 ST 上方

### 2.2 当前 B1 的启动层

```python
b1_startup_base = (
    (hh40 / (ll40 + eps) >= 1.25)
    & (explosive_bull.rolling(30, min_periods=1).sum() >= 1)
    & (recent_high.rolling(15, min_periods=1).sum() >= 1)
    & b1_structure
)
```

含义：

- 40 日内必须有足够强的波动空间
- 近 30 日至少出现过一次放量长阳式启动
- 近 15 日至少触碰过一次高点附近

### 2.3 当前 B1 的回调层

```python
b1_retrace_ready = (
    (g["j"] < 20)
    & (c <= g["st"] * 1.02)
    & (c >= g["lt"] * 0.99)
    & (l >= g["lt"] * 0.97)
    & (dist_from_prev_high >= 0.04)
    & (dist_from_prev_high <= 0.15)
    & (days_since_recent_high >= 2)
    & (days_since_recent_high <= 12)
    & ((c < g["lt"]).rolling(10, min_periods=1).sum() <= 1)
    & (pullback_big_bear.rolling(12, min_periods=1).sum() == 0)
    & (pullback_distribution_bear.rolling(12, min_periods=1).sum() == 0)
)
```

含义：

- 进入回调可交易区间
- 回调贴近 LT/ST
- 回撤幅度不能太浅也不能太深
- 离前高不能太近，也不能拖太久
- 不能频繁跌破 LT
- 回调期不能出现大阴、放量派发阴

### 2.4 当前 B1 的衰竭层

```python
b1_extreme_shrink = (
    (v <= v.rolling(10, min_periods=1).min())
    & (v < g["vol_ma20"] * 0.65)
)

b1_kline = (
    (g["body_pct"] <= 0.022)
    & (g["range_pct_prev_close"] <= 0.055)
)
```

含义：

- 必须极致缩量
- K 线必须小实体、小振幅

### 2.5 当前 B1 的禁做层

```python
b1_forbidden = (recent_distribution & ~repair) | b1_pullback_distribution_forbidden
```

说明该策略已经把“近期出现过明显派发痕迹、且尚未修复”的个股排除掉。

### 2.6 当前 B1 的候选、确认与失效

```python
b1_candidate_v5 = (
    eligible_entry
    & b1_startup_base
    & b1_retrace_ready
    & b1_extreme_shrink
    & b1_kline
    & ~b1_forbidden
)
```

```python
b1_confirm = (
    b1_confirm_window
    & (c > b1_signal_high * 1.01)
    & (c > g["st"])
    & (c > h.shift(1))
    & (v > np.maximum(v.shift(1) * 1.2, g["vol_ma20"] * 1.2))
)
```

```python
b1_probe_invalid = (
    (b1_probe_bars <= 5)
    & (
        (c < b1_signal_low * 0.99)
        | (c < g["lt"] * 0.98)
        | ((b1_probe_bars >= 5) & (c < g["st"]))
    )
).fillna(False)
```

这意味着当前实现不是“单日买点策略”，而是：

- 先抓一个 B1 候选日作为 `probe`
- 之后 1~5 日等待 `confirm`
- 如果先破坏结构，则 `probe_invalid`

---

## 3. 为什么要做 B1-soft，而不是直接删掉 `ST > LT`

### 3.1 直接删除的问题

如果只删掉：

```python
(g["st"] > g["lt"])
```

而不改其它条件，会带来两个副作用：

1. `COUNT(C > ST, 20)` 变得偏宽松。  
   因为当 `ST < LT` 时，“站上 ST”不再代表短强，很多弱反弹也会被放进来。

2. `C <= ST * 1.02` 这条“贴 ST 回踩”会整体下移。  
   结果会把下跌中继、平台下沿反抽、弱修复也纳入候选。

### 3.2 B1-soft 的正确思路

不是“取消 ST/ LT 关系”，而是：

- **保留 LT 主导的中期结构判断**
- **允许 ST 与 LT 的关系进入窄带/交叉/贴合状态**
- **把 ST/LT 的相对位置交给模型学习**

换句话说：

- `LT` 是“方向底座”
- `ST/LT` 是“强弱细节”
- 是否值得做，最后由排序模型判定

---

## 4. B1-soft 的总改造原则

### 4.1 哪些条件继续保留为硬门槛

这些条件建议继续保留，因为它们更像是“B1 成立的骨架”：

1. `LT > REF(LT, 3)`：LT 必须上行或至少轻度抬升。  
2. `COUNT(C > LT, 30) >= 20`：价格大部分时间仍在 LT 之上。  
3. 有启动段：`hh40/ll40 >= 1.25` + 近 30 日至少一根强启动阳。  
4. 回调深度有边界：不能过浅，也不能过深。  
5. 回调期不能出现明显派发大阴或放量派发阴。  
6. 必须存在缩量衰竭，不可放任“带量回调”。

### 4.2 哪些条件应从硬门槛改为软条件

这些条件最适合交给模型学习，而不是规则硬裁：

1. `ST > LT`
2. `COUNT(C > ST, 20) >= 12`
3. `C <= ST * 1.02`
4. `body_pct <= 0.022`
5. `range_pct_prev_close <= 0.055`
6. `v < vol_ma20 * 0.65`

这些约束并不是没用，而是现在写得**过硬**，更适合改成：

- 分桶特征
- 连续特征
- 软阈值 + 模型排序

---

## 5. B1-soft 版本定义

### 5.1 B1-soft 结构层

#### 原版

```python
b1_structure = (
    (g["lt"] > g["lt"].shift(3))
    & (g["st"] > g["lt"])
    & (count_above_lt_30 >= 20)
    & (count_above_st_20 >= 12)
)
```

#### 改造后建议

```python
b1_structure_soft = (
    (g["lt"] > g["lt"].shift(3))
    & (count_above_lt_30 >= 20)
    & (g["close"] >= g["lt"] * 0.985)
)
```

说明：

- 去掉硬性的 `ST > LT`
- 去掉硬性的 `count_above_st_20 >= 12`
- 改为：只要价格没有明显跌离 LT，允许平台整理

这里建议把“是否贴着 LT”作为结构保护条件，而不再强制要求短均线一定在长均线上方。

### 5.2 B1-soft 启动层

启动层建议**基本不动**，因为它决定了候选是否来自“有过主升启动的个股”，这是 B1 与普通超跌反抽最重要的区别。

```python
b1_startup_base_soft = (
    (hh40 / (ll40 + eps) >= 1.25)
    & (explosive_bull.rolling(30, min_periods=1).sum() >= 1)
    & (recent_high.rolling(15, min_periods=1).sum() >= 1)
    & b1_structure_soft
)
```

### 5.3 B1-soft 回调层

#### 原版过硬的地方

- `C <= ST * 1.02`
- `J < 20`
- `days_since_recent_high <= 12`

这些都会压缩样本。

#### 改造后建议

```python
b1_retrace_ready_soft = (
    (g["j"] < 28)
    & (c >= g["lt"] * 0.985)
    & (l >= g["lt"] * 0.965)
    & (dist_from_prev_high >= 0.05)
    & (dist_from_prev_high <= 0.18)
    & (days_since_recent_high >= 2)
    & (days_since_recent_high <= 15)
    & ((c < g["lt"]).rolling(10, min_periods=1).sum() <= 2)
    & (pullback_big_bear.rolling(12, min_periods=1).sum() == 0)
    & (pullback_distribution_bear.rolling(12, min_periods=1).sum() == 0)
)
```

放松点：

- `J < 20` → `J < 28`：允许更温和的回踩
- 不再要求 `C <= ST * 1.02`
- 回撤从 `4%~15%` 放到 `5%~18%`
- 回调时长从 `2~12` 放到 `2~15`
- 近 10 日跌破 LT 次数从 `<=1` 放到 `<=2`

### 5.4 B1-soft 缩量层

#### 原版

```python
b1_extreme_shrink = (
    (v <= v.rolling(10, min_periods=1).min())
    & (v < g["vol_ma20"] * 0.65)
)
```

#### 改造后建议

```python
b1_shrink_soft = (
    (v <= v.rolling(10, min_periods=1).quantile(0.2))
    & (v < g["vol_ma20"] * 0.80)
)
```

或者更稳妥地：

```python
b1_shrink_soft = (
    (g["vol_rank_10"] <= 0.25)
    & (v < g["vol_ma20"] * 0.80)
)
```

说明：

- 不再要求“必须等于近 10 日最低量”
- 只要处于缩量底部区域即可
- 这样更容易抓到“极致缩量前一日、后一日、假阴真阳日”这种好样本

### 5.5 B1-soft K线层

#### 原版

```python
b1_kline = (
    (g["body_pct"] <= 0.022)
    & (g["range_pct_prev_close"] <= 0.055)
)
```

#### 改造后建议

```python
b1_kline_soft = (
    (g["body_pct"] <= 0.035)
    & (g["range_pct_prev_close"] <= 0.075)
)
```

说明：

- 允许小阴、小十字、假阴真阳
- 允许略有波动，但仍排除明显失控大阴

### 5.6 B1-soft 禁做层

禁做层建议不放松，反而可以更重视：

- 近 30 日是否出现派发
- 是否已经形成双头派发、加速衰竭、阶梯出货

这一层本质上是**过滤错误的 B1 环境**，对胜率帮助通常比 `ST > LT` 更大。

### 5.7 B1-soft 候选信号

```python
b1_candidate_soft = (
    eligible_entry
    & b1_startup_base_soft
    & b1_retrace_ready_soft
    & b1_shrink_soft
    & b1_kline_soft
    & ~b1_forbidden
)
```

---

## 6. 规则改造的关键点：把 `ST > LT` 从“硬规则”降级为“特征”

### 6.1 保留连续特征

当前仓库已经在事件框架里保留了这一列：

```python
f_st_over_lt = ST / LT - 1
```

这是 B1-soft 最关键的基础。建议继续保留，并新增分桶字段：

```python
st_lt_band = pd.cut(
    f_st_over_lt,
    bins=[-999, -0.03, -0.015, -0.005, 0.005, 0.015, 0.03, 999],
    labels=[
        "lt_far_above",
        "lt_above",
        "lt_slight_above",
        "flat",
        "st_slight_above",
        "st_above",
        "st_far_above",
    ],
)
```

这样可以直接回答：

- 横盘区间 `ST < LT` 的样本到底有没有价值
- 哪一个区间的 `label_cls` 命中率最高
- 哪一个区间的 `label_rank` 最好

### 6.2 相关特征建议一起保留

除 `f_st_over_lt` 外，还建议重点观察：

- `f_cnt_above_lt`
- `f_cnt_above_st`
- `f_close_lt_dev`
- `f_close_st_dev`
- `f_low_lt_dev`
- `f_pullback_from_hh`
- `f_days_from_recent_high`
- `f_vol_to_ma20`
- `f_vol_rank_10`
- `f_vol_rank_20`
- `f_body_pct`
- `f_range_pct`
- `f_upper_wick`
- `f_lower_wick`
- `f_big_down_cnt_12`
- `f_dist_down_cnt_12`

这些特征在当前仓库事件框架中已经大部分存在，正好适合做：

- 命中率分桶
- SHAP 排序
- score decile 单调性检查

---

## 7. 标签体系维持不变

### 7.1 分类标签

建议沿用当前仓库已定义好的：

```python
label_cls = 1 if (
    confirm_hit
    and mfe_10 >= 0.10
    and not probe_fail
) else 0
```

这是最贴合“高质量 B1”的定义，不建议换掉。

### 7.2 排序标签

建议沿用：

```python
label_rank = (
    1.2 * clip(mfe_10, 0.00, 0.15)
    - 1.0 * clip(-mae_5, 0.00, 0.08)
    + 0.4 * int(confirm_hit)
    - 0.8 * int(probe_fail)
)
```

这非常适合 B1-soft，因为你的目标不是单纯提高命中率，而是：

- 既要能涨
- 又不能先大回撤
- 还要尽快确认
- 不能在 probe 阶段就被打掉

---

## 8. 训练方案

### 8.1 核心思路

不是让模型替代 B1，而是：

- 先由 B1-soft 圈出“像 B1 的候选”
- 再让模型在候选内做排序

### 8.2 实施顺序

#### 第一阶段：只替换候选，不改卖出

先把：

- `b1_candidate_v5`

替换为：

- `b1_candidate_soft`

但维持：

- `confirm`
- `probe_invalid`
- `exit`

全部不变。

目的：先验证放松候选后，是否能增加高质量样本密度。

#### 第二阶段：候选内排序

训练一个 `LightGBM / LGBMRanker / LGBMClassifier`，输出 `model_score`。

然后做：

```python
final_score = 0.30 * quality_score + 0.70 * model_score
```

在每日候选中：

- 只买 Top 3~5
- 或按 score 分层分配仓位

#### 第三阶段：再考虑 probe 仓位与 confirm 加仓

当候选质量明显改善后，再把模型分数用于：

- `probe` 的初始试探仓大小
- 是否允许 `confirm` 加仓

---

## 9. 数据切分与防泄漏

必须沿用当前仓库方案中的时间切分与 embargo 设计。

### 9.1 时间切分

建议：

- 训练：24 个月
- 验证：6 个月
- 测试：6 个月
- 每 3 个月滚动一次

### 9.2 embargo

建议继续保持：

- 同一股票 `embargo = 10` 交易日

这是必要的，因为标签本身使用未来 5~10 日窗口，相邻样本很容易泄漏。

---

## 10. 你真正要回答的研究问题

B1-soft 最重要的不是“能不能放松”，而是要用历史数据回答下面 6 个问题：

### 10.1 `ST/LT` 最优区间是什么

重点统计：

- `f_st_over_lt` 分桶命中率
- 分桶平均 `label_rank`
- 分桶 `probe_fail` 比例

目标：找出是否存在：

- `ST` 略低于 `LT` 反而更优
- `ST ≈ LT` 最优
- `ST` 明显高于 `LT` 才最优

### 10.2 最优回撤深度是什么

对 `f_pullback_from_hh` 分桶：

- 0~4%
- 4~6%
- 6~8%
- 8~10%
- 10~12%
- 12~15%
- 15%+

通常真正高质量 B1 会集中在中间带，而不是两端。

### 10.3 最优回调时长是什么

对 `f_days_from_recent_high` 分桶：

- 1~2
- 3~5
- 6~8
- 9~12
- 13~15
- 15+

判断哪一段最容易形成“缩量衰竭后再上”。

### 10.4 最优缩量程度是什么

对：

- `f_vol_to_ma20`
- `f_vol_rank_10`
- `f_vol_rank_20`

做分桶。

研究结论通常会告诉你：

- 是否必须极致地量到最低
- 还是只要“低于均量显著”就够

### 10.5 最优 K 线形态是什么

对：

- `f_body_pct`
- `f_range_pct`
- `f_upper_wick`
- `f_lower_wick`

做交叉分析。

目标是回答：

- 小阴线是否优于十字星
- 假阴真阳是否优于标准十字
- 上影过长是否明显拖累成功率

### 10.6 环境型过滤是否比 `ST > LT` 更重要

统计：

- `f_big_down_cnt_12`
- `f_dist_down_cnt_12`
- `recent_distribution`

对 `label_cls` 的影响。

很可能你会发现：

> “近 10~12 日是否有出货痕迹” 对胜率的解释力度，大于 “ST 是否大于 LT”。

---

## 11. 推荐的研究输出表

### 表 1：`f_st_over_lt` 分桶表现

| 分桶 | 样本数 | label_cls命中率 | 平均label_rank | probe_fail占比 |
|---|---:|---:|---:|---:|
| `< -3%` |  |  |  |  |
| `-3% ~ -1.5%` |  |  |  |  |
| `-1.5% ~ -0.5%` |  |  |  |  |
| `-0.5% ~ +0.5%` |  |  |  |  |
| `+0.5% ~ +1.5%` |  |  |  |  |
| `+1.5% ~ +3%` |  |  |  |  |
| `> +3%` |  |  |  |  |

### 表 2：回撤深度分桶表现

| `f_pullback_from_hh` | 样本数 | label_cls命中率 | 平均label_rank |
|---|---:|---:|---:|
| `0~4%` |  |  |  |
| `4~6%` |  |  |  |
| `6~8%` |  |  |  |
| `8~10%` |  |  |  |
| `10~12%` |  |  |  |
| `12~15%` |  |  |  |
| `15%+` |  |  |  |

### 表 3：缩量程度分桶表现

| `f_vol_to_ma20` | 样本数 | label_cls命中率 | 平均label_rank |
|---|---:|---:|---:|
| `<0.45` |  |  |  |
| `0.45~0.60` |  |  |  |
| `0.60~0.75` |  |  |  |
| `0.75~0.90` |  |  |  |
| `0.90+` |  |  |  |

---

## 12. 代码改造建议

### 12.1 需要新增的候选定义

在 `strategy.py` 中，保留 `b1_candidate_v5`，另加：

- `b1_structure_soft`
- `b1_startup_base_soft`
- `b1_retrace_ready_soft`
- `b1_shrink_soft`
- `b1_kline_soft`
- `b1_candidate_soft`

避免直接覆盖旧逻辑，方便 AB 测试。

### 12.2 推荐的最小可执行补丁

```python
b1_structure_soft = (
    (g["lt"] > g["lt"].shift(3))
    & (count_above_lt_30 >= 20)
    & (c >= g["lt"] * 0.985)
)

b1_startup_base_soft = (
    (hh40 / (ll40 + eps) >= 1.25)
    & (explosive_bull.rolling(30, min_periods=1).sum() >= 1)
    & (recent_high.rolling(15, min_periods=1).sum() >= 1)
    & b1_structure_soft
)

b1_retrace_ready_soft = (
    (g["j"] < 28)
    & (c >= g["lt"] * 0.985)
    & (l >= g["lt"] * 0.965)
    & (dist_from_prev_high >= 0.05)
    & (dist_from_prev_high <= 0.18)
    & (days_since_recent_high >= 2)
    & (days_since_recent_high <= 15)
    & ((c < g["lt"]).rolling(10, min_periods=1).sum() <= 2)
    & (pullback_big_bear.rolling(12, min_periods=1).sum() == 0)
    & (pullback_distribution_bear.rolling(12, min_periods=1).sum() == 0)
)

b1_shrink_soft = (
    (g["vol_rank_10"] <= 0.25)
    & (v < g["vol_ma20"] * 0.80)
)

b1_kline_soft = (
    (g["body_pct"] <= 0.035)
    & (g["range_pct_prev_close"] <= 0.075)
)

b1_candidate_soft = (
    eligible_entry
    & b1_startup_base_soft
    & b1_retrace_ready_soft
    & b1_shrink_soft
    & b1_kline_soft
    & ~b1_forbidden
)
```

### 12.3 确认与失效先不改

建议先保持：

- `b1_confirm`
- `b1_probe_invalid`
- `b1_exit_signal`

不动。

原因很简单：

- 你现在的核心问题是“候选样本太硬、太少、误杀横盘结构”
- 不宜同时改候选、确认、卖出，否则回测无法归因

---

## 13. 回测与比较框架

### 13.1 AB 对照

建议至少做三组：

#### A 组：原版 B1-v5

- 原候选
- 原 confirm
- 原 exit

#### B 组：B1-soft（只改候选）

- `b1_candidate_soft`
- 原 confirm
- 原 exit

#### C 组：B1-soft + 排序

- `b1_candidate_soft`
- 原 confirm
- 原 exit
- 候选内用 `model_score` 排序，只买前 3~5

### 13.2 重点观察指标

不只看总收益，更要看：

1. 候选日数量是否显著增加
2. `label_cls` 命中率是否下降过多
3. `probe_invalid` 占比是否恶化
4. `confirm_hit` 比例是否改善
5. 首次止盈触发率是否提高
6. 最大回撤是否扩大
7. 单票持仓天数是否过长

---

## 14. 预期结果

如果 B1-soft 成功，通常会出现以下特征：

1. 候选数量增加，但不是爆炸式增加。  
2. `ST < LT` 但接近 LT 的横盘类样本被找回来。  
3. 低质量样本虽然增加，但可通过排序模型再筛掉。  
4. 高质量样本的覆盖率提高，尤其是：
   - 平台缩量
   - 假阴真阳
   - 轻微交叉的 ST/LT 结构
5. `probe_invalid` 不一定立刻变低，但 Top 分组的 `label_cls` 会更集中。

---

## 15. 最终建议

### 15.1 结论

对于这个仓库，最合理的路线不是：

- 直接删除 `ST > LT`

而是：

- 保留 LT 主结构
- 放松短均线关系
- 把 `ST/LT` 变成连续特征
- 用历史事件样本去找“最赚钱的 B1 图形”

### 15.2 最推荐的落地路径

按优先级排序：

1. **新增 `b1_candidate_soft`，不覆盖旧版**  
2. **保留 confirm / invalid / exit 不变**  
3. **用现有事件框架导出样本**  
4. **先做分桶分析，验证 `ST/LT` 的最优区间**  
5. **再训练 `LightGBM` 排序模型**  
6. **最后才把 `model_score` 用于仓位与 confirm 加仓**

### 15.3 一句话概括

B1-soft 的本质不是“让规则更松”，而是：

> 用更宽的候选去覆盖真实 B1，用更强的排序去保住胜率。

---

## 16. 后续可继续追加的内容

这份文档之后最适合继续补两部分：

1. **B1-soft 的代码 patch 版**  
   直接按仓库 `strategy.py` 的风格给出替换段。

2. **B1-soft 的研究脚本版**  
   直接输出：
   - 分桶统计表
   - SHAP 特征重要性
   - Top decile / bottom decile 对比
