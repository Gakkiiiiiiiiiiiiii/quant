这是b1策略的完美图形，原则上完美图形干错也要干，请根据这些案例的，找出符合b1策略的特征（）

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

以下是b1的案例：
华纳药厂 2025.05.12
宁波韵升 2025.08.06
微芯生物 2025.06.20
方正科技 2025.07.23
野马电池 2025.07.31
光电股份 2025.07.10
新瀚新材 2025.08.01
航天发展 2025.11.12
双杰电气 2025.01.30
昊志机电 2025.12.23
和林微纳 2025.12.30
太极实业 2026.02.11
新特电气 2025.10.23
康平科技 2025.08.12
皇马科技 2025.08.22
航亚科技 2025.02.02
优刻得 2026.01.21
三花智控 2025.09.04
晶科科技 2025.12.30
长飞光纤 2026.01.14
润泽科技 2025.12.26
福石控股 2025.11.14
新易盛 2025.05.27
天孚通信 2025.07.25



