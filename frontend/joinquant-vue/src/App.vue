<script setup>
import { computed, onMounted, reactive, ref, watch } from 'vue'

import { apiGet, apiPost } from './api'
import DataTable from './components/DataTable.vue'
import LineChart from './components/LineChart.vue'
import MetricStrip from './components/MetricStrip.vue'

const sections = ['收益概述', '交易详情', '每日持仓&收益', '日志输出', '性能分析', '策略代码', 'Qlib 全市场', '形态实验室']
const tradeTabs = [
  { key: 'pattern_actions', label: '每日交易记录' },
  { key: 'pattern_decisions', label: '每日决策' },
  { key: 'rules', label: '风险指标' },
]
const sectionMeta = {
  收益概述: { code: '01', note: '权益、超额与关键指标' },
  交易详情: { code: '02', note: '成交、决策与报告' },
  '每日持仓&收益': { code: '03', note: '资产轨迹与仓位快照' },
  日志输出: { code: '04', note: '系统、测试与接口日志' },
  性能分析: { code: '05', note: '状态、告警与持仓画像' },
  策略代码: { code: '06', note: '策略模板与运行入口' },
  'Qlib 全市场': { code: '07', note: '历史数据与全市场回测' },
  形态实验室: { code: '08', note: '实验策略与可视化产物' },
}

const profile = ref('backtest')
const selectedPatternReportDir = ref('')
const activeSection = ref('收益概述')
const activeTradeTab = ref('pattern_actions')
const selectedLogSource = ref('QMT 客户端')
const refreshInterval = ref(0)

const state = reactive({
  loading: false,
  refreshing: false,
  error: '',
  payload: null,
  refreshedAt: Date.now(),
})

const strategyForm = reactive({
  strategy_label: '',
  name: '',
  implementation: '',
  rebalance_frequency: 'weekly',
  lookback_days: 20,
  top_n: 2,
  lot_size: 100,
})

const qlibForm = reactive({
  history_universe_sector: '',
  history_universe_limit: 0,
  history_start: '',
  history_adjustment: 'front',
  history_batch_size: 200,
  qlib_n_drop: 1,
  qlib_force_rebuild: false,
  name: '',
  implementation: '',
  rebalance_frequency: 'weekly',
  lookback_days: 20,
  top_n: 2,
  lot_size: 100,
})

const patternForm = reactive({
  selection_label: '四策略对比',
  start_date: '2023-01-01',
  end_date: '2026-03-24',
  account: 500000,
  max_holdings: 10,
  risk_degree: 0.95,
  max_holding_days: 15,
})

const actionState = reactive({
  busy: '',
  strategy: null,
  qlib: null,
  pattern: null,
})

function formatMoney(value) {
  if (value === null || value === undefined || value === '') {
    return '--'
  }
  return Number(value).toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

function formatRatio(value) {
  if (value === null || value === undefined || value === '') {
    return '--'
  }
  return `${(Number(value) * 100).toFixed(2)}%`
}

function formatInteger(value) {
  if (value === null || value === undefined || value === '') {
    return '--'
  }
  return Number(value).toLocaleString('zh-CN', { maximumFractionDigits: 0 })
}

function formatSignedMoney(value) {
  if (value === null || value === undefined || value === '') {
    return '--'
  }
  const number = Number(value)
  if (!Number.isFinite(number)) {
    return '--'
  }
  return number.toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

function formatDatetime(value) {
  if (!value) {
    return '--'
  }
  return String(value).replace('T', ' ').replace('.000Z', '')
}

function parseDateValue(value) {
  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) {
    return null
  }
  return parsed.getTime()
}

function toneFromValue(value) {
  const number = Number(value)
  if (!Number.isFinite(number)) {
    return ''
  }
  return number >= 0 ? 'positive' : 'negative'
}

function normalizeCurvePoints(points) {
  const normalized = points
    .map((point) => ({
      ...point,
      value: Number(point.value),
    }))
    .filter((point) => Number.isFinite(point.value))

  if (normalized.length === 0 || normalized[0].value === 0) {
    return []
  }

  const baseValue = normalized[0].value
  return normalized.map((point) => ({
    ...point,
    value: point.value / baseValue - 1,
  }))
}

function hydrateForms(payload) {
  const defaults = payload.strategy_defaults || {}
  strategyForm.strategy_label = defaults.label || ''
  strategyForm.name = defaults.name || ''
  strategyForm.implementation = defaults.implementation || ''
  strategyForm.rebalance_frequency = defaults.rebalance_frequency || 'weekly'
  strategyForm.lookback_days = Number(defaults.lookback_days || 20)
  strategyForm.top_n = Number(defaults.top_n || 2)
  strategyForm.lot_size = Number(defaults.lot_size || 100)

  const qlibRuntime = payload.qlib?.runtime || {}
  qlibForm.history_universe_sector = qlibRuntime.history_universe_sector || ''
  qlibForm.history_universe_limit = Number(qlibRuntime.history_universe_limit || 0)
  qlibForm.history_start = qlibRuntime.history_start || ''
  qlibForm.history_adjustment = qlibRuntime.history_adjustment || 'front'
  qlibForm.history_batch_size = Number(qlibRuntime.history_batch_size || 200)
  qlibForm.qlib_n_drop = Number(qlibRuntime.qlib_n_drop || 1)
  qlibForm.qlib_force_rebuild = Boolean(qlibRuntime.qlib_force_rebuild)
  qlibForm.name = strategyForm.name
  qlibForm.implementation = strategyForm.implementation
  qlibForm.rebalance_frequency = strategyForm.rebalance_frequency
  qlibForm.lookback_days = strategyForm.lookback_days
  qlibForm.top_n = strategyForm.top_n
  qlibForm.lot_size = strategyForm.lot_size

  const patternDefaults = payload.pattern?.defaults || {}
  patternForm.selection_label = patternDefaults.selection_label || '四策略对比'
  patternForm.start_date = patternDefaults.start_date || '2023-01-01'
  patternForm.end_date = patternDefaults.end_date || '2026-03-24'
  patternForm.account = Number(patternDefaults.account || 500000)
  patternForm.max_holdings = Number(patternDefaults.max_holdings || 10)
  patternForm.risk_degree = Number(patternDefaults.risk_degree || 0.95)
  patternForm.max_holding_days = Number(patternDefaults.max_holding_days || 15)
  selectedPatternReportDir.value = payload.pattern?.selected_report_dir || ''
}

async function fetchBootstrap({ preserveForms = false } = {}) {
  state.loading = !state.payload
  state.refreshing = Boolean(state.payload)
  state.error = ''
  try {
    const payload = await apiGet('/api/bootstrap', {
      profile: profile.value,
      pattern_report_dir: selectedPatternReportDir.value,
    })
    state.payload = payload
    state.refreshedAt = Date.now()
    if (!preserveForms) {
      hydrateForms(payload)
      activeTradeTab.value = (payload.data?.pattern_actions || []).length > 0 ? 'pattern_actions' : ((payload.data?.pattern_decisions || []).length > 0 ? 'pattern_decisions' : 'rules')
    }
  } catch (error) {
    state.error = error.message
  } finally {
    state.loading = false
    state.refreshing = false
  }
}

async function runStrategy(action) {
  actionState.busy = 'strategy'
  try {
    const response = await apiPost('/api/actions/strategy', {
      profile: profile.value,
      action,
      ...strategyForm,
    })
    actionState.strategy = response.result
    await fetchBootstrap({ preserveForms: true })
  } catch (error) {
    actionState.strategy = { ok: false, stderr: error.message }
  } finally {
    actionState.busy = ''
  }
}

async function runQlib(action) {
  actionState.busy = 'qlib'
  try {
    const response = await apiPost('/api/actions/qlib', {
      profile: profile.value,
      action,
      ...qlibForm,
    })
    actionState.qlib = response.result
    await fetchBootstrap({ preserveForms: true })
  } catch (error) {
    actionState.qlib = { ok: false, stderr: error.message }
  } finally {
    actionState.busy = ''
  }
}

async function runPattern(runAll) {
  actionState.busy = 'pattern'
  try {
    const response = await apiPost('/api/actions/pattern', {
      ...patternForm,
      pattern_report_dir: selectedPatternReportDir.value,
      run_all: runAll,
    })
    actionState.pattern = response.result
    await fetchBootstrap({ preserveForms: true })
  } catch (error) {
    actionState.pattern = { ok: false, stderr: error.message }
  } finally {
    actionState.busy = ''
  }
}

const settings = computed(() => state.payload?.settings || {})
const overview = computed(() => state.payload?.overview || {})
const connection = computed(() => state.payload?.connection || {})
const alerts = computed(() => state.payload?.alerts || [])
const logs = computed(() => state.payload?.logs || {})
const dashboardData = computed(() => state.payload?.data || {})
const qlibStatus = computed(() => state.payload?.qlib?.status || {})
const pattern = computed(() => state.payload?.pattern || { summary: [], comparison: [] })
const primaryMode = computed(() => pattern.value.primary_mode || 'B1')
const profileOptions = computed(() => state.payload?.meta?.profiles || [])
const strategyOptions = computed(() => state.payload?.meta?.strategies || [])
const patternDirOptions = computed(() => pattern.value.report_dirs || [])
const activeSectionMeta = computed(() => sectionMeta[activeSection.value] || { code: '--', note: '' })

const logText = computed(() => logs.value[selectedLogSource.value] || '暂无日志')
const rules = computed(() => dashboardData.value.rules || [])
const patternActions = computed(() => dashboardData.value.pattern_actions || pattern.value.daily_actions || [])
const patternDecisions = computed(() => dashboardData.value.pattern_decisions || pattern.value.daily_decisions || [])
const genericTrades = computed(() => dashboardData.value.trades || [])
const positions = computed(() => dashboardData.value.positions || [])
const assets = computed(() => dashboardData.value.assets || [])
const benchmarkCurve = computed(() => dashboardData.value.benchmark_curve || [])
const patternSummary = computed(() => pattern.value.summary || [])
const patternComparison = computed(() => pattern.value.comparison || [])
const isMicrocapBacktest = computed(() => assets.value.some((item) => item.account_id === 'joinquant_microcap_alpha'))
const patternImageUrl = computed(() => {
  if (!pattern.value.png_url) {
    return ''
  }
  const joiner = pattern.value.png_url.includes('?') ? '&' : '?'
  return `${pattern.value.png_url}${joiner}ts=${state.refreshedAt}`
})
const assetDateRange = computed(() => {
  if (assets.value.length === 0) {
    return '--'
  }
  const points = assets.value
    .map((item) => formatDatetime(item.snapshot_time || item.datetime).slice(0, 10))
    .filter((item) => item && item !== '--')
  if (points.length === 0) {
    return '--'
  }
  return `${points[0]} → ${points[points.length - 1]}`
})
const benchmarkReturn = computed(() => {
  const points = benchmarkCurve.value
    .map((item) => Number(item.benchmark_equity))
    .filter((value) => Number.isFinite(value))
  if (points.length < 2 || points[0] === 0) {
    return null
  }
  return points[points.length - 1] / points[0] - 1
})
const excessReturn = computed(() => {
  const total = Number(overview.value.total_return)
  const benchmark = Number(benchmarkReturn.value)
  if (!Number.isFinite(total) || !Number.isFinite(benchmark)) {
    return null
  }
  return total - benchmark
})
const recentTradePreview = computed(() => {
  return [...genericTrades.value]
    .sort((left, right) => (parseDateValue(right.trading_date) || 0) - (parseDateValue(left.trading_date) || 0))
    .slice(0, 8)
})
const overviewTradeColumns = computed(() => ([
  { key: 'trading_date', label: '交易日', width: 104 },
  { key: 'symbol', label: '代码', width: 92, code: true },
  { key: 'side', label: '方向', width: 70, align: 'center' },
  { key: 'shares', label: '股数', width: 78, align: 'right', formatter: formatInteger },
  { key: 'amount', label: '成交额', width: 110, align: 'right', formatter: formatSignedMoney },
]))
const overviewHeroFacts = computed(() => ([
  {
    label: '策略收益',
    value: formatRatio(overview.value.total_return),
    tone: toneFromValue(overview.value.total_return),
    foot: `Benchmark ${formatRatio(benchmarkReturn.value)}`,
  },
  {
    label: '超额收益',
    value: formatRatio(excessReturn.value),
    tone: toneFromValue(excessReturn.value),
    foot: '相对基准',
  },
  {
    label: '当前仓位',
    value: formatRatio(overview.value.exposure),
    foot: `现金 ${formatMoney(overview.value.cash)}`,
  },
  {
    label: '回测区间',
    value: assetDateRange.value,
    foot: isMicrocapBacktest.value ? '10万资金 / 微盘 Alpha' : (connection.value.label || '--'),
  },
]))

const overviewMetrics = computed(() => [
  { label: '策略收益', value: formatRatio(overview.value.total_return), tone: toneFromValue(overview.value.total_return) },
  { label: '最大回撤', value: formatRatio(overview.value.drawdown), tone: 'negative' },
  { label: '累计换手', value: formatMoney(overview.value.turnover) },
  { label: '订单数', value: String(overview.value.order_count ?? 0) },
  { label: '成交数', value: String(overview.value.trade_count ?? 0) },
])

const performanceMetrics = computed(() => [
  { label: '总资产', value: formatMoney(overview.value.total_asset) },
  { label: '现金', value: formatMoney(overview.value.cash) },
  { label: '持仓市值', value: formatMoney(overview.value.market_value) },
  { label: '仓位', value: formatRatio(overview.value.exposure) },
])

const equitySeries = computed(() => {
  const strategyPoints = assets.value
    .filter((item) => (item.total_asset ?? item.account) !== null && (item.total_asset ?? item.account) !== undefined)
    .map((item) => ({
      label: formatDatetime(item.snapshot_time || item.datetime).slice(0, 10),
      value: Number(item.total_asset ?? item.account),
    }))
    .sort((left, right) => left.label.localeCompare(right.label))
  const benchmarkPoints = benchmarkCurve.value
    .filter((item) => item.benchmark_equity !== null && item.benchmark_equity !== undefined)
    .map((item) => ({
      label: formatDatetime(item.trading_date || item.datetime).slice(0, 10),
      value: Number(item.benchmark_equity),
    }))
    .sort((left, right) => left.label.localeCompare(right.label))

  const strategySeries = {
    name: isMicrocapBacktest.value ? '聚宽微盘 Alpha' : (primaryMode.value || '策略曲线'),
    color: '#2e62ad',
    points: normalizeCurvePoints(strategyPoints),
  }
  const benchmarkSeries = {
    name: 'Benchmark',
    color: '#f59e0b',
    points: normalizeCurvePoints(benchmarkPoints),
  }
  return [strategySeries, benchmarkSeries].filter((item) => item.points.length > 0)
})

const patternSeries = computed(() => {
  const palette = {
    B1: '#1d4ed8',
    B2: '#d97706',
    B3: '#0f766e',
    Benchmark: '#64748b',
  }
  const groups = new Map()
  patternComparison.value.forEach((item) => {
    const key = item.series || 'Unknown'
    if (!groups.has(key)) {
      groups.set(key, [])
    }
    groups.get(key).push({
      label: formatDatetime(item.datetime).slice(0, 10),
      value: Number(item.equity),
    })
  })
  return Array.from(groups.entries())
    .map(([name, points]) => ({
      name,
      color: palette[name] || '#334155',
      points: normalizeCurvePoints(points.sort((left, right) => left.label.localeCompare(right.label))),
    }))
    .filter((item) => item.points.length > 0)
})

const actionColumns = computed(() => ([
  { key: '日期', label: '买入日', sticky: true, width: 108 },
  { key: 'SELL日期', label: '卖出日', width: 108 },
  { key: '股票代码', label: '代码', sticky: true, width: 104, code: true },
  { key: '标的名称', label: '名称', sticky: true, width: 112 },
  { key: '策略(B1 B2 B3)', label: '策略', width: 76, align: 'center' },
  { key: '买入评分', label: '买分', width: 84, align: 'right' },
  { key: '卖出评分', label: '卖分', width: 84, align: 'right' },
  { key: 'BUY金额', label: '买入额', width: 112, align: 'right', formatter: formatSignedMoney },
  { key: 'BUY股数', label: '股数', width: 84, align: 'right', formatter: formatInteger },
  { key: '买入价格', label: '买价', width: 84, align: 'right' },
  { key: '卖出价格', label: '卖价', width: 84, align: 'right' },
  { key: '卖出原因', label: '卖出原因', minWidth: 168, maxWidth: 220, wrap: true, code: true },
  { key: '这个标的这次操作的盈亏金额', label: '盈亏', width: 112, align: 'right', formatter: formatSignedMoney, tone: 'pnl' },
  { key: '收益率', label: '收益率', width: 92, align: 'right', formatter: formatRatio, tone: 'return' },
]))

const decisionColumns = computed(() => ([
  { key: 'trading_date', label: '交易日', sticky: true, width: 108 },
  { key: 'mode', label: '策略', sticky: true, width: 76, align: 'center' },
  { key: 'signal_count', label: '信号', width: 76, align: 'right', formatter: formatInteger },
  { key: 'buy_count', label: '买入', width: 76, align: 'right', formatter: formatInteger },
  { key: 'sell_count', label: '卖出', width: 76, align: 'right', formatter: formatInteger },
  { key: 'hold_count', label: '持仓', width: 76, align: 'right', formatter: formatInteger },
  { key: 'candidate_symbols', label: '候选标的', minWidth: 240, maxWidth: 320, wrap: true, code: true },
  { key: 'buy_symbols', label: '买入标的', minWidth: 200, maxWidth: 280, wrap: true, code: true },
  { key: 'sell_symbols', label: '卖出标的', minWidth: 200, maxWidth: 280, wrap: true, code: true },
  { key: 'hold_symbols', label: '持仓标的', minWidth: 220, maxWidth: 320, wrap: true, code: true },
]))

const ruleColumns = computed(() => ([
  { key: 'mode', label: '策略', width: 76, align: 'center' },
  { key: 'metric', label: '指标', minWidth: 160, maxWidth: 220, wrap: true, code: true },
  { key: 'value', label: '数值', width: 120, align: 'right' },
]))

const genericTradeColumns = computed(() => ([
  { key: 'trading_date', label: '交易日', sticky: true, width: 108 },
  { key: 'symbol', label: '代码', sticky: true, width: 104, code: true },
  { key: 'instrument_name', label: '名称', sticky: true, width: 120 },
  { key: 'side', label: '方向', width: 76, align: 'center' },
  { key: 'shares', label: '股数', width: 84, align: 'right', formatter: formatInteger },
  { key: 'price', label: '价格', width: 84, align: 'right' },
  { key: 'amount', label: '成交额', width: 112, align: 'right', formatter: formatSignedMoney },
  { key: 'fee', label: '费用', width: 96, align: 'right', formatter: formatSignedMoney },
  { key: 'reason', label: '原因', minWidth: 180, maxWidth: 260, wrap: true, code: true },
]))

const tradeColumns = computed(() => {
  if (activeTradeTab.value === 'pattern_actions') {
    return actionColumns.value
  }
  if (activeTradeTab.value === 'pattern_decisions') {
    return decisionColumns.value
  }
  return ruleColumns.value
})

const tradeRows = computed(() => {
  if (activeTradeTab.value === 'pattern_actions') {
    return patternActions.value
  }
  if (activeTradeTab.value === 'pattern_decisions') {
    return patternDecisions.value
  }
  return rules.value
})

const tradeTableEmptyText = computed(() => {
  if (activeTradeTab.value === 'pattern_actions') {
    return '暂无每日交易记录'
  }
  if (activeTradeTab.value === 'pattern_decisions') {
    return '暂无每日决策记录'
  }
  return '暂无风险指标'
})

const hasPatternTradeData = computed(() => (
  !isMicrocapBacktest.value && (patternActions.value.length > 0 || patternDecisions.value.length > 0 || rules.value.length > 0)
))

watch(profile, () => {
  fetchBootstrap()
})

watch(
  logs,
  (current) => {
    const names = Object.keys(current || {})
    if (names.length > 0 && !names.includes(selectedLogSource.value)) {
      selectedLogSource.value = names[0]
    }
  },
  { immediate: true },
)

watch(refreshInterval, (value) => {
  if (window.__jqRefreshTimer) {
    window.clearInterval(window.__jqRefreshTimer)
    window.__jqRefreshTimer = null
  }
  if (Number(value) > 0) {
    window.__jqRefreshTimer = window.setInterval(() => {
      fetchBootstrap({ preserveForms: true })
    }, Number(value) * 1000)
  }
})

onMounted(() => {
  fetchBootstrap()
})
</script>

<template>
  <div class="app-shell">
    <header class="jq-topbar">
      <div class="jq-topbar__main">
        <div class="jq-topbar__eyebrow">JoinQuant Inspired Workspace</div>
        <h1>JoinQuant 风格量化平台</h1>
        <div class="jq-topbar__sub">Vue 版前端，默认展示当前回测产物，并保留形态实验室与全市场入口。</div>
        <div class="jq-topbar__chips">
          <span class="hero-chip hero-chip--brand">{{ isMicrocapBacktest ? '聚宽微盘 Alpha' : (primaryMode || '策略面板') }}</span>
          <span class="hero-chip">{{ connection.label || '--' }}</span>
          <span class="hero-chip">区间 {{ assetDateRange }}</span>
        </div>
      </div>
      <div class="jq-topbar__meta">
        <article class="hero-stat hero-stat--primary">
          <span>总资产</span>
          <strong>{{ formatMoney(overview.total_asset) }}</strong>
          <small>最新快照 {{ formatDatetime(overview.latest_time).slice(0, 10) }}</small>
        </article>
        <article class="hero-stat">
          <span>最大回撤</span>
          <strong>{{ formatRatio(overview.drawdown) }}</strong>
          <small>当前仓位 {{ formatRatio(overview.exposure) }}</small>
        </article>
        <article class="hero-stat">
          <span>生成时间</span>
          <strong>{{ formatDatetime(state.payload?.generated_at).slice(0, 16) }}</strong>
          <small>模式 {{ settings.environment || profile }}</small>
        </article>
      </div>
    </header>

    <div class="jq-toolbar">
      <div class="jq-toolbar__left">
        <label>
          <span>视图模式</span>
          <select v-model="profile">
            <option v-for="item in profileOptions" :key="item.key" :value="item.key">{{ item.label }}</option>
          </select>
        </label>
        <label>
          <span>自动刷新</span>
          <select v-model="refreshInterval">
            <option :value="0">关闭</option>
            <option :value="10">10 秒</option>
            <option :value="20">20 秒</option>
            <option :value="30">30 秒</option>
          </select>
        </label>
        <label>
          <span>结果目录</span>
          <select v-model="selectedPatternReportDir" @change="fetchBootstrap({ preserveForms: true })">
            <option v-for="item in patternDirOptions" :key="item.value" :value="item.value">{{ item.label }}</option>
          </select>
        </label>
      </div>
      <div class="jq-toolbar__right">
        <div class="toolbar-stat">
          <span>总资产</span>
          <strong>{{ formatMoney(overview.total_asset) }}</strong>
        </div>
        <div class="toolbar-stat">
          <span>账户状态</span>
          <strong>{{ connection.label || '--' }}</strong>
        </div>
        <button class="btn btn--primary" :disabled="state.refreshing" @click="fetchBootstrap({ preserveForms: true })">
          {{ state.refreshing ? '刷新中...' : '刷新面板' }}
        </button>
      </div>
    </div>

    <div v-if="state.error" class="page-alert page-alert--error">{{ state.error }}</div>
    <div v-if="state.loading" class="page-loading">正在加载聚宽风格面板...</div>

    <main v-else class="page-layout">
      <aside class="side-nav">
        <div class="side-nav__eyebrow">Workspace</div>
        <button
          v-for="section in sections"
          :key="section"
          class="side-nav__item"
          :class="{ 'is-active': activeSection === section }"
          @click="activeSection = section"
        >
          <span class="side-nav__item-mark">{{ sectionMeta[section].code }}</span>
          <span class="side-nav__item-copy">
            <strong>{{ section }}</strong>
            <small>{{ sectionMeta[section].note }}</small>
          </span>
        </button>
        <div class="side-nav__hint">概览页默认展示当前回测结果，形态实验室仍可单独查看 {{ primaryMode }} 的实验产物。</div>
      </aside>

      <section class="content-stage">
        <template v-if="activeSection === '收益概述'">
          <section class="overview-hero">
            <div class="overview-hero__copy">
              <div class="overview-hero__kicker">{{ activeSectionMeta.code }} / {{ isMicrocapBacktest ? 'Microcap Alpha Board' : `${primaryMode} Strategy Board` }}</div>
              <h2>{{ isMicrocapBacktest ? '聚宽微盘 Alpha 回测总览' : `${primaryMode} 策略总览` }}</h2>
              <p>
                {{ isMicrocapBacktest
                  ? '主图展示策略与 Benchmark 的累计收益，右侧收口到最近成交，首屏优先给出结果而不是表单。'
                  : '当前版式强调收益曲线、策略状态和实验入口的层级关系，减少信息抢占。'
                }}
              </p>
            </div>
            <div class="overview-hero__facts">
              <article v-for="item in overviewHeroFacts" :key="item.label" class="overview-fact">
                <span>{{ item.label }}</span>
                <strong :class="item.tone || ''">{{ item.value }}</strong>
                <small>{{ item.foot }}</small>
              </article>
            </div>
          </section>
          <div class="overview-grid">
            <div class="panel-card panel-card--main">
              <div class="panel-card__header">
                <div>
                  <h2>收益概述</h2>
                  <p>策略净值、Benchmark 与关键收益指标。</p>
                </div>
              </div>
              <MetricStrip :items="overviewMetrics" />
              <LineChart :series="equitySeries" :height="520" :as-percent="true" />
            </div>
            <div class="panel-card panel-card--side">
              <div v-if="isMicrocapBacktest">
                <div class="panel-card__header">
                  <div>
                    <h2>近期成交</h2>
                    <p>首屏展示最近的调仓记录，避免把重要信息压到明细页。</p>
                  </div>
                </div>
                <DataTable :rows="recentTradePreview" :columns="overviewTradeColumns" :max-height="430" empty-text="暂无成交记录" />
              </div>
              <div v-else>
                <div class="panel-card__header">
                  <div>
                    <h2>形态策略看板</h2>
                    <p>{{ primaryMode }} 与 Benchmark 的最新对比。</p>
                  </div>
                </div>
                <LineChart :series="patternSeries" :height="320" :as-percent="true" />
                <DataTable :rows="patternSummary" :max-height="220" empty-text="暂无形态策略回测结果" />
              </div>
            </div>
          </div>
        </template>

        <template v-else-if="activeSection === '交易详情'">
          <div class="panel-card">
            <div class="panel-card__header panel-card__header--tabs">
              <div>
                <h2>交易详情</h2>
                <p>{{ hasPatternTradeData ? `${primaryMode} 回测的逐日交易记录、决策与风险指标。` : '当前回测的成交明细与日终报告。' }}</p>
              </div>
              <div v-if="hasPatternTradeData" class="mini-tabs">
                <button
                  v-for="tab in tradeTabs"
                  :key="tab.key"
                  class="mini-tabs__item"
                  :class="{ 'is-active': activeTradeTab === tab.key }"
                  @click="activeTradeTab = tab.key"
                >
                  {{ tab.label }}
                </button>
              </div>
            </div>
            <DataTable
              v-if="hasPatternTradeData"
              :rows="tradeRows"
              :columns="tradeColumns"
              :max-height="560"
              :empty-text="tradeTableEmptyText"
            />
            <DataTable
              v-else
              :rows="genericTrades"
              :columns="genericTradeColumns"
              :max-height="560"
              empty-text="暂无成交记录"
            />
            <div class="markdown-report">
              <h3>日终报告</h3>
              <pre>{{ dashboardData.report_text || '暂无日终报告。' }}</pre>
            </div>
          </div>
        </template>

        <template v-else-if="activeSection === '每日持仓&收益'">
          <div class="panel-card">
            <div class="panel-card__header">
              <div>
                <h2>每日持仓&收益</h2>
                <p>账户快照、现金与回撤轨迹。</p>
              </div>
            </div>
            <DataTable :rows="assets" :max-height="620" empty-text="暂无资产快照" />
          </div>
        </template>

        <template v-else-if="activeSection === '日志输出'">
          <div class="panel-card">
            <div class="panel-card__header panel-card__header--tabs">
              <div>
                <h2>日志输出</h2>
                <p>QMT、系统、测试、UI 与 API 日志尾部。</p>
              </div>
              <select v-model="selectedLogSource" class="compact-select">
                <option v-for="name in Object.keys(logs)" :key="name" :value="name">{{ name }}</option>
              </select>
            </div>
            <pre class="log-view">{{ logText }}</pre>
          </div>
        </template>

        <template v-else-if="activeSection === '性能分析'">
          <div class="panel-card">
            <div class="panel-card__header">
              <div>
                <h2>性能分析</h2>
                <p>账户状态、告警与当前持仓。</p>
              </div>
            </div>
            <MetricStrip :items="performanceMetrics" />
            <div class="status-grid">
              <article class="status-card">
                <div class="status-card__label">连接状态</div>
                <div class="status-card__value">{{ connection.label || '--' }}</div>
                <p>{{ connection.detail || '暂无连接信息。' }}</p>
              </article>
              <article class="status-card">
                <div class="status-card__label">告警面板</div>
                <div class="status-card__alerts">
                  <div v-if="alerts.length === 0" class="empty-state">暂无告警</div>
                  <div v-for="item in alerts" :key="`${item.title}-${item.detail}`" class="alert-chip" :class="`tone-${item.severity}`">
                    <strong>{{ item.title }}</strong>
                    <span>{{ item.detail }}</span>
                  </div>
                </div>
              </article>
            </div>
            <DataTable :rows="positions" :max-height="320" empty-text="暂无持仓快照" />
          </div>
        </template>

        <template v-else-if="activeSection === '策略代码'">
          <div class="panel-card">
            <div class="panel-card__header">
              <div>
                <h2>策略代码与运行</h2>
                <p>聚宽风格控制台，直接触发回测、仿真和实盘探测。</p>
              </div>
            </div>
            <div class="form-grid form-grid--strategy">
              <label>
                <span>策略模板</span>
                <select v-model="strategyForm.strategy_label">
                  <option v-for="item in strategyOptions" :key="item.label" :value="item.label">{{ item.label }}</option>
                </select>
              </label>
              <label><span>策略名</span><input v-model="strategyForm.name" type="text" /></label>
              <label><span>实现标识</span><input v-model="strategyForm.implementation" type="text" /></label>
              <label><span>调仓频率</span>
                <select v-model="strategyForm.rebalance_frequency">
                  <option value="daily">daily</option>
                  <option value="weekly">weekly</option>
                </select>
              </label>
              <label><span>观察窗口</span><input v-model.number="strategyForm.lookback_days" type="number" min="1" max="250" /></label>
              <label><span>Top N</span><input v-model.number="strategyForm.top_n" type="number" min="1" max="20" /></label>
              <label><span>最小交易单位</span><input v-model.number="strategyForm.lot_size" type="number" min="1" max="10000" /></label>
            </div>
            <div class="action-row">
              <button class="btn btn--primary" :disabled="actionState.busy === 'strategy'" @click="runStrategy('backtest')">运行回测</button>
              <button class="btn" :disabled="actionState.busy === 'strategy'" @click="runStrategy('paper')">运行仿真</button>
              <button class="btn" :disabled="actionState.busy === 'strategy'" @click="runStrategy('probe')">实盘探测</button>
              <button class="btn" :disabled="actionState.busy === 'strategy'" @click="runStrategy('strategy')">运行实盘</button>
            </div>
            <pre v-if="actionState.strategy" class="task-output">{{ JSON.stringify(actionState.strategy, null, 2) }}</pre>
          </div>
        </template>

        <template v-else-if="activeSection === 'Qlib 全市场'">
          <div class="panel-card">
            <div class="panel-card__header">
              <div>
                <h2>Qlib 全市场回测</h2>
                <p>历史更新、Provider 缓存与全市场回测统一入口。</p>
              </div>
            </div>
            <MetricStrip :items="[
              { label: '历史记录数', value: String(qlibStatus.row_count ?? 0) },
              { label: '股票数', value: String(qlibStatus.symbol_count ?? 0) },
              { label: '最新交易日', value: qlibStatus.latest_trading_date || '--' },
              { label: '历史文件', value: qlibStatus.history_path || '--' },
            ]" />
            <div class="form-grid form-grid--qlib">
              <label><span>股票池板块</span><input v-model="qlibForm.history_universe_sector" type="text" /></label>
              <label><span>历史起始日</span><input v-model="qlibForm.history_start" type="text" /></label>
              <label><span>复权口径</span>
                <select v-model="qlibForm.history_adjustment">
                  <option value="front">front</option>
                  <option value="back">back</option>
                  <option value="none">none</option>
                </select>
              </label>
              <label><span>股票上限</span><input v-model.number="qlibForm.history_universe_limit" type="number" min="0" max="10000" /></label>
              <label><span>批量下载大小</span><input v-model.number="qlibForm.history_batch_size" type="number" min="1" max="1000" /></label>
              <label><span>换仓数</span><input v-model.number="qlibForm.qlib_n_drop" type="number" min="1" max="20" /></label>
              <label class="check-field"><input v-model="qlibForm.qlib_force_rebuild" type="checkbox" /><span>回测前强制重建 Qlib provider</span></label>
            </div>
            <div class="action-row action-row--wrap">
              <button class="btn" :disabled="actionState.busy === 'qlib'" @click="runQlib('incremental')">增量更新历史</button>
              <button class="btn" :disabled="actionState.busy === 'qlib'" @click="runQlib('full')">全量重建历史</button>
              <button class="btn" :disabled="actionState.busy === 'qlib'" @click="runQlib('cleanup-history')">清理历史缓存</button>
              <button class="btn" :disabled="actionState.busy === 'qlib'" @click="runQlib('cleanup-qlib')">清理 Qlib 缓存</button>
              <button class="btn btn--primary" :disabled="actionState.busy === 'qlib'" @click="runQlib('backtest')">运行全市场回测</button>
            </div>
            <pre v-if="actionState.qlib" class="task-output">{{ JSON.stringify(actionState.qlib, null, 2) }}</pre>
          </div>
        </template>

        <template v-else-if="activeSection === '形态实验室'">
          <div class="panel-card">
            <div class="panel-card__header">
              <div>
                <h2>形态实验室</h2>
                <p>按聚宽式回测面板展示 {{ primaryMode }} 策略结果，并保留直接运行入口。</p>
              </div>
              <a v-if="pattern.html_url" class="text-link" :href="pattern.html_url" target="_blank" rel="noreferrer">打开交互图</a>
            </div>
            <div class="toolbar-stat">
              <span>当前目录</span>
              <strong>{{ pattern.selected_report_dir || '--' }}</strong>
            </div>
            <div class="form-grid form-grid--pattern">
              <label>
                <span>策略选择</span>
                <select v-model="patternForm.selection_label">
                  <option v-for="(value, label) in pattern.options || {}" :key="label" :value="label">{{ label }}</option>
                </select>
              </label>
              <label><span>开始日期</span><input v-model="patternForm.start_date" type="date" /></label>
              <label><span>结束日期</span><input v-model="patternForm.end_date" type="date" /></label>
              <label><span>初始资金</span><input v-model.number="patternForm.account" type="number" min="10000" step="10000" /></label>
              <label><span>最大持仓数</span><input v-model.number="patternForm.max_holdings" type="number" min="1" max="50" /></label>
              <label><span>风险系数</span><input v-model.number="patternForm.risk_degree" type="number" min="0.1" max="1" step="0.05" /></label>
              <label><span>最大持有天数</span><input v-model.number="patternForm.max_holding_days" type="number" min="1" max="60" /></label>
            </div>
            <div class="action-row">
              <button class="btn btn--primary" :disabled="actionState.busy === 'pattern'" @click="runPattern(false)">运行当前策略</button>
              <button class="btn" :disabled="actionState.busy === 'pattern'" @click="runPattern(true)">运行四策略对比</button>
            </div>
            <DataTable :rows="patternSummary" :max-height="260" empty-text="暂无形态策略结果" />
            <img v-if="patternImageUrl" class="pattern-image" :src="patternImageUrl" alt="形态策略权益对比图" />
            <pre v-if="actionState.pattern" class="task-output">{{ JSON.stringify(actionState.pattern, null, 2) }}</pre>
          </div>
        </template>
      </section>
    </main>
  </div>
</template>


