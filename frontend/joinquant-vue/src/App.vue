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

const profile = ref('backtest')
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
  selection_label: '三策略对比',
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

function formatDatetime(value) {
  if (!value) {
    return '--'
  }
  return String(value).replace('T', ' ').replace('.000Z', '')
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
  patternForm.selection_label = patternDefaults.selection_label || '三策略对比'
  patternForm.start_date = patternDefaults.start_date || '2023-01-01'
  patternForm.end_date = patternDefaults.end_date || '2026-03-24'
  patternForm.account = Number(patternDefaults.account || 500000)
  patternForm.max_holdings = Number(patternDefaults.max_holdings || 10)
  patternForm.risk_degree = Number(patternDefaults.risk_degree || 0.95)
  patternForm.max_holding_days = Number(patternDefaults.max_holding_days || 15)
}

async function fetchBootstrap({ preserveForms = false } = {}) {
  state.loading = !state.payload
  state.refreshing = Boolean(state.payload)
  state.error = ''
  try {
    const payload = await apiGet('/api/bootstrap', { profile: profile.value })
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

const logText = computed(() => logs.value[selectedLogSource.value] || '暂无日志')
const rules = computed(() => dashboardData.value.rules || [])
const patternActions = computed(() => dashboardData.value.pattern_actions || pattern.value.daily_actions || [])
const patternDecisions = computed(() => dashboardData.value.pattern_decisions || pattern.value.daily_decisions || [])
const positions = computed(() => dashboardData.value.positions || [])
const assets = computed(() => dashboardData.value.assets || [])
const benchmarkCurve = computed(() => dashboardData.value.benchmark_curve || [])
const patternSummary = computed(() => pattern.value.summary || [])
const patternComparison = computed(() => pattern.value.comparison || [])
const patternImageUrl = computed(() => {
  if (!pattern.value.png_url) {
    return ''
  }
  const joiner = pattern.value.png_url.includes('?') ? '&' : '?'
  return `${pattern.value.png_url}${joiner}ts=${state.refreshedAt}`
})

const overviewMetrics = computed(() => [
  { label: '策略收益', value: formatRatio(overview.value.total_return), tone: Number(overview.value.total_return) >= 0 ? 'positive' : 'negative' },
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
  const strategySeries = {
    name: primaryMode.value || '策略曲线',
    color: '#2e62ad',
    points: assets.value
      .filter((item) => (item.total_asset ?? item.account) !== null && (item.total_asset ?? item.account) !== undefined)
      .map((item) => ({
        label: formatDatetime(item.snapshot_time || item.datetime).slice(0, 10),
        value: Number(item.total_asset ?? item.account),
      })),
  }
  const benchmarkSeries = {
    name: 'Benchmark',
    color: '#f59e0b',
    points: benchmarkCurve.value
      .filter((item) => item.benchmark_equity !== null && item.benchmark_equity !== undefined)
      .map((item) => ({ label: formatDatetime(item.trading_date || item.datetime).slice(0, 10), value: Number(item.benchmark_equity) })),
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
    groups.get(key).push({ label: formatDatetime(item.datetime).slice(0, 10), value: item.equity })
  })
  return Array.from(groups.entries()).map(([name, points]) => ({ name, color: palette[name] || '#334155', points }))
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
      <div>
        <div class="jq-topbar__eyebrow">JoinQuant Inspired Workspace</div>
        <h1>JoinQuant 风格量化平台</h1>
        <div class="jq-topbar__sub">Vue 版前端，当前只展示 {{ primaryMode }} 策略的本地回测产物。</div>
      </div>
      <div class="jq-topbar__meta">
        <span>模式: {{ settings.environment || profile }}</span>
        <span>最新快照: {{ formatDatetime(overview.latest_time) }}</span>
        <span>生成时间: {{ formatDatetime(state.payload?.generated_at) }}</span>
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
        <button
          v-for="section in sections"
          :key="section"
          class="side-nav__item"
          :class="{ 'is-active': activeSection === section }"
          @click="activeSection = section"
        >
          {{ section }}
        </button>
        <div class="side-nav__hint">当前页面已收口到 {{ primaryMode }} 策略，概览、交易、报告与曲线均来自同一套回测产物。</div>
      </aside>

      <section class="content-stage">
        <template v-if="activeSection === '收益概述'">
          <div class="panel-card">
            <div class="panel-card__header">
              <div>
                <h2>收益概述</h2>
                <p>策略净值、Benchmark 与关键收益指标。</p>
              </div>
            </div>
            <MetricStrip :items="overviewMetrics" />
            <LineChart :series="equitySeries" :height="360" />
          </div>
          <div class="panel-card">
            <div class="panel-card__header">
              <div>
                <h2>形态策略看板</h2>
                <p>{{ primaryMode }} 与 Benchmark 的最新对比。</p>
              </div>
            </div>
            <LineChart :series="patternSeries" :height="300" />
            <DataTable :rows="patternSummary" :max-height="260" empty-text="暂无形态策略回测结果" />
          </div>
        </template>

        <template v-else-if="activeSection === '交易详情'">
          <div class="panel-card">
            <div class="panel-card__header panel-card__header--tabs">
              <div>
                <h2>交易详情</h2>
                <p>{{ primaryMode }} 回测的逐日交易记录、决策与风险指标。</p>
              </div>
              <div class="mini-tabs">
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
            <DataTable :rows="tradeRows" :max-height="560" :empty-text="tradeTableEmptyText" />
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
              <button class="btn" :disabled="actionState.busy === 'pattern'" @click="runPattern(true)">运行三策略对比</button>
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
