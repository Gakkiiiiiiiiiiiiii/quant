<script setup>
import { computed, reactive, ref } from 'vue'

const props = defineProps({
  labels: {
    type: Array,
    default: () => [],
  },
  strategy: {
    type: Array,
    default: () => [],
  },
  benchmark: {
    type: Array,
    default: () => [],
  },
  excess: {
    type: Array,
    default: () => [],
  },
  drawdown: {
    type: Array,
    default: () => [],
  },
  height: {
    type: Number,
    default: 760,
  },
})

const width = 1280
const padding = { top: 22, right: 22, bottom: 34, left: 68 }
const gap = 16
const panelCount = 3
const containerRef = ref(null)
const activeIndex = ref(null)
const hoverState = reactive({
  visible: false,
  left: 0,
  top: 0,
  alignRight: false,
})

const points = computed(() => {
  const length = Math.max(
    props.labels.length,
    props.strategy.length,
    props.benchmark.length,
    props.excess.length,
    props.drawdown.length,
  )
  return Array.from({ length }, (_, index) => ({
    label: props.labels[index] || '',
    strategy: Number(props.strategy[index]),
    benchmark: Number(props.benchmark[index]),
    excess: Number(props.excess[index]),
    drawdown: Number(props.drawdown[index]),
  })).filter((item) => item.label)
})

const plotWidth = computed(() => width - padding.left - padding.right)
const panelHeight = computed(() => Math.max(120, (props.height - padding.top - padding.bottom - gap * (panelCount - 1)) / panelCount))
const totalHeight = computed(() => padding.top + padding.bottom + panelHeight.value * panelCount + gap * (panelCount - 1))
const returnRange = computed(() => buildRange([
  ...points.value.map((item) => item.strategy),
  ...points.value.map((item) => item.benchmark),
]))
const excessRange = computed(() => buildRange(points.value.map((item) => item.excess), 0.12))
const drawdownRange = computed(() => {
  const clean = points.value.map((item) => item.drawdown).filter((item) => Number.isFinite(item))
  const min = Math.min(0, ...(clean.length > 0 ? clean : [0]))
  return { min, max: 0 }
})

const xTicks = computed(() => {
  if (points.value.length <= 1) {
    return []
  }
  const tickCount = Math.min(8, points.value.length)
  const indexes = Array.from({ length: tickCount }, (_, index) => (
    Math.round(((points.value.length - 1) * index) / Math.max(tickCount - 1, 1))
  ))
  return [...new Set(indexes)].map((index) => ({
    index,
    x: xPosition(index),
    label: points.value[index]?.label || '',
  }))
})

const activePoint = computed(() => {
  if (activeIndex.value === null) {
    return null
  }
  return points.value[activeIndex.value] || null
})

function buildRange(values, paddingRatio = 0.08) {
  const clean = values.filter((item) => Number.isFinite(item))
  if (clean.length === 0) {
    return { min: 0, max: 1 }
  }
  const min = Math.min(...clean)
  const max = Math.max(...clean)
  if (min === max) {
    const pad = Math.abs(min || 1) * paddingRatio
    return { min: min - pad, max: max + pad }
  }
  const pad = (max - min) * paddingRatio
  return { min: min - pad, max: max + pad }
}

function panelTop(panelIndex) {
  return padding.top + panelIndex * (panelHeight.value + gap)
}

function xPosition(index) {
  return padding.left + (plotWidth.value * index) / Math.max(points.value.length - 1, 1)
}

function yPosition(value, range, panelIndex) {
  const ratio = (value - range.min) / Math.max(range.max - range.min, 1e-9)
  return panelTop(panelIndex) + panelHeight.value - ratio * panelHeight.value
}

function polyline(accessor, range, panelIndex) {
  return points.value
    .map((item, index) => `${xPosition(index)},${yPosition(accessor(item), range, panelIndex)}`)
    .join(' ')
}

function areaPath(accessor, range, panelIndex, baselineValue = 0) {
  if (points.value.length === 0) {
    return ''
  }
  const baselineY = yPosition(baselineValue, range, panelIndex)
  const firstX = xPosition(0)
  const lastX = xPosition(points.value.length - 1)
  const trace = points.value
    .map((item, index) => `${index === 0 ? 'M' : 'L'} ${xPosition(index)} ${yPosition(accessor(item), range, panelIndex)}`)
    .join(' ')
  return `${trace} L ${lastX} ${baselineY} L ${firstX} ${baselineY} Z`
}

function ticksFor(range, count = 4) {
  const rows = []
  const step = (range.max - range.min) / count
  for (let index = 0; index <= count; index += 1) {
    rows.push(range.max - step * index)
  }
  return rows
}

function formatPercent(value) {
  if (!Number.isFinite(value)) {
    return '--'
  }
  return `${(value * 100).toFixed(2)}%`
}

function handleMouseMove(event) {
  if (!containerRef.value || points.value.length === 0) {
    return
  }
  const svgRect = event.currentTarget.getBoundingClientRect()
  const containerRect = containerRef.value.getBoundingClientRect()
  const x = ((event.clientX - svgRect.left) / Math.max(svgRect.width, 1)) * width
  let nearestIndex = 0
  let nearestDistance = Number.POSITIVE_INFINITY
  points.value.forEach((_item, index) => {
    const distance = Math.abs(xPosition(index) - x)
    if (distance < nearestDistance) {
      nearestDistance = distance
      nearestIndex = index
    }
  })
  activeIndex.value = nearestIndex
  hoverState.visible = true
  hoverState.left = event.clientX - containerRect.left
  hoverState.top = event.clientY - containerRect.top
  hoverState.alignRight = hoverState.left > containerRect.width * 0.7
}

function handleMouseLeave() {
  hoverState.visible = false
  activeIndex.value = null
}
</script>

<template>
  <div ref="containerRef" class="triptych-chart">
    <div class="triptych-chart__legend">
      <span><i class="is-strategy"></i>策略收益</span>
      <span><i class="is-benchmark"></i>基准收益</span>
      <span><i class="is-excess"></i>超额收益</span>
      <span><i class="is-drawdown"></i>回撤</span>
    </div>
    <div
      v-if="hoverState.visible && activePoint"
      class="triptych-chart__tooltip"
      :class="{ 'is-right': hoverState.alignRight }"
      :style="{ left: `${hoverState.left}px`, top: `${hoverState.top}px` }"
    >
      <div class="triptych-chart__tooltip-date">{{ activePoint.label }}</div>
      <div>策略: {{ formatPercent(activePoint.strategy) }}</div>
      <div>基准: {{ formatPercent(activePoint.benchmark) }}</div>
      <div>超额: {{ formatPercent(activePoint.excess) }}</div>
      <div>回撤: {{ formatPercent(activePoint.drawdown) }}</div>
    </div>
    <svg :viewBox="`0 0 ${width} ${totalHeight}`" preserveAspectRatio="none" @mousemove="handleMouseMove" @mouseleave="handleMouseLeave">
      <g v-for="value in ticksFor(returnRange)" :key="`ret-${value}`">
        <line :x1="padding.left" :x2="width - padding.right" :y1="yPosition(value, returnRange, 0)" :y2="yPosition(value, returnRange, 0)" class="triptych-chart__grid" />
        <text :x="padding.left - 12" :y="yPosition(value, returnRange, 0) + 4" class="triptych-chart__axis triptych-chart__axis--left">{{ formatPercent(value) }}</text>
      </g>
      <text :x="padding.left" :y="panelTop(0) - 6" class="triptych-chart__panel-title">策略收益 vs 基准</text>

      <g v-for="value in ticksFor(excessRange)" :key="`excess-${value}`">
        <line :x1="padding.left" :x2="width - padding.right" :y1="yPosition(value, excessRange, 1)" :y2="yPosition(value, excessRange, 1)" class="triptych-chart__grid" />
        <text :x="padding.left - 12" :y="yPosition(value, excessRange, 1) + 4" class="triptych-chart__axis triptych-chart__axis--left">{{ formatPercent(value) }}</text>
      </g>
      <text :x="padding.left" :y="panelTop(1) - 6" class="triptych-chart__panel-title">超额收益</text>

      <g v-for="value in ticksFor(drawdownRange)" :key="`dd-${value}`">
        <line :x1="padding.left" :x2="width - padding.right" :y1="yPosition(value, drawdownRange, 2)" :y2="yPosition(value, drawdownRange, 2)" class="triptych-chart__grid" />
        <text :x="padding.left - 12" :y="yPosition(value, drawdownRange, 2) + 4" class="triptych-chart__axis triptych-chart__axis--left">{{ formatPercent(value) }}</text>
      </g>
      <text :x="padding.left" :y="panelTop(2) - 6" class="triptych-chart__panel-title">回撤</text>

      <line :x1="padding.left" :x2="width - padding.right" :y1="yPosition(0, excessRange, 1)" :y2="yPosition(0, excessRange, 1)" class="triptych-chart__zero" />
      <line :x1="padding.left" :x2="width - padding.right" :y1="yPosition(0, drawdownRange, 2)" :y2="yPosition(0, drawdownRange, 2)" class="triptych-chart__zero" />

      <path :d="areaPath((item) => item.strategy, returnRange, 0, returnRange.min)" class="triptych-chart__area triptych-chart__area--strategy" />
      <polyline :points="polyline((item) => item.strategy, returnRange, 0)" class="triptych-chart__line triptych-chart__line--strategy" />
      <polyline :points="polyline((item) => item.benchmark, returnRange, 0)" class="triptych-chart__line triptych-chart__line--benchmark" />

      <path :d="areaPath((item) => item.excess, excessRange, 1, 0)" class="triptych-chart__area triptych-chart__area--excess" />
      <polyline :points="polyline((item) => item.excess, excessRange, 1)" class="triptych-chart__line triptych-chart__line--excess" />

      <path :d="areaPath((item) => item.drawdown, drawdownRange, 2, 0)" class="triptych-chart__area triptych-chart__area--drawdown" />
      <polyline :points="polyline((item) => item.drawdown, drawdownRange, 2)" class="triptych-chart__line triptych-chart__line--drawdown" />

      <line
        v-if="activeIndex !== null"
        :x1="xPosition(activeIndex)"
        :x2="xPosition(activeIndex)"
        :y1="padding.top"
        :y2="totalHeight - padding.bottom"
        class="triptych-chart__crosshair"
      />

      <g v-for="tick in xTicks" :key="`${tick.index}-${tick.label}`">
        <text :x="tick.x" :y="totalHeight - 10" class="triptych-chart__axis triptych-chart__axis--bottom">{{ tick.label }}</text>
      </g>
    </svg>
  </div>
</template>
