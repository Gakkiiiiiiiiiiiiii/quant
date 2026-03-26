<script setup>
import { computed, reactive, ref } from 'vue'

const props = defineProps({
  series: {
    type: Array,
    default: () => [],
  },
  height: {
    type: Number,
    default: 320,
  },
  asPercent: {
    type: Boolean,
    default: false,
  },
  benchmarkName: {
    type: String,
    default: 'Benchmark',
  },
  excessLabel: {
    type: String,
    default: '超额收益',
  },
})

const width = 1080
const padding = { top: 18, right: 20, bottom: 30, left: 54 }
const containerRef = ref(null)
const activeIndex = ref(null)
const hoverState = reactive({
  visible: false,
  left: 0,
  top: 0,
  alignRight: false,
  dockBottom: false,
})

const visibleSeries = computed(() => props.series.filter((item) => Array.isArray(item.points) && item.points.length > 0))
const referenceSeries = computed(() => visibleSeries.value[0] ?? null)
const allValues = computed(() => visibleSeries.value.flatMap((item) => item.points.map((point) => Number(point.value)).filter((value) => Number.isFinite(value))))
const yRange = computed(() => {
  if (allValues.value.length === 0) {
    return { min: 0, max: 1 }
  }
  const min = Math.min(...allValues.value)
  const max = Math.max(...allValues.value)
  if (min === max) {
    return { min: min - 1, max: max + 1 }
  }
  const paddingValue = (max - min) * 0.08
  return { min: min - paddingValue, max: max + paddingValue }
})
const longestLength = computed(() => Math.max(0, ...visibleSeries.value.map((item) => item.points.length)))
const plotWidth = computed(() => width - padding.left - padding.right)
const plotHeight = computed(() => props.height - padding.top - padding.bottom)
const yTicks = computed(() => {
  const ticks = []
  const step = (yRange.value.max - yRange.value.min) / 4
  for (let index = 0; index < 5; index += 1) {
    const value = yRange.value.max - step * index
    const y = padding.top + (plotHeight.value / 4) * index
    ticks.push({ value, y })
  }
  return ticks
})
const xTicks = computed(() => {
  if (longestLength.value <= 1 || visibleSeries.value.length === 0) {
    return []
  }
  const source = visibleSeries.value[0].points
  const rawIndexes = [0, Math.floor((source.length - 1) / 2), source.length - 1]
  return rawIndexes.map((index) => ({
    x: padding.left + (plotWidth.value * index) / Math.max(source.length - 1, 1),
    label: source[index]?.label ?? '',
  }))
})
const zeroLineY = computed(() => {
  if (yRange.value.min > 0 || yRange.value.max < 0) {
    return null
  }
  return yPosition(0)
})
const plottedSeries = computed(() => visibleSeries.value.map((item) => ({
  ...item,
  points: item.points.map((point, index) => ({
    ...point,
    numericValue: Number(point.value),
    x: xPosition(index, item.points.length),
    y: yPosition(Number(point.value)),
  })),
})))
const activeLabel = computed(() => {
  const series = plottedSeries.value[0]
  if (!series || activeIndex.value === null) {
    return ''
  }
  const point = series.points[Math.min(activeIndex.value, series.points.length - 1)]
  return point?.label ?? ''
})
const activePoints = computed(() => {
  if (activeIndex.value === null || !activeLabel.value) {
    return []
  }
  return plottedSeries.value
    .map((item) => {
      const point = item.points.find((candidate) => candidate.label === activeLabel.value)
        ?? item.points[Math.min(activeIndex.value, item.points.length - 1)]
      if (!point) {
        return null
      }
      return {
        ...point,
        name: item.name,
        color: item.color,
      }
    })
    .filter(Boolean)
})
const primaryActivePoint = computed(() => activePoints.value.find((item) => item.name !== props.benchmarkName) ?? activePoints.value[0] ?? null)
const benchmarkActivePoint = computed(() => activePoints.value.find((item) => item.name === props.benchmarkName) ?? null)
const excessValue = computed(() => {
  if (!primaryActivePoint.value || !benchmarkActivePoint.value) {
    return null
  }
  return primaryActivePoint.value.numericValue - benchmarkActivePoint.value.numericValue
})

function yPosition(value) {
  const ratio = (value - yRange.value.min) / (yRange.value.max - yRange.value.min)
  return padding.top + plotHeight.value - ratio * plotHeight.value
}

function xPosition(index, length) {
  return padding.left + (plotWidth.value * index) / Math.max(length - 1, 1)
}

function buildPolyline(points) {
  if (!points || points.length === 0) {
    return ''
  }
  return points
    .map((point, index) => `${xPosition(index, points.length)},${yPosition(Number(point.value))}`)
    .join(' ')
}

function formatAxis(value) {
  if (props.asPercent) {
    return `${(Number(value) * 100).toFixed(1)}%`
  }
  return Number(value).toLocaleString('zh-CN', { maximumFractionDigits: 0 })
}

function formatValue(value) {
  if (!Number.isFinite(Number(value))) {
    return '--'
  }
  if (props.asPercent) {
    return `${(Number(value) * 100).toFixed(2)}%`
  }
  return Number(value).toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

function handleMouseMove(event) {
  const series = referenceSeries.value
  if (!series || series.points.length === 0 || !containerRef.value) {
    return
  }
  const svgRect = event.currentTarget.getBoundingClientRect()
  const containerRect = containerRef.value.getBoundingClientRect()
  const x = ((event.clientX - svgRect.left) / Math.max(svgRect.width, 1)) * width

  let nearestIndex = 0
  let nearestDistance = Number.POSITIVE_INFINITY
  series.points.forEach((point, index) => {
    const distance = Math.abs(xPosition(index, series.points.length) - x)
    if (distance < nearestDistance) {
      nearestDistance = distance
      nearestIndex = index
    }
  })

  activeIndex.value = nearestIndex
  hoverState.visible = true
  hoverState.left = event.clientX - containerRect.left
  hoverState.top = event.clientY - containerRect.top
  hoverState.alignRight = hoverState.left > containerRect.width * 0.68
  hoverState.dockBottom = hoverState.top < 110
}

function handleMouseLeave() {
  hoverState.visible = false
  activeIndex.value = null
}
</script>

<template>
  <div ref="containerRef" class="line-chart">
    <div class="line-chart__legend">
      <span v-for="item in visibleSeries" :key="item.name" class="line-chart__legend-item">
        <i :style="{ background: item.color }"></i>
        {{ item.name }}
      </span>
    </div>
    <div
      v-if="hoverState.visible && activePoints.length > 0"
      class="line-chart__tooltip"
      :class="{ 'is-right': hoverState.alignRight, 'is-bottom': hoverState.dockBottom }"
      :style="{ left: `${hoverState.left}px`, top: `${hoverState.top}px` }"
    >
      <div class="line-chart__tooltip-date">{{ activeLabel }}</div>
      <div v-for="point in activePoints" :key="`${point.name}-${point.label}`" class="line-chart__tooltip-row">
        <span class="line-chart__tooltip-name">
          <i class="line-chart__tooltip-swatch" :style="{ background: point.color }"></i>
          {{ point.name }}
        </span>
        <strong class="line-chart__tooltip-value">{{ formatValue(point.numericValue) }}</strong>
      </div>
      <div v-if="excessValue !== null" class="line-chart__tooltip-row line-chart__tooltip-row--excess">
        <span>{{ excessLabel }}</span>
        <strong class="line-chart__tooltip-value">{{ formatValue(excessValue) }}</strong>
      </div>
    </div>
    <svg :viewBox="`0 0 ${width} ${height}`" preserveAspectRatio="none" @mousemove="handleMouseMove" @mouseleave="handleMouseLeave">
      <g>
        <line
          v-for="tick in yTicks"
          :key="tick.y"
          :x1="padding.left"
          :x2="width - padding.right"
          :y1="tick.y"
          :y2="tick.y"
          class="line-chart__grid"
        />
        <line
          v-if="zeroLineY !== null"
          :x1="padding.left"
          :x2="width - padding.right"
          :y1="zeroLineY"
          :y2="zeroLineY"
          class="line-chart__zero"
        />
        <text v-for="tick in yTicks" :key="`${tick.y}-label`" :x="padding.left - 10" :y="tick.y + 4" class="line-chart__axis line-chart__axis--left">
          {{ formatAxis(tick.value) }}
        </text>
        <text v-for="tick in xTicks" :key="`${tick.x}-label`" :x="tick.x" :y="height - 8" class="line-chart__axis line-chart__axis--bottom">
          {{ tick.label }}
        </text>
        <polyline
          v-for="item in visibleSeries"
          :key="item.name"
          :points="buildPolyline(item.points)"
          fill="none"
          :stroke="item.color"
          stroke-width="3"
          stroke-linecap="round"
          stroke-linejoin="round"
        />
        <template v-if="hoverState.visible && primaryActivePoint">
          <line
            :x1="primaryActivePoint.x"
            :x2="primaryActivePoint.x"
            :y1="padding.top"
            :y2="height - padding.bottom"
            class="line-chart__crosshair line-chart__crosshair--vertical"
          />
          <line
            :x1="padding.left"
            :x2="width - padding.right"
            :y1="primaryActivePoint.y"
            :y2="primaryActivePoint.y"
            class="line-chart__crosshair line-chart__crosshair--horizontal"
          />
          <circle
            v-for="point in activePoints"
            :key="`${point.name}-${point.label}-marker`"
            :cx="point.x"
            :cy="point.y"
            r="6"
            class="line-chart__point"
            :style="{ stroke: point.color }"
          />
        </template>
        <rect
          :x="padding.left"
          :y="padding.top"
          :width="plotWidth"
          :height="plotHeight"
          fill="transparent"
          class="line-chart__hitbox"
        />
      </g>
    </svg>
  </div>
</template>
