<script setup>
import { computed, ref } from 'vue'

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
})

const width = 1080
const padding = { top: 18, right: 20, bottom: 30, left: 54 }

const visibleSeries = computed(() => props.series.filter((item) => Array.isArray(item.points) && item.points.length > 0))
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
const hoverIndex = ref(-1)
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
    return `${(Number(value) * 100).toFixed(0)}%`
  }
  return Number(value).toLocaleString('zh-CN', { maximumFractionDigits: 0 })
}

function formatTooltipValue(value) {
  if (props.asPercent) {
    return `${(Number(value) * 100).toFixed(2)}%`
  }
  return Number(value).toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

const hoverData = computed(() => {
  if (hoverIndex.value < 0 || visibleSeries.value.length === 0) {
    return null
  }
  const source = visibleSeries.value[0].points
  const index = Math.min(hoverIndex.value, source.length - 1)
  const rows = visibleSeries.value
    .map((item) => {
      const point = item.points[Math.min(index, item.points.length - 1)]
      if (!point) {
        return null
      }
      return {
        name: item.name,
        color: item.color,
        value: Number(point.value),
        text: formatTooltipValue(point.value),
      }
    })
    .filter(Boolean)
  if (rows.length >= 2) {
    const excess = rows[0].value - rows[1].value
    rows.push({
      name: `${rows[0].name}-超额`,
      color: '#0f766e',
      value: excess,
      text: formatTooltipValue(excess),
    })
  }
  return {
    index,
    x: xPosition(index, source.length),
    label: source[index]?.label ?? '',
    rows,
  }
})

function onMouseMove(event) {
  if (visibleSeries.value.length === 0) {
    hoverIndex.value = -1
    return
  }
  const bounds = event.currentTarget.getBoundingClientRect()
  const offsetX = event.clientX - bounds.left
  const ratio = Math.min(Math.max((offsetX / bounds.width), 0), 1)
  const sourceLength = visibleSeries.value[0].points.length
  hoverIndex.value = Math.round(ratio * Math.max(sourceLength - 1, 0))
}

function onMouseLeave() {
  hoverIndex.value = -1
}
</script>

<template>
  <div class="line-chart" @mousemove="onMouseMove" @mouseleave="onMouseLeave">
    <div class="line-chart__legend">
      <span v-for="item in visibleSeries" :key="item.name" class="line-chart__legend-item">
        <i :style="{ background: item.color }"></i>
        {{ item.name }}
      </span>
    </div>
    <svg :viewBox="`0 0 ${width} ${height}`" preserveAspectRatio="none">
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
        <line
          v-if="hoverData"
          :x1="hoverData.x"
          :x2="hoverData.x"
          :y1="padding.top"
          :y2="height - padding.bottom"
          class="line-chart__crosshair"
        />
        <template v-if="hoverData">
          <circle
            v-for="item in visibleSeries"
            :key="`${item.name}-dot-${hoverData.index}`"
            :cx="hoverData.x"
            :cy="yPosition(Number(item.points[Math.min(hoverData.index, item.points.length - 1)]?.value || 0))"
            r="4.5"
            :fill="item.color"
            class="line-chart__dot"
          />
        </template>
      </g>
    </svg>
    <div v-if="hoverData" class="line-chart__tooltip" :style="{ left: `${(hoverData.x / width) * 100}%` }">
      <div class="line-chart__tooltip-title">{{ hoverData.label }}</div>
      <div v-for="row in hoverData.rows" :key="row.name" class="line-chart__tooltip-row">
        <span class="line-chart__tooltip-dot" :style="{ background: row.color }"></span>
        <span>{{ row.name }}</span>
        <strong>{{ row.text }}</strong>
      </div>
    </div>
  </div>
</template>
