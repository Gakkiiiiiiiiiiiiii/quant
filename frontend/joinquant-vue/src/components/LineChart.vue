<script setup>
import { computed } from 'vue'

const props = defineProps({
  series: {
    type: Array,
    default: () => [],
  },
  height: {
    type: Number,
    default: 320,
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
  return Number(value).toLocaleString('zh-CN', { maximumFractionDigits: 0 })
}
</script>

<template>
  <div class="line-chart">
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
      </g>
    </svg>
  </div>
</template>
