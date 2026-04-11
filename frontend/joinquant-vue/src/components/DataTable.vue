<script setup>
import { computed } from 'vue'

const props = defineProps({
  rows: {
    type: Array,
    default: () => [],
  },
  columns: {
    type: Array,
    default: () => [],
  },
  emptyText: {
    type: String,
    default: '暂无数据',
  },
  maxHeight: {
    type: Number,
    default: 420,
  },
})

const normalizedColumns = computed(() => {
  if (props.columns.length > 0) {
    return props.columns.map((item) => (typeof item === 'string' ? { key: item, label: item } : item))
  }
  const first = props.rows[0] || {}
  return Object.keys(first).map((key) => ({ key, label: key }))
})

const stickyOffsets = computed(() => {
  let left = 0
  return normalizedColumns.value.map((column) => {
    if (!column.sticky) {
      return null
    }
    const current = left
    left += Number(column.width || column.minWidth || 140)
    return current
  })
})

function rawNumber(value) {
  if (value === null || value === undefined || value === '') {
    return null
  }
  const num = Number(value)
  return Number.isFinite(num) ? num : null
}

function formatCell(value, column, row) {
  if (typeof column?.formatter === 'function') {
    return column.formatter(value, row, column)
  }
  if (value === null || value === undefined || value === '') {
    return '--'
  }
  if (typeof value === 'number') {
    if (Math.abs(value) >= 1000) {
      return value.toLocaleString('zh-CN', { maximumFractionDigits: 2 })
    }
    return value.toLocaleString('zh-CN', { maximumFractionDigits: 4 })
  }
  if (typeof value === 'string' && value.includes('T')) {
    return value.replace('T', ' ').replace('.000Z', '')
  }
  return String(value)
}

function cellClasses(column, row) {
  const classes = []
  if (column.align) {
    classes.push(`is-${column.align}`)
  }
  if (column.wrap) {
    classes.push('is-wrap')
  }
  if (column.code) {
    classes.push('is-code')
  }
  if (column.sticky) {
    classes.push('is-sticky')
  }
  const number = rawNumber(row?.[column.key])
  if (column.tone === 'pnl' || column.tone === 'return') {
    if (number !== null && number > 0) {
      classes.push('is-positive')
    } else if (number !== null && number < 0) {
      classes.push('is-negative')
    }
  }
  return classes
}

function cellStyle(column, index) {
  const style = {}
  if (column.width) {
    style.width = `${column.width}px`
    style.minWidth = `${column.width}px`
  }
  if (column.minWidth) {
    style.minWidth = `${column.minWidth}px`
  }
  if (column.maxWidth) {
    style.maxWidth = `${column.maxWidth}px`
  }
  if (column.sticky) {
    style.left = `${stickyOffsets.value[index] || 0}px`
  }
  return style
}
</script>

<template>
  <div class="data-table">
    <div v-if="rows.length === 0" class="data-table__empty">{{ emptyText }}</div>
    <div v-else class="data-table__scroll" :style="{ maxHeight: `${maxHeight}px` }">
      <table>
        <thead>
          <tr>
            <th
              v-for="(column, index) in normalizedColumns"
              :key="column.key"
              :class="cellClasses(column, {})"
              :style="cellStyle(column, index)"
            >
              {{ column.label }}
            </th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(row, index) in rows" :key="index">
            <td
              v-for="(column, columnIndex) in normalizedColumns"
              :key="column.key"
              :class="cellClasses(column, row)"
              :style="cellStyle(column, columnIndex)"
              :title="formatCell(row[column.key], column, row)"
            >
              {{ formatCell(row[column.key], column, row) }}
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
