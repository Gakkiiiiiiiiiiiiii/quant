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

function formatCell(value) {
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
</script>

<template>
  <div class="data-table">
    <div v-if="rows.length === 0" class="data-table__empty">{{ emptyText }}</div>
    <div v-else class="data-table__scroll" :style="{ maxHeight: `${maxHeight}px` }">
      <table>
        <thead>
          <tr>
            <th v-for="column in normalizedColumns" :key="column.key">{{ column.label }}</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(row, index) in rows" :key="index">
            <td v-for="column in normalizedColumns" :key="column.key">{{ formatCell(row[column.key]) }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
