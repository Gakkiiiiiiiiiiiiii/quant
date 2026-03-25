<script setup>
defineProps({
  items: {
    type: Array,
    default: () => [],
  },
})
</script>

<template>
  <div class="metric-strip">
    <article v-for="item in items" :key="item.label" class="metric-card">
      <div class="metric-card__label">{{ item.label }}</div>
      <div class="metric-card__value" :class="item.tone || ''">{{ item.value }}</div>
      <div v-if="item.foot" class="metric-card__foot">{{ item.foot }}</div>
    </article>
  </div>
</template>
