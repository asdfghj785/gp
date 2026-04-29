<template>
  <button
    type="button"
    :class="['stock-link', { mono, block }]"
    :title="titleText"
    :disabled="cleanCode.length !== 6"
    @click.stop.prevent="openStock"
  >
    <slot>{{ displayText }}</slot>
  </button>
</template>

<script setup>
import { computed, inject } from 'vue'

const props = defineProps({
  code: { type: [String, Number], required: true },
  name: { type: String, default: '' },
  label: { type: [String, Number], default: '' },
  mono: { type: Boolean, default: false },
  block: { type: Boolean, default: false },
})

const openStockMarket = inject('openStockMarket', null)

const cleanCode = computed(() => String(props.code || '').replace(/\D/g, '').slice(-6))
const displayText = computed(() => String(props.label || props.name || cleanCode.value || '-'))
const titleText = computed(() => {
  if (cleanCode.value.length !== 6) return '股票代码不可用'
  const displayName = props.name && props.name !== cleanCode.value ? `${props.name} ` : ''
  return `查看 ${displayName}${cleanCode.value} 行情库`
})

const openStock = () => {
  if (cleanCode.value.length !== 6) return
  const payload = {
    code: cleanCode.value,
    name: props.name || displayText.value,
  }
  if (typeof openStockMarket === 'function') {
    openStockMarket(payload)
    return
  }
  window.dispatchEvent(new CustomEvent('quant:open-stock-market', { detail: payload }))
}
</script>

<style scoped>
.stock-link {
  display: inline-flex;
  min-width: 0;
  max-width: 100%;
  align-items: center;
  border: 0;
  padding: 0;
  background: transparent;
  color: inherit;
  font: inherit;
  font-weight: inherit;
  letter-spacing: 0;
  text-align: left;
  cursor: pointer;
}

.stock-link.block {
  display: flex;
  width: fit-content;
}

.stock-link.mono {
  font-family: 'SF Mono', Menlo, Consolas, monospace;
}

.stock-link:hover,
.stock-link:focus-visible {
  color: #69b1ff;
}

.stock-link:focus-visible {
  outline: 2px solid rgba(105, 177, 255, 0.72);
  outline-offset: 3px;
}

.stock-link:disabled {
  color: inherit;
  cursor: default;
  opacity: 0.65;
}
</style>
