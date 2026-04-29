<template>
  <el-card class="quant-card" shadow="never">
    <template #header>
      <div class="card-head">
        <div>
          <p class="eyebrow">{{ eyebrow }}</p>
          <h2>{{ title }}</h2>
        </div>
        <el-tag effect="dark" class="count-tag">{{ displayRows.length }} 条</el-tag>
      </div>
    </template>

    <el-tabs v-if="useMonths && months.length" v-model="selectedMonth" class="month-tabs">
      <el-tab-pane v-for="month in months" :key="month" :name="month" :label="`${month} (${monthGroups[month]?.length || 0})`" />
    </el-tabs>

    <el-table
      :data="displayRows"
      class="selection-table"
      :height="tableHeight"
      row-key="id"
      empty-text="暂无记录"
    >
      <el-table-column prop="selection_date" label="日期" width="112" fixed />
      <el-table-column label="股票" min-width="190">
        <template #default="{ row }">
          <div class="stock-cell">
            <StockLink :code="row.code" :name="row.name" :label="row.code" mono class="stock-code-link" />
            <StockLink :code="row.code" :name="row.name" :label="row.name" class="stock-name-link" />
            <Sparkline :row="row" />
          </div>
        </template>
      </el-table-column>
      <el-table-column label="策略" min-width="150">
        <template #default="{ row }">
          <span :class="strategyClass(row.strategy_type)">{{ strategyLabel(row.strategy_type) }}</span>
        </template>
      </el-table-column>
      <el-table-column :label="primaryHeader" align="right" min-width="150">
        <template #default="{ row }">
          <strong :class="numberClass(primaryValue(row))">{{ primaryText(row) }}</strong>
        </template>
      </el-table-column>
      <el-table-column :label="secondaryHeader" align="right" min-width="150">
        <template #default="{ row }">
          <span :class="numberClass(secondaryValue(row))">{{ secondaryText(row) }}</span>
        </template>
      </el-table-column>
      <el-table-column label="14:50 快照" align="right" min-width="130">
        <template #default="{ row }">
          <div class="stacked">
            <small>{{ row.snapshot_time || '14:50' }}</small>
            <strong>{{ money(row.snapshot_price ?? row.selection_price ?? row.price) }}</strong>
          </div>
        </template>
      </el-table-column>
      <el-table-column label="卖出/观察" min-width="150">
        <template #default="{ row }">
          {{ exitText(row) }}
        </template>
      </el-table-column>
      <el-table-column label="结算价 / 收益" align="right" min-width="150">
        <template #default="{ row }">
          <div v-if="row.is_closed" class="stacked">
            <small>{{ row.close_reason || row.close_date || '已结清' }}</small>
            <strong :class="numberClass(resultValue(row))">
              {{ money(row.close_price) }} / {{ pct(resultValue(row)) }}
            </strong>
          </div>
          <span v-else class="muted-inline">{{ isSwingStrategy(row) ? '持仓观察' : '待结算' }}</span>
        </template>
      </el-table-column>
      <el-table-column label="状态" width="118">
        <template #default="{ row }">
          <span :class="stateClass(row)">{{ stateText(row) }}</span>
        </template>
      </el-table-column>
      <el-table-column v-if="showInspect" label="风控" width="112" fixed="right">
        <template #default="{ row }">
          <el-button link type="primary" @click="$emit('inspect', row)">Ollama</el-button>
        </template>
      </el-table-column>
    </el-table>
  </el-card>
</template>

<script setup>
import { computed, defineComponent, h, ref, watch } from 'vue'
import StockLink from './StockLink.vue'

const props = defineProps({
  rows: { type: Array, default: () => [] },
  title: { type: String, required: true },
  eyebrow: { type: String, default: 'Selection Table' },
  mode: { type: String, default: 'all' },
  useMonths: { type: Boolean, default: false },
  tableHeight: { type: [Number, String], default: 520 },
  showInspect: { type: Boolean, default: false },
})

defineEmits(['inspect'])

const selectedMonth = ref('')

const isSwingStrategy = (rowOrStrategy) => {
  const strategy = typeof rowOrStrategy === 'string' ? rowOrStrategy : rowOrStrategy?.strategy_type
  return strategy === '中线超跌反转' || strategy === '右侧主升浪'
}

const filteredRows = computed(() => {
  if (props.mode === 'swing') return props.rows.filter((row) => isSwingStrategy(row))
  if (props.mode === 'short') return props.rows.filter((row) => !isSwingStrategy(row))
  return props.rows
})

const monthGroups = computed(() => {
  const groups = {}
  for (const row of filteredRows.value) {
    const month = String(row.selection_date || row.date || '').slice(0, 7)
    if (!month) continue
    if (!groups[month]) groups[month] = []
    groups[month].push(row)
  }
  return groups
})

const months = computed(() => Object.keys(monthGroups.value).sort().reverse())

watch(months, (value) => {
  if (!props.useMonths) return
  if (!selectedMonth.value || !value.includes(selectedMonth.value)) {
    selectedMonth.value = value[0] || ''
  }
}, { immediate: true })

const displayRows = computed(() => {
  if (!props.useMonths) return filteredRows.value
  return monthGroups.value[selectedMonth.value] || []
})

const hasOnlySwing = computed(() => displayRows.value.length > 0 && displayRows.value.every((row) => isSwingStrategy(row)))
const hasOnlyShort = computed(() => displayRows.value.length > 0 && displayRows.value.every((row) => !isSwingStrategy(row)))
const primaryHeader = computed(() => hasOnlySwing.value ? '预测涨幅' : hasOnlyShort.value ? '置信度' : '置信度 / 预测涨幅')
const secondaryHeader = computed(() => hasOnlySwing.value ? '结算收益 / 波段状态' : hasOnlyShort.value ? '预期溢价 / 结算收益' : '预期溢价 / 结算收益')

const toNum = (value) => {
  if (value === null || value === undefined || value === '') return null
  const num = Number(value)
  return Number.isFinite(num) ? num : null
}
const pct = (value) => {
  const num = toNum(value)
  return num === null ? '-' : `${num.toFixed(2)}%`
}
const money = (value) => {
  const num = toNum(value)
  return num === null ? '-' : num.toFixed(2)
}
const expected = (row) => toNum(row.expected_t3_max_gain_pct ?? row.expected_premium ?? row.predicted_open_premium ?? row.composite_score)
const resultValue = (row) => {
  const closedReturn = toNum(row.close_return_pct)
  if (row.is_closed && closedReturn !== null) return closedReturn
  return toNum(isSwingStrategy(row) ? row.t3_max_gain_pct : row.open_premium)
}
const primaryValue = (row) => isSwingStrategy(row) ? expected(row) : toNum(row.composite_score ?? row.win_rate)
const primaryText = (row) => isSwingStrategy(row) ? pct(expected(row)) : `${(primaryValue(row) ?? 0).toFixed(2)} 分`
const secondaryValue = (row) => {
  if (row.is_closed) return resultValue(row)
  return isSwingStrategy(row) ? null : toNum(row.expected_premium ?? row.predicted_open_premium)
}
const secondaryText = (row) => {
  if (row.is_closed) return pct(resultValue(row))
  return isSwingStrategy(row) ? 'T+3 持仓观察' : pct(row.expected_premium ?? row.predicted_open_premium)
}
const numberClass = (value) => {
  const num = toNum(value)
  if (num === null || num === 0) return ''
  return num > 0 ? 'rise' : 'fall'
}
const exitText = (row) => {
  if (isSwingStrategy(row)) return row.close_date || row.target_date || 'T+3 观察期'
  return row.close_date || row.target_date || row.next_date || 'T+1 开盘'
}
const stateText = (row) => {
  if (row.is_closed) return '已结清'
  if (row.status === 'pending_open') return '待开盘'
  if (isSwingStrategy(row)) return '波段持仓'
  return '待结算'
}
const stateClass = (row) => [
  'state-badge',
  row.is_closed ? 'state-closed' : row.status === 'pending_open' ? 'state-pending' : 'state-holding',
]
const strategyLabel = (strategy) => {
  if (strategy === '右侧主升浪') return '顺势主升浪'
  if (strategy === '中线超跌反转') return '中线超跌反转'
  if (strategy === '首阴低吸') return '低吸影子'
  return '尾盘突破'
}
const strategyClass = (strategy) => [
  'strategy-badge',
  strategy === '右侧主升浪'
    ? 'strategy-main'
    : strategy === '中线超跌反转'
      ? 'strategy-reversal'
      : strategy === '首阴低吸'
        ? 'strategy-dip'
        : 'strategy-breakout',
]

const sparkValues = (row) => {
  const raw = row.raw?.winner?.trend_features || row.trend_features || {}
  const r3 = toNum(raw.return_3d) ?? 0
  const r5 = toNum(raw.return_5d) ?? 0
  const b5 = toNum(raw.bias_5d) ?? 0
  const b10 = toNum(raw.bias_10d) ?? 0
  return [0, r3 * 0.4, r3, r5, Math.max(r5, b5 * 0.6, b10 * 0.5)]
}

const Sparkline = defineComponent({
  name: 'Sparkline',
  props: { row: { type: Object, required: true } },
  setup(sparkProps) {
    return () => {
      const values = sparkValues(sparkProps.row)
      const min = Math.min(...values)
      const max = Math.max(...values)
      const span = max - min || 1
      const points = values.map((value, index) => {
        const x = 4 + index * 18
        const y = 24 - ((value - min) / span) * 18
        return `${x},${y}`
      }).join(' ')
      const positive = values[values.length - 1] >= values[0]
      return h('svg', { class: ['sparkline', positive ? 'spark-rise' : 'spark-fall'], viewBox: '0 0 80 28', role: 'img' }, [
        h('polyline', { points, fill: 'none', 'stroke-width': '2.2', 'stroke-linecap': 'round', 'stroke-linejoin': 'round' }),
      ])
    }
  },
})
</script>

<style scoped>
.quant-card {
  --el-card-bg-color: var(--terminal-card);
  --el-card-border-color: var(--terminal-border);
  --el-text-color-primary: var(--terminal-text);
  --el-text-color-regular: #a8b3c7;
  height: 100%;
  border-radius: 14px;
}

.card-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 12px;
}

.eyebrow {
  margin: 0 0 4px;
  color: #6f7d95;
  font-size: 0.72rem;
  font-weight: 900;
  text-transform: uppercase;
}

h2 {
  margin: 0;
  color: #f2f6ff;
  font-size: 1rem;
}

.count-tag {
  border-color: #273246;
  background: #1f2937;
}

.month-tabs {
  margin-top: -8px;
}

.selection-table {
  --el-table-bg-color: var(--terminal-card);
  --el-table-tr-bg-color: var(--terminal-card);
  --el-table-header-bg-color: var(--terminal-bg);
  --el-table-row-hover-bg-color: #1b2230;
  --el-table-border-color: rgba(255, 255, 255, 0.07);
  --el-table-text-color: #d7deeb;
  --el-table-header-text-color: #7f8aa1;
  width: 100%;
}

.stock-cell {
  display: grid;
  grid-template-columns: auto minmax(44px, 1fr) 80px;
  gap: 8px;
  align-items: center;
  min-width: 0;
}

.stock-code-link {
  color: #f2f6ff;
  font-weight: 900;
}

.stock-name-link {
  min-width: 0;
  color: #d7deeb;
  font-weight: 800;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mono {
  font-family: 'SF Mono', Menlo, Consolas, monospace;
}

.sparkline {
  width: 80px;
  height: 28px;
}

.sparkline polyline {
  stroke: currentColor;
}

.spark-rise {
  color: var(--quant-neutral);
}

.spark-fall {
  color: var(--quant-fall);
}

.strategy-badge,
.state-badge {
  display: inline-flex;
  align-items: center;
  min-height: 23px;
  border-radius: 999px;
  padding: 2px 8px;
  font-size: 0.72rem;
  font-weight: 900;
  white-space: nowrap;
}

.strategy-breakout {
  border: 1px solid rgba(24, 144, 255, 0.42);
  background: rgba(24, 144, 255, 0.15);
  color: #69b1ff;
}

.strategy-reversal {
  border: 1px solid rgba(245, 34, 45, 0.45);
  background: rgba(245, 34, 45, 0.15);
  color: #ff7875;
}

.strategy-main {
  border: 1px solid rgba(114, 46, 209, 0.48);
  background: rgba(114, 46, 209, 0.2);
  color: #b37feb;
}

.strategy-dip {
  border: 1px solid rgba(250, 140, 22, 0.45);
  background: rgba(250, 140, 22, 0.16);
  color: #ffc069;
}

.state-pending {
  border: 1px solid rgba(250, 173, 20, 0.45);
  background: rgba(250, 173, 20, 0.14);
  color: #ffd666;
}

.state-holding {
  border: 1px solid rgba(24, 144, 255, 0.42);
  background: rgba(24, 144, 255, 0.15);
  color: #69b1ff;
}

.state-closed {
  border: 1px solid rgba(82, 196, 26, 0.45);
  background: rgba(82, 196, 26, 0.14);
  color: #95de64;
}

.stacked {
  display: inline-flex;
  flex-direction: column;
  align-items: flex-end;
  gap: 2px;
}

.stacked small {
  color: #6f7d95;
  font-size: 0.7rem;
}

.muted-inline {
  color: #6f7d95;
  font-size: 0.78rem;
  font-weight: 800;
}

.rise {
  color: var(--quant-rise);
}

.fall {
  color: var(--quant-fall);
}
</style>
