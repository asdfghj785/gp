<template>
  <section class="v3-shell">
    <header class="v3-status">
      <div class="brand-block">
        <div class="brand-icon">Q</div>
        <div>
          <p class="eyebrow">V3.2 Command Center</p>
          <h1>量化指挥中心</h1>
          <small>Global XGBoost · Top 5 锁定 · Qwen Agent</small>
        </div>
      </div>
      <div class="status-grid">
        <article>
          <span>当前时间</span>
          <strong class="mono">{{ clockText }}</strong>
        </article>
        <article :class="{ online: systemStatus.data_pool?.latest_date }">
          <span>冷数据底座</span>
          <strong>{{ systemStatus.data_pool?.latest_date || 'Checking' }}</strong>
        </article>
        <article :class="{ online: systemStatus.xgboost?.ready }">
          <span>XGBoost 引擎</span>
          <strong>{{ xgbStatusText }}</strong>
        </article>
        <article :class="{ online: systemStatus.ollama?.ready }">
          <span>Qwen 本地大模型</span>
          <strong>{{ systemStatus.ollama?.ready ? 'Ready' : 'Watch' }}</strong>
        </article>
      </div>
    </header>

    <section class="v3-grid">
      <el-card class="v3-card sniper-card" shadow="never">
        <template #header>
          <div class="card-head">
            <div>
              <p class="eyebrow">Live Sniper Radar</p>
              <h2>14:50 狙击雷达</h2>
            </div>
            <div class="radar-actions">
              <span class="threshold-chip">Top {{ signalsPayload.top_k || 5 }}</span>
              <span class="elapsed-chip">本次雷达扫描耗时：{{ scanElapsedText }}</span>
              <span :class="['lock-chip', signalsPayload.locked ? 'locked' : 'waiting']">{{ lockStateText }}</span>
            </div>
          </div>
        </template>

        <div v-if="signals.length === 0" class="v3-empty">
          <strong>当前暂无 Top 5 雷达结果</strong>
          <span>系统会在交易日 14:50 自动锁定全市场 Top 5 并推送，锁定后不再刷新或改榜。</span>
        </div>

        <div v-else class="signal-table">
          <div class="signal-row signal-head">
            <span>标的</span>
            <span>XGBoost 暴涨概率</span>
            <span>实时涨幅</span>
            <span>买卖压差极限</span>
            <span>动作</span>
          </div>
          <div v-for="row in displaySignals" :key="row.code" :class="['signal-row', { elite: row.is_elite }]">
            <div>
              <StockLink :code="row.code" :name="row.name" :label="row.name" block class="v3-stock-name" />
              <small>
                <StockLink :code="row.code" :name="row.name" :label="row.code" mono class="v3-stock-code" />
                · {{ row.strategy_type }}
              </small>
            </div>
            <div>
              <b class="rise">{{ row.probability_pct?.toFixed?.(2) || row.probability_pct }}%</b>
              <span v-if="row.is_elite" class="elite-chip">极品 &gt;0.90</span>
              <i class="prob-track"><em :style="{ width: `${Math.min(100, row.probability_pct || 0)}%` }" /></i>
            </div>
            <div :class="numberClass(row.pct_chg)">{{ signedPct(row.pct_chg) }}</div>
            <div class="mono neutral">{{ Number(row.pressure_factor || 0).toFixed(4) }}</div>
            <div class="row-actions">
              <el-button size="small" type="primary" :loading="agentBusyCode === row.code" @click="wakeAgent(row)">
                {{ agentBusyCode === row.code ? '分析中...' : '唤醒 AI' }}
              </el-button>
              <span class="row-lock">已锁定</span>
            </div>
          </div>
        </div>
      </el-card>

      <el-card class="v3-card agent-card" shadow="never">
        <template #header>
          <div class="card-head">
            <div>
              <p class="eyebrow">Qwen Agent Hub</p>
              <h2>AI 右脑舆情控制台</h2>
            </div>
            <span :class="['agent-state', systemStatus.ollama?.ready ? 'ready' : 'watch']">
              {{ systemStatus.ollama?.model || 'qwen2.5' }}
            </span>
          </div>
        </template>
        <div class="terminal-log">
          <p v-for="(line, idx) in terminalLines" :key="idx">
            <span>{{ line.time }}</span>{{ line.text }}
          </p>
          <article v-for="report in agentReports" :key="report.id" class="agent-report">
            <header>
              <span>{{ report.time }}</span>
              <StockLink :code="report.code" :name="report.name" :label="report.name" class="agent-stock-link" />
              (<StockLink :code="report.code" :name="report.name" :label="report.code" mono class="agent-stock-link" />) 风险排查报告
            </header>
            <pre class="agent-markdown">{{ report.markdown }}</pre>
          </article>
        </div>
      </el-card>
    </section>

    <el-card class="v3-card history-card" shadow="never">
      <template #header>
        <div class="card-head">
          <div>
            <p class="eyebrow">Immutable History</p>
            <h2>历史锁定 Top 5</h2>
          </div>
          <span class="elapsed-chip">每日 14:50 定榜 · T+1/T+2/T+3 收盘追踪</span>
        </div>
      </template>

      <div v-if="historyRows.length === 0" class="v3-empty">
        <strong>暂无历史锁定记录</strong>
        <span>完成首个交易日 14:50 自动锁定后，这里会展示每日 5 支候选及后续三日收盘结果。</span>
      </div>

      <el-tabs v-else v-model="activeHistoryMonth" class="history-tabs">
        <el-tab-pane
          v-for="month in historyMonths"
          :key="month"
          :label="`${month} (${historyMonthGroups[month]?.length || 0})`"
          :name="month"
        >
          <div class="history-month">
            <section v-for="day in historyMonthGroups[month]" :key="day.selection_date" class="history-day-block">
              <div class="history-day-head">
                <strong>{{ day.selection_date }}</strong>
                <div class="history-meta">
                  <span>锁定时间：{{ day.locked_at || '-' }}</span>
                  <span>行情源：{{ day.live_source || '-' }}</span>
                  <span>席位：Top {{ day.top_k || 5 }}</span>
                </div>
              </div>

              <div class="history-table">
                <div class="history-row history-head">
                  <span>标的</span>
                  <span>锁定价 / 概率</span>
                  <span>T+1</span>
                  <span>T+2</span>
                  <span>T+3</span>
                </div>
                <div v-for="stock in day.stocks" :key="`${day.selection_date}-${stock.code}`" class="history-row">
                  <div>
                    <StockLink :code="stock.code" :name="stock.name" :label="stock.name" block class="v3-stock-name" />
                    <small>
                      <StockLink :code="stock.code" :name="stock.name" :label="stock.code" mono class="v3-stock-code" />
                      · {{ stock.strategy_type }}
                    </small>
                  </div>
                  <div>
                    <strong class="mono">{{ priceText(stock.locked_price) }}</strong>
                    <small class="rise">{{ pctText(stock.probability_pct) }}</small>
                  </div>
                  <div v-for="item in stock.t_days" :key="item.label" class="tday-cell">
                    <strong :class="numberClass(item.return_pct)">{{ tDayReturnText(item) }}</strong>
                    <small>{{ tDayCloseText(item) }}</small>
                  </div>
                </div>
              </div>
            </section>
          </div>
        </el-tab-pane>
      </el-tabs>
    </el-card>

    <section class="metric-grid">
      <el-card class="v3-card" shadow="never">
        <template #header><h2>Data Pipeline & Model Metrics</h2></template>
        <div class="metric-list">
          <article><span>日线数据池</span><strong>{{ systemStatus.data_pool?.latest_date || '-' }}</strong></article>
          <article><span>今日预测日期</span><strong>{{ signalsPayload.prediction_date || systemStatus.prediction_date || '-' }}</strong></article>
          <article><span>模型特征数</span><strong>{{ systemStatus.xgboost?.feature_count || '-' }}</strong></article>
          <article><span>0.90 高置信 Precision</span><strong class="rise">{{ scanModelPrecision }}</strong></article>
        </div>
      </el-card>

      <el-card class="v3-card" shadow="never">
        <template #header><h2>Top-K 锁定</h2></template>
        <div class="metric-list">
          <article><span>后端选择模式</span><strong>{{ signalsPayload.selection_mode || 'top_k' }}</strong></article>
          <article><span>锁定席位</span><strong>Top {{ signalsPayload.top_k || 5 }}</strong></article>
          <article><span>前置过滤后候选</span><strong>{{ signalsPayload.prefiltered_count ?? '-' }} 只</strong></article>
        </div>
      </el-card>

      <el-card class="v3-card" shadow="never">
        <template #header><h2>接口状态</h2></template>
        <div class="metric-list">
          <article><span>系统接口</span><strong :class="systemStatus.created_at ? 'rise' : 'fall'">{{ systemStatus.created_at ? 'Online' : 'Offline' }}</strong></article>
          <article><span>雷达信号</span><strong>{{ signals.length }} 条</strong></article>
          <article><span>本次雷达扫描耗时</span><strong>{{ scanElapsedText }}</strong></article>
          <article><span>最近扫描</span><strong>{{ signalsPayload.created_at || '-' }}</strong></article>
        </div>
      </el-card>
    </section>
  </section>
</template>

<script setup>
import { computed, onMounted, onUnmounted, ref } from 'vue'
import StockLink from '../components/StockLink.vue'

const API = import.meta.env.VITE_API_BASE || 'http://127.0.0.1:8000'

const systemStatus = ref({})
const signalsPayload = ref({})
const signals = ref([])
const historyPayload = ref({})
const historyRows = ref([])
const activeHistoryMonth = ref('')
const terminalLines = ref([
  { time: '[14:50:02]', text: '等待 14:50 XGBoost 高置信候选池...' },
  { time: '[14:46:05]', text: 'Qwen Agent Hub 已挂载，点击“唤醒 AI”执行单票舆情问询。' },
])
const agentReports = ref([])
const agentBusyCode = ref('')
const loadingSignals = ref(false)
const clockText = ref('')
let clockTimer = null
let signalRefreshTimer = null

const displaySignals = computed(() => signals.value)
const historyMonthGroups = computed(() => {
  const groups = {}
  for (const day of historyRows.value) {
    const month = String(day.selection_date || '').slice(0, 7)
    if (!month) continue
    if (!groups[month]) groups[month] = []
    groups[month].push(day)
  }
  for (const days of Object.values(groups)) {
    days.sort((a, b) => String(b.selection_date || '').localeCompare(String(a.selection_date || '')))
  }
  return groups
})
const historyMonths = computed(() => Object.keys(historyMonthGroups.value).sort().reverse())
const scanModelPrecision = computed(() => signalsPayload.value.model?.high_confidence_precision || systemStatus.value.xgboost?.high_confidence_precision || '85.78%')
const scanElapsedText = computed(() => {
  const seconds = Number(signalsPayload.value.elapsed_seconds)
  if (!Number.isFinite(seconds)) return '-'
  return `${seconds.toFixed(2)} 秒`
})
const xgbStatusText = computed(() => {
  const metrics = systemStatus.value.xgboost?.metrics || {}
  if (!systemStatus.value.xgboost?.ready) return 'Model Missing'
  return `V2 全局版 · Precision ${(Number(metrics.precision || 0) * 100).toFixed(2)}%`
})
const lockStateText = computed(() => {
  if (signalsPayload.value.locked) return `已锁定 ${signalsPayload.value.locked_at || ''}`.trim()
  const status = signalsPayload.value.lock_status || ''
  if (status === 'waiting_for_1450') return '等待 14:50 自动锁定'
  if (status.startsWith?.('quote_date_not_current')) return '等待交易日实时行情'
  if (loadingSignals.value) return '扫描中'
  return '未锁定'
})

const request = async (path, options = {}) => {
  const response = await fetch(`${API}${path}`, options)
  if (!response.ok) throw new Error(await response.text())
  return response.json()
}

const loadStatus = async () => {
  systemStatus.value = await request('/api/v3/system/status')
}

const loadSignals = async ({ silent = false } = {}) => {
  const wasLocked = Boolean(signalsPayload.value.locked)
  if (!silent) loadingSignals.value = true
  try {
    const data = await request('/api/v3/sniper/scan_today?limit=0')
    signalsPayload.value = data
    signals.value = data.rows || []
    if (!silent || (!wasLocked && data.locked)) {
      terminalLines.value.unshift({
        time: `[${new Date().toLocaleTimeString('zh-CN', { hour12: false })}]`,
        text: data.locked
          ? `14:50 雷达已锁定：Top ${data.top_k || 5} 固定 ${signals.value.length} 条，不再改榜。`
          : `雷达扫描完成：Top ${data.top_k || 5} 候选 ${signals.value.length} 条，等待 14:50 锁定。`,
      })
    }
    if (data.locked && signalRefreshTimer) {
      clearInterval(signalRefreshTimer)
      signalRefreshTimer = null
    }
    if (data.locked) await loadHistory()
  } finally {
    if (!silent) loadingSignals.value = false
  }
}

const loadHistory = async () => {
  const data = await request('/api/v3/sniper/history?limit=120')
  historyPayload.value = data
  historyRows.value = data.rows || []
  if (!activeHistoryMonth.value || !historyMonths.value.includes(activeHistoryMonth.value)) {
    activeHistoryMonth.value = historyMonths.value[0] || ''
  }
}

const wakeAgent = async (row) => {
  agentBusyCode.value = row.code
  terminalLines.value.unshift({
    time: `[${new Date().toLocaleTimeString('zh-CN', { hour12: false })}]`,
    text: `${row.name}(${row.code}) 分析中... 正在拉取舆情并唤醒 Qwen。`,
  })
  try {
    const data = await request('/api/v3/agent/analyze_stock', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ code: row.code, name: row.name, candidate: row }),
    })
    appendAgentReport(row, data.markdown || 'AI 未返回 Markdown。')
    terminalLines.value.unshift({
      time: `[${new Date().toLocaleTimeString('zh-CN', { hour12: false })}]`,
      text: `Qwen 分析完毕：${row.name} 风控报告已生成。`,
    })
  } catch (error) {
    appendAgentReport(row, `### AI 分析失败\n- ${error.message}`)
  } finally {
    agentBusyCode.value = ''
  }
}

const appendAgentReport = (row, markdown) => {
  agentReports.value.unshift({
    id: `${row.code}-${Date.now()}`,
    time: `[${new Date().toLocaleTimeString('zh-CN', { hour12: false })}]`,
    code: row.code,
    name: row.name,
    markdown,
  })
  agentReports.value = agentReports.value.slice(0, 8)
}

const signedPct = (value) => {
  const num = Number(value)
  if (!Number.isFinite(num)) return '-'
  return `${num > 0 ? '+' : ''}${num.toFixed(2)}%`
}

const pctText = (value) => {
  const num = Number(value)
  if (!Number.isFinite(num)) return '-'
  return `${num.toFixed(2)}%`
}

const priceText = (value) => {
  const num = Number(value)
  if (!Number.isFinite(num) || num <= 0) return '-'
  return num.toFixed(2)
}

const tDayReturnText = (item) => {
  if (!item || item.status !== 'closed') return '待收盘'
  return signedPct(item.return_pct)
}

const tDayCloseText = (item) => {
  if (!item || item.status !== 'closed') return item?.label || '-'
  return `${item.date || item.label} 收 ${priceText(item.close)}`
}

const numberClass = (value) => {
  const num = Number(value)
  if (!Number.isFinite(num) || num === 0) return 'neutral'
  return num > 0 ? 'rise' : 'fall'
}

const tickClock = () => {
  clockText.value = new Date().toLocaleString('zh-CN', { hour12: false })
}

onMounted(async () => {
  tickClock()
  clockTimer = setInterval(tickClock, 1000)
  await Promise.all([loadStatus(), loadSignals(), loadHistory()])
  if (!signalsPayload.value.locked) {
    signalRefreshTimer = setInterval(() => {
      loadSignals({ silent: true }).catch(() => {})
    }, 60000)
  }
})

onUnmounted(() => {
  if (clockTimer) clearInterval(clockTimer)
  if (signalRefreshTimer) clearInterval(signalRefreshTimer)
})
</script>

<style scoped>
.v3-shell {
  display: grid;
  gap: 16px;
}

.v3-status,
.v3-card {
  border: 1px solid rgba(225, 225, 230, 0.1);
  border-radius: 16px;
  background: rgba(24, 24, 28, 0.92);
  box-shadow: 0 18px 48px rgba(0, 0, 0, 0.24);
}

.v3-status {
  display: flex;
  justify-content: space-between;
  gap: 18px;
  padding: 16px;
}

.brand-block {
  display: flex;
  align-items: center;
  gap: 14px;
}

.brand-icon {
  width: 52px;
  height: 52px;
  display: grid;
  place-items: center;
  border-radius: 14px;
  background: linear-gradient(135deg, #2563eb, #14b8a6);
  color: white;
  font-weight: 1000;
  font-size: 1.5rem;
}

.brand-block h1 {
  margin: 0;
  font-size: 1.45rem;
  color: #f2f6ff;
}

.brand-block small,
.status-grid span,
.metric-list span,
.signal-row small,
.elapsed-chip {
  color: #7f8aa1;
}

.status-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(120px, 1fr));
  gap: 10px;
  min-width: min(780px, 100%);
}

.status-grid article,
.metric-list article {
  border: 1px solid rgba(225, 225, 230, 0.08);
  border-radius: 12px;
  padding: 10px;
  background: #101014;
}

.status-grid article.online {
  border-color: rgba(20, 184, 166, 0.34);
  background: rgba(20, 184, 166, 0.09);
}

.status-grid strong,
.metric-list strong {
  display: block;
  margin-top: 5px;
  color: #e1e1e6;
}

.v3-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.55fr) minmax(360px, 0.95fr);
  gap: 16px;
}

.sniper-card {
  min-width: 0;
}

.card-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.card-head h2 {
  margin: 0;
  color: #f2f6ff;
}

.radar-actions {
  display: flex;
  gap: 10px;
  align-items: center;
  justify-content: flex-end;
  flex-wrap: wrap;
}

.threshold-chip,
.agent-state,
.elapsed-chip,
.lock-chip,
.row-lock {
  border: 1px solid rgba(24, 144, 255, 0.32);
  border-radius: 999px;
  padding: 4px 10px;
  color: #69b1ff;
  background: rgba(24, 144, 255, 0.1);
  font-size: 0.78rem;
  font-weight: 900;
}

.agent-state.ready {
  border-color: rgba(245, 34, 45, 0.36);
  color: #ff7875;
  background: rgba(245, 34, 45, 0.12);
}

.elapsed-chip {
  border-color: rgba(82, 196, 26, 0.28);
  color: #9ee886;
  background: rgba(82, 196, 26, 0.08);
}

.lock-chip.locked,
.row-lock {
  border-color: rgba(82, 196, 26, 0.32);
  color: #9ee886;
  background: rgba(82, 196, 26, 0.09);
}

.lock-chip.waiting {
  border-color: rgba(250, 173, 20, 0.34);
  color: #ffd666;
  background: rgba(250, 173, 20, 0.08);
}

.signal-table {
  display: grid;
  gap: 10px;
}

.signal-row {
  display: grid;
  grid-template-columns: 1.25fr 1fr 0.72fr 0.78fr 1.1fr;
  gap: 12px;
  align-items: center;
  border: 1px solid rgba(225, 225, 230, 0.09);
  border-radius: 12px;
  padding: 12px;
  background: #101014;
}

.signal-row.elite {
  border-color: rgba(245, 34, 45, 0.42);
  box-shadow: inset 3px 0 0 #f5222d, 0 0 20px rgba(245, 34, 45, 0.12);
}

.signal-head {
  padding: 2px 12px;
  border: 0;
  background: transparent;
  color: #6f7d95;
  font-size: 0.75rem;
  font-weight: 900;
}

.v3-stock-name,
.signal-row b {
  display: block;
  color: #e1e1e6;
}

.v3-stock-name {
  font-weight: 900;
}

.v3-stock-code {
  color: #7f8aa1;
  font-weight: 800;
}

.agent-stock-link {
  color: inherit;
  font-weight: 900;
}

.prob-track {
  display: block;
  width: 120px;
  height: 6px;
  margin-top: 6px;
  overflow: hidden;
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.08);
}

.elite-chip {
  display: inline-flex;
  width: fit-content;
  margin-top: 5px;
  border: 1px solid rgba(245, 34, 45, 0.45);
  border-radius: 999px;
  padding: 2px 7px;
  background: rgba(245, 34, 45, 0.13);
  color: #ff7875;
  font-size: 0.68rem;
  font-weight: 900;
}

.prob-track em {
  display: block;
  height: 100%;
  border-radius: inherit;
  background: #f5222d;
}

.row-actions {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  align-items: center;
}

.row-lock {
  white-space: nowrap;
  font-size: 0.75rem;
}

.agent-card :deep(.el-card__body) {
  padding: 0;
}

.terminal-log {
  min-height: 462px;
  max-height: 462px;
  overflow: auto;
  padding: 16px;
  background:
    linear-gradient(rgba(82, 196, 26, 0.055) 50%, rgba(0, 0, 0, 0.02) 50%),
    #07110d;
  background-size: 100% 6px;
  color: #7df77b;
  font-family: 'SF Mono', Menlo, Consolas, monospace;
  font-size: 0.86rem;
  line-height: 1.7;
}

.terminal-log p {
  margin: 0 0 4px;
}

.terminal-log span {
  margin-right: 8px;
  color: #6f7d95;
}

.agent-report {
  margin-top: 12px;
  border-top: 1px solid rgba(125, 247, 123, 0.22);
  padding-top: 10px;
}

.agent-report header {
  color: #9ee886;
  font-weight: 900;
}

.agent-markdown {
  margin: 8px 0 0;
  white-space: pre-wrap;
  color: #d7ffd4;
  font-family: inherit;
}

.metric-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 16px;
}

.history-card {
  min-width: 0;
}

.history-tabs :deep(.el-tabs__nav-wrap::after) {
  background: rgba(225, 225, 230, 0.08);
}

.history-tabs :deep(.el-tabs__item) {
  color: #7f8aa1;
  font-weight: 800;
}

.history-tabs :deep(.el-tabs__item.is-active) {
  color: #69b1ff;
}

.history-month {
  display: grid;
  gap: 14px;
}

.history-day-block {
  display: grid;
  gap: 10px;
  border-top: 1px solid rgba(225, 225, 230, 0.08);
  padding-top: 12px;
}

.history-day-block:first-child {
  border-top: 0;
  padding-top: 0;
}

.history-day-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}

.history-day-head > strong {
  color: #e1e1e6;
  font-size: 0.95rem;
}

.history-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  margin: 0;
  color: #7f8aa1;
  font-size: 0.8rem;
}

.history-meta span {
  border: 1px solid rgba(225, 225, 230, 0.08);
  border-radius: 999px;
  padding: 4px 10px;
  background: #101014;
}

.history-table {
  display: grid;
  gap: 8px;
}

.history-row {
  display: grid;
  grid-template-columns: 1.2fr 0.85fr repeat(3, minmax(120px, 1fr));
  gap: 12px;
  align-items: center;
  border: 1px solid rgba(225, 225, 230, 0.08);
  border-radius: 12px;
  padding: 10px 12px;
  background: #101014;
}

.history-head {
  border: 0;
  padding: 2px 12px;
  background: transparent;
  color: #6f7d95;
  font-size: 0.75rem;
  font-weight: 900;
}

.history-row strong,
.history-row small,
.tday-cell strong,
.tday-cell small {
  display: block;
}

.history-row small,
.tday-cell small {
  margin-top: 4px;
  color: #7f8aa1;
  font-size: 0.74rem;
}

.tday-cell {
  min-width: 0;
}

.metric-list {
  display: grid;
  gap: 10px;
}

.v3-empty {
  display: grid;
  gap: 6px;
  border: 1px dashed rgba(225, 225, 230, 0.15);
  border-radius: 12px;
  padding: 22px;
  color: #7f8aa1;
}

.v3-empty strong,
.rise {
  color: #f5222d !important;
}

.fall {
  color: #52c41a !important;
}

.neutral {
  color: #1890ff !important;
}

.mono {
  font-family: 'SF Mono', Menlo, Consolas, monospace;
}

@media (max-width: 1180px) {
  .v3-status,
  .v3-grid,
  .metric-grid {
    grid-template-columns: 1fr;
  }

  .v3-status {
    display: grid;
  }

  .status-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
    min-width: 0;
  }
}

@media (max-width: 760px) {
  .signal-row,
  .history-row {
    grid-template-columns: 1fr;
  }

  .status-grid {
    grid-template-columns: 1fr;
  }
}
</style>
