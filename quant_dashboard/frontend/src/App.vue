<template>
  <div class="terminal-app dark">
    <StatsHeader
      :title="currentTitle"
      :locked-count="todayLockedCount"
      :latest-sync="latestSync"
      :health="health"
      :model-status="radar.model_status"
    />

    <div class="main-grid">
      <Sidebar v-model:active="activeSection" :health="health" />

      <main class="content">
        <el-alert
          v-if="message.text"
          :title="message.text"
          :type="message.type === 'error' ? 'error' : 'info'"
          show-icon
          :closable="false"
          class="message-alert"
        />

        <section v-show="activeSection === 'dashboard'" class="page-stack">
          <section class="dashboard-grid">
            <el-card class="dark-card signal-card" shadow="never">
              <template #header>
                <div class="card-head">
                  <div>
                    <p class="eyebrow">Signal Control</p>
                    <h2>今日核心出票与指令</h2>
                  </div>
                  <el-tag effect="dark" type="primary">Forward Shadow</el-tag>
                </div>
              </template>

              <div v-if="operationCards.length === 0" class="empty-panel">
                <strong>暂无操作指令</strong>
                <span>等待 14:50 真实快照锁定、09:26 早盘哨兵、14:45 波段巡逻兵自动写入。</span>
              </div>

              <div v-else class="instruction-grid">
                <article v-for="pick in operationCards" :key="`op-${pick.id}`" class="instruction-card">
                  <header>
                    <span :class="strategyBadgeClass(pick.strategy_type)">{{ strategyLabel(pick.strategy_type) }}</span>
                    <strong :class="instructionClass(pick)">{{ instructionTitle(pick) }}</strong>
                  </header>
                  <p><span class="mono">{{ pick.code }}</span> {{ pick.name }}</p>
                  <small>{{ instructionBody(pick) }}</small>
                </article>
              </div>
            </el-card>

            <el-card class="dark-card" shadow="never">
              <template #header>
                <div class="card-head">
                  <div>
                    <p class="eyebrow">System Pulse</p>
                    <h2>实时状态</h2>
                  </div>
                  <el-button :loading="busy.refresh" @click="refreshAll">刷新</el-button>
                </div>
              </template>

              <div class="pulse-grid">
                <article>
                  <span>大盘风控</span>
                  <strong :class="radar.market_gate?.blocked ? 'risk' : 'neutral'">
                    {{ radar.market_gate?.blocked ? '空仓' : (radar.market_gate?.mode || '待扫描') }}
                  </strong>
                </article>
                <article>
                  <span>成交额</span>
                  <strong>{{ amountYi(radar.market_gate?.market_amount_yi) }}</strong>
                </article>
                <article>
                  <span>数据同步</span>
                  <strong>{{ latestSync?.finished_at || '-' }}</strong>
                </article>
                <article>
                  <span>雷达缓存</span>
                  <strong>{{ radar.created_at || '无缓存' }}</strong>
                </article>
              </div>
            </el-card>
          </section>

          <section class="toolbar-panel">
            <div>
              <p class="eyebrow">Realtime Radar</p>
              <h2>14:50 扫描结果</h2>
            </div>
            <div class="toolbar-actions">
              <el-button :loading="busy.refresh" @click="refreshAll">刷新状态</el-button>
              <el-button type="primary" :loading="busy.scan" @click="scanRadar">实时预测</el-button>
            </div>
          </section>

          <el-empty v-if="radar.rows.length === 0" class="radar-empty" description="空仓避险：当前没有达到生产门槛的候选股" />

          <section v-else class="dual-board">
            <SelectionTable
              title="短线极速看板"
              eyebrow="T+1 Breakout"
              :rows="radar.rows"
              mode="short"
              :table-height="360"
              show-inspect
              @inspect="inspectStock"
            />
            <SelectionTable
              title="波段策略看板"
              eyebrow="T+3 Swing"
              :rows="radar.rows"
              mode="swing"
              :table-height="360"
              show-inspect
              @inspect="inspectStock"
            />
          </section>

          <el-card v-if="selectedStock" class="dark-card" shadow="never">
            <template #header>
              <div class="card-head">
                <div>
                  <p class="eyebrow">Ollama Risk Control</p>
                  <h2><span class="mono">{{ selectedStock.code }}</span> {{ selectedStock.name }}</h2>
                </div>
                <el-button type="primary" :loading="busy.analyze" @click="analyzeStock">运行风控</el-button>
              </div>
            </template>
            <div v-if="analysis" class="analysis-grid">
              <article>
                <span>结论</span>
                <strong :class="analysis.analysis?.verdict?.includes('红灯') ? 'risk' : 'buy'">
                  {{ analysis.analysis?.verdict || '-' }}
                </strong>
              </article>
              <article>
                <span>情绪</span>
                <strong>{{ analysis.analysis?.sentiment || '-' }}</strong>
              </article>
              <article class="wide">
                <span>逻辑</span>
                <strong>{{ analysis.analysis?.logic || '-' }}</strong>
              </article>
            </div>
            <p v-else class="muted">点击运行风控后，Ollama 返回的舆情结论会在这里展示。</p>
          </el-card>
        </section>

        <section v-show="activeSection === 'ledger'" class="page-stack">
          <section class="legion-grid">
            <el-card v-for="stat in strategyStats" :key="stat.strategy" class="dark-card legion-card" shadow="never">
              <span :class="strategyBadgeClass(stat.strategy)">{{ strategyLabel(stat.strategy) }}</span>
              <strong>{{ stat.count }} 次</strong>
              <dl>
                <div><dt>胜率</dt><dd>{{ stat.winRate }}</dd></div>
                <div><dt>{{ stat.isSwing ? 'T+3均值' : 'T+1均值' }}</dt><dd :class="numberClass(stat.avgReturnRaw)">{{ stat.avgReturn }}</dd></div>
                <div><dt>持仓</dt><dd>{{ stat.openCount }}</dd></div>
              </dl>
            </el-card>
          </section>

          <SelectionTable
            title="影子账本月度复盘"
            eyebrow="Monthly Shadow Test"
            :rows="dailyPicks.rows"
            mode="all"
            use-months
            :table-height="620"
          />
        </section>

        <section v-show="activeSection === 'validation'" class="validation-grid">
          <el-card class="dark-card" shadow="never">
            <template #header>
              <div class="card-head">
                <div>
                  <p class="eyebrow">15:05 Market Sync</p>
                  <h2>数据同步</h2>
                </div>
                <el-button type="primary" :loading="busy.sync" @click="syncData">立即同步</el-button>
              </div>
            </template>

            <el-descriptions :column="2" border class="dark-desc">
              <el-descriptions-item label="状态">{{ latestSync?.status || '-' }}</el-descriptions-item>
              <el-descriptions-item label="完成时间">{{ latestSync?.finished_at || '-' }}</el-descriptions-item>
              <el-descriptions-item label="同步日期">{{ latestSync?.sync_date || '-' }}</el-descriptions-item>
              <el-descriptions-item label="新增/更新">{{ latestSync ? `${latestSync.inserted_rows} / ${latestSync.updated_rows}` : '-' }}</el-descriptions-item>
            </el-descriptions>
            <pre v-if="syncResult">{{ syncResult }}</pre>
          </el-card>

          <el-card class="dark-card" shadow="never">
            <template #header>
              <div class="card-head">
                <div>
                  <p class="eyebrow">Triple Validation</p>
                  <h2>校验报告</h2>
                </div>
                <el-button :loading="busy.validate" @click="runValidation">运行校验</el-button>
              </div>
            </template>

            <div class="validation-form">
              <el-input-number v-model="validationSample" :min="1" :max="10000" />
              <el-checkbox v-model="sourceCheck">实时源交叉核验</el-checkbox>
            </div>

            <div class="pulse-grid validation-summary">
              <article><span>状态</span><strong>{{ validation.status || overview.latest_report?.status || '-' }}</strong></article>
              <article><span>错误</span><strong>{{ validation.summary?.error_count ?? overview.latest_report?.summary?.error_count ?? 0 }}</strong></article>
              <article><span>警告</span><strong>{{ validation.summary?.warning_count ?? overview.latest_report?.summary?.warning_count ?? 0 }}</strong></article>
              <article><span>最新日期</span><strong>{{ overview.latest_report?.summary?.latest_date_seen || overview.max_date || '-' }}</strong></article>
            </div>
          </el-card>

          <el-card class="dark-card full-span" shadow="never">
            <template #header>
              <div class="card-head">
                <div>
                  <p class="eyebrow">Data Assets</p>
                  <h2>数据资产</h2>
                </div>
              </div>
            </template>
            <div class="asset-grid">
              <article><span>股票数</span><strong>{{ overview.stock_count ?? 0 }}</strong></article>
              <article><span>K 线行数</span><strong>{{ overview.rows_count ?? 0 }}</strong></article>
              <article><span>Parquet 文件</span><strong>{{ overview.parquet_files ?? 0 }}</strong></article>
              <article><span>日期范围</span><strong>{{ overview.min_date || '-' }} / {{ overview.max_date || '-' }}</strong></article>
            </div>
          </el-card>
        </section>
      </main>
    </div>
  </div>
</template>

<script setup>
import { computed, onMounted, reactive, ref } from 'vue'
import Sidebar from './components/Sidebar.vue'
import StatsHeader from './components/StatsHeader.vue'
import SelectionTable from './components/SelectionTable.vue'

const API = import.meta.env.VITE_API_BASE || 'http://127.0.0.1:8000'

const activeSection = ref('dashboard')
const overview = ref({})
const health = ref({})
const radar = reactive({ rows: [], created_at: '', model_status: '', market_gate: null })
const dailyPicks = reactive({ rows: [] })
const validation = reactive({ status: '', summary: null, issues: [] })
const message = reactive({ text: '', type: 'info' })
const selectedStock = ref(null)
const analysis = ref(null)
const syncResult = ref('')
const validationSample = ref(200)
const sourceCheck = ref(false)
const busy = reactive({ refresh: false, scan: false, sync: false, validate: false, analyze: false })

const currentTitle = computed(() => ({
  dashboard: 'Dashboard 总览',
  ledger: 'Shadow Test 影子账本',
  validation: 'Validation 数据校验',
})[activeSection.value] || 'Dashboard 总览')
const latestSync = computed(() => overview.value.latest_sync || null)
const localDateText = (date = new Date()) => {
  const y = date.getFullYear()
  const m = String(date.getMonth() + 1).padStart(2, '0')
  const d = String(date.getDate()).padStart(2, '0')
  return `${y}-${m}-${d}`
}
const todayText = computed(() => localDateText())
const todayLockedCount = computed(() => dailyPicks.rows.filter((row) => row.selection_date === todayText.value).length)
const operationCards = computed(() => dailyPicks.rows.filter((row) => row.selection_date === todayText.value || row.status === 'pending_open' || !row.is_closed).slice(0, 6))
const strategyStats = computed(() => ['右侧主升浪', '中线超跌反转', '尾盘突破'].map((strategy) => {
  const rows = dailyPicks.rows.filter((row) => row.strategy_type === strategy)
  const settled = rows.filter((row) => resultValue(row) !== null)
  const wins = settled.filter((row) => resultValue(row) > 0).length
  const avgRaw = settled.length ? settled.reduce((sum, row) => sum + resultValue(row), 0) / settled.length : null
  return {
    strategy,
    isSwing: isSwingStrategy(strategy),
    count: rows.length,
    openCount: rows.filter((row) => !row.is_closed).length,
    winRate: settled.length ? pct((wins / settled.length) * 100) : '-',
    avgReturnRaw: avgRaw,
    avgReturn: avgRaw === null ? '-' : pct(avgRaw),
  }
}))

const request = async (path, options = {}) => {
  const response = await fetch(`${API}${path}`, options)
  if (!response.ok) {
    const text = await response.text()
    throw new Error(text || response.statusText)
  }
  return response.json()
}
const setMessage = (text, type = 'info') => {
  message.text = text
  message.type = type
}
const refreshAll = async () => {
  busy.refresh = true
  try {
    await Promise.all([loadHealth(), loadOverview(), loadRadarCache(), loadDailyPicks()])
    setMessage('工作站状态已刷新。')
  } catch (error) {
    setMessage(`刷新失败：${error.message}`, 'error')
  } finally {
    busy.refresh = false
  }
}
const loadHealth = async () => {
  try {
    health.value = await request('/health')
  } catch {
    health.value = { ok: false, pushplus: { ok: false, reason: '后端不可达' } }
  }
}
const loadOverview = async () => {
  overview.value = await request('/api/overview')
}
const applyRadarPayload = (data) => {
  radar.rows = data.rows || []
  radar.created_at = data.created_at || ''
  radar.model_status = data.model_status || data.strategy || ''
  radar.market_gate = data.market_gate || null
}
const loadRadarCache = async () => {
  const data = await request('/api/radar/cache')
  applyRadarPayload(data)
}
const loadDailyPicks = async () => {
  const data = await request('/api/daily-picks?limit=500')
  dailyPicks.rows = data.rows || []
}
const scanRadar = async () => {
  busy.scan = true
  try {
    const data = await request('/api/radar/scan?limit=10')
    applyRadarPayload(data)
    setMessage(radar.rows.length ? `扫描完成：${radar.rows.length} 条候选。` : '扫描完成：当前空仓避险。')
  } catch (error) {
    setMessage(`实时预测失败：${error.message}`, 'error')
  } finally {
    busy.scan = false
  }
}
const syncData = async () => {
  busy.sync = true
  try {
    const data = await request('/api/data/market-sync/run', { method: 'POST' })
    syncResult.value = JSON.stringify(data, null, 2)
    setMessage(`同步完成：有效 ${data.valid_rows} 行，新增 ${data.inserted_rows}，更新 ${data.updated_rows}。`)
    await loadOverview()
  } catch (error) {
    setMessage(`同步失败：${error.message}`, 'error')
  } finally {
    busy.sync = false
  }
}
const runValidation = async () => {
  busy.validate = true
  try {
    const data = await request(`/api/data/validate?sample=${validationSample.value}&source_check=${sourceCheck.value}`, { method: 'POST' })
    validation.status = data.status || ''
    validation.summary = data.summary || null
    validation.issues = data.issues || []
    setMessage(`校验完成：${validation.status || '-'}。`, validation.status === 'pass' ? 'info' : 'error')
    await loadOverview()
  } catch (error) {
    setMessage(`校验失败：${error.message}`, 'error')
  } finally {
    busy.validate = false
  }
}
const analyzeStock = async () => {
  if (!selectedStock.value) return
  busy.analyze = true
  try {
    analysis.value = await request('/api/radar/analyze', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ code: selectedStock.value.code, name: selectedStock.value.name }),
    })
  } catch (error) {
    setMessage(`Ollama 风控失败：${error.message}`, 'error')
  } finally {
    busy.analyze = false
  }
}
const inspectStock = (row) => {
  selectedStock.value = row
  analysis.value = null
}

const isSwingStrategy = (value) => {
  const strategy = typeof value === 'string' ? value : value?.strategy_type
  return strategy === '中线超跌反转' || strategy === '右侧主升浪'
}
const resultValue = (row) => {
  const value = isSwingStrategy(row) ? row.t3_max_gain_pct : row.open_premium
  const num = Number(value)
  return Number.isFinite(num) ? num : null
}
const pct = (value) => {
  const num = Number(value)
  return Number.isFinite(num) ? `${num.toFixed(2)}%` : '-'
}
const amountYi = (value) => {
  const num = Number(value)
  return Number.isFinite(num) && num > 0 ? `${num.toFixed(0)} 亿` : '-'
}
const numberClass = (value) => {
  const num = Number(value)
  if (!Number.isFinite(num) || num === 0) return ''
  return num > 0 ? 'buy' : 'risk'
}
const strategyLabel = (strategy) => {
  if (strategy === '右侧主升浪') return '顺势主升浪'
  if (strategy === '中线超跌反转') return '中线超跌反转'
  if (strategy === '首阴低吸') return '低吸影子'
  return '尾盘突破'
}
const strategyBadgeClass = (strategy) => [
  'strategy-badge',
  strategy === '右侧主升浪' ? 'strategy-main' : strategy === '中线超跌反转' ? 'strategy-reversal' : strategy === '首阴低吸' ? 'strategy-dip' : 'strategy-breakout',
]
const instructionTitle = (pick) => {
  const actual = Number(pick.open_premium)
  if (isSwingStrategy(pick)) {
    if (!Number.isFinite(actual)) return '等待哨兵'
    return actual < -4 ? '破位核按钮' : '静默洗盘'
  }
  if (!Number.isFinite(actual)) return '等待开盘'
  if (actual < 0) return '核按钮'
  if (actual >= 3) return '超预期锁仓'
  return '落袋为安'
}
const instructionClass = (pick) => instructionTitle(pick).includes('核') ? 'risk' : instructionTitle(pick).includes('等待') ? 'neutral' : 'buy'
const instructionBody = (pick) => {
  const actual = Number(pick.open_premium)
  if (isSwingStrategy(pick)) {
    if (!Number.isFinite(actual)) return '等待 09:26 开盘哨兵，随后由 14:45 波段巡逻兵管理。'
    return actual < -4 ? `开盘 ${pct(actual)}，洗盘过度，触发止损警告。` : `开盘 ${pct(actual)}，正常洗盘，等待 14:45 指令。`
  }
  if (!Number.isFinite(actual)) return '等待 T+1 集合竞价回填。'
  return `T+1 开盘溢价 ${pct(actual)}，按极速隔夜规则处理。`
}

onMounted(refreshAll)
</script>

<style scoped>
.terminal-app {
  min-height: 100vh;
  background: var(--terminal-bg);
  color: var(--terminal-text);
}

.main-grid {
  display: grid;
  grid-template-columns: 260px minmax(0, 1fr);
  min-height: calc(100vh - 76px);
}

.content {
  min-width: 0;
  padding: 16px;
}

.page-stack {
  display: grid;
  gap: 14px;
}

.dashboard-grid,
.dual-board,
.validation-grid {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
  gap: 14px;
}

.full-span {
  grid-column: 1 / -1;
}

.dark-card {
  --el-card-bg-color: var(--terminal-card);
  --el-card-border-color: var(--terminal-border);
  --el-text-color-primary: var(--terminal-text);
  --el-text-color-regular: #a8b3c7;
  border-radius: 14px;
}

.card-head,
.toolbar-panel {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  align-items: center;
}

.toolbar-panel {
  border: 1px solid var(--terminal-border);
  border-radius: 14px;
  padding: 14px 16px;
  background: var(--terminal-card);
}

.toolbar-actions {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.eyebrow {
  margin: 0 0 4px;
  color: #6f7d95;
  font-size: 0.72rem;
  font-weight: 900;
  text-transform: uppercase;
}

h2,
p {
  margin: 0;
}

h2 {
  color: var(--terminal-text);
  font-size: 1rem;
}

.message-alert {
  margin-bottom: 14px;
}

.empty-panel {
  display: grid;
  gap: 6px;
  border: 1px dashed rgba(255, 255, 255, 0.14);
  border-radius: 12px;
  padding: 18px;
  color: #a8b3c7;
}

.empty-panel strong {
  color: var(--terminal-text);
}

.instruction-grid {
  display: grid;
  gap: 10px;
}

.instruction-card {
  border: 1px solid var(--terminal-border);
  border-radius: 12px;
  padding: 12px;
  background: var(--terminal-bg);
}

.instruction-card header {
  display: flex;
  justify-content: space-between;
  gap: 10px;
  align-items: center;
  margin-bottom: 8px;
}

.instruction-card p {
  color: var(--terminal-text);
  font-weight: 900;
}

.instruction-card small,
.muted {
  display: block;
  margin-top: 6px;
  color: #7f8aa1;
  line-height: 1.5;
}

.pulse-grid,
.asset-grid,
.analysis-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
}

.pulse-grid article,
.asset-grid article,
.analysis-grid article {
  border: 1px solid var(--terminal-border);
  border-radius: 12px;
  padding: 11px;
  background: var(--terminal-bg);
}

.pulse-grid span,
.asset-grid span,
.analysis-grid span,
.legion-card dt {
  display: block;
  color: #6f7d95;
  font-size: 0.75rem;
  font-weight: 800;
}

.pulse-grid strong,
.asset-grid strong,
.analysis-grid strong {
  display: block;
  margin-top: 5px;
  color: var(--terminal-text);
  font-weight: 900;
  overflow-wrap: anywhere;
}

.wide {
  grid-column: 1 / -1;
}

.legion-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px;
}

.legion-card :deep(.el-card__body) {
  display: grid;
  gap: 10px;
}

.legion-card > strong {
  font-size: 1.5rem;
  color: var(--terminal-text);
}

.legion-card dl {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 8px;
  margin: 0;
}

.legion-card dd {
  margin: 4px 0 0;
  color: var(--terminal-text);
  font-weight: 900;
}

.validation-form {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 14px;
  flex-wrap: wrap;
}

.dark-desc {
  --el-fill-color-blank: var(--terminal-bg);
  --el-border-color-lighter: var(--terminal-border);
  --el-text-color-primary: var(--terminal-text);
  --el-text-color-regular: #a8b3c7;
}

pre {
  margin: 12px 0 0;
  max-height: 220px;
  overflow: auto;
  border-radius: 10px;
  padding: 12px;
  background: #0b0b0f;
  color: #d7deeb;
}

.strategy-badge {
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

.mono {
  font-family: 'SF Mono', Menlo, Consolas, monospace;
}

.buy {
  color: var(--quant-rise) !important;
}

.risk {
  color: var(--quant-fall) !important;
}

.neutral {
  color: var(--quant-neutral) !important;
}

.radar-empty {
  border: 1px solid var(--terminal-border);
  border-radius: 14px;
  background: var(--terminal-card);
}

@media (max-width: 1180px) {
  .main-grid,
  .dashboard-grid,
  .dual-board,
  .validation-grid,
  .legion-grid {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 680px) {
  .content {
    padding: 12px;
  }

  .card-head,
  .toolbar-panel {
    align-items: flex-start;
    flex-direction: column;
  }

  .pulse-grid,
  .asset-grid,
  .analysis-grid,
  .legion-card dl {
    grid-template-columns: 1fr;
  }
}
</style>
