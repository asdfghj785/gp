<template>
  <div class="radar-page">
    <div class="radar-shell">
      <header class="radar-header">
        <h1 class="radar-title">🎯 实盘雷达：全市场情绪动量扫描</h1>
        <button class="scan-btn" :disabled="loading" type="button" @click="fetchData">
          {{ loading ? '扫描中...' : '重新扫描' }}
        </button>
      </header>

      <p class="updated-at">最近扫描：{{ updatedAt || '尚未扫描' }}</p>

      <p v-if="error" class="error-text">{{ error }}</p>

      <div class="table-wrap">
        <table class="radar-table">
          <thead>
            <tr>
              <th>股票代码</th>
              <th>名称</th>
              <th>现价</th>
              <th>涨跌幅</th>
              <th>量比</th>
              <th>换手率</th>
              <th>💎 AI 胜率</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            <tr v-if="!loading && rows.length === 0">
              <td colspan="8" class="empty-row">暂无数据</td>
            </tr>
            <tr v-for="item in rows" :key="item.代码">
              <td>
                <StockLink :code="item.代码" :name="item.名称" :label="item.代码" mono class="radar-stock-code" />
              </td>
              <td>
                <StockLink :code="item.代码" :name="item.名称" :label="item.名称" class="name-btn" />
              </td>
              <td>{{ formatNumber(item.最新价) }}</td>
              <td :class="changeClass(item.涨跌幅)">{{ formatPercent(item.涨跌幅) }}</td>
              <td>{{ formatNumber(item.量比) }}</td>
              <td>{{ formatPercent(item.换手率) }}</td>
              <td :class="winClass(item.AI胜率)">{{ formatPercent(item.AI胜率) }}</td>
              <td>
                <button class="action-btn" type="button" @click="openAnalysis(item)">分析</button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <div v-if="drawerOpen" class="drawer-mask" @click.self="closeDrawer">
      <aside class="drawer-panel">
        <header class="drawer-head">
          <div>
            <div class="drawer-stock">
              <StockLink
                v-if="activeStock"
                :code="activeStock.代码"
                :name="activeStock.名称"
                :label="`${activeStock.名称} (${activeStock.代码})`"
              />
            </div>
            <div class="drawer-sub">深度分析引擎</div>
          </div>
          <button class="close-btn" type="button" @click="closeDrawer">✕</button>
        </header>

        <div v-if="analyzing" class="analyzing-box">
          <div class="scanner-orb" />
          <div class="loading-text">AI 正在深度研判中...</div>
        </div>

        <p v-else-if="analysisError" class="error-text">{{ analysisError }}</p>

        <template v-else-if="analysisResult">
          <section :class="['verdict-banner', verdictClass(analysisResult.analysis.verdict)]">
            {{ analysisResult.analysis.verdict }}
          </section>

          <section class="logic-box">
            <h3>情绪与推演</h3>
            <p class="logic-sentiment">情绪判定：{{ analysisResult.analysis.sentiment }}</p>
            <p class="logic-text">{{ analysisResult.analysis.logic }}</p>
          </section>

          <section class="evidence-box">
            <h3>核心佐证数据</h3>
            <article
              v-for="(item, idx) in safeEvidence(analysisResult.analysis.evidence)"
              :key="`${idx}-${item.quote}`"
              class="evidence-card"
            >
              <div class="evidence-source">来源：{{ item.source }}</div>
              <div class="evidence-quote">“{{ item.quote }}”</div>
            </article>
          </section>

          <section class="raw-box">
            <h3>全量透明区（AI 已读取原始标题）</h3>

            <details open>
              <summary>官方公告（{{ safeList(analysisResult.raw_data.announcements).length }}）</summary>
              <ul>
                <li
                  v-for="(title, idx) in safeList(analysisResult.raw_data.announcements)"
                  :key="`ann-${idx}`"
                >
                  {{ title }}
                </li>
              </ul>
            </details>

            <details>
              <summary>行业资讯（{{ safeList(analysisResult.raw_data.news).length }}）</summary>
              <ul>
                <li v-for="(title, idx) in safeList(analysisResult.raw_data.news)" :key="`news-${idx}`">
                  {{ title }}
                </li>
              </ul>
            </details>

            <details>
              <summary>散户讨论（{{ safeList(analysisResult.raw_data.retail).length }}）</summary>
              <ul>
                <li
                  v-for="(title, idx) in safeList(analysisResult.raw_data.retail)"
                  :key="`retail-${idx}`"
                >
                  {{ title }}
                </li>
              </ul>
            </details>
          </section>
        </template>
      </aside>
    </div>
  </div>
</template>

<script setup>
import { onMounted, ref } from 'vue'
import StockLink from './components/StockLink.vue'

const rows = ref([])
const loading = ref(false)
const error = ref('')
const updatedAt = ref('')

const drawerOpen = ref(false)
const activeStock = ref(null)
const analyzing = ref(false)
const analysisError = ref('')
const analysisResult = ref(null)
const API_BASE = 'http://127.0.0.1:8000'

const formatNumber = (value) => Number(value ?? 0).toFixed(2)
const formatPercent = (value) => `${Number(value ?? 0).toFixed(2)}%`
const changeClass = (value) => (Number(value) >= 0 ? 'up' : 'down')
const winClass = (value) => (Number(value) > 60 ? 'win-hot' : 'win-cold')
const verdictClass = (value) =>
  String(value).includes('绿') || String(value).includes('🟢') ? 'verdict-green' : 'verdict-red'

const safeList = (items) => (Array.isArray(items) && items.length > 0 ? items : ['暂无数据'])

const safeEvidence = (items) => {
  if (!Array.isArray(items) || items.length === 0) {
    return [{ source: '系统', quote: '暂无核心证据' }]
  }
  return items
}

const fetchData = async () => {
  loading.value = true
  error.value = ''
  try {
    const response = await fetch(`${API_BASE}/api/radar/scan`)
    if (!response.ok) {
      const detail = await response.text()
      console.error('[Radar Scan] HTTP Error', {
        status: response.status,
        statusText: response.statusText,
        detail,
      })
      throw new Error(`HTTP ${response.status}: ${detail || response.statusText}`)
    }
    const data = await response.json()
    rows.value = Array.isArray(data) ? data : []
    updatedAt.value = new Date().toLocaleString()
  } catch (err) {
    console.error('[Radar Scan] Fetch Failed', err)
    rows.value = []
    error.value = `拉取失败：${err instanceof Error ? err.message : '未知错误'}`
  } finally {
    loading.value = false
  }
}

const openAnalysis = async (stock) => {
  activeStock.value = stock
  drawerOpen.value = true
  analysisResult.value = null
  analysisError.value = ''
  analyzing.value = true

  try {
    const response = await fetch(`${API_BASE}/api/radar/analyze`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ code: stock.代码, name: stock.名称 }),
    })
    if (!response.ok) {
      const detail = await response.text()
      console.error('[Radar Analyze] HTTP Error', {
        status: response.status,
        statusText: response.statusText,
        detail,
        payload: { code: stock.代码, name: stock.名称 },
      })
      throw new Error(`HTTP ${response.status}: ${detail || response.statusText}`)
    }
    analysisResult.value = await response.json()
  } catch (err) {
    console.error('[Radar Analyze] Fetch Failed', err)
    analysisError.value = `分析失败：${err instanceof Error ? err.message : '未知错误'}`
  } finally {
    analyzing.value = false
  }
}

const closeDrawer = () => {
  drawerOpen.value = false
}

onMounted(fetchData)
</script>

<style scoped>
.radar-page {
  min-height: 100vh;
  padding: 24px;
}

.radar-shell {
  margin: 0 auto;
  max-width: 1200px;
  border: 1px solid rgba(54, 77, 109, 0.68);
  border-radius: 16px;
  background:
    radial-gradient(circle at 8% 10%, rgba(255, 183, 77, 0.14), transparent 38%),
    radial-gradient(circle at 90% 20%, rgba(56, 189, 248, 0.13), transparent 34%),
    linear-gradient(160deg, rgba(7, 17, 34, 0.92), rgba(8, 10, 21, 0.95));
  box-shadow:
    0 18px 40px rgba(0, 0, 0, 0.46),
    inset 0 1px 0 rgba(255, 255, 255, 0.05);
  padding: 24px;
}

.radar-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}

.radar-title {
  margin: 0;
  font-size: clamp(1.1rem, 2vw, 1.55rem);
  letter-spacing: 0.02em;
  color: #ecf4ff;
}

.scan-btn {
  border: 1px solid #35f2cf;
  border-radius: 10px;
  padding: 8px 14px;
  background: linear-gradient(135deg, rgba(29, 244, 201, 0.2), rgba(23, 105, 146, 0.3));
  color: #8efee2;
  font-weight: 700;
  cursor: pointer;
  transition: transform 0.2s ease, box-shadow 0.2s ease;
}

.scan-btn:hover:not(:disabled) {
  transform: translateY(-1px);
  box-shadow: 0 8px 18px rgba(60, 216, 246, 0.25);
}

.scan-btn:disabled {
  opacity: 0.7;
  cursor: not-allowed;
}

.updated-at {
  margin: 14px 0 10px;
  color: #90a6c9;
  font-size: 0.92rem;
}

.error-text {
  color: #ff7f8f;
  margin: 0 0 10px;
}

.table-wrap {
  overflow-x: auto;
  border: 1px solid rgba(65, 90, 124, 0.65);
  border-radius: 12px;
}

.radar-table {
  width: 100%;
  border-collapse: collapse;
  min-width: 900px;
}

.radar-table th,
.radar-table td {
  padding: 12px 14px;
  text-align: left;
  border-bottom: 1px solid rgba(69, 95, 130, 0.45);
}

.radar-table th {
  color: #b8cee8;
  font-size: 0.86rem;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  background: rgba(11, 23, 41, 0.8);
}

.radar-table tbody tr {
  transition: background-color 0.2s ease;
}

.radar-table tbody tr:hover {
  background: rgba(103, 165, 255, 0.08);
}

.radar-table td {
  color: #e5eeff;
  font-size: 0.95rem;
}

.name-btn {
  border: 0;
  background: transparent;
  color: #8fd1ff;
  font-weight: 700;
  cursor: pointer;
  padding: 0;
}

.name-btn:hover {
  color: #c0ecff;
}

.radar-stock-code {
  color: #e5eeff;
  font-weight: 800;
}

.action-btn {
  border: 1px solid rgba(108, 255, 218, 0.7);
  background: rgba(27, 57, 76, 0.45);
  color: #7fffe6;
  border-radius: 8px;
  padding: 6px 10px;
  font-weight: 700;
  cursor: pointer;
}

.action-btn:hover {
  background: rgba(37, 91, 117, 0.56);
}

.mono {
  font-family: 'JetBrains Mono', 'SF Mono', Menlo, monospace;
}

.up {
  color: #ff6262;
  font-weight: 700;
}

.down {
  color: #46cc8f;
  font-weight: 700;
}

.win-hot {
  color: #ffc355;
  font-weight: 800;
  text-shadow: 0 0 10px rgba(255, 199, 92, 0.35);
}

.win-cold {
  color: #8292ac;
  font-weight: 700;
}

.empty-row {
  text-align: center;
  color: #8ba2be;
}

.drawer-mask {
  position: fixed;
  inset: 0;
  background: rgba(3, 7, 18, 0.68);
  backdrop-filter: blur(4px);
  display: flex;
  justify-content: flex-end;
  z-index: 90;
}

.drawer-panel {
  width: min(520px, 100vw);
  height: 100vh;
  overflow-y: auto;
  padding: 20px;
  border-left: 1px solid rgba(84, 124, 163, 0.48);
  background: linear-gradient(180deg, #0b1324 0%, #080f1e 45%, #070b16 100%);
}

.drawer-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 12px;
}

.drawer-stock {
  color: #f0f7ff;
  font-weight: 800;
  font-size: 1.1rem;
}

.drawer-sub {
  color: #8ca6c5;
  font-size: 0.85rem;
}

.close-btn {
  border: 1px solid rgba(119, 147, 178, 0.7);
  background: transparent;
  color: #c6daef;
  border-radius: 8px;
  width: 32px;
  height: 32px;
  cursor: pointer;
}

.analyzing-box {
  margin-top: 16px;
  border: 1px solid rgba(79, 143, 197, 0.45);
  border-radius: 12px;
  padding: 24px;
  text-align: center;
  background: rgba(14, 34, 56, 0.45);
}

.scanner-orb {
  width: 62px;
  height: 62px;
  margin: 0 auto 12px;
  border-radius: 50%;
  border: 3px solid rgba(121, 241, 255, 0.18);
  border-top-color: #58f6ff;
  box-shadow: 0 0 22px rgba(88, 246, 255, 0.35);
  animation: spin 1.1s linear infinite;
}

.loading-text {
  color: #8edfff;
  font-weight: 700;
  letter-spacing: 0.04em;
}

.verdict-banner {
  margin-top: 16px;
  border-radius: 12px;
  padding: 16px;
  font-size: 1.35rem;
  font-weight: 900;
  text-align: center;
}

.verdict-red {
  color: #ffc7cf;
  border: 1px solid rgba(255, 76, 112, 0.55);
  background: linear-gradient(145deg, rgba(82, 19, 35, 0.65), rgba(36, 12, 19, 0.82));
}

.verdict-green {
  color: #d4ffe9;
  border: 1px solid rgba(68, 238, 157, 0.52);
  background: linear-gradient(145deg, rgba(16, 70, 52, 0.68), rgba(14, 40, 30, 0.8));
}

.logic-box,
.evidence-box,
.raw-box {
  margin-top: 16px;
  border: 1px solid rgba(76, 104, 136, 0.45);
  border-radius: 12px;
  padding: 14px;
  background: rgba(12, 26, 45, 0.5);
}

.logic-box h3,
.evidence-box h3,
.raw-box h3 {
  margin: 0 0 10px;
  color: #d4e8ff;
  font-size: 1rem;
}

.logic-sentiment {
  color: #8ee7ff;
  font-weight: 700;
  margin: 0 0 8px;
}

.logic-text {
  color: #d1e5fb;
  margin: 0;
  line-height: 1.6;
}

.evidence-card {
  border: 1px solid rgba(114, 163, 209, 0.38);
  border-radius: 10px;
  padding: 10px;
  margin-bottom: 10px;
  background: rgba(13, 33, 56, 0.58);
}

.evidence-source {
  color: #80dbff;
  font-size: 0.84rem;
  margin-bottom: 6px;
}

.evidence-quote {
  color: #e9f4ff;
  line-height: 1.5;
}

.raw-box details {
  margin: 8px 0;
}

.raw-box summary {
  cursor: pointer;
  color: #8ed4ff;
  font-weight: 700;
}

.raw-box ul {
  margin: 8px 0 0;
  padding-left: 16px;
}

.raw-box li {
  margin-bottom: 6px;
  color: #c7dbef;
  line-height: 1.45;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

@media (max-width: 768px) {
  .radar-page {
    padding: 12px;
  }

  .radar-shell {
    padding: 14px;
  }

  .drawer-panel {
    width: 100vw;
  }
}
</style>
