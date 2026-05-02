<template>
  <section class="minute-viewer page-stack">
    <el-card class="dark-card chart-card" shadow="never">
      <template #header>
        <div class="card-head">
          <div>
            <p class="eyebrow">Stock Data Scope</p>
            <h2>单票行情库</h2>
          </div>
          <el-tag effect="dark" type="primary">{{ activeModeLabel }}</el-tag>
        </div>
      </template>

      <div class="minute-toolbar">
        <el-input
          v-model="code"
          class="code-input"
          maxlength="6"
          placeholder="输入股票代码，如 600000"
          clearable
          @keyup.enter="loadStockData(false)"
        />
        <el-select v-model="period" class="period-select" :disabled="viewMode === 'daily'">
          <el-option label="5 分钟" value="5" />
          <el-option label="1 分钟" value="1" />
          <el-option label="15 分钟" value="15" />
          <el-option label="30 分钟" value="30" />
          <el-option label="60 分钟" value="60" />
        </el-select>
        <el-radio-group v-model="viewMode" class="view-toggle">
          <el-radio-button label="minute">5m 分时</el-radio-button>
          <el-radio-button label="daily">日 K</el-radio-button>
        </el-radio-group>
        <el-radio-group v-model="activeRange" class="range-toggle">
          <el-radio-button
            v-for="option in rangeOptions"
            :key="option.value"
            :label="option.value"
          >
            {{ option.label }}
          </el-radio-button>
        </el-radio-group>
        <el-button type="primary" :loading="loading" @click="loadStockData(false)">查询</el-button>
        <el-button :loading="refreshing" @click="loadStockData(true)">刷新 5m</el-button>
      </div>

      <el-alert
        v-if="message"
        :title="message"
        :type="messageType"
        show-icon
        :closable="false"
        class="minute-alert"
      />

      <div class="quote-strip" :class="quoteToneClass">
        <div class="quote-main">
          <div class="quote-title-line">
            <strong>{{ quoteHeader.name }}</strong>
            <span class="mono">{{ quoteHeader.code }}</span>
          </div>
          <div class="quote-price-line">
            <strong>{{ price(quoteHeader.price) }}</strong>
            <span>{{ signedPrice(quoteHeader.change) }}</span>
            <span>{{ signedPct(quoteHeader.changePct) }}</span>
          </div>
        </div>
        <div class="quote-metrics" aria-label="股票当日行情摘要">
          <article><span>高</span><strong>{{ price(quoteHeader.high) }}</strong></article>
          <article><span>低</span><strong>{{ price(quoteHeader.low) }}</strong></article>
          <article><span>开</span><strong>{{ price(quoteHeader.open) }}</strong></article>
          <article><span>昨收</span><strong>{{ price(quoteHeader.preClose) }}</strong></article>
          <article><span>换手</span><strong>{{ pct(quoteHeader.turnover) }}</strong></article>
          <article><span>量比</span><strong>{{ ratio(quoteHeader.volumeRatio) }}</strong></article>
          <article><span>额</span><strong>{{ formatAmount(quoteHeader.amount) }}</strong></article>
          <article class="quote-time"><span>时间</span><strong>{{ quoteHeader.timeLabel }}</strong></article>
        </div>
      </div>

      <div class="minute-summary">
        <article><span>股票代码</span><strong class="mono">{{ cleanCode || '-' }}</strong></article>
        <article><span>日 K 最新</span><strong>{{ latestDailyDate }}</strong></article>
        <article><span>5m 最新</span><strong>{{ latestMinuteTime }}</strong></article>
        <article><span>当前视图</span><strong>{{ activeRows.length }} 根</strong></article>
        <article><span>查看区间</span><strong>{{ activeRangeLabel }}</strong></article>
        <article class="wide-card"><span>本地文件</span><strong class="path-text">{{ activePathText }}</strong></article>
        <article><span>数据源</span><strong>{{ activeSourceText }}</strong></article>
      </div>
    </el-card>

    <el-card class="dark-card" shadow="never">
      <template #header>
        <div class="card-head">
          <div>
            <p class="eyebrow">Candlestick</p>
            <h2>{{ activeChartTitle }}</h2>
          </div>
          <el-tag effect="dark" class="count-tag">{{ activeRows.length }} 根</el-tag>
        </div>
      </template>
      <div ref="chartEl" class="minute-chart" />
    </el-card>

    <el-card class="dark-card" shadow="never">
      <template #header>
        <div class="card-head">
          <div>
            <p class="eyebrow">Raw Data</p>
            <h2>最新 100 行{{ activeModeLabel }}数据</h2>
          </div>
        </div>
      </template>
      <el-table :data="latestRows" class="minute-table" height="420" empty-text="暂无本地行情数据">
        <el-table-column :label="viewMode === 'daily' ? '日期' : '时间'" min-width="170" fixed>
          <template #default="{ row }">{{ rowTime(row) }}</template>
        </el-table-column>
        <el-table-column prop="open" label="开" align="right" width="100">
          <template #default="{ row }">{{ price(row.open) }}</template>
        </el-table-column>
        <el-table-column prop="high" label="高" align="right" width="100">
          <template #default="{ row }">{{ price(row.high) }}</template>
        </el-table-column>
        <el-table-column prop="low" label="低" align="right" width="100">
          <template #default="{ row }">{{ price(row.low) }}</template>
        </el-table-column>
        <el-table-column prop="close" label="收" align="right" width="100">
          <template #default="{ row }">
            <strong :class="closeClass(row)">{{ price(row.close) }}</strong>
          </template>
        </el-table-column>
        <el-table-column v-if="viewMode === 'daily'" prop="change_pct" label="涨跌幅" align="right" width="110">
          <template #default="{ row }">
            <span :class="numberClass(row.change_pct)">{{ pct(row.change_pct) }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="volume" label="量" align="right" min-width="120">
          <template #default="{ row }">{{ formatVolume(row.volume) }}</template>
        </el-table-column>
        <el-table-column prop="source" label="源" min-width="130">
          <template #default="{ row }">{{ row.source || '-' }}</template>
        </el-table-column>
      </el-table>
    </el-card>
  </section>
</template>

<script setup>
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import * as echarts from 'echarts'

const API = import.meta.env.VITE_API_BASE || 'http://127.0.0.1:8000'

const props = defineProps({
  stockCode: { type: [String, Number], default: '' },
  stockRequest: { type: Number, default: 0 },
})

const normalizeCode = (value) => String(value || '').replace(/\D/g, '').slice(-6)
const code = ref(normalizeCode(props.stockCode) || '600000')
const period = ref('5')
const viewMode = ref(normalizeCode(props.stockCode) ? 'daily' : 'minute')
const minuteRange = ref('day')
const dailyRange = ref(normalizeCode(props.stockCode) ? 'all' : 'month')
const minuteRows = ref([])
const dailyRows = ref([])
const minuteMeta = ref({})
const dailyMeta = ref({})
const loading = ref(false)
const refreshing = ref(false)
const message = ref('')
const messageType = ref('info')
const chartEl = ref(null)
let chart = null
let resizeObserver = null
let renderFrame = 0

const cleanCode = computed(() => normalizeCode(code.value))
const activeRange = computed({
  get: () => viewMode.value === 'daily' ? dailyRange.value : minuteRange.value,
  set: (value) => {
    if (viewMode.value === 'daily') dailyRange.value = value
    else minuteRange.value = value
  },
})
const rangeOptions = computed(() => {
  if (viewMode.value === 'daily') {
    return [
      { label: '1 周', value: 'week' },
      { label: '1 月', value: 'month' },
      { label: '全部', value: 'all' },
    ]
  }
  return [
    { label: '当天', value: 'day' },
    { label: '1 周', value: 'week' },
    { label: '1 月', value: 'month' },
    { label: '全部', value: 'all' },
  ]
})
const activeRangeLabel = computed(() => {
  const option = rangeOptions.value.find((item) => item.value === activeRange.value)
  return option?.label || '全部'
})
const activeRows = computed(() => {
  const rows = viewMode.value === 'daily' ? dailyRows.value : minuteRows.value
  return filterRowsByRange(rows, activeRange.value)
})
const latestRows = computed(() => activeRows.value.slice(-100).reverse())
const activeModeLabel = computed(() => {
  const mode = viewMode.value === 'daily' ? '日 K' : `${minuteMeta.value.period || `${period.value}m`} 分时`
  return `${mode} · ${activeRangeLabel.value}`
})
const activeChartTitle = computed(() => viewMode.value === 'daily' ? `日 K 与成交量 · ${activeRangeLabel.value}` : `5m K 线与成交量 · ${activeRangeLabel.value}`)
const latestDailyDate = computed(() => dailyMeta.value.latest_date || lastValue(dailyRows.value, 'date') || '-')
const latestMinuteTime = computed(() => minuteMeta.value.latest_datetime || lastValue(minuteRows.value, 'datetime') || '-')
const activePathText = computed(() => {
  if (viewMode.value === 'daily') return 'SQLite stock_daily'
  const paths = minuteMeta.value.paths || []
  return paths.length ? paths.join(' / ') : (minuteMeta.value.path || '-')
})
const activeSourceText = computed(() => {
  if (viewMode.value === 'daily') {
    const sources = [...new Set(dailyRows.value.map((row) => row.source).filter(Boolean))]
    return sources.length ? sources.join(' / ') : '-'
  }
  const counts = minuteMeta.value.source_counts || {}
  const items = Object.entries(counts).map(([name, count]) => `${name}:${count}`)
  return items.length ? items.join(' / ') : '-'
})
const latestDailyRow = computed(() => lastRow(dailyRows.value) || {})
const previousDailyRow = computed(() => dailyRows.value.length >= 2 ? dailyRows.value[dailyRows.value.length - 2] : {})
const latestMinuteRow = computed(() => lastRow(minuteRows.value) || {})
const latestMinuteDayRows = computed(() => {
  const latestDay = normalizeDateKey(latestMinuteRow.value.datetime)
  if (!latestDay) return []
  return minuteRows.value.filter((row) => normalizeDateKey(row.datetime) === latestDay)
})
const intradayQuote = computed(() => {
  const rows = latestMinuteDayRows.value
  if (!rows.length) return {}
  const first = rows[0]
  const last = rows[rows.length - 1]
  const highs = rows.map((row) => finiteNumber(row.high)).filter((value) => value !== null)
  const lows = rows.map((row) => finiteNumber(row.low)).filter((value) => value !== null)
  const volume = rows.reduce((sum, row) => sum + (finiteNumber(row.volume) || 0), 0)
  const amount = rows.reduce((sum, row) => sum + (finiteNumber(row.amount) || finiteNumber(row.money) || 0), 0)
  return {
    date: normalizeDateKey(last.datetime),
    datetime: last.datetime || '',
    open: finiteNumber(first.open),
    high: highs.length ? Math.max(...highs) : null,
    low: lows.length ? Math.min(...lows) : null,
    close: finiteNumber(last.close),
    volume,
    amount,
  }
})
const quoteHeader = computed(() => {
  const daily = latestDailyRow.value
  const previousDaily = previousDailyRow.value
  const intraday = intradayQuote.value
  const dailyDay = normalizeDateKey(daily.date)
  const intradayDay = normalizeDateKey(intraday.date || intraday.datetime)
  const useIntraday = intraday.close !== null && (!dailyDay || (intradayDay && intradayDay >= dailyDay))
  const quoteDay = useIntraday ? intradayDay : dailyDay
  const dailyMatchesQuoteDay = dailyDay && quoteDay && dailyDay === quoteDay
  const quotePrice = useIntraday ? finiteNumber(intraday.close) : finiteNumber(daily.close)
  const preClose = dailyMatchesQuoteDay
    ? coalesceNumber(daily.pre_close, previousDaily.close)
    : coalesceNumber(daily.close, daily.pre_close, previousDaily.close)
  const computedChange = quotePrice !== null && preClose !== null ? quotePrice - preClose : null
  const computedChangePct = computedChange !== null && preClose ? (computedChange / preClose) * 100 : null
  return {
    code: cleanCode.value || '-',
    name: String(daily.name || cleanCode.value || '-'),
    date: quoteDay || dailyDay || '',
    price: quotePrice,
    change: computedChange,
    changePct: coalesceNumber(computedChangePct, daily.change_pct),
    open: coalesceNumber(dailyMatchesQuoteDay ? daily.open : null, useIntraday ? intraday.open : null, daily.open),
    high: coalesceNumber(dailyMatchesQuoteDay ? daily.high : null, useIntraday ? intraday.high : null, daily.high),
    low: coalesceNumber(dailyMatchesQuoteDay ? daily.low : null, useIntraday ? intraday.low : null, daily.low),
    preClose,
    turnover: dailyMatchesQuoteDay ? finiteNumber(daily.turnover) : null,
    volumeRatio: dailyMatchesQuoteDay ? finiteNumber(daily.volume_ratio) : null,
    amount: dailyMatchesQuoteDay ? coalesceNumber(daily.amount, intraday.amount) : (useIntraday ? finiteNumber(intraday.amount) : finiteNumber(daily.amount)),
    timeLabel: useIntraday ? (intraday.datetime || quoteDay || '-') : (daily.date || '-'),
  }
})
const quoteToneClass = computed(() => {
  const changePct = Number(quoteHeader.value.changePct)
  if (!Number.isFinite(changePct) || changePct === 0) return ''
  return changePct > 0 ? 'quote-rise' : 'quote-fall'
})

const requestJson = async (path) => {
  const response = await fetch(`${API}${path}`)
  if (!response.ok) {
    const text = await response.text()
    throw new Error(text || response.statusText)
  }
  return response.json()
}

const clearStockData = () => {
  minuteRows.value = []
  dailyRows.value = []
  minuteMeta.value = {}
  dailyMeta.value = {}
  chart?.clear()
  scheduleRenderChart()
}

const loadStockData = async (refreshMinute = false) => {
  const clean = cleanCode.value
  if (clean.length !== 6) {
    showMessage('请输入 6 位股票代码。', 'error')
    return
  }
  if (refreshMinute) refreshing.value = true
  else loading.value = true
  try {
    const minutePath = `/api/data/history_min/${clean}?period=${period.value}&limit=50000${refreshMinute ? '&refresh=true' : ''}`
    const [minuteResult, dailyResult] = await Promise.allSettled([
      requestJson(minutePath),
      requestJson(`/api/data/history/${clean}?limit=360`),
    ])

    const notes = []
    if (minuteResult.status === 'fulfilled') {
      minuteRows.value = minuteResult.value.rows || []
      minuteMeta.value = minuteResult.value
      notes.push(`5m ${minuteRows.value.length} 根`)
    } else {
      minuteRows.value = []
      minuteMeta.value = {}
      notes.push(`5m 失败：${minuteResult.reason.message}`)
    }

    if (dailyResult.status === 'fulfilled') {
      dailyRows.value = dailyResult.value.rows || []
      dailyMeta.value = dailyResult.value
      notes.push(`日K ${dailyRows.value.length} 根`)
    } else {
      dailyRows.value = []
      dailyMeta.value = {}
      notes.push(`日K 失败：${dailyResult.reason.message}`)
    }

    const failed = [minuteResult, dailyResult].some((item) => item.status === 'rejected')
    showMessage(`已查询 ${clean}：${notes.join('，')}。`, failed ? 'warning' : 'info')
    if (viewMode.value === 'minute' && !minuteRows.value.length && dailyRows.value.length) {
      viewMode.value = 'daily'
    }
    await nextTick()
    scheduleRenderChart()
  } finally {
    loading.value = false
    refreshing.value = false
  }
}

const showMessage = (text, type = 'info') => {
  message.value = text
  messageType.value = type
}

const scheduleRenderChart = () => {
  if (renderFrame) cancelAnimationFrame(renderFrame)
  renderFrame = requestAnimationFrame(() => {
    renderFrame = 0
    renderChart()
  })
}

const renderChart = () => {
  if (!chartEl.value) return
  const box = chartEl.value.getBoundingClientRect()
  if (box.width < 240 || box.height < 320) return
  if (!chart) {
    chart = echarts.init(chartEl.value, 'dark', { renderer: 'canvas' })
  }
  chart.resize({ width: Math.round(box.width), height: Math.round(box.height) })
  const rows = activeRows.value
  const times = rows.map((row) => rowTime(row))
  const candles = rows.map((row) => [row.open, row.close, row.low, row.high])
  const volumes = rows.map((row) => ({
    value: row.volume,
    itemStyle: { color: Number(row.close) >= Number(row.open) ? 'rgba(245,34,45,0.72)' : 'rgba(82,196,26,0.72)' },
  }))
  const ma5 = movingAverage(rows, 5)
  const ma10 = movingAverage(rows, 10)
  const ma20 = movingAverage(rows, 20)
  const minZoomSpan = viewMode.value === 'daily' && times.length
    ? Math.min(100, (5 / times.length) * 100)
    : undefined
  const zoomLimit = minZoomSpan ? { minSpan: minZoomSpan } : {}
  chart.setOption({
    backgroundColor: '#0f1117',
    animation: false,
    legend: {
      top: 8,
      left: 18,
      itemWidth: 18,
      itemHeight: 8,
      textStyle: { color: '#9aa6ba', fontSize: 12 },
      data: ['K线', 'MA5', 'MA10', 'MA20', '成交量'],
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' },
      backgroundColor: 'rgba(15,17,23,0.96)',
      borderColor: 'rgba(105,177,255,0.28)',
      borderWidth: 1,
      padding: [10, 12],
      textStyle: { color: '#e1e7f2', fontSize: 12 },
    },
    axisPointer: { link: [{ xAxisIndex: 'all' }] },
    grid: [
      { left: 74, right: 42, top: 48, height: '63%' },
      { left: 74, right: 42, top: '78%', height: '14%' },
    ],
    xAxis: [
      {
        type: 'category',
        data: times,
        boundaryGap: true,
        axisLine: { lineStyle: { color: '#334155' } },
        axisTick: { show: false },
        axisLabel: { color: '#8a94a8', hideOverlap: true },
      },
      {
        type: 'category',
        gridIndex: 1,
        data: times,
        boundaryGap: true,
        axisLabel: { color: '#7f8aa1', hideOverlap: true },
        axisTick: { show: false },
        axisLine: { lineStyle: { color: '#334155' } },
      },
    ],
    yAxis: [
      {
        scale: true,
        splitNumber: 6,
        axisLine: { show: false },
        axisTick: { show: false },
        splitLine: { lineStyle: { color: 'rgba(148,163,184,0.12)' } },
        axisLabel: { color: '#9aa6ba', margin: 12 },
      },
      {
        scale: true,
        gridIndex: 1,
        splitNumber: 3,
        axisLine: { show: false },
        axisTick: { show: false },
        splitLine: { lineStyle: { color: 'rgba(148,163,184,0.08)' } },
        axisLabel: { color: '#7f8aa1', margin: 12, formatter: formatAxisVolume },
      },
    ],
    dataZoom: [
      {
        type: 'inside',
        xAxisIndex: [0, 1],
        start: Math.max(0, 100 - (140 / Math.max(times.length, 140)) * 100),
        end: 100,
        zoomOnMouseWheel: true,
        moveOnMouseWheel: false,
        moveOnMouseMove: true,
        ...zoomLimit,
      },
      {
        show: true,
        xAxisIndex: [0, 1],
        type: 'slider',
        bottom: 8,
        height: 24,
        borderColor: 'rgba(51,65,85,0.9)',
        backgroundColor: 'rgba(15,23,42,0.88)',
        fillerColor: 'rgba(24,144,255,0.22)',
        handleStyle: { color: '#69b1ff', borderColor: '#69b1ff' },
        textStyle: { color: '#7f8aa1' },
        ...zoomLimit,
      },
    ],
    series: [
      {
        type: 'candlestick',
        name: 'K线',
        data: candles,
        barWidth: '58%',
        itemStyle: {
          color: '#f5222d',
          color0: '#52c41a',
          borderColor: '#f5222d',
          borderColor0: '#52c41a',
          borderWidth: 1.2,
        },
      },
      lineSeries('MA5', ma5, '#f5c542'),
      lineSeries('MA10', ma10, '#69b1ff'),
      lineSeries('MA20', ma20, '#b37feb'),
      {
        type: 'bar',
        name: '成交量',
        xAxisIndex: 1,
        yAxisIndex: 1,
        data: volumes,
        barWidth: '62%',
      },
    ],
  }, true)
}

const resizeChart = () => {
  chart?.resize()
  scheduleRenderChart()
}
const finiteNumber = (value) => {
  const num = Number(value)
  return Number.isFinite(num) ? num : null
}
const coalesceNumber = (...values) => {
  for (const value of values) {
    const num = finiteNumber(value)
    if (num !== null) return num
  }
  return null
}
const lastRow = (rows) => rows.length ? rows[rows.length - 1] : null
const price = (value) => Number.isFinite(Number(value)) ? Number(value).toFixed(2) : '-'
const pct = (value) => Number.isFinite(Number(value)) ? `${Number(value).toFixed(2)}%` : '-'
const signedPrice = (value) => {
  const num = Number(value)
  if (!Number.isFinite(num)) return '-'
  return `${num > 0 ? '+' : ''}${num.toFixed(2)}`
}
const signedPct = (value) => {
  const num = Number(value)
  if (!Number.isFinite(num)) return '-'
  return `${num > 0 ? '+' : ''}${num.toFixed(2)}%`
}
const ratio = (value) => {
  const num = Number(value)
  if (!Number.isFinite(num) || num <= 0) return '-'
  return num.toFixed(2)
}
const formatAmount = (value) => {
  const num = Number(value)
  if (!Number.isFinite(num) || num <= 0) return '-'
  if (num >= 100000000) return `${(num / 100000000).toFixed(2)}亿`
  if (num >= 10000) return `${(num / 10000).toFixed(2)}万`
  return num.toFixed(0)
}
const formatVolume = (value) => {
  const num = Number(value)
  if (!Number.isFinite(num)) return '-'
  if (num >= 100000000) return `${(num / 100000000).toFixed(2)}亿`
  if (num >= 10000) return `${(num / 10000).toFixed(2)}万`
  return num.toFixed(0)
}
const closeClass = (row) => {
  const close = Number(row.close)
  const open = Number(row.open)
  if (!Number.isFinite(close) || !Number.isFinite(open) || close === open) return ''
  return close > open ? 'rise' : 'fall'
}
const numberClass = (value) => {
  const num = Number(value)
  if (!Number.isFinite(num) || num === 0) return ''
  return num > 0 ? 'rise' : 'fall'
}
const rowTime = (row) => row.datetime || row.date || '-'
const lastValue = (rows, key) => rows.length ? rows[rows.length - 1]?.[key] : ''
const normalizeDateKey = (value) => {
  const raw = String(value || '').trim()
  if (!raw) return ''
  const compact = raw.match(/^(\d{4})(\d{2})(\d{2})/)
  if (compact) return `${compact[1]}-${compact[2]}-${compact[3]}`
  const dashed = raw.match(/^(\d{4})[-/](\d{2})[-/](\d{2})/)
  if (dashed) return `${dashed[1]}-${dashed[2]}-${dashed[3]}`
  const parsed = new Date(raw.replace(' ', 'T'))
  return Number.isNaN(parsed.getTime()) ? '' : dayKey(parsed)
}
const parseRowDate = (row) => {
  const raw = rowTime(row)
  if (!raw || raw === '-') return null
  const parsed = new Date(String(raw).replace(' ', 'T'))
  return Number.isNaN(parsed.getTime()) ? null : parsed
}
const dayKey = (date) => {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}
const startOfRange = (latest, range) => {
  const start = new Date(latest)
  start.setHours(0, 0, 0, 0)
  if (range === 'week') start.setDate(start.getDate() - 7)
  if (range === 'month') start.setMonth(start.getMonth() - 1)
  return start
}
const filterRowsByRange = (rows, range) => {
  if (range === 'all' || !rows.length) return rows
  const latest = [...rows].reverse().map(parseRowDate).find(Boolean)
  if (!latest) return rows
  if (range === 'day') {
    const latestDay = dayKey(latest)
    return rows.filter((row) => {
      const date = parseRowDate(row)
      return date && dayKey(date) === latestDay
    })
  }
  const start = startOfRange(latest, range)
  return rows.filter((row) => {
    const date = parseRowDate(row)
    return date && date >= start && date <= latest
  })
}
const formatAxisVolume = (value) => {
  const num = Number(value)
  if (!Number.isFinite(num)) return ''
  if (num >= 100000000) return `${(num / 100000000).toFixed(1)}亿`
  if (num >= 10000) return `${(num / 10000).toFixed(0)}万`
  return `${num.toFixed(0)}`
}
const movingAverage = (rows, days) => rows.map((_, index) => {
  if (index < days - 1) return null
  const windowRows = rows.slice(index - days + 1, index + 1)
  const values = windowRows.map((item) => Number(item.close)).filter(Number.isFinite)
  if (values.length !== days) return null
  return Number((values.reduce((sum, value) => sum + value, 0) / days).toFixed(4))
})
const lineSeries = (name, data, color) => ({
  type: 'line',
  name,
  data,
  smooth: false,
  showSymbol: false,
  connectNulls: true,
  lineStyle: { width: 1.4, color },
  emphasis: { focus: 'series' },
})

watch(period, () => {
  if (cleanCode.value.length === 6) loadStockData(false)
})

watch(viewMode, async () => {
  await nextTick()
  scheduleRenderChart()
})

watch(activeRange, async () => {
  await nextTick()
  scheduleRenderChart()
})

watch(() => [props.stockCode, props.stockRequest], async ([value]) => {
  const clean = normalizeCode(value)
  if (clean.length !== 6) return
  const sameCode = clean === cleanCode.value
  code.value = clean
  viewMode.value = 'daily'
  dailyRange.value = 'all'
  if (!sameCode) {
    clearStockData()
    showMessage(`正在查询 ${clean} 行情数据...`)
  }
  await nextTick()
  loadStockData(false)
})

onMounted(async () => {
  await nextTick()
  if (chartEl.value) {
    resizeObserver = new ResizeObserver(resizeChart)
    resizeObserver.observe(chartEl.value)
  }
  window.addEventListener('resize', resizeChart)
  loadStockData(false)
})

onBeforeUnmount(() => {
  if (renderFrame) cancelAnimationFrame(renderFrame)
  resizeObserver?.disconnect()
  window.removeEventListener('resize', resizeChart)
  chart?.dispose()
  chart = null
})
</script>

<style scoped>
.dark-card {
  --el-card-bg-color: var(--terminal-card);
  --el-card-border-color: var(--terminal-border);
  --el-text-color-primary: var(--terminal-text);
  --el-text-color-regular: #a8b3c7;
  border-radius: 8px;
}

.page-stack {
  display: grid;
  gap: 14px;
}

.card-head {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  align-items: center;
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
  color: var(--terminal-text);
  font-size: 1rem;
}

.count-tag {
  border-color: #273246;
  background: #1f2937;
}

.minute-toolbar {
  display: flex;
  gap: 10px;
  flex-wrap: wrap;
  align-items: center;
  margin-bottom: 12px;
}

.code-input {
  max-width: 260px;
}

.period-select {
  width: 130px;
}

.view-toggle {
  min-width: 190px;
}

.range-toggle {
  min-width: 236px;
}

.range-toggle :deep(.el-radio-button__inner),
.view-toggle :deep(.el-radio-button__inner) {
  min-width: 58px;
}

.minute-alert {
  margin-bottom: 12px;
}

.quote-strip {
  display: grid;
  grid-template-columns: 280px minmax(0, 1fr);
  gap: 22px;
  align-items: center;
  min-height: 112px;
  padding: 12px 16px;
  margin-bottom: 12px;
  border-top: 1px solid rgba(148, 163, 184, 0.12);
  border-bottom: 1px solid rgba(148, 163, 184, 0.12);
  background: #11131a;
}

.quote-main {
  min-width: 0;
  display: grid;
  align-content: center;
  gap: 7px;
  padding-right: 18px;
  border-right: 1px solid rgba(148, 163, 184, 0.12);
}

.quote-title-line {
  min-width: 0;
  display: flex;
  align-items: baseline;
  gap: 10px;
}

.quote-title-line strong {
  min-width: 0;
  color: var(--terminal-text);
  font-size: 1.18rem;
  line-height: 1.22;
  font-weight: 900;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.quote-title-line span {
  color: #8fa3c1;
  font-size: 0.78rem;
  font-weight: 800;
}

.quote-price-line {
  display: flex;
  align-items: baseline;
  gap: 10px;
  color: #cbd5e1;
  font-variant-numeric: tabular-nums;
}

.quote-price-line strong {
  color: inherit;
  font-size: 2.45rem;
  line-height: 1;
  font-weight: 900;
  letter-spacing: 0;
}

.quote-price-line span {
  color: inherit;
  font-size: 0.98rem;
  font-weight: 900;
}

.quote-rise .quote-price-line {
  color: var(--quant-rise);
}

.quote-fall .quote-price-line {
  color: var(--quant-fall);
}

.quote-metrics {
  min-width: 0;
  display: grid;
  grid-template-columns: repeat(4, minmax(132px, 1fr));
  gap: 10px 18px;
  align-content: center;
}

.quote-metrics article {
  min-width: 0;
  display: grid;
  grid-template-columns: 38px minmax(0, 1fr);
  gap: 9px;
  align-items: baseline;
}

.quote-metrics span {
  color: #7f8aa1;
  font-size: 0.76rem;
  font-weight: 800;
  line-height: 1.2;
}

.quote-metrics strong {
  color: var(--terminal-text);
  font-size: 1rem;
  line-height: 1.25;
  font-weight: 800;
  font-variant-numeric: tabular-nums;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.quote-time strong {
  font-size: 0.94rem;
}

.minute-summary {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  gap: 8px;
}

.minute-summary article {
  min-width: 0;
  border: 1px solid var(--terminal-border);
  border-radius: 8px;
  padding: 9px 10px;
  background: #11131a;
}

.minute-summary span {
  display: block;
  color: #6f7d95;
  font-size: 0.75rem;
  font-weight: 800;
}

.minute-summary strong {
  display: block;
  margin-top: 6px;
  color: var(--terminal-text);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.wide-card {
  grid-column: span 2;
}

.path-text {
  font-size: 0.78rem;
}

.chart-card {
  overflow: hidden;
  border-color: rgba(105, 177, 255, 0.2);
}

.chart-card :deep(.el-card__header) {
  padding: 12px 16px;
  border-bottom-color: rgba(148, 163, 184, 0.12);
}

.chart-card :deep(.el-card__body) {
  padding: 0;
}

.minute-chart {
  width: 100%;
  height: clamp(620px, 72vh, 820px);
  min-height: 620px;
  background: #0f1117;
}

.minute-table {
  --el-table-bg-color: var(--terminal-card);
  --el-table-tr-bg-color: var(--terminal-card);
  --el-table-header-bg-color: var(--terminal-bg);
  --el-table-row-hover-bg-color: #1b2230;
  --el-table-border-color: rgba(255, 255, 255, 0.07);
  --el-table-text-color: #d7deeb;
  --el-table-header-text-color: #7f8aa1;
  width: 100%;
}

.mono {
  font-family: 'SF Mono', Menlo, Consolas, monospace;
}

.rise {
  color: var(--quant-rise);
}

.fall {
  color: var(--quant-fall);
}

@media (max-width: 1180px) {
  .quote-strip {
    grid-template-columns: 1fr;
    gap: 12px;
  }

  .quote-main {
    padding-right: 0;
    padding-bottom: 12px;
    border-right: 0;
    border-bottom: 1px solid rgba(148, 163, 184, 0.12);
  }

  .minute-summary {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
}

@media (max-width: 820px) {
  .quote-metrics {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .minute-summary {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .wide-card {
    grid-column: 1 / -1;
  }
}

@media (max-width: 680px) {
  .quote-strip {
    padding: 12px;
  }

  .quote-title-line,
  .quote-price-line {
    flex-wrap: wrap;
  }

  .quote-metrics {
    grid-template-columns: 1fr;
  }

  .minute-summary {
    grid-template-columns: 1fr;
  }

  .code-input,
  .period-select,
  .view-toggle,
  .range-toggle {
    width: 100%;
    max-width: none;
  }

  .range-toggle :deep(.el-radio-button),
  .range-toggle :deep(.el-radio-button__inner),
  .view-toggle :deep(.el-radio-button),
  .view-toggle :deep(.el-radio-button__inner) {
    width: 100%;
  }

  .minute-chart {
    height: 560px;
    min-height: 560px;
  }
}
</style>
