<template>
  <div class="terminal-app dark">
    <StatsHeader
      :title="currentTitle"
      :locked-count="todayLockedCount"
      :latest-sync="latestSync"
      :health="health"
      :model-status="radar.model_status"
    >
      <template #actions>
        <section :class="['sniper-safe-box', sniperStatus.enabled ? 'armed' : 'locked']">
          <div class="sniper-copy">
            <span>Mac Sniper</span>
            <strong>{{ sniperStatusText }}</strong>
          </div>
          <el-switch
            v-model="sniperStatus.enabled"
            :loading="busy.sniper"
            size="large"
            inline-prompt
            active-text="ON"
            inactive-text="OFF"
            style="--el-switch-on-color: #f5222d; --el-switch-off-color: #4b5563;"
            @change="toggleSniperStatus"
          />
        </section>
      </template>
    </StatsHeader>

    <div class="main-grid">
      <Sidebar v-model:active="activeSection" :health="health" />

      <main class="content">
        <el-alert
          v-if="visibleMessage"
          :title="message.text"
          :type="message.type === 'error' ? 'error' : 'info'"
          show-icon
          :closable="false"
          class="message-alert"
        />

        <section v-show="activeSection === 'dashboard'" class="page-stack">
          <section class="command-strip">
            <article>
              <span>V4.0 Theme Alpha</span>
              <strong>四大核心军团并行出票</strong>
              <small>全局动量狙击 / 右侧主升浪 / 中线超跌反转 / 尾盘突破各自独立 Top1</small>
            </article>
            <article>
              <span>Snapshot Anchor</span>
              <strong>14:50 快照锁定</strong>
              <small>snapshot_price / snapshot_time 写入后禁止篡改</small>
            </article>
            <article>
              <span>AI Right Brain</span>
              <strong>{{ aiStatusText }}</strong>
              <small>Ollama 只做舆情风控，不改写模型分数</small>
            </article>
            <article>
              <span>Minute Factory</span>
              <strong>JQ 冷数据 + 腾讯热数据</strong>
              <small>15:15 跟随盘后日线同步后自动合并 5m Parquet</small>
            </article>
          </section>

          <section class="cockpit-grid">
            <el-card class="dark-card strategy-matrix" shadow="never">
              <template #header>
                <div class="card-head">
                  <div>
                    <p class="eyebrow">Strategy Legion</p>
                    <h2>四大核心军团并行出票</h2>
                  </div>
                  <span class="terminal-chip">XGBRegressor</span>
                </div>
              </template>
              <div class="matrix-list">
                <article v-for="stat in strategyStats" :key="`matrix-${stat.strategy}`" :class="{ 'strategy-disabled': stat.disabled }">
                  <div class="matrix-title">
                    <span :class="strategyBadgeClass(stat.strategy)">{{ strategyLabel(stat.strategy) }}</span>
                    <strong>{{ stat.disabled ? '已暂停' : `预测 ${stat.count} 次` }}</strong>
                  </div>
                  <div class="matrix-meter">
                    <i :style="{ width: stat.t3WinRateWidth }" />
                  </div>
                  <dl>
                    <div><dt>T+1胜率</dt><dd>{{ stat.t1WinRate }}</dd></div>
                    <div><dt>T+1均值</dt><dd :class="numberClass(stat.t1AvgRaw)">{{ stat.t1Avg }}</dd></div>
                    <div><dt>T+3胜率</dt><dd>{{ stat.t3WinRate }}</dd></div>
                    <div><dt>T+3均值</dt><dd :class="numberClass(stat.t3AvgRaw)">{{ stat.t3Avg }}</dd></div>
                  </dl>
                </article>
              </div>
            </el-card>

            <el-card class="dark-card data-pipeline-card" shadow="never">
              <template #header>
                <div class="card-head">
                  <div>
                    <p class="eyebrow">Data Pipeline</p>
                    <h2>冷热数据流</h2>
                  </div>
                  <span class="terminal-chip">Parquet / SQLite</span>
                </div>
              </template>
              <div class="pipeline-flow">
                <article v-for="node in dataPipelineNodes" :key="node.name">
                  <span>{{ node.phase }}</span>
                  <strong>{{ node.name }}</strong>
                  <small>{{ node.detail }}</small>
                </article>
              </div>
            </el-card>

            <el-card class="dark-card ai-card" shadow="never">
              <template #header>
                <div class="card-head">
                  <div>
                    <p class="eyebrow">AI Right Brain</p>
                    <h2>14:46 舆情风控</h2>
                  </div>
                  <span :class="['terminal-chip', ollamaStatus?.ok ? 'chip-hot' : 'chip-warn']">{{ aiStatusText }}</span>
                </div>
              </template>
              <div class="ai-terminal">
                <p><span>LLM</span><strong>{{ ollamaModelText }}</strong></p>
                <p><span>News Fetcher</span><strong>百度/新浪轻量线索 + 本地兜底</strong></p>
                <p><span>Prompt Guard</span><strong>严格 JSON，失败降级为谨慎复核</strong></p>
                <p><span>Hook</span><strong>14:50 PushPlus Markdown 独立区块</strong></p>
              </div>
            </el-card>

            <el-card class="dark-card sentinel-card" shadow="never">
              <template #header>
                <div class="card-head">
                  <div>
                    <p class="eyebrow">Sentinel Timeline</p>
                    <h2>自动化哨兵时间轴</h2>
                  </div>
                  <span class="terminal-chip">LaunchAgent</span>
                </div>
              </template>
              <ol class="sentinel-timeline">
                <li v-for="task in sentinelTimeline" :key="task.time">
                  <time>{{ task.time }}</time>
                  <div>
                    <strong>{{ task.name }}</strong>
                    <small>{{ task.desc }}</small>
                  </div>
                </li>
              </ol>
            </el-card>
          </section>

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
                <span>等待 14:50 真实快照锁定、09:25 T+1 开盘闭环、15:10 T+3 收盘结算自动写入。</span>
              </div>

              <div v-else class="instruction-grid">
                <article v-for="pick in operationCards" :key="`op-${pick.id}`" class="instruction-card">
                  <header>
                    <span :class="strategyBadgeClass(pick.strategy_type)">{{ strategyLabel(pick.strategy_type) }}</span>
                    <strong :class="instructionClass(pick)">{{ instructionTitle(pick) }}</strong>
                  </header>
                  <p>
                    <StockLink :code="pick.code" :name="pick.name" :label="pick.code" mono class="inline-stock-code" />
                    <StockLink :code="pick.code" :name="pick.name" :label="pick.name" class="inline-stock-name" />
                  </p>
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

          <el-empty v-if="dashboardSignalRows.length === 0" class="radar-empty" description="空仓避险：当前没有达到动态底线的候选股" />

          <section v-else class="dual-board">
            <SelectionTable
              title="短线极速看板"
              eyebrow="T+1 Breakout"
              :rows="dashboardSignalRows"
              mode="short"
              :table-height="360"
              show-inspect
              @inspect="inspectStock"
              @explain="openFactorExplain"
            />
            <SelectionTable
              title="波段策略看板"
              eyebrow="T+3 Swing"
              :rows="dashboardSignalRows"
              mode="swing"
              :table-height="360"
              show-inspect
              @inspect="inspectStock"
              @explain="openFactorExplain"
            />
          </section>

          <el-card v-if="selectedStock" class="dark-card" shadow="never">
            <template #header>
              <div class="card-head">
                <div>
                  <p class="eyebrow">Ollama Risk Control</p>
                  <h2>
                    <StockLink :code="selectedStock.code" :name="selectedStock.name" :label="selectedStock.code" mono class="heading-stock-code" />
                    <StockLink :code="selectedStock.code" :name="selectedStock.name" :label="selectedStock.name" class="heading-stock-name" />
                  </h2>
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
            <el-card
              v-for="stat in strategyStats"
              :key="stat.strategy"
              :class="['dark-card', 'legion-card', { 'strategy-disabled': stat.disabled }]"
              shadow="never"
            >
              <span :class="strategyBadgeClass(stat.strategy)">{{ strategyLabel(stat.strategy) }}</span>
              <strong>{{ stat.disabled ? '已暂停' : `预测 ${stat.count} 次` }}</strong>
              <dl>
                <div><dt>T+1胜率</dt><dd>{{ stat.t1WinRate }}</dd></div>
                <div><dt>T+1均值</dt><dd :class="numberClass(stat.t1AvgRaw)">{{ stat.t1Avg }}</dd></div>
                <div><dt>T+3胜率</dt><dd>{{ stat.t3WinRate }}</dd></div>
                <div><dt>T+3均值</dt><dd :class="numberClass(stat.t3AvgRaw)">{{ stat.t3Avg }}</dd></div>
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
            @explain="openFactorExplain"
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

          <el-card class="dark-card fetch-card" shadow="never">
            <template #header>
              <div class="card-head">
                <div>
                  <p class="eyebrow">JQDATA COLD 5M</p>
                  <h2>聚宽每日获取情况</h2>
                </div>
                <span :class="['terminal-chip', fetchStatusClass(jqFetch)]">{{ jqFetch.status_label || '-' }}</span>
              </div>
            </template>
            <div class="pulse-grid fetch-summary">
              <article><span>最后获取</span><strong>{{ jqFetch.last_fetch_at || '-' }}</strong></article>
              <article><span>本次新增</span><strong>{{ fetchCoverage(jqFetch) }}</strong></article>
              <article><span>失败</span><strong>{{ jqFetch.failed ?? 0 }}</strong></article>
              <article><span>已处理股票</span><strong>{{ jqProcessedCodesText }}</strong></article>
              <article><span>完成月切片</span><strong>{{ jqSegmentProgressText }}</strong></article>
              <article><span>约等价完成</span><strong>{{ jqEquivalentProgressText }}</strong></article>
              <article :title="jqFetch.eta_basis || ''"><span>预计捕捉完成</span><strong>{{ jqEtaText }}</strong></article>
              <article><span>剩余额度</span><strong>{{ jqQuotaText }}</strong></article>
              <article><span>数据区间</span><strong>{{ jqFetch.range || '-' }}</strong></article>
            </div>
          </el-card>

          <el-card class="dark-card fetch-card" shadow="never">
            <template #header>
              <div class="card-head">
                <div>
                  <p class="eyebrow">ASHARE HOT 5M</p>
                  <h2>Ashare 每日获取情况</h2>
                </div>
                <span :class="['terminal-chip', fetchStatusClass(ashareFetch)]">{{ ashareFetch.status_label || '-' }}</span>
              </div>
            </template>
            <div class="pulse-grid fetch-summary">
              <article><span>最后获取</span><strong>{{ ashareFetch.last_fetch_at || '-' }}</strong></article>
              <article><span>今日覆盖</span><strong>{{ fetchCoverage(ashareFetch) }}</strong></article>
              <article><span>失败</span><strong>{{ ashareFetch.failed ?? 0 }}</strong></article>
              <article><span>每股拉取</span><strong>{{ ashareFetch.count ? `${ashareFetch.count} 根` : '-' }}</strong></article>
              <article><span>数据源</span><strong>{{ ashareFetch.source || '-' }}</strong></article>
              <article><span>运行日期</span><strong>{{ ashareFetch.run_date || '-' }}</strong></article>
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

        <section v-show="activeSection === 'account'" class="page-stack account-page">
          <section class="account-summary-grid">
            <article v-for="item in accountSummaryCards" :key="item.label">
              <span>{{ item.label }}</span>
              <strong :class="item.className">{{ item.value }}</strong>
              <small>{{ item.hint }}</small>
            </article>
          </section>

          <section class="account-control-grid">
            <el-card class="dark-card" shadow="never">
              <template #header>
                <div class="card-head">
                  <div>
                    <p class="eyebrow">Shadow Cash</p>
                    <h2>资金池金额</h2>
                  </div>
                  <span class="terminal-chip">Sizing Base</span>
                </div>
              </template>
              <div class="account-form">
                <label>
                  <span>可用资金</span>
                  <el-input-number
                    v-model="accountForm.available_cash"
                    :min="0"
                    :step="1000"
                    :precision="2"
                    controls-position="right"
                  />
                </label>
                <div class="account-actions">
                  <el-button type="primary" :loading="busy.accountCash" @click="saveShadowCash">更新资金池</el-button>
                  <el-button type="warning" plain :loading="busy.accountSync" @click="syncBrokerAccount">同步同花顺资金/持仓</el-button>
                </div>
                <p class="muted">优先从同花顺交易页同步；该金额会直接参与 14:50 自动下单算股。</p>
              </div>
            </el-card>

            <el-card class="dark-card" shadow="never">
              <template #header>
                <div class="card-head">
                  <div>
                    <p class="eyebrow">Auto Fire Drill</p>
                    <h2>全自动休市试射</h2>
                  </div>
                  <span :class="['terminal-chip', sniperStatus.enabled ? 'chip-hot' : '']">
                    {{ sniperStatus.enabled ? 'Unlocked' : 'Locked' }}
                  </span>
                </div>
              </template>
              <div class="account-form test-order-form">
                <label>
                  <span>代码</span>
                  <el-input v-model="testOrderForm.code" maxlength="6" />
                </label>
                <label>
                  <span>名称</span>
                  <el-input v-model="testOrderForm.name" />
                </label>
                <label>
                  <span>参考价</span>
                  <el-input-number v-model="testOrderForm.current_price" :min="0.01" :step="0.1" :precision="2" controls-position="right" disabled />
                </label>
                <label>
                  <span>仓位比例</span>
                  <el-input-number v-model="testOrderForm.position_pct" :min="0.01" :max="1" :step="0.01" :precision="2" controls-position="right" />
                </label>
                <div class="test-order-actions">
                  <el-button type="warning" plain :loading="busy.accountSync" @click="syncBrokerAccount">同步当前交易页</el-button>
                  <el-button :loading="busy.accountPreview" @click="runShadowTestOrder(false)">算股预览</el-button>
                  <el-button type="danger" :loading="busy.accountTest" @click="runShadowTestOrder(true)">全自动休市试射</el-button>
                </div>
                <pre v-if="testOrderPreview" class="account-preview">{{ testOrderPreview }}</pre>
              </div>
            </el-card>
          </section>

          <section class="account-table-grid">
            <el-card class="dark-card" shadow="never">
              <template #header>
                <div class="card-head">
                  <div>
                    <p class="eyebrow">Broker Confirmed Records</p>
                    <h2>本地成交确认记录</h2>
                  </div>
                  <span class="terminal-chip">{{ tradeRecordRows.length }} 条</span>
                </div>
              </template>
              <el-table :data="tradeRecordRows" class="dark-table" height="360">
                <el-table-column prop="fired_at" label="时间" width="168" />
                <el-table-column label="标的" min-width="150">
                  <template #default="{ row }">
                    <strong class="mono-code">{{ row.code }}</strong>
                    <span>{{ row.name || '-' }}</span>
                  </template>
                </el-table-column>
                <el-table-column prop="shares" label="股数" width="90" />
                <el-table-column label="参考价" width="100">
                  <template #default="{ row }">{{ money(row.reference_price) }}</template>
                </el-table-column>
                <el-table-column label="预估成本" width="120">
                  <template #default="{ row }">{{ money(row.estimated_cost) }}</template>
                </el-table-column>
                <el-table-column label="仓位" width="90">
                  <template #default="{ row }">{{ positionPctText(row.position_pct) }}</template>
                </el-table-column>
                <el-table-column prop="source" label="来源" min-width="180" />
              </el-table>
            </el-card>

            <el-card class="dark-card" shadow="never">
              <template #header>
                <div class="card-head">
                  <div>
                    <p class="eyebrow">Open Shadow Positions</p>
                    <h2>当前持仓 / 锁定订单</h2>
                  </div>
                  <span class="terminal-chip">{{ positionRows.length }} 只</span>
                </div>
              </template>
              <el-table :data="positionRows" class="dark-table" height="360">
                <el-table-column label="标的" min-width="150">
                  <template #default="{ row }">
                    <strong class="mono-code">{{ row.code }}</strong>
                    <span>{{ row.name || '-' }}</span>
                  </template>
                </el-table-column>
                <el-table-column prop="shares" label="股数" width="90" />
                <el-table-column label="均价" width="100">
                  <template #default="{ row }">{{ money(row.avg_price) }}</template>
                </el-table-column>
                <el-table-column label="锁定资金" width="120">
                  <template #default="{ row }">{{ money(row.estimated_cost) }}</template>
                </el-table-column>
                <el-table-column prop="status" label="状态" width="110" />
                <el-table-column prop="latest_reserved_at" label="最近发单" width="168" />
              </el-table>
            </el-card>
          </section>
        </section>

        <section v-if="activeSection === 'minute'" class="page-stack">
          <MinKlineViewer
            :key="stockMarketViewerKey"
            :stock-code="stockMarketCode"
            :stock-request="stockMarketRequest"
          />
        </section>
      </main>
    </div>

    <el-drawer
      v-model="factorExplain.visible"
      size="620px"
      direction="rtl"
      modal-class="factor-drawer-modal"
      class="factor-drawer"
      :with-header="false"
    >
      <section class="factor-drawer-head">
        <div>
          <p class="eyebrow">Factor Explain</p>
          <h2>
            <StockLink
              v-if="factorExplain.row"
              :code="factorExplain.row.code"
              :name="factorExplain.row.name"
              :label="factorExplain.row.code"
              mono
              class="heading-stock-code"
            />
            <span>{{ factorExplainTitle }}</span>
          </h2>
        </div>
        <el-button link @click="factorExplain.visible = false">关闭</el-button>
      </section>

      <el-skeleton v-if="factorExplain.loading" animated :rows="8" />
      <el-alert v-else-if="factorExplain.error" :title="factorExplain.error" type="error" show-icon :closable="false" />

      <section v-else-if="factorExplain.payload" class="factor-panel">
        <el-alert
          v-if="factorExplain.payload.partial"
          :title="factorExplain.payload.partial_reasons?.join('；') || '部分解释：输入向量不完整'"
          type="warning"
          show-icon
          :closable="false"
        />

        <div class="factor-summary-grid">
          <article>
            <span>策略</span>
            <strong>{{ strategyLabel(factorExplain.payload.identity?.strategy_type) }}</strong>
          </article>
          <article>
            <span>模型输出</span>
            <strong :class="numberClass(factorExplainSignedPrediction)">
              {{ factorExplain.payload.prediction?.formatted || '-' }}
            </strong>
          </article>
          <article>
            <span>候选排名</span>
            <strong>{{ factorExplain.payload.selection?.rank?.text || '-' }}</strong>
          </article>
          <article>
            <span>训练样本</span>
            <strong>{{ factorDatasetRowsText }}</strong>
          </article>
        </div>

        <section class="factor-section">
          <h3>入选链路</h3>
          <ol class="factor-chain">
            <li v-for="item in factorExplain.payload.selection?.chain || []" :key="`${item.label}-${item.value}`">
              <span>{{ item.label }}</span>
              <strong>{{ item.value }}</strong>
            </li>
          </ol>
        </section>

        <section class="factor-section">
          <h3>单票贡献排行</h3>
          <div class="contribution-list">
            <article v-for="item in factorContributionRows" :key="item.feature" class="contribution-row">
              <div>
                <span>{{ item.rank }}. {{ item.displayLabel }}</span>
                <small>{{ item.displayDescription }}</small>
                <small>{{ item.feature }} / {{ item.formatted_value }}</small>
              </div>
              <strong :class="item.contribution >= 0 ? 'buy' : 'risk'">
                {{ contributionText(item.contribution) }}
              </strong>
            </article>
          </div>
        </section>

        <section class="factor-section split-factor-section">
          <div>
            <h3>正向因子</h3>
            <p v-for="item in factorPositiveRows" :key="`pos-${item.feature}`">
              <span>{{ item.displayLabel }}</span>
              <strong class="buy">{{ contributionText(item.contribution) }}</strong>
            </p>
          </div>
          <div>
            <h3>负向因子</h3>
            <p v-for="item in factorNegativeRows" :key="`neg-${item.feature}`">
              <span>{{ item.displayLabel }}</span>
              <strong class="risk">{{ contributionText(item.contribution) }}</strong>
            </p>
          </div>
        </section>

        <section class="factor-section">
          <h3>模型全局重要因子</h3>
          <div class="importance-grid">
            <article v-for="item in factorGlobalImportanceRows" :key="item.feature">
              <span>{{ item.rank }}. {{ item.displayLabel }}</span>
              <strong>{{ factorNumber(item.importance, 6) }}</strong>
            </article>
          </div>
        </section>

        <section class="factor-section">
          <h3>因子值明细</h3>
          <el-table :data="factorValueRows" class="dark-table factor-value-table" height="280">
            <el-table-column label="因子" min-width="210">
              <template #default="{ row }">
                <div class="factor-name-cell">
                  <strong>{{ row.displayLabel }}</strong>
                  <small>{{ row.displayDescription }}</small>
                  <em>{{ row.feature }}</em>
                </div>
              </template>
            </el-table-column>
            <el-table-column prop="displayGroup" label="分组" width="110" />
            <el-table-column prop="formatted" label="当日值" width="110" align="right" />
            <el-table-column label="贡献" width="110" align="right">
              <template #default="{ row }">
                <strong :class="row.contribution >= 0 ? 'buy' : 'risk'">{{ contributionText(row.contribution) }}</strong>
              </template>
            </el-table-column>
          </el-table>
        </section>

        <section class="factor-section">
          <h3>训练与数据口径</h3>
          <dl class="lineage-list">
            <div v-for="item in factorExplain.payload.data_lineage || []" :key="`${item.label}-${item.value}`">
              <dt>{{ item.label }}</dt>
              <dd>{{ item.value }}</dd>
            </div>
          </dl>
          <p class="factor-note">{{ factorExplain.payload.notes?.[0] }}</p>
        </section>
      </section>
    </el-drawer>
  </div>
</template>

<script setup>
import { ElMessage } from 'element-plus'
import { computed, nextTick, onBeforeUnmount, onMounted, provide, reactive, ref, watch } from 'vue'
import Sidebar from './components/Sidebar.vue'
import StatsHeader from './components/StatsHeader.vue'
import SelectionTable from './components/SelectionTable.vue'
import MinKlineViewer from './components/MinKlineViewer.vue'
import StockLink from './components/StockLink.vue'
import { resolveInitialSection } from './router'

const API = import.meta.env.VITE_API_BASE || 'http://127.0.0.1:8000'
const IGNORE_BROKER_CASH_FOR_TEST_ORDER = false
const VALID_SECTIONS = new Set(['dashboard', 'ledger', 'validation', 'minute', 'account'])

const activeSection = ref(resolveInitialSection())
const overview = ref({})
const health = ref({})
const ollamaStatus = ref({})
const radar = reactive({ rows: [], created_at: '', model_status: '', market_gate: null })
const dailyPicks = reactive({ rows: [] })
const sniperStatus = reactive({ enabled: false })
const shadowAccount = reactive({
  available_cash: 0,
  locked_capital: 0,
  total_shadow_equity: 0,
  positions: [],
  trade_records: [],
  updated_at: '',
})
const brokerSnapshot = reactive({
  account: {},
  order_form: {},
  positions: [],
  synced_at: '',
})
const validation = reactive({ status: '', summary: null, issues: [] })
const minuteFetch = ref({})
const message = reactive({ text: '', type: 'info', scope: 'global' })
const selectedStock = ref(null)
const factorExplain = reactive({ visible: false, loading: false, error: '', payload: null, row: null })
const stockMarketCode = ref('')
const stockMarketRequest = ref(0)
const analysis = ref(null)
const syncResult = ref('')
const validationSample = ref(200)
const sourceCheck = ref(false)
const accountForm = reactive({ available_cash: 30000 })
const testOrderForm = reactive({ code: '002747', name: '埃斯顿', current_price: 19.2, position_pct: 0.25 })
const testOrderPreview = ref('')
const busy = reactive({
  refresh: false,
  scan: false,
  sync: false,
  validate: false,
  analyze: false,
  sniper: false,
  accountCash: false,
  accountSync: false,
  accountPreview: false,
  accountTest: false,
})

const currentTitle = computed(() => ({
  dashboard: '四大军团统一总览',
  ledger: 'Shadow Test 影子账本',
  validation: 'Validation 数据校验',
  minute: '单票行情库',
  account: 'V5.0 资金池',
})[activeSection.value] || '四大军团统一总览')

watch(activeSection, (section) => {
  if (!VALID_SECTIONS.has(section)) activeSection.value = 'dashboard'
}, { immediate: true })
const latestSync = computed(() => overview.value.latest_sync || null)
const localDateText = (date = new Date()) => {
  const y = date.getFullYear()
  const m = String(date.getMonth() + 1).padStart(2, '0')
  const d = String(date.getDate()).padStart(2, '0')
  return `${y}-${m}-${d}`
}
const todayText = computed(() => localDateText())
const todayLockedCount = computed(() => dailyPicks.rows.filter((row) => row.selection_date === todayText.value).length)
const operationCards = computed(() => dailyPicks.rows.filter((row) => row.selection_date === todayText.value).slice(0, 6))
const jqFetch = computed(() => minuteFetch.value.jq || {})
const ashareFetch = computed(() => minuteFetch.value.ashare || {})
const jqProcessedCodesText = computed(() => {
  const codes = Number(jqFetch.value.progress_codes)
  const universe = Number(jqFetch.value.universe)
  if (!Number.isFinite(codes)) return '-'
  if (!Number.isFinite(universe) || universe <= 0) return `${codes} 股`
  return `${codes} / ${universe} 股`
})
const jqSegmentProgressText = computed(() => {
  const done = Number(jqFetch.value.progress_segments)
  const total = Number(jqFetch.value.progress_total_segments)
  const perCode = Number(jqFetch.value.progress_segments_per_code)
  if (!Number.isFinite(done)) return '-'
  const base = Number.isFinite(total) && total > 0 ? `${done} / ${total} 段` : `${done} 段`
  return Number.isFinite(perCode) && perCode > 0 ? `${base} (${perCode}段/股)` : base
})
const jqEquivalentProgressText = computed(() => {
  const equivalentCodes = Number(jqFetch.value.progress_equivalent_codes)
  const universe = Number(jqFetch.value.universe)
  const pct = Number(jqFetch.value.progress_pct)
  if (!Number.isFinite(equivalentCodes)) return '-'
  const base = Number.isFinite(universe) && universe > 0
    ? `${equivalentCodes.toFixed(1)} / ${universe} 股`
    : `${equivalentCodes.toFixed(1)} 股`
  return Number.isFinite(pct) ? `${base} (${pct.toFixed(2)}%)` : base
})
const jqEtaText = computed(() => {
  const date = jqFetch.value.eta_date
  const days = Number(jqFetch.value.eta_days)
  const rate = Number(jqFetch.value.eta_rate_codes_per_day)
  if (!date) return '-'
  const dayText = Number.isFinite(days) ? ` / ${days} 天` : ''
  const rateText = Number.isFinite(rate) ? ` @${rate.toFixed(1)}股/天` : ''
  return `${date}${dayText}${rateText}`
})
const jqQuotaText = computed(() => {
  const spare = Number(jqFetch.value.quota_spare)
  const total = Number(jqFetch.value.quota_total)
  if (!Number.isFinite(spare) || !Number.isFinite(total) || total <= 0) return '-'
  return `${spare} / ${total}`
})
const visibleMessage = computed(() => {
  if (!message.text) return false
  if (message.scope === 'minute') return activeSection.value === 'minute'
  return true
})
const sniperStatusText = computed(() => sniperStatus.enabled ? '🔥 实盘狙击 (物理外挂已解锁)' : '🤫 静默盯盘 (外挂已上锁)')
const positionRows = computed(() => shadowAccount.positions || [])
const tradeRecordRows = computed(() => (shadowAccount.trade_records || []).filter((row) => row.status === 'broker_confirmed'))
const latestTradeText = computed(() => tradeRecordRows.value[0]?.fired_at || '-')
const accountSummaryCards = computed(() => [
  { label: '可用资金', value: money(shadowAccount.available_cash), hint: '下单仓位计算基准', className: 'buy' },
  { label: '锁定资金', value: money(shadowAccount.locked_capital), hint: `${positionRows.value.length} 只持仓/挂起`, className: 'neutral' },
  { label: '影子总资产', value: money(shadowAccount.total_shadow_equity), hint: '可用资金 + 锁定成本', className: '' },
  { label: '最近成交', value: latestTradeText.value, hint: `${tradeRecordRows.value.length} 条 broker confirmed 流水`, className: 'risk' },
])
const LEGION_STRATEGIES = ['全局动量狙击', '右侧主升浪', '中线超跌反转', '尾盘突破']
const PAUSED_STRATEGIES = new Set(['右侧主升浪', '中线超跌反转'])

const normalizedRadarRows = computed(() => radar.rows.map((row) => {
  const winner = row?.raw?.winner || {}
  return {
    ...row,
    id: row?.id || `radar-${row?.strategy_type || '全局动量狙击'}-${row?.code || winner.code || ''}`,
    selection_date: row?.selection_date || row?.date || todayText.value,
    strategy_type: row?.strategy_type || winner.strategy_type || '全局动量狙击',
    snapshot_price: row?.snapshot_price ?? row?.selection_price ?? row?.price ?? row?.close ?? winner.price,
    selection_price: row?.selection_price ?? row?.price ?? row?.close ?? winner.price,
    selection_change: row?.selection_change ?? row?.change ?? row?.pct_chg ?? winner.change,
    composite_score: row?.composite_score ?? row?.global_probability_pct ?? row?.probability_pct ?? winner.composite_score,
    sort_score: row?.sort_score ?? row?.global_probability_pct ?? row?.probability_pct ?? winner.sort_score,
    expected_t3_max_gain_pct: row?.expected_t3_max_gain_pct ?? row?.expected_premium ?? winner.expected_t3_max_gain_pct ?? winner.expected_premium,
    theme_name: row?.core_theme ?? row?.theme_name ?? winner.core_theme ?? winner.theme_name ?? '-',
    theme_pct_chg_3: row?.theme_momentum ?? row?.theme_pct_chg_3 ?? winner.theme_momentum ?? winner.theme_pct_chg_3 ?? null,
    core_theme: row?.core_theme ?? row?.theme_name ?? winner.core_theme ?? winner.theme_name ?? '',
    theme_momentum: row?.theme_momentum ?? row?.theme_pct_chg_3 ?? winner.theme_momentum ?? winner.theme_pct_chg_3 ?? null,
    suggested_position: row?.suggested_position ?? winner.suggested_position ?? null,
    selection_tier: row?.selection_tier ?? winner.selection_tier ?? 'base',
    risk_warning: row?.risk_warning ?? winner.risk_warning ?? '',
    status: row?.status || 'radar_preview',
  }
}).filter((row) => !PAUSED_STRATEGIES.has(row.strategy_type)))

const dashboardSignalRows = computed(() => {
  const merged = []
  const seen = new Set()
  const pushRow = (row) => {
    const key = `${row.selection_date || row.date || todayText.value}-${row.strategy_type || ''}-${row.code || ''}`
    if (seen.has(key)) return
    seen.add(key)
    merged.push(row)
  }
  dailyPicks.rows.filter((row) => row.selection_date === todayText.value).forEach(pushRow)
  normalizedRadarRows.value.forEach(pushRow)
  return merged
})

const strategyStats = computed(() => LEGION_STRATEGIES.map((strategy) => {
  const disabled = PAUSED_STRATEGIES.has(strategy)
  const rows = disabled ? [] : dailyPicks.rows.filter((row) => row.strategy_type === strategy)
  const t1 = metricSummary(rows.map(t1ResultValue).filter((value) => value !== null))
  const t3 = metricSummary(rows.map(t3CloseResultValue).filter((value) => value !== null))
  return {
    strategy,
    isSwing: isSwingStrategy(strategy),
    disabled,
    count: rows.length,
    openCount: rows.filter((row) => !row.is_closed).length,
    t1Count: t1.count,
    t3Count: t3.count,
    t1WinRate: t1.winRate,
    t3WinRate: t3.winRate,
    t1AvgRaw: t1.avgRaw,
    t3AvgRaw: t3.avgRaw,
    t1Avg: t1.avg,
    t3Avg: t3.avg,
    t3WinRateWidth: t3.count ? t3.width : t1.width,
  }
}))
const dataPipelineNodes = computed(() => [
  {
    phase: '15:15',
    name: '5m 热数据归档',
    detail: '盘后随日线同步节奏，腾讯/Ashare 最近 100 根 5m K 线增量 upsert 到 Parquet',
  },
  {
    phase: '14:30',
    name: '诱多快照',
    detail: `尾盘拉升超过 ${latePullTrapText.value} 触发过滤`,
  },
  {
    phase: '14:50',
    name: '四军团雷达',
    detail: '四大核心军团同台竞技，统一 14:50 快照价',
  },
  {
    phase: '15:05',
    name: '盘后同步校验',
    detail: `${overview.value.stock_count ?? 0} 股 / ${overview.value.rows_count ?? 0} 行日线资产`,
  },
])
const sentinelTimeline = [
  { time: '09:16', name: '竞价预热观察', desc: '读取腾讯虚拟匹配价，只推送不写库' },
  { time: '09:21', name: '撤单关闭审计', desc: '虚拟溢价大偏离时预警或超预期提示' },
  { time: '09:25', name: 'T+1 开盘闭环', desc: '尾盘突破从 14:50 买入，T+1 开盘价卖出结算' },
  { time: '15:10', name: 'T+3 收盘结算', desc: '波段策略只按目标交易日 15:00 收盘价闭环' },
  { time: '14:50', name: '多轨出票推送', desc: 'XGBoost 后接 AI 右脑，再推送 PushPlus' },
]
const aiStatusText = computed(() => ollamaStatus.value?.ok ? 'Ollama Online' : 'Ollama Watch')
const ollamaModelText = computed(() => ollamaStatus.value?.model || radar.model_status?.match(/qwen[^; ]+|deepseek[^; ]+/)?.[0] || 'qwen2.5:14b')
const latePullTrapText = computed(() => '4.00%')
const stockMarketViewerKey = computed(() => stockMarketCode.value ? `${stockMarketCode.value}-${stockMarketRequest.value}` : 'default')
const factorExplainTitle = computed(() => {
  const payload = factorExplain.payload
  const row = factorExplain.row || {}
  return payload?.identity?.name || row.name || row.code || '因子解释'
})
const factorContributionRows = computed(() => localizeFactorRows(factorExplain.payload?.feature_contributions || []).slice(0, 12))
const factorPositiveRows = computed(() => localizeFactorRows(factorExplain.payload?.positive_contributions || []).slice(0, 6))
const factorNegativeRows = computed(() => localizeFactorRows(factorExplain.payload?.negative_contributions || []).slice(0, 6))
const factorValueRows = computed(() => localizeFactorRows(factorExplain.payload?.feature_values || []))
const factorGlobalImportanceRows = computed(() => localizeFactorRows(factorExplain.payload?.model?.global_importance || []).slice(0, 10))
const factorDatasetRowsText = computed(() => {
  const dataset = factorExplain.payload?.model?.dataset || {}
  if (dataset.rows) return `${Number(dataset.rows).toLocaleString('zh-CN')} 行`
  if (dataset.train_rows || dataset.test_rows) {
    return `${Number(dataset.train_rows || 0).toLocaleString('zh-CN')} / ${Number(dataset.test_rows || 0).toLocaleString('zh-CN')}`
  }
  return '-'
})
const factorExplainSignedPrediction = computed(() => {
  const value = Number(factorExplain.payload?.prediction?.value)
  if (!Number.isFinite(value)) return null
  return value > 1 ? value : value * 100
})

const FACTOR_GROUP_LABELS = {
  技术因子: '技术因子',
  波动率: '波动率',
  量能流动性: '量能流动性',
  主题强度: '主题强度',
  均线位置: '均线趋势',
  价格动量: '价格动量',
  K线结构: 'K线结构',
  大盘环境: '大盘环境',
}
const FACTOR_EXPLAIN_OVERRIDES = {
  turn: ['换手率', '当天成交活跃度，数值越高表示筹码交换越充分。', '量能流动性'],
  turnover: ['换手率', '当天成交活跃度，数值越高表示筹码交换越充分。', '量能流动性'],
  量比: ['量比', '当前成交量相对近期平均成交量的放大倍数。', '量能流动性'],
  真实涨幅点数: ['当日涨跌幅', '当天价格相对前一交易日的实际涨跌幅。', '价格动量'],
  pricechange: ['价格变动值', '当天价格相对前一交易日的绝对变动。', '价格动量'],
  pctChg: ['当日涨跌幅', '当天收盘价相对前一交易日收盘价的涨跌幅。', '价格动量'],
  change_pct: ['当日涨跌幅', '当天价格相对前一交易日的涨跌幅。', '价格动量'],
  pre_close: ['昨收价', '前一交易日收盘价，是涨跌幅计算基准。', '价格动量'],
  per: ['市盈率', '估值指标，反映价格相对盈利的水平。', '估值因子'],
  pb: ['市净率', '估值指标，反映价格相对净资产的水平。', '估值因子'],
  mktcap: ['总市值', '公司整体市场规模。', '估值因子'],
  nmc: ['流通市值', '可流通股份对应的市场规模。', '估值因子'],
  volume_ratio: ['量比', '成交量相对近期均量的放大倍数。', '量能流动性'],
  实体比例: ['K线实体比例', '收盘价相对开盘价的实体涨跌幅，衡量当天攻击力度。', 'K线结构'],
  上影线比例: ['上影线比例', '冲高回落的幅度，上影线越长说明上方抛压越明显。', 'K线结构'],
  下影线比例: ['下影线比例', '下探后回收的幅度，下影线越长说明低位承接越强。', 'K线结构'],
  日内振幅: ['日内振幅', '当天最高价与最低价之间的波动范围。', '波动率'],
  '5日累计涨幅': ['5日累计涨幅', '最近5个交易日累计涨跌幅，衡量短线动量。', '价格动量'],
  '3日累计涨幅': ['3日累计涨幅', '最近3个交易日累计涨跌幅，衡量短线加速。', '价格动量'],
  '5日均线乖离率': ['5日均线乖离率', '价格相对5日均线的偏离程度，反映短线超涨或回踩。', '均线趋势'],
  '10日均线乖离率': ['10日均线乖离率', '价格相对10日均线的偏离程度。', '均线趋势'],
  '20日均线乖离率': ['20日均线乖离率', '价格相对20日均线的偏离程度。', '均线趋势'],
  '3日平均换手率': ['3日平均换手率', '最近3日平均成交活跃度。', '量能流动性'],
  '5日量能堆积': ['5日量能堆积', '当天成交量相对5日均量的放大程度。', '量能流动性'],
  '10日量比': ['10日量比', '当天成交量相对10日均量的放大程度。', '量能流动性'],
  '3日红盘比例': ['3日红盘比例', '最近3日收红的比例，反映短线持续性。', '价格动量'],
  '5日地量标记': ['5日地量标记', '是否出现近期低成交量，常用于识别缩量洗盘。', '量能流动性'],
  缩量下跌标记: ['缩量下跌标记', '下跌时成交量收缩，通常代表抛压衰减或观望。', '量能流动性'],
  振幅换手比: ['振幅换手比', '单位换手带来的价格波动，用于识别虚拉或筹码松动。', '波动率'],
  缩量大涨标记: ['缩量大涨标记', '上涨但成交量不足，可能代表跟风不足或尾盘虚拉。', '量能流动性'],
  极端下影线标记: ['极端下影线标记', '盘中深跌后明显拉回，反映低位承接或剧烈分歧。', 'K线结构'],
  近3日断头铡刀标记: ['近3日断头铡刀标记', '近期是否出现大阴破位，属于短线风险过滤信号。', 'K线结构'],
  '60日高位比例': ['60日高位比例', '当前价格在近60日高点附近的位置。', '价格动量'],
  高位爆量标记: ['高位爆量标记', '高位区域成交量突然放大，可能代表分歧或派发风险。', '量能流动性'],
  尾盘诱多标记: ['尾盘诱多标记', '尾盘异常拉升但持续性存疑的风险信号。', 'K线结构'],
  market_up_rate: ['全市场上涨占比', '全市场上涨股票占比，用于判断大盘赚钱效应。', '大盘环境'],
  market_avg_change: ['全市场平均涨跌幅', '市场整体平均涨跌幅，用于判断指数环境。', '大盘环境'],
  market_down_count: ['全市场下跌家数', '下跌股票数量，反映市场风险扩散程度。', '大盘环境'],
  body_pct: ['K线实体涨跌幅', '收盘价相对开盘价的实体幅度，衡量当天攻击或回落力度。', 'K线结构'],
  upper_shadow_pct: ['上影线幅度', '冲高回落幅度，越大说明上方抛压越强。', 'K线结构'],
  lower_shadow_pct: ['下影线幅度', '下探回收幅度，越大说明低位承接越强。', 'K线结构'],
  bar_range_pct: ['K线全日振幅', '最高价和最低价的全日波动范围。', '波动率'],
  close_location_value: ['收盘位置强弱', '收盘价在当日高低区间中的位置，越高说明收盘越强。', 'K线结构'],
  gap_pct: ['跳空幅度', '开盘价相对前收盘价的跳空比例。', '价格动量'],
  amount_per_volume: ['单量成交额', '成交额除以成交量，用于近似成交价格结构。', '量能流动性'],
  macd: ['MACD快慢线差', '趋势动量指标，反映中短期均线差。', '价格动量'],
  macd_signal: ['MACD信号线', 'MACD的平滑信号线，用于判断趋势拐点。', '价格动量'],
  macd_hist: ['MACD柱体', 'MACD与信号线差值，反映动量扩张或收缩。', '价格动量'],
  macd_hist_delta: ['MACD柱体变化', 'MACD柱体相对上一日的变化，反映动量边际变化。', '价格动量'],
  MACD_DIF: ['MACD DIF线', '快线与慢线差值，反映趋势动量。', '价格动量'],
  MACD_DEA: ['MACD DEA线', 'DIF的平滑信号线，用于确认趋势。', '价格动量'],
  MACD_hist: ['MACD柱体', 'DIF与DEA差值，反映动量强弱。', '价格动量'],
  obv: ['OBV能量潮', '把成交量按涨跌方向累加，衡量资金推动方向。', '量能流动性'],
  obv_delta_5: ['5日OBV变化', '最近5日能量潮变化，衡量资金流入流出趋势。', '量能流动性'],
  theme_pct_chg_1: ['主题1日涨幅', '所属主题指数最近1日涨跌幅。', '主题强度'],
  theme_pct_chg_3: ['主题3日动量', '所属主题指数最近3日累计涨跌幅。', '主题强度'],
  theme_volatility_5: ['主题5日波动', '所属主题指数最近5日波动程度，越高表示主题分歧越大。', '主题强度'],
  rs_stock_vs_theme: ['个股相对主题强度', '个股涨跌相对所属主题的强弱差。', '主题强度'],
  rs_theme_ema_5: ['主题强度EMA5', '主题相对强度的5日指数平滑值。', '主题强度'],
  f4: ['备用因子F4', '模型元数据中的兼容字段，当前仅作为训练特征保留。', '技术因子'],
  f5: ['备用因子F5', '模型元数据中的兼容字段，当前仅作为训练特征保留。', '技术因子'],
}
const localizeFactorRows = (rows) => rows.map((row) => {
  const info = explainFactor(row.feature, row.label, row.group)
  return {
    ...row,
    displayLabel: info.label,
    displayDescription: info.description,
    displayGroup: info.group,
  }
})
const explainFactor = (feature, fallbackLabel = '', fallbackGroup = '') => {
  const key = String(feature || '').trim()
  const override = FACTOR_EXPLAIN_OVERRIDES[key]
  if (override) return factorInfo(override[0], override[1], override[2] || fallbackGroup)

  let match = key.match(/^MA(\d+)$/)
  if (match) return factorInfo(`${match[1]}日均线`, `${match[1]}日移动平均价，用来衡量该周期趋势中枢。`, '均线趋势')
  match = key.match(/^ma_bias_(\d+)$/)
  if (match) return factorInfo(`${match[1]}日均线乖离`, `收盘价相对${match[1]}日均线的偏离程度，判断超涨、回踩或趋势强度。`, '均线趋势')
  match = key.match(/^ema_bias_(\d+)$/)
  if (match) return factorInfo(`${match[1]}日EMA乖离`, `收盘价相对${match[1]}日指数均线的偏离程度，对近期价格更敏感。`, '均线趋势')
  match = key.match(/^rsi_(\d+)$/)
  if (match) return factorInfo(`${match[1]}日RSI强弱`, `${match[1]}日相对强弱指标，用于衡量超买、超卖和动量强弱。`, '价格动量')
  match = key.match(/^atr_pct_(\d+)$/)
  if (match) return factorInfo(`${match[1]}日ATR波动率占比`, `${match[1]}日真实波幅相对股价的比例，衡量近期波动风险。`, '波动率')
  match = key.match(/^atr_(\d+)$/)
  if (match) return factorInfo(`${match[1]}日真实波幅`, `${match[1]}日平均真实波幅，衡量价格绝对波动。`, '波动率')
  match = key.match(/^boll_width_(\d+)$/)
  if (match) return factorInfo(`${match[1]}日布林带宽度`, `${match[1]}日布林带上下轨距离，衡量波动扩张或收敛。`, '波动率')
  match = key.match(/^boll_pos_(\d+)$/)
  if (match) return factorInfo(`${match[1]}日布林位置`, `收盘价在${match[1]}日布林带区间中的位置。`, '价格动量')
  match = key.match(/^donchian_pos_(\d+)$/)
  if (match) return factorInfo(`${match[1]}日通道位置`, `收盘价在近${match[1]}日高低通道中的相对位置。`, '价格动量')
  match = key.match(/^volume_ratio_(\d+)$/)
  if (match) return factorInfo(`${match[1]}日量比`, `成交量相对${match[1]}日均量的放大倍数。`, '量能流动性')
  match = key.match(/^amount_ratio_(\d+)$/)
  if (match) return factorInfo(`${match[1]}日成交额比`, `成交额相对${match[1]}日均成交额的放大倍数。`, '量能流动性')
  match = key.match(/^turn_mean_(\d+)$/)
  if (match) return factorInfo(`${match[1]}日平均换手`, `最近${match[1]}日平均换手率，衡量持续成交活跃度。`, '量能流动性')
  match = key.match(/^ret_mean_(\d+)$/)
  if (match) return factorInfo(`${match[1]}日平均收益`, `最近${match[1]}日平均日收益，衡量趋势方向。`, '价格动量')
  match = key.match(/^ret_std_(\d+)$/)
  if (match) return factorInfo(`${match[1]}日收益波动`, `最近${match[1]}日收益率标准差，衡量短期价格波动。`, '波动率')
  match = key.match(/^close_zscore_(\d+)$/)
  if (match) return factorInfo(`${match[1]}日收盘价Z分位`, `收盘价相对${match[1]}日均值和波动的标准化位置。`, '价格动量')
  match = key.match(/^range_pct_(\d+)$/)
  if (match) return factorInfo(`${match[1]}日区间振幅`, `近${match[1]}日最高最低区间相对价格的波动范围。`, '波动率')
  match = key.match(/^typical_bias_(\d+)$/)
  if (match) return factorInfo(`${match[1]}日典型价乖离`, `典型价相对${match[1]}日均值的偏离，综合高低收位置。`, '价格动量')
  match = key.match(/^momentum_(\d+)d$/)
  if (match) return factorInfo(`${match[1]}日动量`, `收盘价相对${match[1]}日前的涨跌幅。`, '价格动量')
  match = key.match(/^cumret_(\d+)d$/)
  if (match) return factorInfo(`${match[1]}日累计收益`, `最近${match[1]}日累计收益率。`, '价格动量')
  match = key.match(/^up_days_(\d+)d$/)
  if (match) return factorInfo(`${match[1]}日上涨天数`, `最近${match[1]}日中收涨的交易日数量。`, '价格动量')
  match = key.match(/^down_days_(\d+)d$/)
  if (match) return factorInfo(`${match[1]}日下跌天数`, `最近${match[1]}日中收跌的交易日数量。`, '价格动量')

  const label = fallbackLabel && fallbackLabel !== key ? fallbackLabel : key
  const group = FACTOR_GROUP_LABELS[fallbackGroup] || fallbackGroup || '技术因子'
  return factorInfo(label, '该字段为模型输入特征，当前仅做前端中文展示，不改变后端解释结果。', group)
}
const factorInfo = (label, description, group) => ({
  label,
  description,
  group: FACTOR_GROUP_LABELS[group] || group || '技术因子',
})

const normalizeStockCode = (value) => String(value || '').replace(/\D/g, '').slice(-6)
const openStockMarket = async (stock) => {
  const clean = normalizeStockCode(stock?.code)
  if (clean.length !== 6) {
    setMessage(`无法跳转行情库：非法股票代码 ${stock?.code || '-'}`, 'error')
    return
  }
  stockMarketCode.value = clean
  stockMarketRequest.value += 1
  activeSection.value = 'minute'
  setMessage(`已跳转到行情库：${stock?.name || clean}(${clean})。`, 'info', 'minute')
  await nextTick()
  window.scrollTo({ top: 0, behavior: 'smooth' })
}
const handleStockMarketEvent = (event) => openStockMarket(event.detail || {})

provide('openStockMarket', openStockMarket)

const request = async (path, options = {}) => {
  let response
  try {
    response = await fetch(`${API}${path}`, { cache: 'no-store', ...options })
  } catch (error) {
    throw new Error(`后端连接失败：${error.message || error}`)
  }
  if (!response.ok) {
    const text = await response.text()
    throw new Error(text || response.statusText)
  }
  return response.json()
}
const formatApiError = (error) => {
  const raw = String(error?.message || error || '未知错误')
  try {
    const parsed = JSON.parse(raw)
    const detail = parsed.detail
    if (typeof detail === 'string') return detail
    if (detail?.stderr) return detail.stderr
    if (detail?.hint) return detail.hint
    if (detail?.error) return detail.error
    if (detail?.status) return `执行状态：${detail.status}`
    return JSON.stringify(detail || parsed)
  } catch {
    return raw
  }
}
const setMessage = (text, type = 'info', scope = 'global') => {
  message.text = text
  message.type = type
  message.scope = scope
}
const refreshAll = async () => {
  busy.refresh = true
  try {
    await Promise.all([
      loadHealth(),
      loadOllamaStatus(),
      loadOverview(),
      loadRadarCache(),
      loadDailyPicks(),
      loadMinuteFetchStatus(),
      loadSniperStatus(),
      loadShadowAccount(),
    ])
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
const loadOllamaStatus = async () => {
  try {
    ollamaStatus.value = await request('/api/ollama/status')
  } catch (error) {
    ollamaStatus.value = { ok: false, error: error.message }
  }
}
const loadOverview = async () => {
  overview.value = await request('/api/overview')
}
const loadMinuteFetchStatus = async () => {
  minuteFetch.value = await request('/api/data/minute-fetch/status')
}
const loadSniperStatus = async () => {
  try {
    const data = await request('/api/sniper/status')
    sniperStatus.enabled = Boolean(data.enabled)
  } catch (error) {
    sniperStatus.enabled = false
    setMessage(`物理外挂状态读取失败：${error.message}`, 'error')
  }
}
const applyShadowAccount = (data) => {
  shadowAccount.available_cash = Number(data.available_cash || 0)
  shadowAccount.locked_capital = Number(data.locked_capital || 0)
  shadowAccount.total_shadow_equity = Number(data.total_shadow_equity || 0)
  shadowAccount.positions = data.positions || []
  shadowAccount.trade_records = data.trade_records || []
  shadowAccount.updated_at = data.updated_at || ''
  accountForm.available_cash = shadowAccount.available_cash
}
const applyBrokerSnapshot = (snapshot = {}) => {
  brokerSnapshot.account = snapshot.account || {}
  brokerSnapshot.order_form = snapshot.order_form || {}
  brokerSnapshot.positions = snapshot.positions || []
  brokerSnapshot.synced_at = snapshot.synced_at || new Date().toLocaleString()
  const orderForm = brokerSnapshot.order_form || {}
  const code = normalizeStockCode(orderForm.code)
  if (code.length === 6) testOrderForm.code = code
  if (orderForm.name) testOrderForm.name = orderForm.name
  const price = Number(orderForm.current_price || orderForm.limit_price)
  if (Number.isFinite(price) && price > 0) testOrderForm.current_price = price
  const availableCash = Number((brokerSnapshot.account || {}).available_cash)
  if (!IGNORE_BROKER_CASH_FOR_TEST_ORDER && Number.isFinite(availableCash) && availableCash >= 0) accountForm.available_cash = availableCash
}
const loadShadowAccount = async () => {
  try {
    applyShadowAccount(await request('/api/shadow-account'))
  } catch (error) {
    setMessage(`资金池读取失败：${formatApiError(error)}`, 'error')
  }
}
const toggleSniperStatus = async (enabled) => {
  const nextValue = Boolean(enabled)
  const previous = !nextValue
  busy.sniper = true
  try {
    const data = await request('/api/sniper/toggle', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ enabled: nextValue }),
    })
    sniperStatus.enabled = Boolean(data.enabled)
    ElMessage({
      message: '物理外挂状态已更新',
      type: sniperStatus.enabled ? 'error' : 'info',
      duration: 1800,
    })
  } catch (error) {
    sniperStatus.enabled = previous
    ElMessage.error(`物理外挂状态更新失败：${formatApiError(error)}`)
  } finally {
    busy.sniper = false
  }
}
const saveShadowCash = async () => {
  busy.accountCash = true
  try {
    const data = await request('/api/shadow-account/cash', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ available_cash: accountForm.available_cash }),
    })
    applyShadowAccount(data)
    ElMessage.success('资金池金额已更新')
  } catch (error) {
    ElMessage.error(`资金池更新失败：${formatApiError(error)}`)
  } finally {
    busy.accountCash = false
  }
}
const syncBrokerAccount = async () => {
  busy.accountSync = true
  const previousCash = accountForm.available_cash
  try {
    const data = await request('/api/shadow-account/sync-broker', { method: 'POST' })
    if (data.shadow_account) applyShadowAccount(data.shadow_account)
    if (data.broker_snapshot) applyBrokerSnapshot(data.broker_snapshot)
    if (IGNORE_BROKER_CASH_FOR_TEST_ORDER) accountForm.available_cash = previousCash
    const form = data.broker_snapshot?.order_form || {}
    const brokerCount = Number(data.broker_snapshot?.position_count ?? data.broker_snapshot?.positions?.length ?? 0)
    const shadowCount = Number(data.shadow_account?.position_count ?? data.shadow_account?.positions?.length ?? 0)
    const warning = data.shadow_account?.broker_sync_warning || data.broker_snapshot?.parse_warning || data.warning
    if (warning) {
      ElMessage.warning(warning)
    } else {
      ElMessage.success(`同花顺同步完成：券商持仓 ${brokerCount} 只，本地持仓 ${shadowCount} 只。${form.code || ''} ${form.current_price ? money(form.current_price) : ''}`.trim())
    }
  } catch (error) {
    ElMessage.error(`同花顺同步失败：${formatApiError(error)}`)
  } finally {
    busy.accountSync = false
  }
}
const runShadowTestOrder = async (execute) => {
  const key = execute ? 'accountTest' : 'accountPreview'
  busy[key] = true
  try {
    const data = await request('/api/shadow-account/test_order', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        code: testOrderForm.code,
        name: testOrderForm.name,
        current_price: testOrderForm.current_price,
        position_pct: testOrderForm.position_pct,
        available_cash: accountForm.available_cash,
        execute,
      }),
    })
    if (data.shadow_account) applyShadowAccount(data.shadow_account)
    testOrderPreview.value = JSON.stringify({
      status: data.status,
      order: data.order,
      mac_sniper: data.mac_sniper,
    }, null, 2)
    if (execute) {
      const alertMessage = data.mac_sniper?.broker_alert?.message
      ElMessage.success(alertMessage ? `券商弹窗已记录：${alertMessage}` : (data.message || '全自动试射指令已发送'))
    } else {
      ElMessage.success(`算股预览完成：${data.order?.shares || 0} 股`)
    }
  } catch (error) {
    ElMessage.error(`${execute ? '全自动试射失败' : '算股预览失败'}：${formatApiError(error)}`)
  } finally {
    busy[key] = false
  }
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
const explainRequestRow = (row = {}) => ({
  id: row.id,
  code: row.code,
  name: row.name,
  date: row.date,
  selection_date: row.selection_date,
  strategy_type: row.strategy_type,
  source: row.raw?.source || row.status || row.ledger_view || 'frontend',
  turnover: row.turnover,
  turn: row.turn,
  volume_ratio: row.volume_ratio,
  snapshot_vol_ratio: row.snapshot_vol_ratio,
  change: row.change,
  selection_change: row.selection_change,
  pct_chg: row.pct_chg,
  expected_premium: row.expected_premium,
  expected_t3_max_gain_pct: row.expected_t3_max_gain_pct,
  composite_score: row.composite_score,
  sort_score: row.sort_score,
  suggested_position: row.suggested_position,
  selection_tier: row.selection_tier,
  risk_warning: row.risk_warning,
  theme_momentum_3d: row.theme_momentum_3d,
  theme_momentum: row.theme_momentum,
  theme_pct_chg_3: row.theme_pct_chg_3,
  tech_features: row.tech_features,
  trend_features: row.trend_features,
  market_context: row.market_context,
  raw: row.raw,
})
const openFactorExplain = async (row) => {
  factorExplain.visible = true
  factorExplain.loading = true
  factorExplain.error = ''
  factorExplain.payload = null
  factorExplain.row = row
  const selectionDate = row?.selection_date || row?.date || ''
  try {
    factorExplain.payload = await request('/api/explain/pick', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        code: row?.code,
        date: selectionDate,
        strategy_type: row?.strategy_type,
        source: row?.raw?.source || row?.status || row?.ledger_view || 'frontend',
        months: 12,
        row: explainRequestRow(row),
      }),
    })
  } catch (error) {
    factorExplain.error = `因子解释失败：${formatApiError(error)}`
  } finally {
    factorExplain.loading = false
  }
}
const normalizeBacktestLedgerRows = (rows) => (rows || []).map((row, index) => {
  const swing = isSwingStrategy(row)
  const selectionDate = row.date || row.selection_date || ''
  const targetDate = swing ? (row.t3_exit_date || row.next_date || selectionDate) : (row.next_date || selectionDate)
  const position = row.suggested_position ?? (swing ? 0.30 : 0.10)
  const winner = row.raw?.winner || {}
  const t3CloseReturn = row.t3_close_return_pct ?? winner.t3_close_return_pct ?? null
  const t3SettlementReturn = row.t3_settlement_return_pct ?? winner.t3_settlement_return_pct ?? t3CloseReturn
  const t3SettlementPrice = row.t3_settlement_price ?? winner.t3_settlement_price ?? row.t3_close ?? winner.t3_close ?? null
  const resultPct = swing ? t3SettlementReturn : row.open_premium
  const resultPrice = swing ? t3SettlementPrice : row.next_open
  const coreTheme = row.core_theme || row.theme_name || winner.core_theme || winner.theme_name || ''
  const themeMomentum =
    row.theme_momentum_3d ?? row.theme_momentum ?? row.theme_pct_chg_3 ??
    winner.theme_momentum_3d ?? winner.theme_momentum ?? winner.theme_pct_chg_3 ?? null
  return {
    ...row,
    id: row.id || `v55-${selectionDate}-${row.strategy_type}-${row.code}-${index}`,
    selection_date: selectionDate,
    target_date: targetDate,
    selected_at: `${selectionDate}T15:00:00`,
    snapshot_time: '15:00',
    snapshot_price: row.snapshot_price ?? row.close,
    selection_price: row.selection_price ?? row.close,
    selection_change: row.selection_change ?? row.change,
    expected_t3_max_gain_pct: swing ? row.expected_premium : null,
    close_date: targetDate,
    close_reason: swing ? 'T+3收盘收益回测' : 'T+1开盘回测',
    close_price: resultPrice,
    close_return_pct: resultPct,
    is_closed: row.success !== null && row.success !== undefined,
    status: 'historical_backtest_closed',
    suggested_position: position,
    selection_tier: row.selection_tier || 'base',
    core_theme: coreTheme,
    theme_name: coreTheme,
    theme_momentum: themeMomentum,
    theme_momentum_3d: themeMomentum,
    theme_pct_chg_3: themeMomentum,
    raw: {
      ...(row.raw || {}),
      source: 'top_pick_backtest_m12_cache',
      winner: {
        ...winner,
        ...row,
        suggested_position: position,
        selection_tier: row.selection_tier || 'base',
        core_theme: coreTheme,
        theme_name: coreTheme,
        theme_momentum: themeMomentum,
        theme_momentum_3d: themeMomentum,
        theme_pct_chg_3: themeMomentum,
        t3_max_gain_pct: row.t3_max_gain_pct ?? winner.t3_max_gain_pct ?? null,
        t3_close: row.t3_close ?? winner.t3_close ?? null,
        t3_close_return_pct: t3CloseReturn,
        t3_settlement_price: t3SettlementPrice,
        t3_settlement_return_pct: t3SettlementReturn,
      },
    },
  }
})
const normalizeSentinel5mLedgerRows = (rows) => (rows || []).map((row, index) => {
  const selectionDate = row.selection_date || row.date || ''
  const winner = row.raw?.winner || {}
  const closeReturn = row.close_return_pct ?? row.yield_pct ?? row.t3_settlement_return_pct ?? winner.t3_settlement_return_pct ?? null
  const closePrice = row.close_price ?? row.exit_price ?? row.t3_settlement_price ?? winner.t3_settlement_price ?? null
  const closeDate = row.close_date || String(row.close_time || row.exit_time || '').slice(0, 10) || row.target_date || ''
  const position = row.suggested_position ?? winner.suggested_position ?? (row.selection_tier === 'dynamic_floor' ? 0.05 : 0.10)
  const coreTheme = row.core_theme || row.theme_name || winner.core_theme || winner.theme_name || ''
  const sellStrategy = row.sell_strategy || row.exit_policy || winner.sell_strategy || winner.exit_policy || row.close_reason || row.exit_reason || ''
  const themeMomentum =
    row.theme_momentum_3d ?? row.theme_momentum ?? row.theme_pct_chg_3 ??
    winner.theme_momentum_3d ?? winner.theme_momentum ?? winner.theme_pct_chg_3 ?? null
  return {
    ...row,
    id: row.id || `sentinel5m-${selectionDate}-${row.strategy_type}-${row.code}-${index}`,
    selection_date: selectionDate,
    target_date: row.target_date || row.t3_date || closeDate || selectionDate,
    selected_at: row.selected_at || `${selectionDate}T14:50:00`,
    snapshot_time: row.snapshot_time || '14:50:00',
    snapshot_price: row.snapshot_price ?? row.selection_price ?? row.price ?? row.cost_price,
    selection_price: row.selection_price ?? row.snapshot_price ?? row.price ?? row.cost_price,
    expected_t3_max_gain_pct: row.expected_t3_max_gain_pct ?? winner.expected_t3_max_gain_pct ?? row.expected_premium ?? null,
    close_date: closeDate,
    close_reason: row.close_reason || row.exit_reason || '5m回放结算',
    sell_strategy: sellStrategy,
    exit_policy: sellStrategy,
    close_price: closePrice,
    close_return_pct: closeReturn,
    t3_settlement_price: row.t3_settlement_price ?? closePrice,
    t3_settlement_return_pct: row.t3_settlement_return_pct ?? closeReturn,
    t3_max_gain_pct: row.t3_max_gain_pct ?? row.highest_gain_pct ?? winner.t3_max_gain_pct ?? null,
    is_closed: closeReturn !== null && closeReturn !== undefined,
    status: row.status || 'sentinel_5m_closed',
    suggested_position: position,
    selection_tier: row.selection_tier || row.tier || winner.selection_tier || 'base',
    core_theme: coreTheme,
    theme_name: coreTheme,
    theme_momentum: themeMomentum,
    theme_momentum_3d: themeMomentum,
    theme_pct_chg_3: themeMomentum,
    raw: {
      ...(row.raw || {}),
      source: 'sentinel_5m_backtest',
      winner: {
        ...winner,
        ...row,
        suggested_position: position,
        selection_tier: row.selection_tier || row.tier || winner.selection_tier || 'base',
        core_theme: coreTheme,
        theme_name: coreTheme,
        theme_momentum: themeMomentum,
        theme_momentum_3d: themeMomentum,
        theme_pct_chg_3: themeMomentum,
        t3_max_gain_pct: row.t3_max_gain_pct ?? row.highest_gain_pct ?? winner.t3_max_gain_pct ?? null,
        t3_settlement_price: row.t3_settlement_price ?? closePrice,
        t3_settlement_return_pct: row.t3_settlement_return_pct ?? closeReturn,
        sell_strategy: sellStrategy,
        exit_policy: sellStrategy,
      },
    },
  }
})
const normalizeDailyPickLedgerRows = (rows) => (rows || []).map((row, index) => {
  const selectionDate = row.selection_date || row.date || ''
  const winner = row.raw?.winner || {}
  const closeReturn =
    row.close_return_pct ??
    row.t3_settlement_return_pct ??
    winner.t3_settlement_return_pct ??
    row.t3_close_return_pct ??
    winner.t3_close_return_pct ??
    row.open_premium ??
    null
  const closePrice =
    row.close_price ??
    row.t3_settlement_price ??
    winner.t3_settlement_price ??
    row.t3_close ??
    winner.t3_close ??
    row.open_price ??
    row.next_open ??
    null
  const closeDate =
    row.close_date ||
    String(row.close_time || row.close_checked_at || '').slice(0, 10) ||
    row.target_date ||
    winner.t3_exit_date ||
    winner.next_date ||
    ''
  const position = row.suggested_position ?? winner.suggested_position ?? (row.selection_tier === 'dynamic_floor' ? 0.05 : null)
  const selectionTier = row.selection_tier || row.tier || winner.selection_tier || 'base'
  const coreTheme = row.core_theme || row.theme_name || winner.core_theme || winner.theme_name || ''
  const themeMomentum =
    row.theme_momentum_3d ?? row.theme_momentum ?? row.theme_pct_chg_3 ??
    winner.theme_momentum_3d ?? winner.theme_momentum ?? winner.theme_pct_chg_3 ?? null
  const sellStrategy =
    row.sell_strategy ||
    row.exit_policy ||
    winner.sell_strategy ||
    winner.exit_policy ||
    row.close_reason ||
    (row.is_closed ? '真实账本闭环结算' : (isSwingStrategy(row) ? '真实账本T+3观察中' : '真实账本T+1待闭环'))
  const isClosed = Boolean(row.is_closed || (closeReturn !== null && closeReturn !== undefined))
  return {
    ...row,
    id: row.id || `daily-pick-${selectionDate}-${row.strategy_type}-${row.code}-${index}`,
    selection_date: selectionDate,
    target_date: row.target_date || winner.t3_exit_date || winner.next_date || closeDate || selectionDate,
    selected_at: row.selected_at || `${selectionDate}T14:50:00`,
    snapshot_time: row.snapshot_time || '14:50:00',
    snapshot_price: row.snapshot_price ?? row.selection_price ?? row.price ?? winner.price,
    selection_price: row.selection_price ?? row.snapshot_price ?? row.price ?? winner.price,
    expected_t3_max_gain_pct: row.expected_t3_max_gain_pct ?? winner.expected_t3_max_gain_pct ?? null,
    close_date: closeDate,
    close_reason: row.close_reason || (isClosed ? '真实账本闭环结算' : ''),
    sell_strategy: sellStrategy,
    exit_policy: sellStrategy,
    close_price: closePrice,
    close_return_pct: closeReturn,
    t3_settlement_price: row.t3_settlement_price ?? closePrice,
    t3_settlement_return_pct: row.t3_settlement_return_pct ?? closeReturn,
    t3_max_gain_pct: row.t3_max_gain_pct ?? winner.t3_max_gain_pct ?? null,
    is_closed: isClosed,
    suggested_position: position,
    selection_tier: selectionTier,
    tier: row.tier || selectionTier,
    core_theme: coreTheme,
    theme_name: coreTheme,
    theme_momentum: themeMomentum,
    theme_momentum_3d: themeMomentum,
    theme_pct_chg_3: themeMomentum,
    raw: {
      ...(row.raw || {}),
      source: row.raw?.source || 'daily_picks',
      winner: {
        ...winner,
        ...row,
        suggested_position: position,
        selection_tier: selectionTier,
        core_theme: coreTheme,
        theme_name: coreTheme,
        theme_momentum: themeMomentum,
        theme_momentum_3d: themeMomentum,
        theme_pct_chg_3: themeMomentum,
        t3_settlement_price: row.t3_settlement_price ?? closePrice,
        t3_settlement_return_pct: row.t3_settlement_return_pct ?? closeReturn,
        sell_strategy: sellStrategy,
        exit_policy: sellStrategy,
      },
    },
  }
})
const loadDailyPicks = async () => {
  const data = await request('/api/daily-picks?view=strategy_top1&limit=1000')
  dailyPicks.rows = normalizeDailyPickLedgerRows(
    (data.rows || []).filter((row) => !PAUSED_STRATEGIES.has(row.strategy_type))
  )
}
const scanRadar = async () => {
  busy.scan = true
  try {
    const data = await request('/api/radar/scan?limit=1')
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
    await loadMinuteFetchStatus()
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
    await loadMinuteFetchStatus()
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
  return strategy === '中线超跌反转' || strategy === '右侧主升浪' || strategy === '全局动量狙击'
}
const toResultNumber = (value) => {
  if (value === null || value === undefined || value === '') return null
  const num = Number(value)
  return Number.isFinite(num) ? num : null
}
const t1ResultValue = (row) => toResultNumber(row.open_premium)
const t3CloseResultValue = (row) => {
  const rawSettlement = toResultNumber(row.raw?.winner?.t3_settlement_return_pct)
  if (rawSettlement !== null) return rawSettlement
  const settlement = toResultNumber(row.t3_settlement_return_pct)
  if (settlement !== null) return settlement
  const rawT3Close = toResultNumber(row.raw?.winner?.t3_close_return_pct)
  if (rawT3Close !== null) return rawT3Close
  if (isSwingStrategy(row)) return toResultNumber(row.close_return_pct)
  return null
}
const metricSummary = (values) => {
  const clean = values.filter((value) => value !== null)
  const count = clean.length
  const wins = clean.filter((value) => value > 0).length
  const avgRaw = count ? clean.reduce((sum, value) => sum + value, 0) / count : null
  const winPct = count ? (wins / count) * 100 : null
  return {
    count,
    winRate: winPct === null ? '-' : pct(winPct),
    width: winPct === null ? '6%' : `${Math.min(100, Math.max(6, winPct)).toFixed(1)}%`,
    avgRaw,
    avg: avgRaw === null ? '-' : pct(avgRaw),
  }
}
const resultValue = (row) => {
  const closedReturn = toResultNumber(row.close_return_pct)
  if (row.is_closed && closedReturn !== null) return closedReturn
  return isSwingStrategy(row) ? t3CloseResultValue(row) : toResultNumber(row.open_premium)
}
const pct = (value) => {
  const num = Number(value)
  return Number.isFinite(num) ? `${num.toFixed(2)}%` : '-'
}
const factorNumber = (value, digits = 4) => {
  const num = Number(value)
  return Number.isFinite(num) ? num.toFixed(digits) : '-'
}
const contributionText = (value) => {
  const num = Number(value)
  if (!Number.isFinite(num)) return '-'
  return `${num >= 0 ? '+' : ''}${num.toFixed(4)}`
}
const fetchCoverage = (item) => {
  const success = Number(item?.success)
  const universe = Number(item?.universe)
  if (!Number.isFinite(success) && !Number.isFinite(universe)) return '-'
  return `${Number.isFinite(success) ? success : 0} / ${Number.isFinite(universe) ? universe : 0}`
}
const fetchStatusClass = (item) => {
  if (item?.status === 'success' || item?.status === 'quota_exhausted') return 'chip-hot'
  if (item?.status === 'partial') return 'chip-warn'
  return ''
}
const amountYi = (value) => {
  const num = Number(value)
  return Number.isFinite(num) && num > 0 ? `${num.toFixed(0)} 亿` : '-'
}
const money = (value) => {
  const num = Number(value)
  if (!Number.isFinite(num)) return '-'
  return `¥${num.toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
}
const positionPctText = (value) => {
  const num = Number(value)
  if (!Number.isFinite(num)) return '-'
  return `${(num * 100).toFixed(0)}%`
}
const numberClass = (value) => {
  const num = Number(value)
  if (!Number.isFinite(num) || num === 0) return ''
  return num > 0 ? 'buy' : 'risk'
}
const strategyLabel = (strategy) => {
  if (strategy === '全局动量狙击') return '全局狙击'
  if (strategy === '右侧主升浪') return '顺势主升浪'
  if (strategy === '中线超跌反转') return '中线超跌反转'
  if (strategy === '首阴低吸') return '低吸影子'
  return '尾盘突破'
}
const strategyBadgeClass = (strategy) => [
  'strategy-badge',
  strategy === '全局动量狙击'
    ? 'strategy-global'
    : strategy === '右侧主升浪'
      ? 'strategy-main'
      : strategy === '中线超跌反转'
        ? 'strategy-reversal'
        : strategy === '首阴低吸'
          ? 'strategy-dip'
          : 'strategy-breakout',
]
const instructionTitle = (pick) => {
  const actual = Number(pick.open_premium)
  if (isSwingStrategy(pick)) {
    if (!Number.isFinite(actual)) return '等待开盘记录'
    return '等待T+3收盘'
  }
  if (!Number.isFinite(actual)) return '等待开盘'
  if (actual < 0) return 'T+1开盘卖出'
  if (actual >= 3) return 'T+1高开兑现'
  return 'T+1开盘卖出'
}
const instructionClass = (pick) => instructionTitle(pick).includes('核') ? 'risk' : instructionTitle(pick).includes('等待') ? 'neutral' : 'buy'
const instructionBody = (pick) => {
  const actual = Number(pick.open_premium)
  if (isSwingStrategy(pick)) {
    if (!Number.isFinite(actual)) return '等待 09:25 记录开盘价；T+3 策略不触发盘中卖出。'
    return `开盘 ${pct(actual)} 已记录，等待目标交易日 15:00 收盘价结算。`
  }
  if (!Number.isFinite(actual)) return '等待 T+1 集合竞价回填。'
  return `T+1 开盘溢价 ${pct(actual)}，按极速隔夜规则处理。`
}

onMounted(() => {
  window.addEventListener('quant:open-stock-market', handleStockMarketEvent)
  refreshAll()
})

onBeforeUnmount(() => {
  window.removeEventListener('quant:open-stock-market', handleStockMarketEvent)
})
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

.sniper-safe-box {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  flex-wrap: nowrap;
  gap: 8px;
  min-height: 52px;
  padding: 8px 10px;
  border: 1px solid var(--terminal-border);
  border-radius: 10px;
  background: var(--terminal-card);
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.04);
}

.sniper-copy {
  min-width: 0;
  flex: 0 1 190px;
  max-width: 220px;
}

.sniper-copy span,
.sniper-copy strong {
  display: block;
}

.sniper-copy span {
  color: #6f7d95;
  font-size: 0.72rem;
  font-weight: 900;
  text-transform: uppercase;
}

.sniper-copy strong {
  margin-top: 3px;
  font-size: 0.84rem;
  font-weight: 950;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.sniper-safe-box.locked {
  border-color: rgba(148, 163, 184, 0.18);
  box-shadow: inset 0 1px 0 rgba(148, 163, 184, 0.08);
}

.sniper-safe-box.locked strong {
  color: #9ca3af;
}

.sniper-safe-box.armed {
  border-color: rgba(245, 34, 45, 0.5);
  background:
    linear-gradient(90deg, rgba(245, 34, 45, 0.22), rgba(15, 17, 21, 0.96) 66%),
    var(--terminal-card);
  box-shadow: 0 0 18px rgba(245, 34, 45, 0.2), inset 0 1px 0 rgba(245, 34, 45, 0.24);
}

.sniper-safe-box.armed strong {
  color: #ff7875;
  text-shadow: 0 0 14px rgba(245, 34, 45, 0.48);
}

.content {
  min-width: 0;
  padding: 16px;
  background:
    linear-gradient(90deg, rgba(24, 144, 255, 0.035) 1px, transparent 1px),
    linear-gradient(180deg, rgba(24, 144, 255, 0.028) 1px, transparent 1px);
  background-size: 28px 28px;
}

.page-stack {
  display: grid;
  gap: 14px;
}

.dashboard-grid,
.dual-board,
.validation-grid,
.account-control-grid,
.account-table-grid {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
  gap: 14px;
}

.account-summary-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
}

.account-summary-grid article {
  min-width: 0;
  border: 1px solid rgba(245, 34, 45, 0.18);
  border-radius: 12px;
  padding: 12px;
  background:
    linear-gradient(180deg, rgba(245, 34, 45, 0.09), rgba(24, 144, 255, 0.02)),
    var(--terminal-card);
}

.account-summary-grid span,
.account-summary-grid small {
  display: block;
  color: #6f7d95;
  font-size: 0.72rem;
  font-weight: 800;
}

.account-summary-grid strong {
  display: block;
  margin: 6px 0 4px;
  color: var(--terminal-text);
  font-size: 1.12rem;
  font-weight: 950;
  overflow-wrap: anywhere;
}

.account-form {
  display: grid;
  gap: 12px;
}

.account-form label {
  display: grid;
  gap: 6px;
  color: #a8b3c7;
  font-size: 0.78rem;
  font-weight: 900;
}

.account-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.test-order-form {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.test-order-actions {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
  grid-column: 1 / -1;
}

.account-preview {
  grid-column: 1 / -1;
  max-height: 180px;
  overflow: auto;
  margin: 0;
  border: 1px solid var(--terminal-border);
  border-radius: 10px;
  padding: 10px;
  background: #07090d;
  color: #a8b3c7;
  font-size: 0.78rem;
}

.mono-code {
  display: inline-block;
  min-width: 66px;
  margin-right: 8px;
  color: #69b1ff;
  font-family: 'SF Mono', Menlo, Consolas, monospace;
}

.command-strip {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
}

.command-strip article {
  min-width: 0;
  border: 1px solid rgba(24, 144, 255, 0.16);
  border-radius: 12px;
  padding: 12px;
  background:
    linear-gradient(180deg, rgba(24, 144, 255, 0.08), rgba(24, 144, 255, 0.015)),
    var(--terminal-card);
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.04);
}

.command-strip span,
.command-strip small {
  display: block;
  color: #6f7d95;
  font-size: 0.72rem;
  font-weight: 800;
}

.command-strip strong {
  display: block;
  margin: 6px 0 4px;
  color: var(--terminal-text);
  font-size: 1rem;
  overflow-wrap: anywhere;
}

.cockpit-grid {
  display: grid;
  grid-template-columns: 1.15fr 1fr;
  gap: 14px;
}

.terminal-chip {
  display: inline-flex;
  align-items: center;
  min-height: 24px;
  border: 1px solid rgba(24, 144, 255, 0.32);
  border-radius: 999px;
  padding: 2px 9px;
  background: rgba(24, 144, 255, 0.12);
  color: #69b1ff;
  font-size: 0.72rem;
  font-weight: 900;
  white-space: nowrap;
}

.chip-hot {
  border-color: rgba(245, 34, 45, 0.36);
  background: rgba(245, 34, 45, 0.12);
  color: #ff7875;
}

.chip-warn {
  border-color: rgba(245, 197, 66, 0.4);
  background: rgba(245, 197, 66, 0.12);
  color: #f5c542;
}

.matrix-list,
.pipeline-flow,
.ai-terminal {
  display: grid;
  gap: 10px;
}

.matrix-list article,
.pipeline-flow article,
.ai-terminal p {
  margin: 0;
  min-width: 0;
  border: 1px solid var(--terminal-border);
  border-radius: 12px;
  padding: 11px;
  background: var(--terminal-bg);
}

.matrix-list article.strategy-disabled,
.legion-card.strategy-disabled {
  border-color: rgba(148, 163, 184, 0.18);
  background: rgba(31, 41, 55, 0.45);
  filter: grayscale(1);
  opacity: 0.58;
}

.matrix-list article.strategy-disabled .matrix-meter i {
  background: #64748b;
  box-shadow: none;
}

.matrix-list article.strategy-disabled dd,
.legion-card.strategy-disabled dd,
.legion-card.strategy-disabled > strong {
  color: #8b98ad;
}

.matrix-title {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  margin-bottom: 9px;
}

.matrix-title strong {
  color: var(--terminal-text);
  font-size: 1.05rem;
}

.matrix-meter {
  height: 8px;
  overflow: hidden;
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.06);
}

.matrix-meter i {
  display: block;
  height: 100%;
  border-radius: inherit;
  background: linear-gradient(90deg, var(--quant-neutral), var(--quant-rise));
  box-shadow: 0 0 16px rgba(245, 34, 45, 0.3);
}

.matrix-list dl {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 8px;
  margin: 10px 0 0;
}

.matrix-list dt,
.pipeline-flow span,
.pipeline-flow small,
.ai-terminal span,
.sentinel-timeline small {
  display: block;
  color: #6f7d95;
  font-size: 0.72rem;
  font-weight: 800;
}

.matrix-list dd {
  margin: 4px 0 0;
  color: var(--terminal-text);
  font-weight: 900;
}

.pipeline-flow article {
  position: relative;
  padding-left: 15px;
}

.pipeline-flow article::before {
  content: "";
  position: absolute;
  left: 0;
  top: 13px;
  bottom: 13px;
  width: 3px;
  border-radius: 999px;
  background: var(--quant-neutral);
}

.pipeline-flow strong,
.ai-terminal strong {
  display: block;
  margin: 5px 0 3px;
  color: var(--terminal-text);
  font-size: 0.95rem;
  overflow-wrap: anywhere;
}

.sentinel-timeline {
  display: grid;
  gap: 10px;
  margin: 0;
  padding: 0;
  list-style: none;
}

.sentinel-timeline li {
  display: grid;
  grid-template-columns: 58px minmax(0, 1fr);
  gap: 10px;
  align-items: start;
  border: 1px solid var(--terminal-border);
  border-radius: 12px;
  padding: 10px;
  background: var(--terminal-bg);
}

.sentinel-timeline time {
  display: grid;
  place-items: center;
  height: 32px;
  border: 1px solid rgba(24, 144, 255, 0.3);
  border-radius: 9px;
  color: #69b1ff;
  background: rgba(24, 144, 255, 0.1);
  font-family: 'SF Mono', Menlo, Consolas, monospace;
  font-size: 0.78rem;
  font-weight: 900;
}

.sentinel-timeline strong {
  display: block;
  color: var(--terminal-text);
  font-size: 0.92rem;
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
  display: flex;
  gap: 8px;
  align-items: baseline;
  min-width: 0;
  color: var(--terminal-text);
  font-weight: 900;
}

.inline-stock-code,
.heading-stock-code {
  color: #f2f6ff;
  font-weight: 900;
}

.inline-stock-name,
.heading-stock-name {
  color: var(--terminal-text);
  font-weight: 900;
}

.heading-stock-name {
  margin-left: 8px;
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
  grid-template-columns: repeat(4, minmax(0, 1fr));
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
  grid-template-columns: repeat(4, minmax(0, 1fr));
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
  border: 1px solid rgba(250, 173, 20, 0.58);
  background: rgba(250, 173, 20, 0.16);
  color: #ffd666;
}

.strategy-main {
  border: 1px solid rgba(114, 46, 209, 0.48);
  background: rgba(114, 46, 209, 0.2);
  color: #b37feb;
}

.strategy-global {
  border: 1px solid rgba(245, 34, 45, 0.48);
  background: rgba(245, 34, 45, 0.18);
  color: #ff7875;
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

.factor-drawer :deep(.el-drawer__body) {
  overflow: auto;
  padding: 18px;
  background: var(--terminal-bg);
  color: var(--terminal-text);
}

.factor-drawer-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 18px;
}

.factor-drawer-head h2 {
  display: flex;
  align-items: center;
  gap: 8px;
  margin: 0;
  color: #f2f6ff;
  font-size: 1rem;
}

.factor-panel {
  display: grid;
  gap: 14px;
}

.factor-summary-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
}

.factor-summary-grid article,
.factor-section {
  border: 1px solid var(--terminal-border);
  border-radius: 8px;
  background: var(--terminal-card);
}

.factor-summary-grid article {
  display: grid;
  gap: 6px;
  padding: 12px;
}

.factor-summary-grid span,
.factor-chain span,
.contribution-row small,
.split-factor-section span,
.importance-grid span,
.lineage-list dt {
  color: #7f8aa1;
  font-size: 0.74rem;
}

.factor-summary-grid strong,
.factor-chain strong,
.contribution-row strong,
.split-factor-section strong,
.importance-grid strong {
  color: #f2f6ff;
}

.factor-section {
  padding: 14px;
}

.factor-section h3 {
  margin: 0 0 12px;
  color: #dbe7ff;
  font-size: 0.9rem;
}

.factor-chain {
  display: grid;
  gap: 8px;
  margin: 0;
  padding: 0;
  list-style: none;
}

.factor-chain li,
.contribution-row,
.importance-grid article,
.lineage-list div {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.contribution-list {
  display: grid;
  gap: 8px;
}

.contribution-row {
  min-height: 42px;
  border-bottom: 1px solid rgba(255, 255, 255, 0.06);
}

.contribution-row:last-child {
  border-bottom: 0;
}

.contribution-row div {
  display: grid;
  gap: 3px;
  min-width: 0;
}

.contribution-row span {
  color: #e6edf7;
  font-weight: 800;
}

.split-factor-section {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 14px;
}

.split-factor-section p,
.importance-grid article {
  display: flex;
  justify-content: space-between;
  gap: 10px;
  margin: 0 0 8px;
}

.importance-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px 14px;
}

.factor-value-table {
  width: 100%;
}

.factor-name-cell {
  display: grid;
  gap: 3px;
  min-width: 0;
}

.factor-name-cell strong {
  color: #e6edf7;
}

.factor-name-cell small {
  color: #7f8aa1;
  line-height: 1.35;
}

.factor-name-cell em {
  color: #607089;
  font-family: 'SF Mono', Menlo, Consolas, monospace;
  font-size: 0.68rem;
  font-style: normal;
}

.lineage-list {
  display: grid;
  gap: 8px;
  margin: 0;
}

.lineage-list dd {
  margin: 0;
  color: #d7deeb;
  text-align: right;
  word-break: break-all;
}

.factor-note {
  margin: 12px 0 0;
  color: #98a6bd;
  font-size: 0.8rem;
  line-height: 1.6;
}

@media (max-width: 1180px) {
  .main-grid,
  .dashboard-grid,
  .dual-board,
  .validation-grid,
  .account-control-grid,
  .account-table-grid,
  .account-summary-grid,
  .legion-grid,
  .cockpit-grid,
  .command-strip {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 680px) {
  .sniper-safe-box {
    align-items: flex-start;
    justify-content: flex-start;
    flex-direction: column;
  }

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
  .account-summary-grid,
  .test-order-form,
  .legion-card dl,
  .factor-summary-grid,
  .split-factor-section,
  .importance-grid {
    grid-template-columns: 1fr;
  }
}
</style>
