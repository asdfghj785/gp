<template>
  <main class="page">
    <header class="topbar">
      <div>
        <p class="eyebrow">Local Quant Workstation</p>
        <h1>离岸量化数据控制台</h1>
      </div>
      <div class="toolbar">
        <button type="button" :disabled="busy.overview" @click="loadOverview">刷新状态</button>
        <button type="button" :disabled="busy.scan" @click="scanRadar">
          {{ busy.scan ? '扫描中' : '实时预测' }}
        </button>
      </div>
    </header>

    <section class="status-grid">
      <article class="metric">
        <span>数据库股票数</span>
        <strong>{{ overview.stock_count ?? 0 }}</strong>
      </article>
      <article class="metric">
        <span>数据库 K 线</span>
        <strong>{{ overview.rows_count ?? 0 }}</strong>
      </article>
      <article class="metric">
        <span>Parquet 文件</span>
        <strong>{{ overview.parquet_files ?? 0 }}</strong>
      </article>
      <article class="metric">
        <span>日期范围</span>
        <strong>{{ overview.min_date || '未同步' }} / {{ overview.max_date || '-' }}</strong>
      </article>
      <article class="metric sync-metric">
        <span>最近盘后同步</span>
        <strong :class="latestSync?.status === 'success' ? 'pass' : 'fail'">
          {{ latestSync ? syncStatusText(latestSync) : '暂无记录' }}
        </strong>
        <small v-if="latestSync">
          {{ latestSync.finished_at }} · 新增 {{ latestSync.inserted_rows }} · 更新 {{ latestSync.updated_rows }}
        </small>
      </article>
    </section>

    <section v-if="message" :class="['notice', messageType]">{{ message }}</section>

    <nav class="tabbar" aria-label="功能分区">
      <button
        v-for="tab in tabs"
        :key="tab.id"
        type="button"
        :class="{ active: activeTab === tab.id }"
        @click="setActiveTab(tab.id)"
      >
        <span>{{ tab.label }}</span>
        <small>{{ tab.hint }}</small>
      </button>
    </nav>

    <section v-show="activeTab === 'radar' || activeTab === 'data'" class="workbench single-panel">
      <div v-show="activeTab === 'radar'" class="panel radar-panel">
        <div class="panel-head">
          <div>
            <h2>预测雷达</h2>
            <p>{{ radar.created_at ? `缓存时间 ${radar.created_at}` : '点击实时预测后缓存 10 只候选股' }}</p>
            <p v-if="radar.market_gate" :class="radar.market_gate.blocked ? 'fail' : 'pass'">
              大盘风控：{{ marketGateText(radar.market_gate) }}
            </p>
            <p v-if="radar.intraday_snapshot" class="muted-line">
              14:30 快照：{{ intradaySnapshotText(radar.intraday_snapshot) }}
            </p>
          </div>
          <span class="model-state">{{ radar.model_status || '未扫描' }}</span>
        </div>

        <section v-if="radar.rows.length === 0" class="empty-state risk-off">
          <strong>空仓避险</strong>
          <span>{{ radarEmptyReason }}</span>
        </section>

        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>代码</th>
                <th>名称</th>
                <th>策略</th>
                <th class="num">现价</th>
                <th class="num">涨跌幅</th>
                <th class="num">换手</th>
                <th class="num">收益信号</th>
                <th class="num">预期溢价</th>
                <th class="num">综合评分</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              <tr v-if="radar.rows.length === 0">
                <td colspan="10" class="empty">当前没有达到高置信生产规则的候选股</td>
              </tr>
              <tr v-for="row in radar.rows" :key="row.code">
                <td class="mono">{{ row.code }}</td>
                <td>{{ row.name }}</td>
                <td><span :class="strategyBadgeClass(row.strategy_type)">{{ row.strategy_type || '尾盘突破' }}</span></td>
                <td class="num">{{ money(row.price) }}</td>
                <td :class="['num', row.change >= 0 ? 'up' : 'down']">{{ pct(row.change) }}</td>
                <td class="num">{{ pct(row.turnover) }}</td>
                <td class="num score">{{ pct(row.win_rate) }}</td>
                <td :class="['num', Number(row.expected_premium) >= 0 ? 'up' : 'down']">{{ pct(row.expected_premium) }}</td>
                <td class="num score">{{ scoreText(row.composite_score) }}</td>
                <td>
                  <button class="link-btn" type="button" @click="inspectStock(row)">验算</button>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <aside v-show="activeTab === 'data'" class="panel audit-panel">
        <div class="panel-head compact">
          <div>
            <h2>数据三重验证</h2>
            <p>结构完整性、金融逻辑、来源真实性</p>
          </div>
        </div>

        <div class="audit-actions">
          <label>
            抽检文件数
            <input v-model.number="validationSample" min="1" max="10000" type="number" />
          </label>
          <label class="check-line">
            <input v-model="sourceCheck" type="checkbox" />
            实时源交叉核验
          </label>
          <button type="button" :disabled="busy.validate" @click="runValidation">
            {{ busy.validate ? '验证中' : '开始验证' }}
          </button>
        </div>

        <div v-if="validation.summary" class="audit-summary">
          <div>
            <span>状态</span>
            <strong :class="validation.status === 'pass' ? 'pass' : 'fail'">{{ validation.status }}</strong>
          </div>
          <div>
            <span>错误</span>
            <strong>{{ validation.summary.error_count }}</strong>
          </div>
          <div>
            <span>警告</span>
            <strong>{{ validation.summary.warning_count }}</strong>
          </div>
        </div>

        <div class="issue-list">
          <article v-for="(issue, index) in validation.issues.slice(0, 12)" :key="index" class="issue">
            <div>
              <strong :class="issue.level">{{ issue.level }}</strong>
              <span>{{ issue.stage }}</span>
            </div>
            <p>{{ issue.code || '全局' }} {{ issue.date || '' }} {{ issue.message }}</p>
          </article>
          <p v-if="validation.issues.length === 0" class="empty">尚未发现异常或尚未运行验证</p>
        </div>
      </aside>
    </section>

    <section v-show="activeTab === 'radar'" class="panel pick-panel">
      <div class="panel-head compact">
        <div>
          <h2>14:50 推送标的跟踪</h2>
          <p>工作日 14:50 PushPlus 推送成功后自动锁定该标的，不允许前端手动修改；次日开盘后自动输出结果</p>
        </div>
        <div class="toolbar">
          <button class="secondary-btn" type="button" @click="loadDailyPicks">刷新记录</button>
        </div>
      </div>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>保存日</th>
              <th>目标开盘日</th>
              <th>股票</th>
              <th class="num">收益信号</th>
              <th class="num">保存价</th>
              <th class="num">开盘价</th>
              <th class="num">开盘溢价</th>
              <th>结果</th>
            </tr>
          </thead>
          <tbody>
            <tr v-if="dailyPicks.rows.length === 0">
              <td colspan="8" class="empty">暂无 14:50 推送锁定记录</td>
            </tr>
            <tr v-for="pick in dailyPicks.rows" :key="pick.id">
              <td>{{ pick.selection_date }}</td>
              <td>{{ pick.target_date }}</td>
              <td>
                <span class="mono">{{ pick.code }}</span> {{ pick.name }}
                <span :class="strategyBadgeClass(pick.strategy_type)">{{ pick.strategy_type || '尾盘突破' }}</span>
              </td>
              <td class="num score">{{ pct(pick.win_rate) }}</td>
              <td class="num">{{ money(pick.selection_price) }}</td>
              <td class="num">{{ pick.open_price ? money(pick.open_price) : '-' }}</td>
              <td :class="['num', Number(pick.open_premium) >= 0 ? 'up' : 'down']">
                {{ pick.open_premium === null || pick.open_premium === undefined ? '-' : pct(pick.open_premium) }}
              </td>
              <td>
                <strong v-if="pick.status === 'pending_open'" class="pending">待开盘</strong>
                <strong v-else :class="pick.success ? 'pass' : 'fail'">{{ pick.success ? '成功' : '失败' }}</strong>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <section v-show="activeTab === 'radar'" class="panel shadow-panel">
      <div class="panel-head compact">
        <div>
          <h2>影子测试与外推精准度观测</h2>
          <p>对比昨日 14:50 预期开盘溢价与 09:25 集合竞价实际溢价，监控 14:50 外推算法偏差</p>
        </div>
        <span class="model-state">09:26 哨兵闭环</span>
      </div>

      <div v-if="shadowRows.length === 0" class="empty-state">
        <strong>暂无影子测试记录</strong>
        <span>14:50 锁定标的并由 09:26 哨兵回填开盘结果后，这里会自动显示预测与实盘误差。</span>
      </div>

      <div v-else class="shadow-grid">
        <article v-for="pick in shadowRows" :key="`shadow-${pick.id}`" class="shadow-card">
          <header>
            <div>
              <strong><span class="mono">{{ pick.code }}</span> {{ pick.name }}</strong>
              <small>{{ pick.selection_date }} → {{ pick.target_date }}</small>
            </div>
            <span :class="exitActionClass(pick)">{{ exitActionText(pick) }}</span>
          </header>

          <div class="shadow-compare">
            <div class="shadow-side">
              <span>14:50 预期</span>
              <strong :class="Number(expectedPremium(pick)) >= 0 ? 'up' : 'down'">{{ pct(expectedPremium(pick)) }}</strong>
              <div class="premium-track">
                <i :class="premiumBarClass(expectedPremium(pick))" :style="premiumBarStyle(expectedPremium(pick))"></i>
              </div>
            </div>
            <div class="shadow-side">
              <span>09:25 实盘</span>
              <strong v-if="actualPremium(pick) !== null" :class="Number(actualPremium(pick)) >= 0 ? 'up' : 'down'">
                {{ pct(actualPremium(pick)) }}
              </strong>
              <strong v-else class="pending">待哨兵</strong>
              <div class="premium-track">
                <i v-if="actualPremium(pick) !== null" :class="premiumBarClass(actualPremium(pick))" :style="premiumBarStyle(actualPremium(pick))"></i>
              </div>
            </div>
          </div>

          <footer>
            <span :class="precisionBadgeClass(pick)">{{ precisionBadgeText(pick) }}</span>
            <span>误差 {{ premiumErrorText(pick) }}</span>
            <span>综合评分 {{ scoreText(pick.composite_score) }}</span>
          </footer>
          <p class="shadow-instruction">{{ pick.exit_instruction || '等待 09:26 哨兵生成操作指令' }}</p>
        </article>
      </div>
    </section>

    <section v-show="activeTab === 'strategy'" class="panel backtest-panel">
      <div class="panel-head compact">
        <div>
          <h2>近十二个月生产策略复盘</h2>
          <p>{{ backtest.summary?.rule || '每日选预期溢价最高股票，次日开盘卖出统计胜率' }}</p>
          <p v-if="backtest.summary?.rank_rule">{{ backtest.summary.rank_rule }}</p>
        </div>
        <button type="button" :disabled="busy.backtest" @click="loadBacktest(true)">
          {{ busy.backtest ? '统计中' : '刷新复盘' }}
        </button>
      </div>

      <div v-if="backtest.summary" class="backtest-summary">
        <div><span>区间</span><strong>{{ backtest.summary.start_date }} / {{ backtest.summary.end_date }}</strong></div>
        <div><span>已评估天数</span><strong>{{ backtest.summary.evaluated_days }}</strong></div>
        <div><span>成功 / 失败</span><strong>{{ backtest.summary.win_count }} / {{ backtest.summary.loss_count }}</strong></div>
        <div><span>开盘胜率</span><strong class="score">{{ pct(backtest.summary.win_rate) }}</strong></div>
        <div><span>平均溢价</span><strong :class="Number(backtest.summary.avg_open_premium) >= 0 ? 'up' : 'down'">{{ pct(backtest.summary.avg_open_premium) }}</strong></div>
        <div><span>中位溢价</span><strong :class="Number(backtest.summary.median_open_premium) >= 0 ? 'up' : 'down'">{{ pct(backtest.summary.median_open_premium) }}</strong></div>
        <div><span>修复昨收/量比</span><strong>{{ backtest.summary.repaired_pre_close_count || 0 }} / {{ backtest.summary.repaired_volume_ratio_count || 0 }}</strong></div>
        <div><span>成交策略分布</span><strong>{{ strategyCountsText(backtest.summary.strategy_counts) }}</strong></div>
        <div><span>候选策略分布</span><strong>{{ strategyCountsText(backtest.summary.candidate_strategy_counts) }}</strong></div>
      </div>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>预测日</th>
              <th>股票</th>
              <th class="num">收益信号</th>
              <th class="num">预期溢价</th>
              <th class="num">综合评分</th>
              <th class="num">收盘买入</th>
              <th>卖出日</th>
              <th class="num">开盘卖出</th>
              <th class="num">开盘溢价</th>
              <th>结果</th>
            </tr>
          </thead>
          <tbody>
            <tr v-if="backtest.rows.length === 0">
              <td colspan="10" class="empty">暂无复盘数据</td>
            </tr>
            <tr v-for="row in backtest.rows" :key="`${row.date}-${row.code}`">
              <td>{{ row.date }}</td>
              <td>
                <span class="mono">{{ row.code }}</span> {{ row.name }}
                <span :class="strategyBadgeClass(row.strategy_type)">{{ row.strategy_type || '尾盘突破' }}</span>
              </td>
              <td class="num score">{{ pct(row.win_rate) }}</td>
              <td :class="['num', Number(row.expected_premium) >= 0 ? 'up' : 'down']">{{ pct(row.expected_premium) }}</td>
              <td class="num score">{{ scoreText(row.composite_score) }}</td>
              <td class="num">{{ money(row.close) }}</td>
              <td>{{ row.next_date || '-' }}</td>
              <td class="num">{{ row.next_open ? money(row.next_open) : '-' }}</td>
              <td :class="['num', Number(row.open_premium) >= 0 ? 'up' : 'down']">
                {{ row.open_premium === null || row.open_premium === undefined ? '-' : pct(row.open_premium) }}
              </td>
              <td>
                <strong v-if="row.success === null" class="pending">待开盘</strong>
                <strong v-else :class="row.success ? 'pass' : 'fail'">{{ row.success ? '成功' : '失败' }}</strong>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <div class="panel-subhead">
        <div>
          <h3>双轨候选观察</h3>
          <p>近两个月按交易日分别展示双轨 Top1；有生产合格标的时优先展示生产合格 Top1，共 {{ backtest.strategy_rows.length }} 条。</p>
        </div>
      </div>

      <div class="table-wrap compact-table">
        <table>
          <thead>
            <tr>
              <th>预测日</th>
              <th>股票</th>
              <th>策略</th>
              <th class="num">预期溢价</th>
              <th class="num">综合评分</th>
              <th class="num">实际开盘</th>
              <th>结果</th>
            </tr>
          </thead>
          <tbody>
            <tr v-if="backtest.strategy_rows.length === 0">
              <td colspan="7" class="empty">暂无双轨候选观察数据</td>
            </tr>
            <tr v-for="row in backtest.strategy_rows" :key="`${row.date}-${row.strategy_type}-${row.code}`">
              <td>{{ row.date }}</td>
              <td><span class="mono">{{ row.code }}</span> {{ row.name }}</td>
              <td><span :class="strategyBadgeClass(row.strategy_type)">{{ row.strategy_type || '尾盘突破' }}</span></td>
              <td :class="['num', Number(row.expected_premium) >= 0 ? 'up' : 'down']">{{ pct(row.expected_premium) }}</td>
              <td class="num score">{{ scoreText(row.composite_score) }}</td>
              <td :class="['num', Number(row.open_premium) >= 0 ? 'up' : 'down']">
                {{ row.open_premium === null || row.open_premium === undefined ? '-' : pct(row.open_premium) }}
              </td>
              <td>
                <strong v-if="row.success === null" class="pending">待开盘</strong>
                <strong v-else :class="row.success ? 'pass' : 'fail'">{{ row.success ? '成功' : '失败' }}</strong>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <section v-show="activeTab === 'strategy'" class="panel strategy-panel">
      <div class="panel-head compact">
        <div>
          <h2>策略实验室</h2>
          <p>{{ strategyLab.summary?.note || '同一批历史候选池对比不同过滤规则、阈值和综合评分表现' }}</p>
        </div>
        <button type="button" :disabled="busy.strategyLab" @click="loadStrategyLab(true)">
          {{ busy.strategyLab ? '实验中' : '刷新实验' }}
        </button>
      </div>

      <div v-if="strategyLab.summary" class="backtest-summary">
        <div><span>候选样本</span><strong>{{ strategyLab.summary.candidate_rows }}</strong></div>
        <div><span>交易日</span><strong>{{ strategyLab.summary.trading_days }}</strong></div>
        <div>
          <span>最佳规则</span>
          <strong>{{ strategyLab.summary.best_strategy?.name || '-' }}</strong>
        </div>
        <div>
          <span>最佳均值</span>
          <strong :class="Number(strategyLab.summary.best_strategy?.avg_open_premium) >= 0 ? 'up' : 'down'">
            {{ pct(strategyLab.summary.best_strategy?.avg_open_premium) }}
          </strong>
        </div>
        <div><span>综合分相关</span><strong>{{ scoreText((strategyLab.summary.correlation?.composite_pearson || 0) * 100) }}</strong></div>
        <div><span>模型状态</span><strong>{{ strategyLab.summary.model_status }}</strong></div>
      </div>

      <div class="strategy-grid">
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>规则</th>
                <th class="num">交易数</th>
                <th class="num">胜率</th>
                <th class="num">平均溢价</th>
                <th class="num">中位溢价</th>
                <th class="num">最差</th>
              </tr>
            </thead>
            <tbody>
              <tr v-if="strategyLab.variants.length === 0">
                <td colspan="6" class="empty">暂无实验数据</td>
              </tr>
              <tr v-for="variant in strategyLab.variants" :key="variant.name">
                <td>
                  <strong>{{ variant.name }}</strong>
                  <small>{{ variant.description }}</small>
                </td>
                <td class="num">{{ variant.trades }}</td>
                <td class="num score">{{ pct(variant.win_rate) }}</td>
                <td :class="['num', Number(variant.avg_open_premium) >= 0 ? 'up' : 'down']">{{ pct(variant.avg_open_premium) }}</td>
                <td :class="['num', Number(variant.median_open_premium) >= 0 ? 'up' : 'down']">{{ pct(variant.median_open_premium) }}</td>
                <td class="num down">{{ pct(variant.worst_open_premium) }}</td>
              </tr>
            </tbody>
          </table>
        </div>

        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>阈值</th>
                <th class="num">交易数</th>
                <th class="num">胜率</th>
                <th class="num">平均溢价</th>
                <th class="num">中位溢价</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="row in strategyLab.thresholds" :key="row.name">
                <td>{{ row.name }}</td>
                <td class="num">{{ row.trades }}</td>
                <td class="num score">{{ pct(row.win_rate) }}</td>
                <td :class="['num', Number(row.avg_open_premium) >= 0 ? 'up' : 'down']">{{ pct(row.avg_open_premium) }}</td>
                <td :class="['num', Number(row.median_open_premium) >= 0 ? 'up' : 'down']">{{ pct(row.median_open_premium) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </section>

    <section v-show="activeTab === 'strategy'" class="panel strategy-panel">
      <div class="panel-head compact">
        <div>
          <h2>失败归因分析</h2>
          <p>{{ failureAnalysis.summary?.optimization_note || '用 12 个月生产策略历史样本，分析失败股票的共同特征和可优化规则' }}</p>
        </div>
        <button type="button" :disabled="busy.failureAnalysis" @click="loadFailureAnalysis(true)">
          {{ busy.failureAnalysis ? '分析中' : '刷新归因' }}
        </button>
      </div>

      <div v-if="failureAnalysis.summary" class="backtest-summary">
        <div><span>区间</span><strong>{{ failureAnalysis.summary.start_date }} / {{ failureAnalysis.summary.end_date }}</strong></div>
        <div><span>交易数</span><strong>{{ failureAnalysis.summary.trades }}</strong></div>
        <div><span>成功 / 失败</span><strong>{{ failureAnalysis.summary.success_count }} / {{ failureAnalysis.summary.failure_count }}</strong></div>
        <div><span>基准胜率</span><strong class="score">{{ pct(failureAnalysis.summary.baseline?.win_rate) }}</strong></div>
        <div>
          <span>基准平均溢价</span>
          <strong :class="Number(failureAnalysis.summary.baseline?.avg_open_premium) >= 0 ? 'up' : 'down'">
            {{ pct(failureAnalysis.summary.baseline?.avg_open_premium) }}
          </strong>
        </div>
        <div><span>模型状态</span><strong>{{ failureAnalysis.summary.model_status }}</strong></div>
      </div>

      <div class="strategy-grid">
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>失败原因</th>
                <th class="num">失败占比</th>
                <th class="num">成功占比</th>
                <th class="num">差值</th>
                <th class="num">失败数</th>
              </tr>
            </thead>
            <tbody>
              <tr v-if="failureAnalysis.reasons.length === 0">
                <td colspan="5" class="empty">暂无失败归因数据</td>
              </tr>
              <tr v-for="reason in failureAnalysis.reasons" :key="reason.reason">
                <td>
                  <strong>{{ reason.reason }}</strong>
                  <small>{{ reason.description }}</small>
                </td>
                <td class="num fail">{{ pct(reason.failure_rate) }}</td>
                <td class="num pass">{{ pct(reason.success_rate) }}</td>
                <td :class="['num', Number(reason.lift_vs_success) >= 0 ? 'fail' : 'pass']">{{ pct(reason.lift_vs_success) }}</td>
                <td class="num">{{ reason.failure_count }}</td>
              </tr>
            </tbody>
          </table>
        </div>

        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>优化规则</th>
                <th class="num">交易数</th>
                <th class="num">胜率变化</th>
                <th class="num">均值变化</th>
                <th class="num">覆盖率</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="rule in failureAnalysis.optimizations" :key="rule.name">
                <td>{{ rule.name }}</td>
                <td class="num">{{ rule.trades }}</td>
                <td :class="['num', Number(rule.delta_win_rate) >= 0 ? 'pass' : 'fail']">{{ pct(rule.delta_win_rate) }}</td>
                <td :class="['num', Number(rule.delta_avg_open_premium) >= 0 ? 'pass' : 'fail']">{{ pct(rule.delta_avg_open_premium) }}</td>
                <td class="num">{{ pct(rule.coverage) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <div class="table-wrap failure-samples">
        <table>
          <thead>
            <tr>
              <th>最差失败日</th>
              <th>股票</th>
              <th class="num">实际溢价</th>
              <th class="num">胜率</th>
              <th class="num">预期溢价</th>
              <th class="num">综合分</th>
              <th class="num">涨跌幅</th>
              <th class="num">上影</th>
              <th class="num">振幅</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in failureAnalysis.sample_failures.slice(0, 10)" :key="`${row.date}-${row.code}`">
              <td>{{ row.date }}</td>
              <td><span class="mono">{{ row.code }}</span> {{ row.name }}</td>
              <td class="num down">{{ pct(row.open_premium) }}</td>
              <td class="num">{{ pct(row.win_rate) }}</td>
              <td :class="['num', Number(row.expected_premium) >= 0 ? 'up' : 'down']">{{ pct(row.expected_premium) }}</td>
              <td class="num score">{{ scoreText(row.composite_score) }}</td>
              <td :class="['num', Number(row.change) >= 0 ? 'up' : 'down']">{{ pct(row.change) }}</td>
              <td class="num">{{ pct(row.upper_shadow) }}</td>
              <td class="num">{{ pct(row.amplitude) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <section v-show="activeTab === 'strategy'" class="panel strategy-panel">
      <div class="panel-head compact">
        <div>
          <h2>次日上涨原因分析</h2>
          <p>{{ upReason.summary?.method || '统计次日开盘上涨股票，并用技术因子模型和本地大模型解释上涨原因' }}</p>
        </div>
        <button type="button" :disabled="busy.upReason" @click="loadUpReason(true)">
          {{ busy.upReason ? '分析中' : '运行上涨归因' }}
        </button>
      </div>

      <div v-if="upReason.summary" class="backtest-summary">
        <div><span>区间</span><strong>{{ upReason.summary.start_date }} / {{ upReason.summary.end_date }}</strong></div>
        <div><span>候选样本</span><strong>{{ upReason.summary.candidate_rows }}</strong></div>
        <div><span>上涨 / 下跌</span><strong>{{ upReason.summary.up_count }} / {{ upReason.summary.down_count }}</strong></div>
        <div><span>上涨率</span><strong class="score">{{ pct(upReason.summary.up_rate) }}</strong></div>
        <div><span>上涨均值</span><strong class="up">{{ pct(upReason.summary.avg_up_premium) }}</strong></div>
        <div><span>下跌均值</span><strong class="down">{{ pct(upReason.summary.avg_down_premium) }}</strong></div>
        <div><span>模型 AUC</span><strong>{{ scoreText(upReason.model_report?.auc) }}</strong></div>
        <div><span>Top10% 上涨率</span><strong>{{ pct(upReason.model_report?.top_decile_up_rate) }}</strong></div>
      </div>

      <section v-if="upReason.llm_summary?.conclusion" class="analysis-box compact-analysis">
        <h3>{{ upReason.llm_summary.primary_driver }} · 置信 {{ scoreText(upReason.llm_summary.confidence) }}</h3>
        <p>{{ upReason.llm_summary.conclusion }}</p>
        <p><strong>舆情状态：</strong>{{ upReason.summary?.sentiment_status }}</p>
      </section>

      <div class="strategy-grid">
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>上涨因子</th>
                <th>类型</th>
                <th class="num">样本</th>
                <th class="num">上涨率</th>
                <th class="num">提升</th>
                <th class="num">均值溢价</th>
              </tr>
            </thead>
            <tbody>
              <tr v-if="upReason.factor_lifts.length === 0">
                <td colspan="6" class="empty">点击运行上涨归因后显示</td>
              </tr>
              <tr v-for="factor in upReason.factor_lifts.slice(0, 12)" :key="factor.factor">
                <td>
                  <strong>{{ factor.factor }}</strong>
                  <small>{{ factor.explanation }}</small>
                </td>
                <td>{{ factor.category }}</td>
                <td class="num">{{ factor.sample_count }}</td>
                <td class="num score">{{ pct(factor.up_rate) }}</td>
                <td :class="['num', Number(factor.lift) >= 0 ? 'pass' : 'fail']">{{ pct(factor.lift) }}</td>
                <td :class="['num', Number(factor.avg_open_premium) >= 0 ? 'up' : 'down']">{{ pct(factor.avg_open_premium) }}</td>
              </tr>
            </tbody>
          </table>
        </div>

        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>模型重要因子</th>
                <th class="num">重要度</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="item in upReason.model_report.feature_importance || []" :key="item.feature">
                <td>{{ item.feature }}</td>
                <td class="num score">{{ scoreText(item.importance) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </section>

    <section v-show="activeTab === 'stock' || activeTab === 'data'" class="detail-grid single-panel">
      <div v-show="activeTab === 'stock'" class="panel">
        <div class="panel-head compact">
          <div>
            <h2>单票数据验算</h2>
            <p>查看数据库历史记录和最近收盘曲线</p>
          </div>
          <div class="stock-search">
            <input v-model.trim="selectedCode" maxlength="6" placeholder="股票代码，如 600519" />
            <button type="button" @click="loadHistory">查看</button>
          </div>
        </div>

        <div v-if="chartData.points.length" class="chart-box" @mouseleave="hoverPoint = null">
          <svg class="chart" viewBox="0 0 760 320" role="img" aria-label="K线与成交量走势">
            <line x1="48" y1="34" x2="48" y2="220" stroke="#d5dce8" />
            <line x1="48" y1="220" x2="730" y2="220" stroke="#d5dce8" />
            <line x1="48" y1="270" x2="730" y2="270" stroke="#d5dce8" />
            <text x="50" y="24" class="axis-label">价格</text>
            <text x="50" y="292" class="axis-label">成交量</text>
            <text x="672" y="24" class="axis-label">最高 {{ money(chartData.maxPrice) }}</text>
            <text x="672" y="218" class="axis-label">最低 {{ money(chartData.minPrice) }}</text>

            <g v-for="point in chartData.points" :key="point.date">
              <rect
                :x="point.x - point.volumeWidth / 2"
                :y="point.volumeY"
                :width="point.volumeWidth"
                :height="point.volumeHeight"
                :fill="point.close >= point.open ? '#ef4444' : '#16a34a'"
                opacity="0.28"
              />
              <line
                :x1="point.x"
                :x2="point.x"
                :y1="point.yHigh"
                :y2="point.yLow"
                :stroke="point.close >= point.open ? '#dc2626' : '#15803d'"
                stroke-width="1.4"
              />
              <rect
                :x="point.x - point.candleWidth / 2"
                :y="Math.min(point.yOpen, point.yClose)"
                :width="point.candleWidth"
                :height="Math.max(2, Math.abs(point.yOpen - point.yClose))"
                :fill="point.close >= point.open ? '#fee2e2' : '#dcfce7'"
                :stroke="point.close >= point.open ? '#dc2626' : '#15803d'"
              />
              <circle
                :cx="point.x"
                :cy="point.yClose"
                r="8"
                fill="transparent"
                @mouseenter="hoverPoint = point"
                @mousemove="hoverPoint = point"
              />
            </g>
          </svg>
          <div
            v-if="hoverPoint"
            class="chart-tooltip"
            :style="{ left: `${Math.min(hoverPoint.x + 18, 560)}px`, top: `${Math.max(hoverPoint.yClose - 78, 10)}px` }"
          >
            <strong>{{ hoverPoint.date }}</strong>
            <span>开盘 {{ money(hoverPoint.open) }} / 收盘 {{ money(hoverPoint.close) }}</span>
            <span>最高 {{ money(hoverPoint.high) }} / 最低 {{ money(hoverPoint.low) }}</span>
            <span>涨跌 {{ pct(hoverPoint.change_pct) }} / 换手 {{ pct(hoverPoint.turnover) }}</span>
            <span>成交额 {{ amountText(hoverPoint.amount) }}</span>
          </div>
        </div>
        <p v-else class="empty">先同步数据库后输入代码查看历史。</p>
      </div>

      <div v-show="activeTab === 'data'" class="panel">
        <div class="panel-head compact">
          <div>
            <h2>盘后数据入库</h2>
            <p>收盘后同步全市场最新行情到数据库，并记录同步审计结果</p>
          </div>
        </div>
        <div class="sync-row">
          <button type="button" :disabled="busy.sync" @click="syncData">
            {{ busy.sync ? '同步中' : '立即盘后同步' }}
          </button>
        </div>
        <div v-if="latestSync" class="sync-detail">
          <div><span>状态</span><strong :class="latestSync.status === 'success' ? 'pass' : 'fail'">{{ latestSync.status }}</strong></div>
          <div><span>完成时间</span><strong>{{ latestSync.finished_at }}</strong></div>
          <div><span>同步日期</span><strong>{{ latestSync.sync_date || '-' }}</strong></div>
          <div><span>拉取行数</span><strong>{{ latestSync.fetched_rows }}</strong></div>
          <div><span>有效行数</span><strong>{{ latestSync.valid_rows }}</strong></div>
          <div><span>新增/更新</span><strong>{{ latestSync.inserted_rows }} / {{ latestSync.updated_rows }}</strong></div>
        </div>
        <pre v-if="syncResult">{{ syncResult }}</pre>
      </div>
    </section>

    <div v-if="drawer.open" class="drawer-mask" @click.self="drawer.open = false">
      <aside class="drawer">
        <header>
          <div>
            <h2>{{ drawer.stock?.name }} {{ drawer.stock?.code }}</h2>
            <p>技术特征与舆情风控</p>
          </div>
          <button type="button" @click="drawer.open = false">关闭</button>
        </header>
        <div v-if="drawer.stock" class="feature-grid">
          <div><span>实体比例</span><strong>{{ pct(drawer.stock.tech_features.body_ratio) }}</strong></div>
          <div><span>上影线</span><strong>{{ pct(drawer.stock.tech_features.upper_shadow) }}</strong></div>
          <div><span>下影线</span><strong>{{ pct(drawer.stock.tech_features.lower_shadow) }}</strong></div>
          <div><span>日内振幅</span><strong>{{ pct(drawer.stock.tech_features.amplitude) }}</strong></div>
          <div><span>3日涨幅</span><strong>{{ pct(drawer.stock.trend_features?.return_3d) }}</strong></div>
          <div><span>5日涨幅</span><strong>{{ pct(drawer.stock.trend_features?.return_5d) }}</strong></div>
          <div><span>5日乖离</span><strong>{{ pct(drawer.stock.trend_features?.bias_5d) }}</strong></div>
          <div><span>20日乖离</span><strong>{{ pct(drawer.stock.trend_features?.bias_20d) }}</strong></div>
          <div><span>5日量能</span><strong>{{ scoreText(drawer.stock.trend_features?.volume_stack_5d) }}</strong></div>
          <div><span>10日量比</span><strong>{{ scoreText(drawer.stock.trend_features?.volume_ratio_10d) }}</strong></div>
          <div><span>3日红盘</span><strong>{{ pct(drawer.stock.trend_features?.red_ratio_3d) }}</strong></div>
          <div><span>60日位置</span><strong>{{ pct(drawer.stock.trend_features?.high_position_60d) }}</strong></div>
          <div><span>振幅/换手</span><strong>{{ scoreText(drawer.stock.trend_features?.amplitude_turnover_ratio) }}</strong></div>
          <div><span>尾盘拉升</span><strong>{{ pct(drawer.stock.trend_features?.late_pull_pct) }}</strong></div>
          <div><span>缩量大涨</span><strong>{{ drawer.stock.trend_features?.is_low_volume_rally ? '是' : '否' }}</strong></div>
          <div><span>极端下影</span><strong>{{ drawer.stock.trend_features?.is_extreme_lower_shadow ? '是' : '否' }}</strong></div>
          <div><span>断头铡刀</span><strong>{{ drawer.stock.trend_features?.is_recent_guillotine ? '是' : '否' }}</strong></div>
        </div>
        <button class="wide-btn" type="button" :disabled="busy.analyze" @click="analyzeStock">
          {{ busy.analyze ? '分析中' : '运行舆情风控' }}
        </button>
        <section v-if="analysis" class="analysis-box">
          <h3 :class="analysis.analysis.verdict.includes('绿') ? 'pass' : 'fail'">{{ analysis.analysis.verdict }}</h3>
          <p><strong>情绪：</strong>{{ analysis.analysis.sentiment }}</p>
          <p>{{ analysis.analysis.logic }}</p>
          <ul>
            <li v-for="(item, index) in analysis.analysis.evidence" :key="index">
              {{ item.source }}：{{ item.quote }}
            </li>
          </ul>
        </section>
      </aside>
    </div>
  </main>
</template>

<script setup>
import { computed, onMounted, reactive, ref } from 'vue'

const API = 'http://127.0.0.1:8000'

const tabs = [
  { id: 'radar', label: '预测', hint: '实时候选与跟踪' },
  { id: 'strategy', label: '策略', hint: '复盘与归因' },
  { id: 'data', label: '数据', hint: '同步与验证' },
  { id: 'stock', label: '单票', hint: 'K线验算' },
]
const activeTab = ref('radar')
const loaded = reactive({
  strategy: false,
  data: false,
  stock: false,
})
const overview = ref({})
const radar = reactive({ rows: [], created_at: '', model_status: '', market_gate: null, strategy: '', intraday_snapshot: null })
const dailyPicks = reactive({ rows: [] })
const backtest = reactive({ summary: null, rows: [], strategy_rows: [], created_at: '' })
const strategyLab = reactive({ summary: null, variants: [], thresholds: [], daily_picks: [], created_at: '' })
const failureAnalysis = reactive({ summary: null, reasons: [], optimizations: [], sample_failures: [], created_at: '' })
const upReason = reactive({ summary: null, factor_lifts: [], model_report: { feature_importance: [] }, llm_summary: {}, up_examples: [], down_examples: [], created_at: '' })
const validation = reactive({ status: '', summary: null, issues: [] })
const drawer = reactive({ open: false, stock: null })
const analysis = ref(null)
const historyRows = ref([])
const hoverPoint = ref(null)
const selectedCode = ref('600519')
const validationSample = ref(200)
const sourceCheck = ref(false)
const syncResult = ref('')
const message = ref('')
const messageType = ref('info')
const busy = reactive({
  overview: false,
  scan: false,
  validate: false,
  sync: false,
  analyze: false,
  backtest: false,
  strategyLab: false,
  failureAnalysis: false,
  upReason: false,
})

const money = (value) => Number(value ?? 0).toFixed(2)
const pct = (value) => `${Number(value ?? 0).toFixed(2)}%`
const scoreText = (value) => Number(value ?? 0).toFixed(2)
const strategyBadgeClass = (strategyType) => [
  'strategy-badge',
  strategyType === '首阴低吸' ? 'strategy-badge-dipbuy' : 'strategy-badge-breakout',
]
const strategyCountsText = (counts) => {
  if (!counts || Object.keys(counts).length === 0) return '-'
  return Object.entries(counts).map(([name, count]) => `${name} ${count}`).join(' / ')
}
const amountText = (value) => {
  const amount = Number(value ?? 0)
  if (amount >= 100000000) return `${(amount / 100000000).toFixed(2)} 亿`
  if (amount >= 10000) return `${(amount / 10000).toFixed(2)} 万`
  return amount.toFixed(0)
}
const latestSync = computed(() => overview.value.latest_sync || null)
const shadowRows = computed(() => dailyPicks.rows.slice(0, 5))
const syncStatusText = (sync) => {
  if (!sync) return '暂无记录'
  if (sync.status !== 'success') return '同步失败'
  return `${sync.sync_date || '-'} 成功`
}
const marketGateText = (gate) => {
  if (!gate) return ''
  const base = `${gate.mode || '-'}，成交额 ${Number(gate.market_amount_yi || 0).toFixed(0)} 亿，红盘率 ${pct(gate.market_up_rate)}，下跌 ${gate.market_down_count || 0}`
  const reason = (gate.reasons || []).join('；')
  return reason ? `${base}；${reason}` : base
}
const intradaySnapshotText = (snapshot) => {
  if (!snapshot) return '未启用'
  if (snapshot.status === 'ready') {
    return `${snapshot.snapshot_at || '-'}，匹配 ${snapshot.matched_count || 0}，拦截 ${snapshot.trapped_count || 0}`
  }
  if (snapshot.status === 'missing_snapshot') return '未找到今日快照'
  if (snapshot.status === 'stale_snapshot') return `快照日期 ${snapshot.snapshot_date || '-'}，非今日`
  return snapshot.status || '未知'
}
const radarEmptyReason = computed(() => {
  if (busy.scan) return '正在重新扫描全市场，请等待模型返回结果。'
  if (!radar.created_at && radar.model_status === 'no_cache') return '还没有预测缓存，点击“实时预测”后会生成最新结果。'
  if (radar.market_gate?.blocked) {
    const reasons = (radar.market_gate.reasons || []).join('；')
    return reasons || '大盘风控触发，系统已主动空仓。'
  }
  if (radar.intraday_snapshot?.trapped_count > 0) {
    return `尾盘快照拦截 ${radar.intraday_snapshot.trapped_count} 只异动股，剩余股票未达到高置信规则。`
  }
  if (radar.model_status) {
    return '当前没有股票同时满足高置信综合评分、正预期溢价、风险过滤和断头铡刀过滤。'
  }
  return '暂无预测数据，点击“实时预测”获取最新候选池。'
})
const expectedPremium = (pick) => Number(pick.expected_premium ?? pick.predicted_open_premium ?? 0)
const actualPremium = (pick) => {
  if (pick.open_premium === null || pick.open_premium === undefined) return null
  return Number(pick.open_premium)
}
const premiumError = (pick) => {
  const actual = actualPremium(pick)
  if (actual === null) return null
  return Math.abs(actual - expectedPremium(pick))
}
const premiumErrorText = (pick) => {
  const value = premiumError(pick)
  return value === null ? '-' : pct(value)
}
const precisionBadgeText = (pick) => {
  const value = premiumError(pick)
  if (value === null) return '等待回填'
  return value < 1 ? '外推精准' : '偏差过大'
}
const precisionBadgeClass = (pick) => [
  'precision-badge',
  premiumError(pick) === null ? 'precision-pending' : premiumError(pick) < 1 ? 'precision-good' : 'precision-bad',
]
const exitActionText = (pick) => pick.exit_action || (actualPremium(pick) === null ? '待审判' : '落袋为安')
const exitActionClass = (pick) => [
  'exit-tag',
  pick.exit_level === 'danger' || exitActionText(pick) === '核按钮'
    ? 'exit-danger'
    : pick.exit_level === 'strong' || exitActionText(pick) === '超预期锁仓'
      ? 'exit-strong'
      : actualPremium(pick) === null
        ? 'exit-pending'
        : 'exit-profit',
]
const premiumBarClass = (value) => ['premium-bar', Number(value) >= 0 ? 'premium-bar-up' : 'premium-bar-down']
const premiumBarStyle = (value) => ({
  width: `${Math.min(Math.max(Math.abs(Number(value ?? 0)) * 18, 4), 100)}%`,
})

const request = async (path, options = {}) => {
  const response = await fetch(`${API}${path}`, options)
  if (!response.ok) {
    const text = await response.text()
    throw new Error(text || response.statusText)
  }
  return response.json()
}

const setMessage = (text, type = 'info') => {
  message.value = text
  messageType.value = type
}

const setActiveTab = async (tabId) => {
  activeTab.value = tabId
  if (tabId === 'strategy' && !loaded.strategy) {
    loaded.strategy = true
    await Promise.all([loadBacktest(), loadStrategyLab(), loadFailureAnalysis()])
  }
  if (tabId === 'data' && !loaded.data) {
    loaded.data = true
    await loadOverview()
  }
  if (tabId === 'stock' && !loaded.stock) {
    loaded.stock = true
    await loadHistory()
  }
}

const loadOverview = async () => {
  busy.overview = true
  try {
    overview.value = await request('/api/overview')
  } catch (error) {
    setMessage(`状态读取失败：${error.message}`, 'error')
  } finally {
    busy.overview = false
  }
}

const applyRadarPayload = (data) => {
  radar.rows = (data.rows || []).slice(0, 10)
  radar.created_at = data.created_at || ''
  radar.model_status = data.model_status || data.strategy || ''
  radar.market_gate = data.market_gate || null
  radar.strategy = data.strategy || ''
  radar.intraday_snapshot = data.intraday_snapshot || null
}

const loadCachedRadar = async () => {
  try {
    const data = await request('/api/radar/cache')
    applyRadarPayload(data)
  } catch (error) {
    setMessage(`预测缓存读取失败：${error.message}`, 'error')
  }
}

const scanRadar = async () => {
  busy.scan = true
  try {
    const data = await request('/api/radar/scan?limit=10')
    applyRadarPayload(data)
    setMessage(radar.rows.length ? `完成实时预测并写入缓存，展示 ${radar.rows.length} 只股票。` : '完成实时预测：当前无高置信候选，空仓避险。', 'info')
    await loadOverview()
  } catch (error) {
    setMessage(`实时预测失败：${error.message}`, 'error')
  } finally {
    busy.scan = false
  }
}

const loadDailyPicks = async () => {
  try {
    const data = await request('/api/daily-picks?limit=20')
    dailyPicks.rows = data.rows || []
  } catch (error) {
    setMessage(`14:50 推送标的读取失败：${error.message}`, 'error')
  }
}

const loadBacktest = async (refresh = false) => {
  busy.backtest = true
  try {
    const data = await request(`/api/backtest/top-pick-open?months=12${refresh ? '&refresh=true' : ''}`)
    backtest.summary = data.summary
    backtest.rows = data.rows || []
    backtest.strategy_rows = data.strategy_rows || []
    backtest.created_at = data.created_at
  } catch (error) {
    setMessage(`复盘统计失败：${error.message}`, 'error')
  } finally {
    busy.backtest = false
  }
}

const loadStrategyLab = async (refresh = false) => {
  busy.strategyLab = true
  try {
    const data = await request(`/api/strategy/lab?months=12${refresh ? '&refresh=true' : ''}`)
    strategyLab.summary = data.summary
    strategyLab.variants = data.variants || []
    strategyLab.thresholds = data.thresholds || []
    strategyLab.daily_picks = data.daily_picks || []
    strategyLab.created_at = data.created_at
  } catch (error) {
    setMessage(`策略实验失败：${error.message}`, 'error')
  } finally {
    busy.strategyLab = false
  }
}

const loadFailureAnalysis = async (refresh = false) => {
  busy.failureAnalysis = true
  try {
    const data = await request(`/api/strategy/failure-analysis?months=12${refresh ? '&refresh=true' : ''}`)
    failureAnalysis.summary = data.summary
    failureAnalysis.reasons = data.reasons || []
    failureAnalysis.optimizations = data.optimizations || []
    failureAnalysis.sample_failures = data.sample_failures || []
    failureAnalysis.created_at = data.created_at
  } catch (error) {
    setMessage(`失败归因分析失败：${error.message}`, 'error')
  } finally {
    busy.failureAnalysis = false
  }
}

const loadUpReason = async (refresh = false) => {
  busy.upReason = true
  try {
    const data = await request(`/api/strategy/up-reason-analysis?months=12${refresh ? '&refresh=true' : ''}`)
    upReason.summary = data.summary
    upReason.factor_lifts = data.factor_lifts || []
    upReason.model_report = data.model_report || { feature_importance: [] }
    upReason.llm_summary = data.llm_summary || {}
    upReason.up_examples = data.up_examples || []
    upReason.down_examples = data.down_examples || []
    upReason.created_at = data.created_at
  } catch (error) {
    setMessage(`上涨原因分析失败：${error.message}`, 'error')
  } finally {
    busy.upReason = false
  }
}

const runValidation = async () => {
  busy.validate = true
  try {
    const data = await request(`/api/data/validate?sample=${validationSample.value}&source_check=${sourceCheck.value}`, {
      method: 'POST',
    })
    validation.status = data.status
    validation.summary = data.summary
    validation.issues = data.issues || []
    setMessage(`验证完成：${data.status}，错误 ${data.summary.error_count}，警告 ${data.summary.warning_count}。`, data.status === 'pass' ? 'info' : 'error')
    await loadOverview()
  } catch (error) {
    setMessage(`验证失败：${error.message}`, 'error')
  } finally {
    busy.validate = false
  }
}

const syncData = async () => {
  busy.sync = true
  try {
    const data = await request('/api/data/market-sync/run', { method: 'POST' })
    syncResult.value = JSON.stringify(data, null, 2)
    setMessage(`盘后同步完成：有效 ${data.valid_rows} 行，新增 ${data.inserted_rows}，更新 ${data.updated_rows}。`, 'info')
    await loadOverview()
  } catch (error) {
    setMessage(`同步失败：${error.message}`, 'error')
  } finally {
    busy.sync = false
  }
}

const loadHistory = async () => {
  if (!/^\d{6}$/.test(selectedCode.value)) {
    setMessage('请输入 6 位股票代码。', 'error')
    return
  }
  try {
    const data = await request(`/api/data/history/${selectedCode.value}?limit=120`)
    historyRows.value = data.rows || []
  } catch (error) {
    historyRows.value = []
    setMessage(`历史读取失败：${error.message}`, 'error')
  }
}

const inspectStock = async (stock) => {
  drawer.open = true
  drawer.stock = stock
  analysis.value = null
  selectedCode.value = stock.code
  await loadHistory()
}

const analyzeStock = async () => {
  if (!drawer.stock) return
  busy.analyze = true
  try {
    analysis.value = await request('/api/radar/analyze', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ code: drawer.stock.code, name: drawer.stock.name }),
    })
  } catch (error) {
    setMessage(`舆情分析失败：${error.message}`, 'error')
  } finally {
    busy.analyze = false
  }
}

const chartData = computed(() => {
  const rows = historyRows.value.filter((row) => Number(row.close) > 0)
  if (rows.length < 2) return { points: [], minPrice: 0, maxPrice: 0 }
  const visibleRows = rows.slice(-90)
  const prices = visibleRows.flatMap((row) => [Number(row.high), Number(row.low), Number(row.open), Number(row.close)]).filter((value) => value > 0)
  const volumes = visibleRows.map((row) => Number(row.volume ?? 0))
  const minPrice = Math.min(...prices)
  const maxPrice = Math.max(...prices)
  const maxVolume = Math.max(...volumes, 1)
  const priceSpread = maxPrice - minPrice || 1
  const left = 54
  const right = 724
  const top = 38
  const bottom = 218
  const volumeBottom = 270
  const volumeMaxHeight = 42
  const step = (right - left) / Math.max(visibleRows.length - 1, 1)
  const candleWidth = Math.max(3, Math.min(9, step * 0.58))
  const volumeWidth = Math.max(2, Math.min(7, step * 0.42))
  const yPrice = (value) => bottom - ((Number(value) - minPrice) / priceSpread) * (bottom - top)

  return {
    minPrice,
    maxPrice,
    points: visibleRows.map((row, index) => {
      const volumeHeight = (Number(row.volume ?? 0) / maxVolume) * volumeMaxHeight
      return {
        ...row,
        date: row.date,
        open: Number(row.open ?? 0),
        high: Number(row.high ?? 0),
        low: Number(row.low ?? 0),
        close: Number(row.close ?? 0),
        change_pct: Number(row.change_pct ?? 0),
        turnover: Number(row.turnover ?? 0),
        amount: Number(row.amount ?? 0),
        x: left + index * step,
        yOpen: yPrice(row.open),
        yHigh: yPrice(row.high),
        yLow: yPrice(row.low),
        yClose: yPrice(row.close),
        candleWidth,
        volumeWidth,
        volumeHeight,
        volumeY: volumeBottom - volumeHeight,
      }
    })
  }
})

onMounted(async () => {
  await loadOverview()
  await loadCachedRadar()
  await loadDailyPicks()
})
</script>

<style scoped>
* {
  box-sizing: border-box;
}

.page {
  min-height: 100vh;
  background:
    linear-gradient(180deg, #eef4fb 0, #f7f8fb 260px),
    #f7f8fb;
  color: #162033;
  padding: 24px;
}

.topbar,
.panel,
.metric,
.notice,
.tabbar {
  border: 1px solid #d9e0ea;
  background: #ffffff;
  border-radius: 8px;
}

.topbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 18px 20px;
  box-shadow: 0 10px 24px rgba(22, 32, 51, 0.06);
}

.eyebrow {
  margin: 0 0 4px;
  color: #66758c;
  font-size: 0.82rem;
  text-transform: uppercase;
}

h1,
h2,
h3,
p {
  margin: 0;
}

h1 {
  font-size: 1.55rem;
  line-height: 1.2;
}

h2 {
  font-size: 1.05rem;
  line-height: 1.25;
}

button {
  border: 1px solid #1f5fbf;
  background: #2563eb;
  color: #ffffff;
  border-radius: 6px;
  padding: 8px 12px;
  font-weight: 700;
  cursor: pointer;
  min-height: 36px;
  max-width: 100%;
  white-space: nowrap;
}

button:hover:not(:disabled) {
  filter: brightness(0.98);
}

button:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

input {
  border: 1px solid #c9d3e1;
  border-radius: 6px;
  padding: 8px 10px;
  color: #162033;
  background: #ffffff;
  max-width: 100%;
}

.toolbar,
.sync-row,
.stock-search,
.audit-actions {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.toolbar button {
  flex: 0 0 auto;
}

.status-grid {
  display: grid;
  grid-template-columns: repeat(5, minmax(150px, 1fr));
  gap: 10px;
  margin: 14px 0;
}

.metric {
  padding: 12px 14px;
  min-width: 0;
}

.metric span,
.panel-head p,
.issue span,
label {
  color: #66758c;
  font-size: 0.86rem;
}

.metric strong {
  display: block;
  margin-top: 5px;
  font-size: 1.05rem;
  line-height: 1.25;
  overflow-wrap: anywhere;
}

.metric small {
  display: block;
  margin-top: 6px;
  color: #66758c;
  line-height: 1.4;
}

.notice {
  margin-bottom: 16px;
  padding: 12px 14px;
}

.tabbar {
  position: sticky;
  top: 12px;
  z-index: 5;
  display: grid;
  grid-template-columns: repeat(4, minmax(128px, 1fr));
  gap: 8px;
  padding: 8px;
  margin-bottom: 16px;
  box-shadow: 0 8px 22px rgba(22, 32, 51, 0.05);
}

.tabbar button {
  display: grid;
  gap: 2px;
  min-height: 56px;
  border-color: transparent;
  background: transparent;
  color: #44546a;
  text-align: left;
  padding: 9px 12px;
}

.tabbar button.active {
  border-color: #bfd1ea;
  background: #eef6ff;
  color: #123b74;
}

.tabbar span {
  font-size: 0.98rem;
}

.tabbar small {
  color: #74839a;
  font-weight: 600;
}

.notice.error {
  border-color: #ef9a9a;
  background: #fff5f5;
  color: #b91c1c;
}

.workbench {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 380px;
  gap: 16px;
}

.single-panel {
  grid-template-columns: minmax(0, 1fr);
}

.detail-grid {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 380px;
  gap: 16px;
  margin-top: 16px;
}

.pick-panel,
.shadow-panel,
.backtest-panel,
.strategy-panel {
  margin-top: 16px;
}

.panel {
  padding: 16px;
  min-width: 0;
  box-shadow: 0 8px 18px rgba(22, 32, 51, 0.04);
}

.panel-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 14px;
  min-width: 0;
}

.panel-head.compact {
  align-items: center;
  flex-wrap: wrap;
}

.panel-head > div {
  min-width: 0;
}

.panel-head p {
  line-height: 1.45;
  overflow-wrap: anywhere;
}

.model-state {
  color: #40516b;
  font-size: 0.78rem;
  max-width: 260px;
  text-align: right;
  overflow-wrap: anywhere;
}

.muted-line {
  color: #66758c;
}

.empty-state {
  display: grid;
  gap: 6px;
  border: 1px solid #d9e0ea;
  border-radius: 8px;
  padding: 14px 16px;
  margin-bottom: 12px;
  background: #f8fafc;
}

.empty-state strong {
  font-size: 1rem;
}

.empty-state span {
  color: #66758c;
  line-height: 1.5;
}

.risk-off {
  border-color: #fed7aa;
  background: #fff7ed;
}

.risk-off strong {
  color: #b45309;
}

.table-wrap {
  overflow-x: auto;
  max-width: 100%;
  -webkit-overflow-scrolling: touch;
}

table {
  width: 100%;
  border-collapse: collapse;
  min-width: 860px;
}

th,
td {
  border-bottom: 1px solid #e5eaf1;
  padding: 9px 8px;
  text-align: left;
  font-size: 0.89rem;
  vertical-align: middle;
}

th {
  color: #66758c;
  font-size: 0.78rem;
  text-transform: uppercase;
  background: #f8fafc;
}

tbody tr:hover {
  background: #fbfdff;
}

.num {
  text-align: right;
}

.mono {
  font-family: 'SF Mono', Menlo, monospace;
}

.up,
.fail,
.error {
  color: #c2410c;
}

.down,
.pass {
  color: #15803d;
}

.pending {
  color: #b45309;
}

.score {
  font-weight: 800;
}

.pill {
  display: inline-flex;
  align-items: center;
  min-height: 24px;
  padding: 3px 8px;
  border: 1px solid #c9d3e1;
  border-radius: 999px;
  background: #f8fafc;
  color: #334155;
  font-size: 0.78rem;
  font-weight: 800;
  white-space: nowrap;
}

.strategy-badge {
  display: inline-flex;
  align-items: center;
  min-height: 22px;
  margin-left: 6px;
  padding: 2px 8px;
  border: 1px solid transparent;
  border-radius: 999px;
  font-size: 0.74rem;
  font-weight: 800;
  line-height: 1.2;
  white-space: nowrap;
  vertical-align: middle;
}

.strategy-badge-breakout {
  border-color: #bfdbfe;
  background: #dbeafe;
  color: #1e40af;
}

.strategy-badge-dipbuy {
  border-color: #fed7aa;
  background: #ffedd5;
  color: #9a3412;
}

.shadow-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
  gap: 12px;
}

.shadow-card {
  display: grid;
  gap: 12px;
  border: 1px solid #e1e7f0;
  border-radius: 8px;
  padding: 12px;
  background: #fbfcfe;
  min-width: 0;
}

.shadow-card header,
.shadow-card footer {
  display: flex;
  justify-content: space-between;
  gap: 10px;
  align-items: flex-start;
  min-width: 0;
  flex-wrap: wrap;
}

.shadow-card header strong {
  display: block;
  line-height: 1.35;
  overflow-wrap: anywhere;
}

.shadow-card small,
.shadow-side span,
.shadow-card footer span {
  color: #66758c;
  font-size: 0.78rem;
  line-height: 1.35;
}

.shadow-compare {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
}

.shadow-side {
  display: grid;
  gap: 7px;
  border: 1px solid #e5eaf1;
  border-radius: 6px;
  padding: 10px;
  background: #ffffff;
  min-width: 0;
}

.shadow-side strong {
  font-size: 1.05rem;
}

.premium-track {
  position: relative;
  height: 8px;
  border-radius: 999px;
  background: #e5eaf1;
  overflow: hidden;
}

.premium-bar {
  display: block;
  height: 100%;
  border-radius: inherit;
}

.premium-bar-up {
  background: #f97316;
}

.premium-bar-down {
  background: #22c55e;
}

.precision-badge,
.exit-tag {
  display: inline-flex;
  align-items: center;
  min-height: 24px;
  padding: 3px 8px;
  border: 1px solid transparent;
  border-radius: 999px;
  font-size: 0.75rem;
  font-weight: 800;
  white-space: nowrap;
}

.precision-good {
  border-color: #bbf7d0;
  background: #dcfce7;
  color: #166534;
}

.precision-bad,
.exit-danger {
  border-color: #fecaca;
  background: #fee2e2;
  color: #b91c1c;
}

.precision-pending,
.exit-pending {
  border-color: #fde68a;
  background: #fffbeb;
  color: #92400e;
}

.exit-profit {
  border-color: #bbf7d0;
  background: #dcfce7;
  color: #166534;
}

.exit-strong {
  border-color: #fed7aa;
  background: #ffedd5;
  color: #9a3412;
}

.shadow-instruction {
  color: #40516b;
  font-size: 0.86rem;
  line-height: 1.45;
}

td > .strategy-badge:first-child {
  margin-left: 0;
}

td small {
  display: block;
  margin-top: 4px;
  color: #66758c;
  line-height: 1.35;
}

.link-btn {
  border-color: #c9d3e1;
  background: #ffffff;
  color: #1f5fbf;
  min-width: 56px;
}

.audit-actions {
  align-items: flex-end;
  margin-bottom: 14px;
}

.audit-actions label,
.sync-row label {
  display: grid;
  gap: 5px;
}

.sync-detail {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px;
  margin-top: 12px;
}

.sync-detail div {
  border: 1px solid #e1e7f0;
  border-radius: 6px;
  padding: 10px;
  background: #fbfcfe;
}

.sync-detail span {
  display: block;
  color: #66758c;
  font-size: 0.78rem;
}

.sync-detail strong {
  display: block;
  margin-top: 5px;
  overflow-wrap: anywhere;
}

.check-line {
  display: flex !important;
  align-items: center;
  grid-template-columns: none;
}

.audit-summary,
.backtest-summary,
.feature-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 8px;
  margin-bottom: 12px;
}

.strategy-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.2fr) minmax(320px, 0.8fr);
  gap: 14px;
}

.panel-subhead {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  margin: 16px 0 10px;
}

.panel-subhead h3 {
  margin: 0 0 4px;
  font-size: 0.98rem;
}

.panel-subhead p {
  margin: 0;
  color: #66758c;
  line-height: 1.45;
}

.compact-table table {
  min-width: 760px;
}

.compact-table {
  max-height: 420px;
  border: 1px solid #e5eaf1;
  border-radius: 6px;
  overflow: auto;
}

.compact-table thead th {
  position: sticky;
  top: 0;
  z-index: 1;
  box-shadow: 0 1px 0 #e5eaf1;
}

.failure-samples {
  margin-top: 14px;
}

.audit-summary div,
.backtest-summary div,
.feature-grid div,
.issue,
.analysis-box {
  border: 1px solid #e1e7f0;
  border-radius: 6px;
  padding: 10px;
  background: #fbfcfe;
  min-width: 0;
}

.audit-summary span,
.backtest-summary span,
.feature-grid span {
  display: block;
  color: #66758c;
  font-size: 0.78rem;
}

.backtest-summary strong {
  display: block;
  margin-top: 5px;
  overflow-wrap: anywhere;
}

.issue-list {
  display: grid;
  gap: 8px;
  max-height: 360px;
  overflow-y: auto;
}

.issue div {
  display: flex;
  justify-content: space-between;
  gap: 8px;
}

.issue p {
  margin-top: 6px;
  line-height: 1.45;
  font-size: 0.88rem;
}

.empty {
  color: #74839a;
  text-align: center;
  padding: 18px;
}

.chart-box {
  position: relative;
  border: 1px solid #e1e7f0;
  border-radius: 6px;
  background: #fbfcfe;
  overflow: hidden;
}

.chart {
  width: 100%;
  height: 320px;
  display: block;
}

.axis-label {
  fill: #66758c;
  font-size: 11px;
}

.chart-tooltip {
  position: absolute;
  z-index: 3;
  min-width: 180px;
  border: 1px solid #bfdbfe;
  border-radius: 6px;
  background: rgba(255, 255, 255, 0.96);
  box-shadow: 0 10px 26px rgba(15, 23, 42, 0.16);
  padding: 9px 10px;
  pointer-events: none;
}

.chart-tooltip strong,
.chart-tooltip span {
  display: block;
  font-size: 0.8rem;
  line-height: 1.5;
}

.chart-tooltip strong {
  color: #1e40af;
}

pre {
  margin: 14px 0 0;
  max-height: 220px;
  overflow: auto;
  background: #0f172a;
  color: #dbeafe;
  border-radius: 6px;
  padding: 12px;
}

.drawer-mask {
  position: fixed;
  inset: 0;
  display: flex;
  justify-content: flex-end;
  background: rgba(15, 23, 42, 0.35);
  z-index: 20;
}

.drawer {
  width: min(520px, 100vw);
  height: 100vh;
  overflow-y: auto;
  background: #ffffff;
  border-left: 1px solid #d9e0ea;
  padding: 18px;
}

.drawer header {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 16px;
  align-items: flex-start;
}

.feature-grid {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.wide-btn {
  width: 100%;
  margin: 8px 0 14px;
}

.analysis-box {
  line-height: 1.6;
}

.compact-analysis {
  margin-bottom: 12px;
}

.analysis-box ul {
  padding-left: 18px;
}

@media (prefers-color-scheme: dark) {
  .strategy-badge-breakout {
    border-color: #60a5fa;
    background: #172554;
    color: #bfdbfe;
  }

  .strategy-badge-dipbuy {
    border-color: #fb923c;
    background: #431407;
    color: #fed7aa;
  }

  .precision-good,
  .exit-profit {
    border-color: #22c55e;
    background: #052e16;
    color: #bbf7d0;
  }

  .precision-bad,
  .exit-danger {
    border-color: #ef4444;
    background: #450a0a;
    color: #fecaca;
  }

  .precision-pending,
  .exit-pending,
  .exit-strong {
    border-color: #fb923c;
    background: #431407;
    color: #fed7aa;
  }
}

@media (max-width: 980px) {
  .status-grid,
  .tabbar,
  .workbench,
  .detail-grid,
  .strategy-grid {
    grid-template-columns: 1fr;
  }

  .topbar {
    align-items: flex-start;
    flex-direction: column;
  }

  .toolbar {
    width: 100%;
  }

  .toolbar button {
    flex: 1 1 128px;
  }

  .model-state {
    text-align: left;
    max-width: none;
  }
}

@media (max-width: 640px) {
  .page {
    padding: 14px;
  }

  .panel,
  .topbar {
    padding: 14px;
  }

  .panel-head {
    flex-direction: column;
  }

  .audit-actions,
  .stock-search,
  .sync-row {
    align-items: stretch;
  }

  .audit-actions > *,
  .stock-search > *,
  .sync-row > *,
  .toolbar button {
    width: 100%;
  }

  .feature-grid {
    grid-template-columns: 1fr;
  }

  .shadow-compare {
    grid-template-columns: 1fr;
  }

  table {
    min-width: 760px;
  }
}
</style>
