# AGENTS.md - A 股量化工作站开发上下文

更新时间：2026-05-03

本文件给后续接手本项目的 Codex/Agent 使用。进入项目后先读本文件，再读 `TECHNICAL_DOC.md`、`PROJECT_STRUCTURE_GUIDE.md` 和相关源码。项目根目录固定为 `/Users/eudis/ths`。

## 项目定位

这是运行在本地 macOS/M4 上的 A 股量化工作站。核心目标是采集本地日线/分钟线/实时快照数据，训练 XGBoost 结构化模型，执行 14:50 多策略影子出票，使用 PushPlus 推送交易指令，并通过 FastAPI + Vue 深色金融终端展示结果。

当前系统版本：V4.4 "Theme Alpha"。

当前系统同时包含三条主线：

1. 生产影子测试主线：`quant_core/engine/predictor.py` 驱动四大核心军团独立分档出票，每个策略基准线最多 Top 3；若无达标票，则按当日合规池 99 分位动态下探 1 只并提示风偏。
2. V4.4 Theme Alpha 全局日线模型主线：`quant_dashboard/backend/routers/v3_sniper.py` 同时挂载 `/api/v4/sniper/*`，使用带 `theme_*` 因子的全市场 XGBoost 分类模型，实时拼接当日腾讯行情后输出高置信候选。
3. 09:15 实时巡逻兵主线：`live_sentinel.py` 从上一交易日 14:50 标的重建 `shadow_ledger.json`，盘中执行止损、追踪止盈、分钟级爆量滞涨和五档盘口委比反转预警。

重要原则：

- LLM/Ollama 只做舆情、公告、新闻风控，不直接预测价格，也不覆盖 XGBoost 排序。
- 实盘影子测试买入锚点是 14:50 的 `snapshot_price` 与 `snapshot_time`，这些字段必须不可篡改。
- 生产出票必须保留 V4.4 风控契约：`selection_tier`、`risk_warning`、`dynamic_floor`/`score_floor`、`suggested_position` 必须随 `raw_json.winner` 落库，并在 PushPlus 与前端展示。
- 禁止把 `.env`、PushPlus token、聚宽账号密码等密钥写入文档、日志或提交。
- 禁止新增或恢复 AkShare/东方财富 行情与板块数据接入；实时快照优先使用腾讯，行业/板块日线只走 mpquant/Ashare `get_price` + 本地缓存。
- 可以存在脏工作区。不要回滚或覆盖用户已有修改，除非用户明确要求。

## 技术栈

后端与量化：

- Python 3.9+、FastAPI、Uvicorn
- Pandas、NumPy、PyArrow/Parquet
- XGBoost、scikit-learn
- SQLite
- Requests
- jqdatasdk
- Ollama 本地大模型
- PushPlus 微信推送

前端：

- Vue 3
- Vite
- Element Plus
- 深色交易终端主题
- 轻量自定义路由解析，当前没有完整 `vue-router` 依赖

自动化：

- macOS LaunchAgent
- `scripts/shell/*.sh`
- 日志在 `logs/` 及各服务目录下

## 关键目录

```text
/Users/eudis/ths
├── AGENTS.md
├── TECHNICAL_DOC.md
├── PROJECT_STRUCTURE_GUIDE.md
├── data/
│   ├── all_kline/                 # 全市场日线 Parquet，约 5515 只股票
│   ├── min_kline/5m/              # 5 分钟线 Parquet
│   ├── concept_kline/             # 细分概念指数日线 Parquet，Theme Pipeline 优先引擎
│   ├── sector_kline/              # 一级行业指数日线 Parquet，Theme Pipeline 兜底引擎
│   ├── concept_stock_map.json     # 个股 -> 主概念映射
│   ├── core_db/quant_workstation.sqlite3
│   └── intraday/price_1430.json   # 14:30 快照
├── models/                        # XGBoost 模型与 meta
├── live_sentinel.py                # 实时巡逻兵入口
├── shadow_ledger.json              # 实时巡逻兵账本
├── quant_core/                    # 核心量化、数据、执行、AI、回测
├── quant_dashboard/               # FastAPI + Vue 看板
├── scripts/                       # 数据集、训练、shell、工具脚本
├── launch_agents/                 # LaunchAgent 源 plist
├── tests/                         # 干跑/连通性测试
└── rebuild_historical_picks.py    # 历史账本重建
```

## 核心配置

配置入口：`/Users/eudis/ths/quant_core/config.py`。

常用路径：

- `BASE_DIR=/Users/eudis/ths`
- `DATA_DIR=/Users/eudis/ths/data/all_kline`
- `MIN_KLINE_DIR=/Users/eudis/ths/data/min_kline`
- `SQLITE_PATH=/Users/eudis/ths/data/core_db/quant_workstation.sqlite3`
- `MODELS_DIR=/Users/eudis/ths/models`

关键策略门槛默认值：

- `QUANT_BREAKOUT_MIN_SCORE=65.50`
- `QUANT_DIPBUY_MIN_SCORE=99.00`，首阴低吸保留但生产冻结
- `QUANT_REVERSAL_MIN_SCORE=3.00`
- `QUANT_MAIN_WAVE_MIN_SCORE=3.00`
- `QUANT_GLOBAL_MIN_SCORE=0.85`

推送配置：

- `PUSHPLUS_TOKEN` 从 `.env` 读取。
- 使用 `quant_core.config.check_push_config()` 检查配置。
- 不要在任何输出中泄露 token。

Ollama：

- 默认接口：`OLLAMA_API=http://127.0.0.1:11434/api/generate`
- 当前常用模型：`qwen2.5:14b`
- `quant_core/ai_agent/llm_engine.py` 支持 OpenAI compatible `/v1/chat/completions` 和 `/api/generate` 兜底。

## 数据源与数据口径

日线：

- 主库是 `data/all_kline/*_daily.parquet`。
- 常见字段：`symbol,date,open,high,low,close,volume,amount,turn,pctChg,MA5,MA10,MA20,量比,MACD_DIF` 等。
- V4.4 全局模型会读取本地历史日线尾部约 80 天，再拼接今天实时行情行。

分钟线：

- 路径：`data/min_kline/5m/{code}.parquet`。
- 冷数据：聚宽，定时入口 `scripts/shell/run_jq_cold_5m.sh`，主脚本 `scripts/data_pipeline/batch_fetch_historical_min.py`；`scripts/data_pipeline/batch_fetch_jq_history.py` 为旧手工入口。
- 聚宽账号历史权限窗口会随自然日滚动，冷数据脚本会用 `JQ_ROLLING_START_LOOKBACK_DAYS=465` 自动把请求起点夹到当前允许窗口内，避免固定早期日期越界。
- 聚宽断点文件是 `data/min_kline/5m/jq_cold_5m_progress.json`，汇总文件是 `data/min_kline/5m/jq_summary_*.json`。
- 热数据：腾讯/Ashare 风格，脚本 `scripts/data_pipeline/daily_ashare_archiver.py`、`fast_fetch_today_m5.py`。
- Theme Pipeline：`quant_core/data_pipeline/concept_engine.py` 负责细分概念指数日线缓存与 `data/concept_stock_map.json` 主概念映射，`quant_core/data_pipeline/sector_engine.py` 负责一级行业指数日线与 `stock_sector_map.parquet/csv` 兜底映射。
- `daily_factor_factory.py` 的 `theme_*` 特征固定按“细分概念优先，一级行业兜底，无数据释放 NaN”级联计算；禁止把缺失主题数据填 0，XGBoost 原生学习 NaN 分裂。
- 板块/主题日线只允许 mpquant/Ashare `get_price`、腾讯/新浪底层或本地合成缓存；禁止新增或恢复 AkShare/东方财富 行情与板块数据接入。

实时行情：

- 腾讯实时引擎：`quant_core/data_pipeline/tencent_engine.py`。
- `get_tencent_realtime(code)` 使用 `http://qt.gtimg.cn/q={symbol}`，直接裸连，`trust_env=False`。
- `get_tencent_m5(code, count=48)` 使用腾讯 5m K 线接口。
- `quant_core/data_pipeline/market.py` 是实时行情门面。个股实时和竞价应优先走腾讯。

竞价与快照：

- 09:16/09:21/09:25 早盘哨兵使用实时快照。
- 14:30 快照写入 `data/intraday/price_1430.json`，用于尾盘诱多过滤。
- 14:50 影子出票写入 `daily_picks.snapshot_price`、`snapshot_time`、`snapshot_vol_ratio`。

## 数据库

SQLite 主库：`/Users/eudis/ths/data/core_db/quant_workstation.sqlite3`。

核心存储模块：`quant_core/storage.py`。

重点表：

- `daily_picks`：生产影子测试/历史账本。
- `market_sync_runs`：盘后同步记录。
- validation/report/cache 相关表由存储层维护。

`daily_picks` 关键字段：

- `selection_date`
- `code`
- `name`
- `strategy_type`
- `snapshot_time`
- `snapshot_price`
- `snapshot_vol_ratio`
- `is_shadow_test`
- `expected_premium`
- `composite_score`
- `open_price`
- `open_premium`
- `t3_max_gain_pct`
- `is_closed`
- `close_date`
- `close_return_pct`
- push 状态字段
- `raw_json.winner.selection_tier`
- `raw_json.winner.risk_warning`
- `raw_json.winner.dynamic_floor` / `score_floor`
- `raw_json.winner.suggested_position`

安全边界：

- `snapshot_price` 和 `snapshot_time` 是影子测试原始证据，盘后修复、早盘哨兵、历史重建都不能覆盖真实实盘记录。
- 存储层有不可变触发器。不要绕过 `storage.py` 直接写 SQL 去改快照字段。
- 若需要清空模拟账本，先确认用户明确要求，并区分 `is_shadow_test` 真实数据。

## 生产策略

### 尾盘突破

- 策略名：`尾盘突破`
- 模型：`models/overnight_premium_xgboost.json`
- 生命周期：T 日 14:50 锁定，T+1 开盘验证。
- 结果字段：`open_premium`
- 风控：非主板/ST过滤、尾盘诱多、断头铡刀、上影线、高位爆量、准涨停未封。

### 中线超跌反转

- 策略名：`中线超跌反转`
- 模型：`models/reversal_t3_xgboost.json`
- 生命周期：T 日 14:50 锁定，T+1 到 T+3 波段观察。
- 结果字段：`t3_max_gain_pct`
- 物理过滤：跌破 60 日线、60 日回撤、地量洗盘、倍量一阳穿线、防伪 2.0 均线高压与斜率过滤。

### 右侧主升浪

- 策略名：`右侧主升浪`
- 模型：`models/main_wave_t3_xgboost.json`
- 生命周期：T 日 14:50 锁定，T+1 到 T+3 波段观察。
- 结果字段：`t3_max_gain_pct`
- 物理过滤：20 日线 > 60 日线、强势区间、高位缩量蓄势、平台突破、实体攻击、温和放量。

### 全局动量狙击

- 策略名：`全局动量狙击`
- 模型：`models/xgboost_daily_swing_global_v1.json`
- 生命周期：T 日 14:50 锁定，T+1 到 T+3 波段观察。
- 结果字段：`t3_max_gain_pct`
- 核心因子：`theme_pct_chg_1`、`theme_pct_chg_3`、`theme_volatility_5`、`rs_stock_vs_theme`、`rs_theme_ema_5`。
- Theme Pipeline：细分概念优先，一级行业兜底，无数据保持 NaN。
- 实盘硬过滤：只保留 `00/60` 主板，剔除 ST / *ST，剔除 14:50 实时涨幅 `>= 9.0%` 的涨停或准涨停票。

### V4.4 分档与仓位

- `ABSOLUTE_BOTTOM_PROBA=0.55` 是全策略通用绝对安全底线，禁止恢复人工 `_FLOOR_SCORE` 静态配置。
- `select_strategy_top_picks()` 对每个策略先找 `score >= MIN_SCORE` 的 `base` 档，最多取 Top 3。
- 若 `base` 档为空，计算 `dynamic_floor=max(ABSOLUTE_BOTTOM_PROBA, legal_pool['score'].quantile(0.99))`，只有 Top 1 分数达到动态底线才以 `selection_tier=dynamic_floor` 出票。
- `base` 档仓位使用 Half-Kelly，限制在 10% 到 30%；`dynamic_floor` 档固定 5% 试错轻仓。
- 终端日志必须打印 `[AdaptiveFloor] ... dynamic_floor=...`，便于复盘当天动态及格线。

### 首阴低吸

- 策略名：`首阴低吸`
- 模型：`models/dipbuy_premium_xgboost.json`
- 当前默认 `QUANT_DIPBUY_MIN_SCORE=99.00`，等价生产冻结。
- 保留代码、模型和可视化兼容，不要随意删除。

## V4.4 Theme Alpha 全局日线 XGBoost 雷达

核心模块：

- `quant_core/engine/daily_factor_factory.py`
- `quant_core/engine/daily_model_trainer.py`
- `quant_core/engine/model_evaluator.py`
- `quant_core/data_pipeline/concept_engine.py`
- `quant_core/data_pipeline/sector_engine.py`
- 模型：`models/xgboost_daily_swing_global_v1.json`
- 元数据：`models/xgboost_daily_swing_global_v1.meta.json`

标签：

- 预测未来 3 个交易日最高收益率是否超过 4%。
- 严格按全局时间切分训练/测试，禁止随机打乱。

近期样本外阈值表现：

- `0.80` 精确率约 `67.80%`
- `0.90` 精确率约 `84.75%` 到 `85.78%`

FastAPI 实盘推理入口：

- `GET /api/v4/sniper/scan_today`
- `GET /api/v3/sniper/scan_today`
- 逻辑：本地历史日线尾部 + 腾讯今日实时行 -> `generate_daily_factors()` -> 按 meta 的 `feature_columns` 对齐 -> `predict_proba()`。
- 响应应包含 `prediction_date`、`live_data`、`live_source=tencent.qt`，每只股票必须带 `theme_name`、`theme_source`、`theme_pct_chg_3`。
- `/api/v3/sniper/*` 是兼容入口；新前端和新 Agent 优先使用 `/api/v4/sniper/*`。

注意：

- 必须按模型 `.meta.json` 的特征顺序对齐。
- `limit=0` 表示全市场扫描，可能较慢；默认可用缓存。
- 如果接口只读到旧日期，检查 `_stitch_live_daily_row()` 和 `fetch_realtime_quote()`。

## AI 右脑

目录：`quant_core/ai_agent/`。

职责：

- 新闻/搜索线索抓取：`news_fetcher.py`
- Prompt：`prompts_repo.py`
- Ollama 调用：`llm_engine.py`
- 融合入口：`agent_gateway.py::run_1446_ai_interview`

约束：

- 新闻抓取失败必须返回兜底文本，不得抛异常阻断交易流程。
- LLM 输出只能作为风险排查字段进入 PushPlus 或前端，不能改写结构化模型分数。

## 执行与自动化流程

定时任务源文件在 `launch_agents/`，安装后在 `/Users/eudis/Library/LaunchAgents/`。

常见任务：

- `01:20` 聚宽历史 5m 冷数据额度任务，按月切片补齐，使用断点文件跳过已完成切片，额度耗尽后正常停止并等待次日继续。
- `09:00` PushPlus 心跳。
- `09:15` 实时巡逻兵启动，监控上一交易日 14:50 推送标的。
- `09:16` 早盘预观察。
- `09:21` 撤单关闭后竞价审计。
- `09:25` 终极开盘哨兵，只此阶段允许回填 `open_price`。
- `14:30` 保存全市场快照。
- `14:45` 波段巡逻兵，处理 T+3 策略止盈/止损/清退。
- `14:50` 四大核心军团出票与 PushPlus 推送。
- `14:50` V4.4 全局动量狙击雷达锁定。
- `15:05` 盘后日线同步。
- `15:15` Ashare/腾讯 5m 热数据归档。

重要脚本：

- `scripts/shell/run_backend_api.sh`
- `scripts/shell/run_frontend_dev.sh`
- `scripts/shell/run_exit_sentinel.sh`
- `scripts/shell/run_swing_patrol.sh`
- `scripts/shell/run_live_sentinel.sh`
- `scripts/shell/run_v3_sniper_lock.sh`
- `scripts/shell/run_push_top_pick.sh`
- `scripts/shell/run_market_close_sync.sh`
- `scripts/shell/run_daily_ashare_archiver.sh`
- `scripts/shell/run_jq_cold_5m.sh`
- `scripts/shell/update_launch_agents.sh`

## 实时巡逻兵

入口：

- `live_sentinel.py`
- `scripts/shell/run_live_sentinel.sh`
- `launch_agents/com.eudis.quant.live-sentinel.plist`

账本：

- `shadow_ledger.json`
- `positions[*].buy_price`：上一交易日 14:50 快照价。
- `positions[*].highest_price`：盘中最高价，初始等于 `buy_price`。
- `positions[*].volume_alert_triggered`：当天分钟级爆量滞涨预警去重。
- `positions[*].order_book_alert_triggered`：当天五档盘口委比极限反转预警去重。

运行规则：

- 每天 09:15 启动，默认用上一交易日 14:50 的 `daily_picks` 重建监控列表。
- 09:15 到 11:30、13:00 到 15:00 每 30 秒扫描；午休不扫描；15:00 停止。
- 开仓硬止损：`current_price <= buy_price * 0.97`，推送【初始止损报警】并移除持仓。
- 动态追踪止盈：最高价曾达到 `buy_price * 1.05` 后，若 `current_price <= highest_price * 0.97`，推送【追踪止盈触发】并移除持仓。
- T+3 超时：保持原到期清仓逻辑。
- 分钟级爆量滞涨和五档盘口委比 `<= -80` 只触发 PushPlus 风险预警并写入账本，不直接移除持仓。
- 止损、追踪止盈、T+3 平仓后必须调用 `mark_daily_pick_closed()`，把 `close_price`、`close_return_pct` 和 `close_signal` 同步回 `daily_picks`。

## FastAPI 后端

入口：`quant_dashboard/backend/main.py`。

启动：

```bash
cd /Users/eudis/ths
python3 -m uvicorn quant_dashboard.backend.main:app --host 127.0.0.1 --port 8000
```

LaunchAgent 启动脚本：

```bash
/Users/eudis/ths/scripts/shell/run_backend_api.sh
```

核心接口：

- `GET /health`
- `GET /api/overview`
- `GET /api/radar/cache`
- `GET /api/radar/scan?limit=10`
- `GET /api/daily-picks`
- `GET /api/data/market-sync/latest`
- `GET /api/data/history/{code}`
- `GET /api/data/history_min/{code}?period=5`
- `GET /api/data/minute-fetch/status`
- `GET /api/v3/system/status`
- `GET /api/v3/sniper/signals`
- `GET /api/v3/sniper/scan_today`
- `GET /api/v4/sniper/scan_today`
- `POST /api/v3/agent/analyze`
- `POST /api/v3/agent/analyze_stock`

Python 兼容性：

- 当前后端环境可能是 Python 3.9。
- Pydantic 模型字段请优先使用 `Optional[str]`，不要使用 `str | None`。

## Vue 前端

目录：`quant_dashboard/frontend`。

启动：

```bash
cd /Users/eudis/ths/quant_dashboard/frontend
npm run dev -- --host 127.0.0.1 --port 5173
```

构建：

```bash
cd /Users/eudis/ths/quant_dashboard/frontend
npm run build
```

核心文件：

- `src/App.vue`：V4.4 Theme Alpha 主工作站入口，Dashboard 统一承载四大军团。
- `src/components/Sidebar.vue`：左侧导航。
- `src/components/StatsHeader.vue`：顶部状态条。
- `src/components/SelectionTable.vue`：影子账本/策略表格。
- `src/components/MinKlineViewer.vue`：5m 分时观测。
- `src/router/index.js`：轻量路由解析，不是完整 vue-router。
- `src/style.css`：暗夜金融终端样式。

数据状态卡口径：

- Validation 页的 JQDATA COLD 5M 卡片读取 `GET /api/data/minute-fetch/status`。
- 聚宽卡片的“本次新增”是最近一次 `jq_summary_*.json` 中的 `success / universe`，不包含本次因断点已完成而跳过的股票。
- 聚宽卡片的“断点进度”来自 `jq_cold_5m_progress.json`，格式为 `progress_codes 股 / progress_segments 段`；一只股票完整冷数据大约对应 13 个自然月切片。
- Ashare/Tencent 热数据卡片的“今日覆盖”仍表示当天热数据归档成功数。

设计约束：

- 暗夜模式，专业金融交易终端风格。
- A 股颜色：红涨绿跌。
- 波段策略和 T+1 策略不要混用字段：波段显示 T+3 最大涨幅，突破显示 T+1 开盘溢价。
- 历史账本使用月份 Tabs，避免一次渲染几百行。

## 训练与评估命令

日线全局模型：

```bash
cd /Users/eudis/ths
python3 -m quant_core.engine.daily_model_trainer --limit 100
python3 -m quant_core.engine.daily_model_trainer --limit 0
python3 -m quant_core.engine.model_evaluator --limit 100
```

分钟线因子与模型：

```bash
cd /Users/eudis/ths
python3 -m quant_core.engine.factor_factory
python3 -m quant_core.engine.model_trainer
```

策略训练：

```bash
cd /Users/eudis/ths
python3 scripts/dataset/build_smart_overnight_dataset.py
python3 scripts/training/quant_train_premium_models.py
python3 scripts/dataset/build_reversal_dataset.py
python3 scripts/training/quant_train_reversal_models.py
python3 scripts/dataset/build_main_wave_dataset.py
python3 scripts/training/quant_train_main_wave_models.py
```

历史账本重建：

```bash
cd /Users/eudis/ths
python3 rebuild_historical_picks.py
```

事件驱动回测：

```bash
cd /Users/eudis/ths
python3 run_backtest.py
```

## 常用验证命令

后端健康：

```bash
curl -s http://127.0.0.1:8000/health
curl -s http://127.0.0.1:8000/api/v3/system/status
```

V4 实时拼接推理：

```bash
curl -s 'http://127.0.0.1:8000/api/v4/sniper/scan_today?limit=3&threshold=0&cache_seconds=0'
```

早盘哨兵干跑：

```bash
cd /Users/eudis/ths
python3 -m quant_core.execution.exit_sentinel --dry-run
```

波段巡逻干跑：

```bash
cd /Users/eudis/ths
python3 -m quant_core.execution.swing_patrol --dry-run
```

实时巡逻兵干跑：

```bash
cd /Users/eudis/ths
python3 live_sentinel.py --once --dry-run --no-push
```

PushPlus 今日补发：

```bash
cd /Users/eudis/ths
python3 -m quant_core.execution.pushplus_tasks resend-today
```

腾讯实时测试：

```bash
cd /Users/eudis/ths
python3 - <<'PY'
from quant_core.data_pipeline.tencent_engine import get_tencent_realtime, get_tencent_m5
print(get_tencent_realtime("600000"))
print(get_tencent_m5("002709", count=10).tail())
PY
```

前端构建：

```bash
cd /Users/eudis/ths/quant_dashboard/frontend
npm run build
```

LaunchAgent 重载后端：

```bash
launchctl kickstart -k gui/$(id -u)/com.eudis.quant.backend-api
```

更新全部 LaunchAgent：

```bash
cd /Users/eudis/ths
bash scripts/shell/update_launch_agents.sh
```

## 开发注意事项

- 优先使用 `rg`、`rg --files` 查找代码。
- 手动修改文件使用 `apply_patch`。
- 不要用 `git reset --hard`、`git checkout --` 等破坏用户改动。
- 新增模型路径要写入 `quant_core/config.py`，不要散落硬编码。
- 新增生产字段必须在 `quant_core/storage.py` 做自动迁移。
- 写入 `daily_picks` 时必须支持多标的并行，唯一性应考虑 `selection_date + strategy_type + code`。
- 旧全局狙击页的历史锁定记录保存在 `v3_sniper_locks`，不会自动出现在影子账本；如需补录到 `daily_picks`，必须保留原始快照价/时间并写入历史导入风险提示。
- 推送逻辑必须能处理空策略、列表结果、网络失败和重试。
- V4 sniper 接口如果报 502，先看模型文件、meta 特征列、Theme Pipeline 缓存、腾讯实时行情、Python 3.9 类型注解。
- 前端如果打不开，先检查 Vite 端口 5173、后端 8000、`npm run build`。
- 后端如果拒绝连接，检查 LaunchAgent、`scripts/shell/run_backend_api.sh`、`quant_dashboard/backend/backend_server.log`。
- `quant_core/predictor.py`、`quant_core/market.py` 等根文件多为兼容入口，新代码优先放在 DDD 后的新目录。
- 实验脚本可放 `scripts/utils` 或 `tests`，不要污染根目录。
- 访问外部实时数据前确认数据源：不得新增 AkShare/东财链路；实时快照优先腾讯，行业/板块日线只允许 mpquant/Ashare + 本地缓存。

## 最近验证过的状态

- `quant_dashboard/frontend` 的 `npm run build` 最近已通过。
- `bash scripts/shell/run_jq_cold_5m.sh` 最近可正常登录聚宽并按断点继续，`2026-05-01` 实测结果为 `success=177`、`failed=0`、`stopped_by_quota=true`，额度自然耗尽后退出码为 0。
- `GET /api/data/minute-fetch/status` 最近返回 JQ 状态 `quota_exhausted`，并能读取最新全量摘要 `jq_summary_20260501_185555.json`。
- `GET /api/v3/system/status` 最近返回模型 ready、Ollama ready。
- `GET /api/v4/sniper/scan_today?limit=3&threshold=0&cache_seconds=0` 最近返回 `prediction_date=2026-05-02`，每行包含 `theme_name` 与 `theme_pct_chg_3`。
- 后端 LaunchAgent `com.eudis.quant.backend-api` 可通过 `launchctl kickstart` 重启。
- `live_sentinel.py --once --dry-run --no-push` 最近通过；腾讯五档盘口字段可解析，合成委比 `-98%` 样本可触发盘口抢跑预警。

## 后续 Agent 接手建议

1. 先确认工作目录：`cd /Users/eudis/ths`。
2. 读 `AGENTS.md`、`TECHNICAL_DOC.md`、相关源码。
3. 如果任务涉及实盘影子测试，先检查 `daily_picks` 快照字段，不要覆盖真实记录。
4. 如果任务涉及前端，修改后必须跑 `npm run build`。
5. 如果任务涉及后端 API，至少跑 `python3 -m compileall` 对相关文件做语法检查，并用 `curl` 做烟测。
6. 如果任务涉及模型特征，必须检查 `.meta.json` 特征顺序和训练/推理一致性。
7. 如果任务涉及推送，先调用 `check_push_config()`，再用 dry-run 或补发命令验证。
