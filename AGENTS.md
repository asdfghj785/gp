# AGENTS.md - A 股量化工作站开发上下文

更新时间：2026-05-08

本文件给后续接手本项目的 Codex/Agent 使用。进入项目后先读本文件，再读 `TECHNICAL_DOC.md`、`PROJECT_STRUCTURE_GUIDE.md` 和相关源码。项目根目录固定为 `/Users/eudis/ths`。

## 项目定位

这是运行在本地 macOS/M4 上的 A 股量化工作站。核心目标是采集本地日线/分钟线/实时快照数据，训练 XGBoost 结构化模型，执行 14:50 多策略影子出票，使用 PushPlus 推送交易指令，并通过 FastAPI + Vue 深色金融终端展示结果。

当前系统版本：V5.6 "5m Sentinel / Theme Alpha / Mac Sniper"。

当前系统同时包含五条主线：

1. 生产影子测试主线：`quant_core/engine/predictor.py` 驱动多策略独立分档出票。当前生产启用 `全局动量狙击`、`尾盘突破` 与 `尾盘突破-ST特情`，每个启用策略只取 Top1，总输出上限 3；`右侧主升浪` 与 `中线超跌反转` 保留代码和模型但生产暂停。
2. V5.6 5m Sentinel 离线回放主线：`scripts/backtest/simulate_sentinel_5m.py` 以 `daily_picks` 出票事实为底座，优先读取统一 SQLite 5m 表 `stock_minute_5m`，验证非对称日内风控、追踪止盈、尾盘结构止损与 T+3 兜底。
3. Theme Alpha 全局日线模型主线：`quant_dashboard/backend/routers/v3_sniper.py` 同时挂载 `/api/v4/sniper/*`，使用带 `theme_*` 因子的全市场 XGBoost 分类模型，实时拼接当日腾讯行情后输出高置信候选。
4. 09:15 实时巡逻兵主线：`live_sentinel.py` 从上一交易日 14:50 标的重建 `shadow_ledger.json`，盘中执行止损、追踪止盈、分钟级爆量滞涨和五档盘口委比反转预警。
5. V5.0 资金池与 Mac Sniper 主线：`data/shadow_account.json` 记录影子资金池、锁定持仓和券商确认流水；`data/sniper_status.json` 是前端保险匣、14:50 总线和巡逻兵共享的物理外挂开关。

重要原则：

- LLM/Ollama 只做舆情、公告、新闻风控，不直接预测价格，也不覆盖 XGBoost 排序。
- 14:50 主推送不得等待 Ollama；AI 舆情与风险排查通过 `quant_core.execution.pushplus_tasks ai-supplement` 异步补发，成功推送后再写回 `daily_picks.raw_json`。
- 实盘影子测试买入锚点是 14:50 的 `snapshot_price` 与 `snapshot_time`，这些字段必须不可篡改。
- 生产出票必须保留 V5.6 风控契约：`selection_tier`、`risk_warning`、`dynamic_floor`/`score_floor`、`suggested_position`、`core_theme`、`theme_momentum_3d` 必须随 `raw_json.winner` 落库，并在 PushPlus、复盘 API 与前端展示。
- 14:50 真实出票只允许 `com.eudis.quant.push-top-pick` 在 14:50 到 15:05 写入；当天已有真实锁定票后禁止重新扫描，同策略不得用盘后更高分候选替换。
- 真实账本数据默认使用 `/api/daily-picks?view=strategy_top1&coverage=complete_5m&exclude_st_limit_up=true`：先过滤 `is_shadow_test=1`，再按 `selection_date + strategy_type` 折叠，同一天允许 `全局动量狙击`、`尾盘突破` 与 `尾盘突破-ST特情` 各一只；`view=all` 只用于审计原始候选。
- 5m回测数据单独使用 `/api/backtest/sentinel-5m-ledger?start_date=2025-01-01`：只展示已结算且完整 5m 覆盖的回测样本；买入策略、生产阈值或 5m 卖出策略实装变更后必须重算回测缓存再展示。
- 历史复盘和历史账本重建使用 `stock_daily` 的 15:00 完整日线底座；5m Sentinel 回放只用于卖出引擎评估，不恢复历史 14:50 截面截断代理。
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
- jqdatasdk 仅保留旧兼容；默认禁用聚宽主动获取
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
│   ├── intraday/price_1430.json   # 14:30 快照
│   ├── strategy_cache/            # 复盘/策略分析缓存
│   ├── shadow_account.json        # V5.0 影子资金池
│   └── sniper_status.json         # Mac Sniper 保险匣共享状态
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

- `QUANT_BREAKOUT_MIN_SCORE=62.00`
- `QUANT_ST_BREAKOUT_MIN_SCORE=62.00`
- `QUANT_DIPBUY_MIN_SCORE=99.00`，首阴低吸保留但生产冻结
- `QUANT_REVERSAL_MIN_SCORE=6.00`
- `QUANT_MAIN_WAVE_MIN_SCORE=6.60`
- `QUANT_GLOBAL_MIN_SCORE=0.90`

推送配置：

- PushPlus 主配置改为 SQLite 表 `pushplus_tokens`，前端 `PushPlus` 页面支持多 token 增删改查、启停和测试推送。
- `.env` 的 `PUSHPLUS_TOKEN` 作为兼容 token 与数据库启用 token 一起参与循环推送；若与数据库 token 重复则自动去重。
- 使用 `quant_core.config.check_push_config()` 检查配置；`send_pushplus()` 会按启用 token 循环推送。
- API、前端、日志和文档不得展示 token 明文。

Ollama：

- 默认接口：`OLLAMA_API=http://127.0.0.1:11434/api/generate`
- 当前常用模型：`qwen2.5:14b`
- `quant_core/ai_agent/llm_engine.py` 支持 OpenAI compatible `/v1/chat/completions` 和 `/api/generate` 兜底。
- 14:50 主报告不等待 LLM；`ai-supplement` 负责异步补发 AI 风险报告，`run_ollama_ensure.sh` 负责登录和盘前预热检查。

## 数据源与数据口径

日线：

- 主库是 `data/all_kline/*_daily.parquet`。
- 常见字段：`symbol,date,open,high,low,close,volume,amount,turn,pctChg,MA5,MA10,MA20,量比,MACD_DIF` 等。
- Theme Alpha 全局模型会读取本地历史日线尾部约 80 天，再拼接今天实时行情行；历史复盘批量推演使用 `stock_daily_1500` 完整日线行。

分钟线：

- 生产读取表：`stock_minute_5m`，位于 `/Users/eudis/ths/data/core_db/quant_workstation.sqlite3`。
- 路径 `data/min_kline/5m/{code}.parquet` 保留为腾讯/Ashare 兼容热数据归档和审计来源，不再作为前端和 Sentinel 的优先读取口径。
- V5.6 5m Sentinel 回放优先读取统一 SQLite 表 `stock_minute_5m`；`/Users/eudis/5min/organized_5min_pre_adj` 与项目本地 Parquet 仅作为导入、审计或模拟补充来源。
- 聚宽冷数据主动获取已停用：`QUANT_ENABLE_JQ_FETCH` 默认关闭，`scripts/shell/run_jq_cold_5m.sh` 仅记录停用日志并退出 0，`com.eudis.quant.jq-cold-5m` 不再安装启用。
- 历史冷数据缓存仍可读取：`/Users/eudis/5min/organized_5min_pre_adj` 与项目本地 `data/min_kline/5m` 可作为导入、审计或模拟补充来源，不要删除已有 Parquet、断点或摘要文件。
- 聚宽旧脚本 `scripts/data_pipeline/batch_fetch_historical_min.py`、`scripts/data_pipeline/batch_fetch_jq_history.py` 保留为兼容入口，但默认返回 `disabled`，不得在生产定时任务中重新启用。
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
- 盘后误扫写入的候选不是证据，确认误入库后必须物理删除，不允许以隔离行形式继续保留在生产账本或参与 Top1 折叠。
- 若需要清空模拟账本，先确认用户明确要求，并区分 `is_shadow_test` 真实数据。
- `raw_json.source='historical_production_replay'` 的已结算行保持 `is_shadow_test=0`；未结算行必须提升为 `is_shadow_test=1`，作为真实影子持仓继续观察，并与前端、早盘哨兵、实时巡逻兵、15:35 卖出闭环保持同一集合。

## 生产策略

### 尾盘突破

- 策略名：`尾盘突破`
- 模型：`models/overnight_premium_xgboost.json`
- 生命周期：T 日 14:50 锁定，T+1 开盘验证。
- 结果字段：`open_premium`
- 风控：非主板/ST过滤、尾盘诱多、断头铡刀、上影线、高位爆量、准涨停未封。

### 尾盘突破-ST特情

- 策略名：`尾盘突破-ST特情`
- 生命周期：T 日 14:50 锁定，T+1 开盘验证。
- 结果字段：`open_premium`
- 风控：专门承接 ST/*ST 尾盘突破样本；必须过滤封死涨停、尾盘成交机会不足、成交量过小导致实盘无法买入的样本；Top1 不可买时允许在同策略候选池中按分数退到仍满足阈值的 Top2/Top3 并标记替补原因。

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

### V5.6 分档与仓位

- `ABSOLUTE_BOTTOM_PROBA=0.55` 是全策略通用绝对安全底线，禁止恢复人工 `_FLOOR_SCORE` 静态配置。
- 当前生产启用策略来自 `PRODUCTION_STRATEGY_TYPES - PAUSED_STRATEGY_TYPES`，默认输出 `全局动量狙击`、`尾盘突破` 与 `尾盘突破-ST特情`。
- `select_strategy_top_picks()` 对每个启用策略先找 `score >= MIN_SCORE` 的 `base` 档，每个策略最多取 Top1；总输出上限由 `PRODUCTION_TOTAL_PICK_LIMIT=3` 控制。
- 若 `base` 档为空，计算 `dynamic_floor=max(ABSOLUTE_BOTTOM_PROBA, legal_pool['score'].quantile(0.99))`，只有 Top 1 分数达到动态底线才以 `selection_tier=dynamic_floor` 出票。
- `base` 档仓位使用 Half-Kelly，限制在 10% 到 30%；`dynamic_floor` 档固定 5% 试错轻仓。
- 终端日志必须打印 `[AdaptiveFloor] ... dynamic_floor=...`，便于复盘当天动态及格线。

### V5.6 5m Sentinel 卖出引擎

- 脚本：`scripts/backtest/simulate_sentinel_5m.py`。
- 数据：优先读取统一 SQLite 5m 表 `stock_minute_5m`；前复权冷数据 `/Users/eudis/5min/organized_5min_pre_adj` 与本地热数据 `data/min_kline/5m` 只作为导入、审计或模拟补充来源。无完整 5m 覆盖时，全局狙击按 T+3 15:00 收盘兜底，尾盘突破/ST特情按 T+1 09:30 开盘兜底。
- 口径：买入样本按 `selection_date + strategy_type` 折叠为每策略 Top1，并过滤 `PAUSED_STRATEGY_TYPES`。
- 日内防爆止损：正规军 `-6%`，敢死队/狙击/突破 `-4%`。
- 动态追踪止盈：最高浮盈达到 `+4%` 激活，随后从最高价回撤 `-2%` 触发。
- 尾盘结构止损：14:50/14:55，正规军收盘跌破 `-3%`，敢死队收盘跌破 `-1.5%`。
- 强制离场：T+3 最后一根 5m bar 平仓；输出缓存写入 `data/strategy_cache/sentinel_5m_backtest_latest.json`。

### 首阴低吸

- 策略名：`首阴低吸`
- 模型：`models/dipbuy_premium_xgboost.json`
- 当前默认 `QUANT_DIPBUY_MIN_SCORE=99.00`，等价生产冻结。
- 保留代码、模型和可视化兼容，不要随意删除。

## Theme Alpha 全局日线 XGBoost 雷达

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

- 后端 API 与前端 Vite 由 `com.eudis.quant.backend-api`、`com.eudis.quant.frontend-dev` 以 `RunAtLoad + KeepAlive` 常驻。
- `08:55` `com.eudis.quant.ollama-ensure` 检查 Ollama，必要时拉起 Ollama.app；该任务也会在登录时运行一次。
- `09:00` PushPlus 心跳。
- `09:15` 实时巡逻兵启动，监控上一交易日 14:50 推送标的。
- `09:16` 早盘预观察。
- `09:21` 撤单关闭后竞价审计。
- `09:25` 终极开盘哨兵，只此阶段允许回填 `open_price`。
- `14:30` 保存全市场快照。
- `14:45` `com.eudis.quant.push-top-pick-prewarm` 预热扫描链路与 Ollama。
- `14:50` 启用策略分策略 Top1 出票与 PushPlus 推送。
- `14:50` V4 全局动量狙击 Top5 雷达锁定/推送已停用；只保留接口和脚本用于手动审计，PushPlus 默认需显式开启 `QUANT_ENABLE_V3_SNIPER_PUSHPLUS=1`。
- `15:05` 盘后日线同步。
- `15:15` Ashare/腾讯 5m 热数据归档。
- `15:35` 5m 卖出闭环与 T+3 收盘结算器，只处理真实未关闭影子持仓。

重要脚本：

- `scripts/shell/run_backend_api.sh`
- `scripts/shell/run_frontend_dev.sh`
- `scripts/shell/run_exit_sentinel.sh`
- `scripts/shell/run_swing_patrol.sh`
- `scripts/shell/run_live_sentinel.sh`
- `scripts/shell/run_v3_sniper_lock.sh`：V4 Top5 锁榜兼容入口，生产定时任务已停用
- `scripts/shell/run_push_top_pick.sh`
- `scripts/shell/run_ollama_ensure.sh`
- `scripts/shell/run_market_close_sync.sh`
- `scripts/shell/run_daily_ashare_archiver.sh`
- `scripts/shell/run_jq_cold_5m.sh`：聚宽停用哨兵脚本，只写停用日志并退出 0；LaunchAgent 默认禁用
- `scripts/shell/update_launch_agents.sh`

默认禁用或退役的旧 Label：`com.eudis.quant.daily-pick-save`、`com.eudis.quant.daily-pick-open`、`com.eudis.quant.exit-sentinel`、`com.eudis.quant.jq-cold-5m`、`com.eudis.quant.v3-sniper-lock`、`com.quant.heartbeat`、`com.quant.sniper`。

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
- 日内防爆止损：正规军 `-6%`，敢死队/狙击/突破 `-4%`，只防暴雷，不处理普通分时洗盘。
- 动态追踪止盈：最高浮盈达到 `+4%` 后激活，若当前价从最高点回撤 `2%`，推送【追踪止盈触发】并移除持仓。
- 尾盘结构止损：14:50 到 15:00，正规军跌破 `-3%`、敢死队跌破 `-1.5%`，推送【尾盘破位卖出】并移除持仓。
- T+3 超时：保持到期清仓逻辑。
- 分钟级爆量滞涨和五档盘口委比 `<= -80` 只触发 PushPlus 风险预警并写入账本，不直接移除持仓。
- 止损、追踪止盈、尾盘破位、T+3 平仓后必须调用 `mark_daily_pick_closed()`，把 `close_price`、`close_return_pct` 和 `close_signal` 同步回 `daily_picks`。

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
- `GET /api/daily-picks?view=strategy_top1`：默认真实账本数据，先过滤 `is_shadow_test=1` 再按每个启用策略 Top1 折叠；`view=all` 仅审计原始记录。
- `GET /api/backtest/sentinel-5m-ledger?start_date=2025-01-01`：5m回测数据，只返回 `is_closed=true` 且 `coverage_status=covered` 的最新买卖策略样本。
- `GET /api/data/market-sync/latest`
- `GET /api/data/history/{code}`
- `GET /api/data/history_min/{code}?period=5`
- `GET /api/data/minute-fetch/status`
- `GET /api/pushplus/tokens`
- `POST /api/pushplus/tokens`
- `PUT /api/pushplus/tokens/{token_id}`
- `DELETE /api/pushplus/tokens/{token_id}`
- `POST /api/pushplus/test`
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

- `src/App.vue`：V5.6 5m Sentinel / Theme Alpha / Mac Sniper 主工作站入口，Dashboard 统一承载策略卡片、真实账本数据、5m回测数据、资金池和物理外挂保险匣；暂停策略卡片灰显但不删除。
- `src/components/Sidebar.vue`：左侧导航。
- `src/components/StatsHeader.vue`：顶部状态条。
- `src/components/SelectionTable.vue`：真实账本/回测数据/策略表格。
- `src/components/MinKlineViewer.vue`：5m 分时观测。
- `src/router/index.js`：轻量路由解析，不是完整 vue-router。
- `src/style.css`：暗夜金融终端样式。

PushPlus 页面口径：

- 侧边栏 `PushPlus` 取代旧 `Validation` 同步/校验页；旧数据同步、校验报告、聚宽冷数据和数据资产卡片不再作为前端页面入口展示。
- `GET /api/pushplus/tokens` 只返回 `token_mask`、启用状态、最近发送状态和统计计数，不返回 token 明文。
- `POST /api/pushplus/test` 通过 `send_pushplus()` 向所有启用 token 循环发送测试消息。
- Dashboard 保留 `Ashare 每日获取情况` 卡片，读取 `GET /api/data/minute-fetch/status` 的 `ashare` 段；不要恢复旧 JQ 冷数据卡。

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

T+3 收盘结算干跑：

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
- 旧全局狙击页的历史锁定记录保存在 `v3_sniper_locks`，不会自动出现在真实账本数据；如需补录到 `daily_picks`，必须保留原始快照价/时间并写入历史导入风险提示。
- 推送逻辑必须能处理空策略、列表结果、网络失败和重试。
- V4 sniper 接口如果报 502，先看模型文件、meta 特征列、Theme Pipeline 缓存、腾讯实时行情、Python 3.9 类型注解。
- 前端如果打不开，先检查 Vite 端口 5173、后端 8000、`npm run build`。
- 后端如果拒绝连接，检查 LaunchAgent、`scripts/shell/run_backend_api.sh`、`quant_dashboard/backend/backend_server.log`。
- `quant_core/predictor.py`、`quant_core/market.py` 等根文件多为兼容入口，新代码优先放在 DDD 后的新目录。
- 实验脚本可放 `scripts/utils` 或 `tests`，不要污染根目录。
- 访问外部实时数据前确认数据源：不得新增 AkShare/东财链路；实时快照优先腾讯，行业/板块日线只允许 mpquant/Ashare + 本地缓存。

## 最近验证过的状态

- `quant_dashboard/frontend` 的 `npm run build` 最近已通过。
- 聚宽主动获取已停用；`bash scripts/shell/run_jq_cold_5m.sh` 应只写停用日志并退出 0。
- PushPlus 页面应能列出 SQLite token 与 `.env` 兼容 token；`.env` token 不可编辑，只展示掩码。
- `GET /api/v3/system/status` 最近返回模型 ready、Ollama ready。
- `GET /api/v4/sniper/scan_today?limit=3&threshold=0&cache_seconds=0` 最近返回 `prediction_date=2026-05-02`，每行包含 `theme_name` 与 `theme_pct_chg_3`。
- 后端 LaunchAgent `com.eudis.quant.backend-api` 与前端 `com.eudis.quant.frontend-dev` 已按 `RunAtLoad + KeepAlive` 常驻；`com.eudis.quant.ollama-ensure` 和 `com.eudis.quant.push-top-pick-prewarm` 已安装用于盘前/尾盘预热。
- `live_sentinel.py --once --dry-run --no-push` 最近通过；腾讯五档盘口字段可解析，合成委比 `-98%` 样本可触发盘口抢跑预警。

## 后续 Agent 接手建议

1. 先确认工作目录：`cd /Users/eudis/ths`。
2. 读 `AGENTS.md`、`TECHNICAL_DOC.md`、相关源码。
3. 如果任务涉及实盘影子测试，先检查 `daily_picks` 快照字段，不要覆盖真实记录。
4. 如果任务涉及前端，修改后必须跑 `npm run build`。
5. 如果任务涉及后端 API，至少跑 `python3 -m compileall` 对相关文件做语法检查，并用 `curl` 做烟测。
6. 如果任务涉及模型特征，必须检查 `.meta.json` 特征顺序和训练/推理一致性。
7. 如果任务涉及推送，先调用 `check_push_config()`，再用 dry-run 或补发命令验证。
