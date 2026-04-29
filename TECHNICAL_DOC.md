# A 股量化工作站 V3.2 技术文档

更新时间：2026-04-29

## 1. 项目定位

本项目是运行在本地 macOS/M4 环境的 A 股量化工作站，用于采集全市场日线、分钟线、实时快照数据，训练结构化机器学习模型，执行 14:50 多轨雷达扫描、14:46 本地 AI 舆情访谈、PushPlus 微信推送、前向影子测试、T+1/T+3 闭环验证、09:15 实时巡逻兵风控和前端可视化复盘。

当前代码已经完成领域驱动式目录重构。生产主干采用 `quant_core/engine` 调度策略工厂，策略实现集中在 `quant_core/strategies`，数据管道集中在 `quant_core/data_pipeline`，盘中执行与推送集中在 `quant_core/execution`。

核心原则：

- 结构化行情模型使用 `XGBRegressor`，不使用 LLM 直接预测价格。
- Ollama 只用于公告、股吧和舆情风控，不参与模型分数计算。
- 生产影子测试以 14:50 实时快照价 `snapshot_price` 为唯一买入锚点，禁止用盘后收盘价回填覆盖。
- 当前为 V3.2 前向影子测试模式：三大生产军团各自独立出票，每天最多锁定 3 只股票，并额外提供全市场 XGBoost 雷达与实时巡逻兵风控。
- `首阴低吸` 保留代码和模型，但默认门槛 `99.00`，不进入生产出票。
- `quant_core/ai_agent` 本地 AI 右脑只做新闻/舆情/公告风险定性，不覆盖结构化模型排序。

## 2. 技术栈

后端：

- Python 3
- FastAPI / Uvicorn
- SQLite
- Pandas / NumPy
- XGBoost
- Requests
- jqdatasdk
- pyarrow / parquet

前端：

- Vue 3
- Vite
- Element Plus
- Tailwind CSS 配置保留
- 深色交易终端主题，入口为 `quant_dashboard/frontend/src/main.js` 和 `style.css`

数据与自动化：

- 新浪行情接口：全市场实时快照、指数行情、兼容旧同步链路。
- 腾讯行情接口：竞价/盘中实时快照、Ashare 风格 5 分钟热数据。
- 聚宽 SDK：授权区间内历史 5 分钟冷数据。
- 本地 Parquet 日线库：`/Users/eudis/ths/data/all_kline`
- 本地 Parquet 分钟线库：`/Users/eudis/ths/data/min_kline`
- SQLite 核心库：`/Users/eudis/ths/data/core_db/quant_workstation.sqlite3`
- XGBoost 模型目录：`/Users/eudis/ths/models`
- Ollama 本地大模型：14:46 AI 舆情与风险排查。
- PushPlus 微信推送。
- macOS LaunchAgent 定时任务。

## 3. 当前目录结构

```text
/Users/eudis/ths
├── AGENTS.md
├── README.md
├── PROJECT_1.0_DOC.md
├── PROJECT_STRUCTURE_GUIDE.md
├── TECHNICAL_DOC.md
├── .env.example                         # 环境变量样例；真实 .env 禁止提交
├── live_sentinel.py                      # 09:15 实时巡逻兵：止损、追踪止盈、盘口/分时异动
├── shadow_ledger.json                    # 实时巡逻兵持仓账本
├── data/
│   ├── all_kline/                         # 全市场日线 Parquet 数据
│   ├── core_db/                           # SQLite 数据库目录
│   ├── min_kline/                         # 聚宽冷数据 + 腾讯/Ashare热数据分钟线 Parquet
│   └── intraday/                          # 14:30 盘中快照
├── models/                                # XGBoost 模型与备份
├── launch_agents/                         # macOS LaunchAgent plist 源文件
├── news_radar/                            # 新闻/舆情相关辅助模块
├── quant_core/                            # 后端核心领域代码
├── quant_dashboard/                       # FastAPI + Vue 看板
├── scripts/                               # 重构后脚本集中目录
├── tests/                                 # 临时连通性/干跑测试脚本
├── rebuild_historical_picks.py            # 历史生产账本重建入口
└── run_backtest.py                        # 事件驱动回测入口
```

## 4. 核心代码结构与职责

### 4.1 `quant_core`

```text
quant_core/
├── config.py                              # 全局路径、模型路径、策略门槛、PushPlus 配置校验
├── storage.py                             # SQLite 表结构、自动迁移、读写、快照字段不可变触发器
├── daily_pick.py                          # 14:50 多轨出票落库、T+1 开盘回填
├── validation.py                          # 数据完整性校验
├── failure_analysis.py                    # 失败归因分析
├── threshold_sweep.py                     # 尾盘突破阈值扫频
├── reversal_threshold_sweep.py            # 波段策略阈值扫频
├── up_reason_analysis.py                  # 上涨原因分析
├── predictor.py                           # 兼容入口，转发到 quant_core.engine.predictor
├── market.py                              # 兼容入口，转发到 data_pipeline.market
├── market_sync.py                         # 兼容入口，转发到 data_pipeline.market_sync
├── intraday_snapshot.py                   # 兼容入口，转发到 data_pipeline.intraday_snapshot
├── exit_sentinel.py                       # 兼容入口，转发到 execution.exit_sentinel
├── swing_patrol.py                        # 兼容入口，转发到 execution.swing_patrol
├── data_pipeline/
│   ├── market.py                          # 实时行情统一门面，竞价/个股快照走腾讯，指数/全市场快照保留新浪
│   ├── market_sync.py                     # 15:05 盘后同步入库
│   ├── fetch_minute_data.py               # 聚宽历史 + 腾讯热数据分钟线抓取与 Parquet 落库
│   ├── tencent_engine.py                  # 腾讯实时快照与 5m K 线底层引擎
│   └── intraday_snapshot.py               # 14:30 快照读取、尾盘诱多拦截
├── ai_agent/
│   ├── llm_engine.py                      # 本地 Ollama 客户端，支持 /v1/chat/completions 与 /api/generate 兜底
│   ├── news_fetcher.py                    # 轻量新闻/搜索线索抓取，失败时返回安全兜底文本
│   ├── prompts_repo.py                    # 14:46 舆情风控 Prompt 模板
│   └── agent_gateway.py                   # run_1446_ai_interview 融合入口
├── engine/
│   ├── predictor.py                       # 四轨扫描、特征拼接、模型推理、生产过滤、三军团独立 Top1
│   ├── daily_factor_factory.py            # V3.2 全局日线模型因子工厂
│   ├── daily_model_trainer.py             # V3.2 全局日线 XGBoost 训练
│   ├── model_evaluator.py                 # V3.2 样本外评估
│   └── backtest.py                        # 历史回测与策略复盘
├── backtest/
│   ├── backtest_engine.py                 # 事件驱动回测引擎
│   ├── execution.py                       # 回测成交模拟
│   ├── portfolio.py                       # 组合状态
│   └── metrics.py                         # 回测指标
├── strategies/
│   ├── base_strategy.py                   # 策略基类：filter / score / get_threshold 接口
│   ├── factory.py                         # 策略注册表
│   ├── breakout.py                        # 尾盘突破策略对象
│   ├── reversal.py                        # 中线超跌反转策略对象
│   ├── main_wave.py                       # 右侧主升浪策略对象
│   └── labs/
│       ├── strategy_lab.py
│       ├── strategy_lab_n_shape.py
│       ├── strategy_lab_bottom_reversal.py
│       └── strategy_lab_main_wave.py      # 临时/实验策略探伤脚本
└── execution/
    ├── daily_pick_cli.py                  # daily_pick 命令行入口
    ├── pushplus_tasks.py                  # 09:00 心跳、14:50 推送、resend-today 补发
    ├── exit_sentinel.py                   # 09:16/09:21/09:25 三阶段竞价哨兵
    └── swing_patrol.py                    # 14:45 T+3 波段巡逻兵
```

### 4.2 `scripts`

```text
scripts/
├── data_pipeline/
│   ├── batch_fetch_historical_min.py      # 历史分时批量下载器，保留旧兼容入口
│   ├── batch_fetch_jq_history.py          # 聚宽 2025-01-18 至 2026-01-23 历史 5m 冷数据下载
│   ├── daily_ashare_archiver.py           # 每日 01:00 腾讯/Ashare 5m 热数据增量归档
│   ├── fast_fetch_today_m5.py             # 指定股票快速抓取今日 5m 数据
│   └── fetch_today_watch.py               # 观察票近期 5m 数据补齐
├── shell/
│   ├── run_backend_api.sh                 # 启动 FastAPI，127.0.0.1:8000
│   ├── run_frontend_dev.sh                # 启动 Vite，127.0.0.1:5173
│   ├── run_exit_sentinel.sh               # 三阶段早盘哨兵
│   ├── run_snapshot_1430.sh               # 14:30 全市场快照
│   ├── run_swing_patrol.sh                # 14:45 波段巡逻
│   ├── run_live_sentinel.sh               # 09:15 实时巡逻兵，监控昨日 14:50 标的
│   ├── run_v3_sniper_lock.sh              # 14:50 V3.2 全局雷达锁定
│   ├── run_push_top_pick.sh               # 14:50 多轨出票 + PushPlus
│   ├── run_push_heartbeat.sh              # 09:00 心跳
│   ├── run_market_close_sync.sh           # 15:05 盘后同步
│   ├── run_daily_ashare_archiver.sh       # 15:15 5m 热数据归档
│   ├── run_jq_cold_5m.sh                  # 01:20 聚宽 5m 冷数据额度任务
│   ├── update_launch_agents.sh            # 重写并重载所有 LaunchAgent
│   └── install_*.sh                       # 分类安装脚本
├── dataset/
│   ├── build_smart_overnight_dataset.py   # 尾盘突破/首阴低吸训练集
│   ├── build_reversal_dataset.py          # 中线超跌反转训练集
│   └── build_main_wave_dataset.py         # 右侧主升浪训练集
├── training/
│   ├── quant_train_premium_models.py      # 尾盘突破 XGBRegressor
│   ├── quant_train_dipbuy_models.py       # 首阴低吸模型
│   ├── quant_train_reversal_models.py     # 中线反转 T+3 模型
│   ├── quant_train_main_wave_models.py    # 主升浪 T+3 模型
│   └── train_xgboost_model.py             # 旧版/通用训练入口
└── utils/
    ├── quant_market_sync.py               # 盘后同步工具
    ├── snapshot_1430.py                   # 14:30 快照工具
    ├── check_data.py / clean_db.py        # 数据检查与清理
    └── realtime_sniper.py 等旧工具        # 历史工具，保留兼容和排查
```

### 4.3 `quant_dashboard`

```text
quant_dashboard/
├── backend/
│   ├── main.py                            # FastAPI API 定义
│   └── requirements.txt
└── frontend/
    ├── package.json                       # Vue / Element Plus / Vite 依赖
    ├── src/main.js                        # Vue 入口，启用 Element Plus 深色模式
    ├── src/App.vue                        # V3 工作站主界面
    ├── src/RadarView.vue                  # 雷达视图相关组件
    ├── src/style.css                      # 深色交易终端主题
    └── dist/                              # npm run build 产物
```

## 5. 模型与策略

模型统一存放在 `/Users/eudis/ths/models`：

| 文件 | 策略 | 目标 |
|---|---|---|
| `overnight_premium_xgboost.json` | 尾盘突破 | 预测 T+1 开盘溢价 |
| `dipbuy_premium_xgboost.json` | 首阴低吸 | 预测 T+1 开盘溢价，当前影子模式 |
| `reversal_t3_xgboost.json` | 中线超跌反转 | 预测 T+1 到 T+3 最大涨幅 |
| `main_wave_t3_xgboost.json` | 右侧主升浪 | 预测 T+1 到 T+3 最大涨幅 |

### 5.1 尾盘突破

生命周期：`T 日 14:50 快照锁定 -> T+1 09:25/09:30 开盘验证`。

生产门槛：

```text
QUANT_BREAKOUT_MIN_SCORE=65.50
```

核心风控：

- 剔除创业板、科创板、北交所、ST/退市。
- 剔除高位爆量、尾盘诱多、准涨停未封板。
- 剔除近 3 日断头铡刀、上影过重等短线杀跌风险。
- 成功口径主要看 `open_premium`。

### 5.2 首阴低吸

当前状态：保留模型与代码，不进入生产出票。

生产门槛：

```text
QUANT_DIPBUY_MIN_SCORE=99.00
```

保留原因：

- 后续可继续做影子观察和失败归因。
- 当前 `PRODUCTION_OUTPUT_STRATEGIES` 只包含 `右侧主升浪`、`中线超跌反转`、`尾盘突破`。

### 5.3 中线超跌反转

生命周期：`T 日 14:50 快照锁定 -> T+1/T+2/T+3 波段观察`。

生产门槛：

```text
QUANT_REVERSAL_MIN_SCORE=3.00
```

目标：

```text
t3_max_gain_pct = T+1 到 T+3 三个交易日最高价相对 T 日快照价的最大涨幅
```

核心物理条件：

- T-1 收盘价低于 60 日均线。
- 过去 60 日最大回撤达到超跌标准。
- T-5 到 T-1 出现地量洗盘。
- T 日实体大阳线，一阳穿 5/10 日均线。
- T 日成交量达到近 10 日均量的放量要求。
- 防伪条件包括 30 日均线斜率和 20/30 日均线乖离限制。

### 5.4 右侧主升浪

生命周期：`T 日 14:50 快照锁定 -> T+1/T+2/T+3 波段观察`。

生产门槛：

```text
QUANT_MAIN_WAVE_MIN_SCORE=3.00
```

目标：

```text
t3_max_gain_pct = T+1 到 T+3 三个交易日最高价相对 T 日快照价的最大涨幅
```

核心物理条件：

- T-1 日 20 日均线 > 60 日均线。
- T-1 收盘价距离 60 日高点回撤不超过强势区间阈值。
- T-5 到 T-1 缩量蓄势，振幅已放宽以适配高频影子测试。
- T 日突破平台最高收盘价。
- T 日实体攻击，成交量温和放大。

## 6. 多轨并行出票机制

当前生产逻辑不是全局唯一 Top1，而是三大策略独立出票：

```text
右侧主升浪 Top1
中线超跌反转 Top1
尾盘突破 Top1
```

实现入口：

```text
quant_core.engine.predictor.scan_market()
quant_core.engine.predictor.select_strategy_top_picks()
```

执行逻辑：

1. 新浪实时快照进入 `_prepare_live_inference_snapshot`。
2. 14:50 实盘用实时最新价平替 `close`。
3. 成交量和成交额按 `LIVE_VOLUME_EXTRAPOLATION_FACTOR=1.05` 做全天外推。
4. 拼接本地日线历史，生成突破、反转、主升浪特征。
5. 各策略分别应用物理过滤和独立门槛。
6. `select_strategy_top_picks(limit_per_strategy=1)` 对每个策略各取 1 只，且避免同一股票重复入选多个策略。
7. `daily_picks` 通过唯一索引 `(selection_date, strategy_type)` 限制同一策略每天只落一条。

14:50 PushPlus 推送链路在 `scan_market` 完成后，会调用 `quant_core.ai_agent.agent_gateway.run_1446_ai_interview()` 对最终 1-3 只标的做 AI 舆情访谈。AI 结论写入每只标的的 `raw_json.winner.ai_interview`，并在微信 Markdown 中以独立区块展示。

## 6.1 AI 右脑：14:46 舆情与风险排查

目录：

```text
/Users/eudis/ths/quant_core/ai_agent
```

核心入口：

```python
from quant_core.ai_agent import run_1446_ai_interview

run_1446_ai_interview(stock_codes, stock_names, candidate_rows)
```

模块职责：

| 文件 | 职责 |
|---|---|
| `llm_engine.py` | 调用本地 Ollama；优先使用 `http://localhost:11434/v1/chat/completions`，404 时自动退回 `/api/generate` |
| `news_fetcher.py` | 轻量新闻/搜索抓取；不请求东方财富；失败时返回正常兜底文本 |
| `prompts_repo.py` | 14:46 舆情风控 Prompt；要求模型输出严格 JSON |
| `agent_gateway.py` | 聚合候选股、新闻线索、Prompt、LLM 输出，并生成 Markdown |

设计边界：

- AI 只做“舆情与风险排查定论”，不参与 XGBoost 打分和排序。
- 新闻抓取失败时必须返回 `暂无该股票今日最新重大新闻线索。`，禁止把 HTTP/HTML/XML 解析错误传给大模型。
- 本地 Ollama 不需要 API Key，默认读取 `.env` 中的 `OLLAMA_MODEL`；未配置时默认 `deepseek-r1`。
- 当前本机验证模型为 `qwen2.5:14b`，可通过 `AI_AGENT_OLLAMA_MODEL` 单独覆盖。

测试命令：

```bash
cd /Users/eudis/ths
python3 -m quant_core.ai_agent.agent_gateway --demo
python3 -m quant_core.ai_agent.agent_gateway --demo --no-llm
```

## 7. 数据库设计

数据库路径：

```text
/Users/eudis/ths/data/core_db/quant_workstation.sqlite3
```

核心表：

| 表 | 作用 |
|---|---|
| `stock_daily` | K 线日线库，来自 Parquet 和实时/盘后同步 |
| `daily_picks` | 14:50 生产影子测试账本 |
| `prediction_snapshots` | 雷达扫描缓存 |
| `validation_reports` | 数据校验报告 |
| `market_sync_runs` | 15:05 盘后同步记录 |

`daily_picks` 关键字段：

| 字段 | 说明 |
|---|---|
| `selection_date` | 选股日期 |
| `target_date` | 目标验证日期，突破为 T+1，波段为 T+3 |
| `strategy_type` | `尾盘突破` / `中线超跌反转` / `右侧主升浪` |
| `selection_price` | 选股价，兼容旧数据 |
| `snapshot_time` | 14:50 实盘锁定时间 |
| `snapshot_price` | 14:50 实盘锁定价 |
| `snapshot_vol_ratio` | 14:50 量比/外推量比 |
| `is_shadow_test` | 前向影子测试标记 |
| `open_price` / `open_premium` | T+1 开盘价和开盘溢价 |
| `t3_max_gain_pct` | T+3 最大区间涨幅 |
| `is_closed` | 是否完成哨兵闭环 |
| `close_price` / `close_return_pct` | 哨兵最终平仓价格和收益 |
| `push_status` / `push_sent_at` | PushPlus 发送状态 |
| `raw_json` | 原始扫描、市场风控、哨兵信号快照 |

快照防篡改：

- `snapshot_time`
- `snapshot_price`
- `snapshot_vol_ratio`

以上字段一旦写入非空值，SQLite 触发器禁止后续更新。

## 8. 数据管道

### 8.1 日线数据

日线 Parquet 目录：

```text
/Users/eudis/ths/data/all_kline
```

同步入库：

```bash
cd /Users/eudis/ths
/usr/bin/python3 /Users/eudis/ths/scripts/utils/quant_market_sync.py run
```

定时任务：15:05 执行 `scripts/shell/run_market_close_sync.sh`。

### 8.2 分钟线数据

模块：

```text
/Users/eudis/ths/quant_core/data_pipeline/fetch_minute_data.py
```

数据源：

```text
双源智能路由：
- 聚宽授权历史冷数据：jqdatasdk.get_price(code, frequency='5m', fq='pre')
- 聚宽延迟区间之外的近期热数据：腾讯 ifzq.gtimg.cn 5 分钟 K 线
- 盘中/竞价实时快照：腾讯 qt.gtimg.cn 实时行情
```

存储路径：

```text
/Users/eudis/ths/data/min_kline/{period}m/{code}.parquet
```

示例：

```text
/Users/eudis/ths/data/min_kline/5m/600000.parquet
```

标准字段：

| 字段 | 说明 |
|---|---|
| `datetime` | 分钟 K 时间 |
| `open` / `high` / `low` / `close` | 前复权分钟 OHLC |
| `volume` / `money` / `amount` | 成交量、成交额、兼容成交额别名 |
| `code` / `jq_code` / `symbol` | 6 位代码、聚宽代码、带市场前缀代码 |
| `period` | 周期，支持 `1`、`5`、`15`、`30`、`60` |
| `source` / `ingested_at` | 数据来源和抓取时间，`source` 可为 `jqdatasdk.get_price`、`tencent.mkline` 或 `tencent.daily_archiver` |

调用方式：

```bash
cd /Users/eudis/ths
python3 -m quant_core.data_pipeline.fetch_minute_data 600000 000001 \
  --period 5 \
  --start-date "2026-04-27 09:30:00" \
  --end-date "2026-04-27 15:00:00"
```

半年批量下载：

```bash
cd /Users/eudis/ths
python3 scripts/data_pipeline/batch_fetch_historical_min.py --period 5
```

明早观察票近期补齐：

```bash
cd /Users/eudis/ths
python3 scripts/data_pipeline/fetch_today_watch.py
```

V3.1 冷热分离采集工厂：

```bash
cd /Users/eudis/ths
python3 -m quant_core.utils.stock_filter
python3 scripts/data_pipeline/batch_fetch_jq_history.py --one --force
python3 scripts/data_pipeline/daily_ashare_archiver.py --limit 50 --count 100
```

双源与额度管理：

- 使用 `.env` 中的 `JQ_USERNAME`、`JQ_PASSWORD` 登录聚宽 SDK。
- `get_stock_min_data()` 默认调用 `smart_fetch_minute()`：历史授权区间走聚宽，近期延迟区间走腾讯 M5 补齐。
- 腾讯实时入口位于 `quant_core/data_pipeline/tencent_engine.py`，包含 `get_tencent_realtime()` 与 `get_tencent_m5()`。
- 核心票池由 `quant_core/utils/stock_filter.py` 生成，只保留 `00/60` 主板并剔除创业板、科创板、北交所、ST、退市风险名称。
- `batch_fetch_jq_history.py` 固定采集聚宽授权区间内的 5m 前复权冷数据；由于 `2026-01-25` 为非交易日且 SDK 边界校验严格，实际请求截止到最后一个交易日 `2026-01-23 15:00:00`，并在 `spare < 20000` 时安全熔断。
- `daily_ashare_archiver.py` 使用 Ashare 风格腾讯底层 API 归档每日最近 100 根 5m 热数据，按 `datetime` 无损 upsert；单票失败只打印 Warning 并继续。
- 批量脚本调用 `jqdatasdk.get_query_count()` 打印 `spare/total` 剩余额度。
- 半年区间默认按自然月切片，请求后立即追加合并到同一个 Parquet 文件；也支持 `--segment day` 按日切片。
- 聚宽额度不足或 SDK 返回额度错误时，脚本会优雅停止并提示“今日额度已耗尽，请明天继续”。
- 启动时扫描 `data/min_kline/5m/`，已存在且大于 10KB 的 Parquet 文件直接跳过。

每日热数据归档定时任务：

```text
Label: com.quant.daily_ashare_archiver
时间: 15:15
脚本: /Users/eudis/ths/scripts/shell/run_daily_ashare_archiver.sh
日志:
  /Users/eudis/ths/logs/daily_ashare_archiver.log
  /Users/eudis/ths/logs/daily_ashare_archiver.err.log
```

每日聚宽冷数据额度任务：

```text
Label: com.eudis.quant.jq-cold-5m
时间: 01:20
脚本: /Users/eudis/ths/scripts/shell/run_jq_cold_5m.sh
范围: 2025-01-19 09:30:00 到 2026-01-23 15:00:00
口径: 5m 前复权，按月切片，已存在 Parquet 自动跳过，额度耗尽后安全停止。
日志:
  /Users/eudis/ths/logs/jq_cold_5m.log
  /Users/eudis/ths/logs/jq_cold_5m.err.log
```

加载命令：

```bash
mkdir -p "$HOME/Library/LaunchAgents"
cp /Users/eudis/ths/launch_agents/com.quant.daily_ashare_archiver.plist "$HOME/Library/LaunchAgents/"
launchctl bootout "gui/$(id -u)" "$HOME/Library/LaunchAgents/com.quant.daily_ashare_archiver.plist" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/com.quant.daily_ashare_archiver.plist"
launchctl enable "gui/$(id -u)/com.quant.daily_ashare_archiver"
```

设计约束：

- 严禁写入 SQLite，分钟线只落 Parquet。
- 单票文件采用覆盖合并逻辑：读取旧文件、合并新数据、按 `code/period/datetime` 去重后重写。
- 前端 API 读取 `money`，同时保留 `amount` 兼容旧图表和表格。

### 8.3 14:30 快照

脚本：

```bash
/Users/eudis/ths/scripts/shell/run_snapshot_1430.sh
```

快照文件：

```text
/Users/eudis/ths/data/intraday/price_1430.json
```

14:50 会根据 14:30 快照计算尾盘拉升幅度，超过：

```text
QUANT_LATE_PULL_TRAP_THRESHOLD_PCT=4.00
```

则触发尾盘诱多过滤。

### 8.4 腾讯实时引擎

底层模块：

```text
/Users/eudis/ths/quant_core/data_pipeline/tencent_engine.py
```

函数：

| 函数 | 作用 |
|---|---|
| `get_tencent_realtime(code)` | 获取腾讯实时快照；竞价阶段 `price` 可作为虚拟匹配价 |
| `get_tencent_m5(code, count=48)` | 获取腾讯 5 分钟 K 线，Ashare 风格轻量调用 |
| `tencent_symbol(code)` | `600000 -> sh600000`，`000001 -> sz000001` |

当前调用链：

```text
exit_sentinel.py 09:16/09:21/09:25 -> market.fetch_realtime_quote -> tencent_engine.get_tencent_realtime
swing_patrol.py 14:45 -> market.fetch_realtime_quote -> tencent_engine.get_tencent_realtime
daily_ashare_archiver.py -> tencent_engine.get_tencent_m5
fast_fetch_today_m5.py -> tencent_engine.get_tencent_m5
```

验收命令：

```bash
cd /Users/eudis/ths
python3 - <<'PY'
from quant_core.data_pipeline.tencent_engine import get_tencent_realtime, get_tencent_m5
print(get_tencent_realtime("600000"))
print(get_tencent_m5("002709", count=48).tail())
PY
```

### 8.5 数据校验

接口：

```text
POST /api/data/validate
GET /api/data/reports
```

校验结果保存到 `validation_reports`。

## 9. 哨兵与推送系统

### 9.1 PushPlus 配置

配置项：

```text
PUSHPLUS_TOKEN
```

校验函数：

```text
quant_core.config.check_push_config()
```

健康检查接口 `/health` 会返回 PushPlus 状态；若 Token 缺失或格式异常，状态为 `critical`。

统一发送入口：

```text
quant_core.execution.pushplus_tasks.send_pushplus()
```

能力：

- 自动重试 3 次。
- 打印 PushPlus 错误响应文本。
- 内容过长自动分段。
- 支持 `resend-today` 补发今日未发送成功的影子测试标的。

### 9.2 09:16 / 09:21 / 09:25 三阶段竞价哨兵

脚本：

```bash
/Users/eudis/ths/scripts/shell/run_exit_sentinel.sh
```

模块：

```text
quant_core.execution.exit_sentinel
/Users/eudis/ths/quant_core/execution/exit_sentinel.py
```

阶段：

| 时间 | 参数 | 作用 | 是否写库 |
|---|---|---|---|
| 09:16 | `--stage preopen` | 竞价预热观察 | 否 |
| 09:21 | `--stage audit --sleep-seconds 5` | 撤单关闭后风险审计 | 否 |
| 09:25 | `--stage final --sleep-seconds 5` | 终极开盘审判 | 是 |

09:21 规则：

- 虚拟溢价 `< -5%`：推送早盘风控预警。
- 虚拟溢价 `> +5%`：推送早盘超预期提示。
- 非 final 阶段只推送，不回填数据库。

09:25 规则：

- `尾盘突破`：低开核按钮、0%~3% 落袋为安、>3% 超预期锁仓。
- `中线超跌反转` / `右侧主升浪`：只处理 `< -4%` 的极端破位；其他情况静默洗盘，等待 14:45。

### 9.3 14:45 波段巡逻兵

脚本：

```bash
/Users/eudis/ths/scripts/shell/run_swing_patrol.sh
```

模块：

```text
quant_core.execution.swing_patrol
/Users/eudis/ths/quant_core/execution/swing_patrol.py
```

适用策略：

- `中线超跌反转`
- `右侧主升浪`

规则：

- 累计涨幅 `>= 5%`：波段自动止盈。
- 当前价跌破主力成本锚定开盘价：防线击穿止损。
- `右侧主升浪` 当前价跌破 T 日开盘价：A 字杀假突破止损。
- T+3 未触发止盈/止损：期满清退。
- 触发动作后调用 `mark_daily_pick_closed`，避免重复报警。

### 9.4 09:15 实时巡逻兵

脚本：

```bash
/Users/eudis/ths/scripts/shell/run_live_sentinel.sh
```

模块与账本：

```text
/Users/eudis/ths/live_sentinel.py
/Users/eudis/ths/shadow_ledger.json
```

运行口径：

- 每天 09:15 由 LaunchAgent 启动，默认从上一交易日 14:50 的 `daily_picks` 重建监控账本。
- 09:15 到 11:30、13:00 到 15:00 每 30 秒扫描；11:30 到 13:00 午休暂停；15:00 后停止。
- 监控对象是昨日 14:50 PushPlus 推送的标的，若某票已经在当日巡逻中被平仓，会在当天重建时跳过。

`shadow_ledger.json` 的 `positions` 字段：

| 字段 | 说明 |
|---|---|
| `code` / `name` | 6 位股票代码和名称 |
| `buy_price` | 昨日 14:50 快照买入锚点 |
| `highest_price` | 巡逻期间记录的盘中最高价；初始等于 `buy_price` |
| `volume_alert_triggered` | 当日分钟级爆量滞涨报警是否已触发 |
| `order_book_alert_triggered` | 当日五档盘口委比极限报警是否已触发 |
| `target_date` | T+3 到期日 |
| `source_selection_date` / `source_pick_id` | 对应 `daily_picks` 原始记录 |

核心规则：

- 开仓硬止损：`current_price <= buy_price * 0.97`，推送【初始止损报警】并移除持仓。
- 动态追踪止盈：最高价曾达到 `buy_price * 1.05` 后，若当前价从最高点回撤 3%，推送【追踪止盈触发】并移除持仓。
- T+3 超时：保持原 T+3 到期清仓逻辑。
- 分钟级爆量滞涨：腾讯 1m 分时最近 3 分钟均量超过全天均量 3 倍且价格不涨，推送【异动报警】；只报警，不移除持仓。
- 五档盘口委比反转：腾讯实时行情解析买一到买五、卖一到卖五挂单量，若 `weibi <= -80`，推送【盘口抢跑预警】；只报警，不移除持仓。

平仓同步：

- 任何止损、追踪止盈或 T+3 触发后都会调用 `mark_daily_pick_closed()`，把巡逻平仓价同步到 `daily_picks.close_price`、`close_return_pct`、`close_signal` 等结算字段。
- `snapshot_price` 与 `snapshot_time` 仍为不可变证据，不会被巡逻兵覆盖。

常用命令：

```bash
cd /Users/eudis/ths
python3 live_sentinel.py --seed-only
python3 live_sentinel.py --once --dry-run --no-push
python3 live_sentinel.py --from-yesterday-picks --interval 30
```

### 9.5 14:50 多轨出票推送

脚本：

```bash
/Users/eudis/ths/scripts/shell/run_push_top_pick.sh
```

模块：

```text
quant_core.execution.pushplus_tasks top-pick
```

功能：

- 若今日已有锁定记录，只补推已锁定内容，不重新扫描、不覆盖快照。
- 若无记录，则调用 `scan_market`，三大军团各自选 Top1。
- XGBoost 选出最终候选后，调用 `run_1446_ai_interview()` 做本地 Ollama 舆情访谈。
- AI 访谈结论写入 PushPlus Markdown 的 `AI舆情与风险排查` 区块，并附加到每只股票行内。
- 成功落库后推送 PushPlus。
- 推送结果写回 `daily_picks.push_status`。

补发：

```bash
cd /Users/eudis/ths
python3 -m quant_core.execution.pushplus_tasks resend-today
```

## 10. LaunchAgent 定时任务

源文件目录：

```text
/Users/eudis/ths/launch_agents
```

统一重写并重载：

```bash
cd /Users/eudis/ths
/Users/eudis/ths/scripts/shell/update_launch_agents.sh
```

当前生产任务：

| 时间 | Label | ProgramArguments |
|---|---|---|
| 启动常驻 | `com.eudis.quant.backend-api` | `/Users/eudis/ths/scripts/shell/run_backend_api.sh` |
| 启动常驻 | `com.eudis.quant.frontend-dev` | `/Users/eudis/ths/scripts/shell/run_frontend_dev.sh` |
| 01:20 | `com.eudis.quant.jq-cold-5m` | `/Users/eudis/ths/scripts/shell/run_jq_cold_5m.sh` |
| 09:00 | `com.eudis.quant.push-heartbeat` | `/Users/eudis/ths/scripts/shell/run_push_heartbeat.sh` |
| 09:15 | `com.eudis.quant.live-sentinel` | `/Users/eudis/ths/scripts/shell/run_live_sentinel.sh` |
| 09:16 | `com.eudis.quant.exit-sentinel-0916` | `/Users/eudis/ths/scripts/shell/run_exit_sentinel.sh --stage preopen` |
| 09:21 | `com.eudis.quant.exit-sentinel-0921` | `/Users/eudis/ths/scripts/shell/run_exit_sentinel.sh --stage audit --sleep-seconds 5` |
| 09:25 | `com.eudis.quant.exit-sentinel-0925` | `/Users/eudis/ths/scripts/shell/run_exit_sentinel.sh --stage final --sleep-seconds 5` |
| 14:30 | `com.eudis.quant.snapshot-1430` | `/Users/eudis/ths/scripts/shell/run_snapshot_1430.sh` |
| 14:45 | `com.eudis.quant.swing-patrol` | `/Users/eudis/ths/scripts/shell/run_swing_patrol.sh` |
| 14:50 | `com.eudis.quant.push-top-pick` | `/Users/eudis/ths/scripts/shell/run_push_top_pick.sh` |
| 14:50 | `com.eudis.quant.v3-sniper-lock` | `/Users/eudis/ths/scripts/shell/run_v3_sniper_lock.sh` |
| 15:05 | `com.eudis.quant.market-close-sync` | `/Users/eudis/ths/scripts/shell/run_market_close_sync.sh` |
| 15:08 | `com.quant.datasync` | `/usr/bin/python3 /Users/eudis/ths/scripts/utils/data_recorder.py` |
| 15:15 | `com.quant.daily_ashare_archiver` | `/Users/eudis/ths/scripts/shell/run_daily_ashare_archiver.sh` |
| 15:30 | `com.eudis.quant.daily-pick-save` | `/usr/bin/python3 /Users/eudis/ths/quant_core/execution/daily_pick_cli.py save` |

查看加载状态：

```bash
launchctl print gui/$(id -u)/com.eudis.quant.push-top-pick
```

## 11. 后端 API

后端启动：

```bash
cd /Users/eudis/ths
python3 -m uvicorn quant_dashboard.backend.main:app --host 127.0.0.1 --port 8000
```

地址：

```text
http://127.0.0.1:8000
http://127.0.0.1:8000/docs
```

状态：

```text
GET /health
GET /api/overview
GET /api/ollama/status
```

雷达：

```text
GET /api/radar/cache
GET /api/radar/scan?limit=10
POST /api/radar/analyze
```

影子账本：

```text
GET /api/daily-picks?limit=500
POST /api/daily-picks/save-now
POST /api/daily-picks/update-open
```

说明：

- `/api/daily-picks` 返回前端影子账本，包含 `strategy_type`、`snapshot_price`、`open_premium`、`t3_max_gain_pct`、`is_closed`、`push_status` 等字段。
- `save-now` 在后端已做保护，不允许前端手动改写 14:50 锁定。
- 前端展示以接口返回的原始字段为准，只做显示维度隔离，不改后端数据。

数据：

```text
POST /api/data/sync
POST /api/data/validate
GET /api/data/reports
GET /api/data/history/{code}?limit=120
GET /api/data/history_min/{code}?period=5&limit=5000
GET /api/data/market-sync/latest
GET /api/data/market-sync/history
POST /api/data/market-sync/run
```

策略分析：

```text
GET /api/backtest/top-pick-open?months=12&refresh=false
GET /api/strategy/lab?months=12&refresh=false
GET /api/strategy/failure-analysis?months=12&refresh=false
GET /api/strategy/up-reason-analysis?months=12&refresh=false
```

## 12. 前端看板

启动：

```bash
cd /Users/eudis/ths/quant_dashboard/frontend
npm run dev -- --host 127.0.0.1 --port 5173
```

访问：

```text
http://127.0.0.1:5173
```

构建：

```bash
cd /Users/eudis/ths/quant_dashboard/frontend
npm run build
```

当前 UI：

- Element Plus 深色模式。
- 顶部状态条：今日锁定数、最近同步时间、后端健康、PushPlus 状态。
- 侧边栏：V3 Command、Dashboard、Shadow Test、单票行情库、Validation。
- V3 Command：Live Sniper Radar、Qwen Agent Hub、历史锁定 Top 5，历史区域按月份切换。
- Dashboard：三大策略卡片展示总预测、已结算次数、胜率、T+3 均值和持仓数，胜率只按已结算样本计算。
- Shadow Test：股票名称和代码支持点击跳转到单票行情库，跳转提示只在点击股票跳转时出现。
- 影子账本：按月份 Tabs 分组，避免几百条记录无限下拉。
- 单票行情库：输入股票代码后读取日 K、5m 和原始表格；支持当日、近一周、近一月等视图范围。
- Validation：展示同步与校验报告，并补充聚宽每日获取情况、Ashare 每日归档情况和最后获取时间。
- 策略列动态渲染：突破展示置信度和 T+1 开盘结果；波段策略展示模型预期涨幅和 T+3 最大涨幅。

## 13. 环境变量

| 变量 | 当前默认值 | 说明 |
|---|---|---|
| `QUANT_BASE_DIR` | `/Users/eudis/ths` | 项目根目录 |
| `QUANT_DATA_DIR` | `/Users/eudis/ths/data/all_kline` | 日线 Parquet 目录 |
| `QUANT_MIN_KLINE_DIR` | `/Users/eudis/ths/data/min_kline` | 分钟线 Parquet 目录 |
| `QUANT_SQLITE_PATH` | `/Users/eudis/ths/data/core_db/quant_workstation.sqlite3` | SQLite 数据库 |
| `QUANT_MODELS_DIR` | `/Users/eudis/ths/models` | 模型目录 |
| `QUANT_PREMIUM_MODEL_PATH` | `models/overnight_premium_xgboost.json` | 尾盘突破模型 |
| `QUANT_DIPBUY_PREMIUM_MODEL_PATH` | `models/dipbuy_premium_xgboost.json` | 首阴低吸模型 |
| `QUANT_REVERSAL_MODEL_PATH` | `models/reversal_t3_xgboost.json` | 中线超跌反转模型 |
| `QUANT_MAIN_WAVE_MODEL_PATH` | `models/main_wave_t3_xgboost.json` | 右侧主升浪模型 |
| `QUANT_BREAKOUT_MIN_SCORE` | `65.50` | 尾盘突破门槛 |
| `QUANT_DIPBUY_MIN_SCORE` | `99.00` | 首阴低吸门槛 |
| `QUANT_REVERSAL_MIN_SCORE` | `3.00` | 中线超跌反转门槛 |
| `QUANT_MAIN_WAVE_MIN_SCORE` | `3.00` | 右侧主升浪门槛 |
| `QUANT_LATE_PULL_TRAP_THRESHOLD_PCT` | `4.00` | 14:30 后尾盘拉升拦截阈值 |
| `PUSHPLUS_TOKEN` | 本地 `.env` 配置 | PushPlus 微信推送令牌 |
| `JQ_USERNAME` | 本地 `.env` 配置 | 聚宽 SDK 登录账号 |
| `JQ_PASSWORD` | 本地 `.env` 配置 | 聚宽 SDK 登录密码 |
| `OLLAMA_API` | `http://127.0.0.1:11434/api/generate` | 本地大模型接口 |
| `OLLAMA_MODEL` | `qwen2.5:14b` | 本地大模型名称 |
| `AI_AGENT_OLLAMA_BASE_URL` | `http://localhost:11434/v1` | AI 右脑 Ollama OpenAI 兼容地址，404 时自动退回 `/api/generate` |
| `AI_AGENT_OLLAMA_MODEL` | 读取 `OLLAMA_MODEL`，否则 `deepseek-r1` | AI 右脑独立模型名 |
| `AI_AGENT_OLLAMA_TIMEOUT` | `30` | AI 右脑 LLM 请求超时秒数 |
| `AI_AGENT_NEWS_TIMEOUT` | `6` | 新闻/搜索抓取超时秒数 |
| `AI_AGENT_MAX_NEWS_PER_STOCK` | `5` | 每只股票最多送入 Prompt 的新闻线索条数 |

## 14. 常用运维命令

健康检查：

```bash
curl -sS http://127.0.0.1:8000/health
```

干跑策略引擎，不落库、不推送：

```bash
cd /Users/eudis/ths
python3 tests/test_engine.py
```

干跑早盘哨兵：

```bash
cd /Users/eudis/ths
python3 -m quant_core.execution.exit_sentinel --date 2026-04-29 --stage final --dry-run=true
```

干跑实时巡逻兵：

```bash
cd /Users/eudis/ths
python3 live_sentinel.py --once --dry-run --no-push
```

补发今日 14:50 推送：

```bash
cd /Users/eudis/ths
python3 -m quant_core.execution.pushplus_tasks resend-today
```

AI 右脑演示：

```bash
cd /Users/eudis/ths
python3 -m quant_core.ai_agent.agent_gateway --demo
python3 -m quant_core.ai_agent.agent_gateway --demo --no-llm
```

单独检查新闻抓取兜底：

```bash
cd /Users/eudis/ths
python3 - <<'PY'
from quant_core.ai_agent.news_fetcher import fetch_stock_news, format_news_context
payload = {"002709": fetch_stock_news("002709", "天赐材料", max_items=5)}
print(format_news_context(payload))
PY
```

每日 5m 热数据归档干跑：

```bash
cd /Users/eudis/ths
python3 scripts/data_pipeline/daily_ashare_archiver.py --limit 5 --count 100 --sleep 0
```

重建历史账本：

```bash
cd /Users/eudis/ths
python3 rebuild_historical_picks.py
```

波段阈值扫频：

```bash
cd /Users/eudis/ths
python3 -m quant_core.reversal_threshold_sweep --strategy main-wave --months 12 --start 3.0 --end 6.5 --step 0.5
```

训练主升浪模型：

```bash
cd /Users/eudis/ths
python3 scripts/dataset/build_main_wave_dataset.py
python3 scripts/training/quant_train_main_wave_models.py
```

训练中线反转模型：

```bash
cd /Users/eudis/ths
python3 scripts/dataset/build_reversal_dataset.py
python3 scripts/training/quant_train_reversal_models.py
```

检查 `daily_picks` 表结构：

```bash
sqlite3 /Users/eudis/ths/data/core_db/quant_workstation.sqlite3 "PRAGMA table_info(daily_picks);"
```

重写并重载 LaunchAgent：

```bash
/Users/eudis/ths/scripts/shell/update_launch_agents.sh
```

## 15. 二次开发规范

新增策略建议流程：

1. 在 `quant_core/strategies` 新增策略类，实现 `BaseStrategy` 的 `filter()`、`score()`、`get_threshold()`。
2. 在 `quant_core/strategies/factory.py` 注册策略。
3. 在 `quant_core/engine/predictor.py` 增加策略特征列、物理条件、模型加载、评分和 API 字段。
4. 若策略是 T+3 生命周期，需要接入 `SWING_STRATEGY_TYPES`、`swing_patrol.py`、`daily_pick.py` 的 target_date 逻辑。
5. 新建独立数据集脚本到 `scripts/dataset`。
6. 新建独立训练脚本到 `scripts/training`。
7. 模型保存到 `models/`，并在 `config.py` 和 `.env.example` 中补齐路径。
8. 前端只做显示隔离，不直接修改后端字段。
9. 修改生产门槛后，运行 `rebuild_historical_picks.py` 重建账本，再检查前端复盘。
10. 若策略需要 AI 定性排查，只把候选摘要传入 `run_1446_ai_interview()`；禁止让 LLM 输出直接改写模型分数、快照价或数据库主字段。

禁止事项：

- 禁止用盘后收盘价覆盖 `snapshot_price`。
- 禁止前端手动修改 14:50 锁定结果。
- 禁止在没有重新训练模型的情况下随意增删模型特征。
- 禁止让单个策略异常阻塞 PushPlus 或哨兵全局执行。
- 禁止把新闻抓取异常、HTML 原文或解析堆栈传给 Ollama；失败必须返回正常兜底文本。
- 禁止把外部参考项目的数据源/数据库代码直接并入生产主干。

## 16. 风险与说明

1. 本项目用于策略研究、影子测试和数据校验，不构成任何投资建议。
2. 前向影子测试必须以真实 14:50 快照价为准。
3. 周末和非交易日不应生成新生产标的。
4. 节假日前最后交易日的 T+1/T+3 计算应跳过休市日。
5. 若 14:30 快照缺失，尾盘诱多过滤会降级，但其他过滤仍生效。
6. 若 PushPlus 不可用，系统会在 `/health` 标记异常，并在终端输出错误。
7. 若 Ollama 不可用，AI 右脑会降级为“谨慎/人工复核”文本，但结构化模型扫描、落库和 PushPlus 仍可运行。
8. 数据完整性优先级高于模型分数；发现 `pre_close`、成交量、复权口径、停牌异常时，应先修数据再讨论策略收益。
