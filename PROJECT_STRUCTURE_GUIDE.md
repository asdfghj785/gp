# A 股量化工作站项目结构与二次开发指南

更新时间：2026-05-04

本文档用于让新的开发者快速理解 `/Users/eudis/ths` 项目结构、运行方式、关键数据流和每个主要文件的职责。当前代码已经不是早期的单一尾盘策略，而是本地 macOS/M4 上运行的多策略 A 股量化工作站。

> 重要提示：本文档描述的是当前代码结构和生产口径。旧版 `TECHNICAL_DOC.md` 中仍保留部分历史 V2.1 描述，不能作为当前实现的唯一依据。

## 1. 项目定位

本项目是一个本地运行的 A 股量化影子测试工作站，核心目标是：

- 同步并校验全市场日线与实时行情数据。
- 使用 XGBoost 回归模型对不同交易策略进行结构化预测。
- 在 14:50 通过实时快照生成候选股票并封存快照价格。
- 通过 PushPlus 发送心跳、预测结果、早盘/尾盘哨兵指令。
- 用 SQLite 保存预测、实盘影子测试记录、回测记录和同步记录。
- 通过 FastAPI + Vue 前端展示雷达、复盘、数据校验和单票 K 线。

当前生产核心保留四大策略框架，但实际出票只启用两条高胜率主线：

1. `全局动量狙击`：V4 Theme Alpha 全市场 T+3 概率策略，带 `theme_*` 主题因子。
2. `尾盘突破`：T+1 短线隔夜策略，预测次日开盘溢价。
3. `右侧主升浪`：T+3 波段策略，预测未来 3 个交易日最大涨幅，当前生产暂停、前端灰显。
4. `中线超跌反转`：T+3 波段策略，预测未来 3 个交易日最大涨幅，当前生产暂停、前端灰显。

`首阴低吸` 仍保留模型与代码，但默认门槛 `QUANT_DIPBUY_MIN_SCORE=99.00`，相当于影子保留，不参与正常生产出票。

V5.6 关键口径：

- 启用策略先过物理风控和买得到过滤，再按各自分数降序排序。
- 达到 `MIN_SCORE` 的 `base` 档，每个启用策略最多取 Top1。
- 若某策略没有基准线票，则计算 `dynamic_floor=max(0.55, legal_pool.score.quantile(0.99))`，只允许 Top1 以 `dynamic_floor` 档下探出票。
- 14:50 生产总输出上限为 `PRODUCTION_TOTAL_PICK_LIMIT=2`，当前应形成 `全局动量狙击` 和 `尾盘突破` 各最多一只，而不是全局只推荐一只。
- `dynamic_floor` 档必须写入 `risk_warning`，建议仓位固定为 `5%`；`base` 档使用 Half-Kelly，仓位限制在 `10%` 到 `30%`。
- `daily_picks` 兼容历史多标的原始记录；默认前端账本与回放统计按 `selection_date + strategy_type` 折叠为每策略 Top1。
- 历史复盘使用 `stock_daily` 的 15:00 完整日线底座，不再强行应用 14:50 分时截面代理；生产 14:50 仍使用实时快照。
- V5.6 5m Sentinel 回放读取 `/Users/eudis/5min/organized_5min_pre_adj` 与 `data/min_kline/5m` 合并后的前复权 5m 数据，只评估卖出引擎，不覆盖真实快照。
- 涨停/准涨停不可交易拦截、极严门槛和主题字段契约必须同时作用于生产出票、历史回放和前端复盘。
- V5.0 资金池使用 `data/shadow_account.json`；Mac Sniper 共享开关使用 `data/sniper_status.json`，前端保险匣、14:50 总线和巡逻兵读取同一状态。

## 2. 技术栈

后端与量化核心：

- Python 3
- FastAPI
- Uvicorn
- SQLite
- Pandas / NumPy
- XGBoost / scikit-learn
- Requests / BeautifulSoup4
- PyArrow Parquet

前端：

- Vue 3
- Vite
- 原生 CSS

本地自动化：

- macOS LaunchAgent
- zsh/bash 启动脚本
- PushPlus 微信推送

外部数据源：

- 新浪实时行情接口
- 腾讯实时行情与 5m 热数据接口
- 聚宽 SDK，提供账号授权滚动窗口内的历史 5m 冷数据
- 本地 Parquet 日线库
- 新闻/搜索线索与本地兜底文本，用于 Ollama 舆情风控辅助
- Ollama 本地大模型接口，仅用于舆情/公告风控，不直接预测价格

## 3. 当前关键运行地址

- 前端页面：http://127.0.0.1:5173/
- 后端 API：http://127.0.0.1:8000/
- FastAPI 文档：http://127.0.0.1:8000/docs
- 健康检查：http://127.0.0.1:8000/health

## 4. 目录总览

```text
/Users/eudis/ths
├── quant_core/                     # 量化核心包：预测、回测、存储、哨兵、校验
├── quant_dashboard/
│   ├── backend/                    # FastAPI 后端
│   └── frontend/                   # Vue/Vite 前端
├── data/
│   ├── all_kline/                  # 当前主日线 Parquet 库
│   ├── all_kline_old/              # 旧日线备份库
│   ├── core_db/                    # 当前 SQLite 主库
│   ├── intraday/                   # 14:30 盘中快照
│   ├── kline/                      # 其他 K 线缓存
│   ├── limit_up/                   # 涨停相关数据
│   ├── ml_dataset/                 # 训练数据集输出目录
│   ├── strategy_cache/             # 后端复盘/策略分析 JSON/Parquet 缓存
│   ├── shadow_account.json         # V5.0 影子资金池、锁定持仓、确认流水
│   └── sniper_status.json          # Mac Sniper 保险匣共享状态
├── launch_agents/                  # macOS LaunchAgent 配置
├── news_radar/                     # 旧/辅助新闻雷达模块
├── *.json                          # XGBoost 模型文件及备份
├── build_*.py                      # 训练集构建脚本
├── quant_train_*.py                # 模型训练脚本
├── run_*.sh                        # 自动化任务执行脚本
├── install_*.sh                    # LaunchAgent 安装脚本
├── rebuild_historical_picks.py     # 12 个月历史账本重建
├── analyze_backtest_performance.py # SQLite daily_picks 复利资金曲线分析
├── quant_pushplus_tasks.py         # PushPlus 心跳与 14:50 推送入口
├── PROJECT_STRUCTURE_GUIDE.md      # 当前文档
└── TECHNICAL_DOC.md                # 历史技术文档
```

## 5. 环境变量与配置

配置入口是 `quant_core/config.py`，默认从环境变量读取；示例文件为 `.env.example`。

重要配置：

| 配置名 | 默认值 | 作用 |
|---|---:|---|
| `QUANT_BASE_DIR` | `/Users/eudis/ths` | 项目根目录 |
| `QUANT_DATA_DIR` | `data/all_kline` | 日线 Parquet 数据目录 |
| `QUANT_SQLITE_PATH` | `data/core_db/quant_workstation.sqlite3` | SQLite 主库 |
| `QUANT_PREMIUM_MODEL_PATH` | `overnight_premium_xgboost.json` | 尾盘突破模型 |
| `QUANT_DIPBUY_PREMIUM_MODEL_PATH` | `dipbuy_premium_xgboost.json` | 首阴低吸模型 |
| `QUANT_REVERSAL_MODEL_PATH` | `reversal_t3_xgboost.json` | 中线超跌反转模型 |
| `QUANT_MAIN_WAVE_MODEL_PATH` | `main_wave_t3_xgboost.json` | 右侧主升浪模型 |
| `QUANT_BREAKOUT_MIN_SCORE` | `72.00` | 尾盘突破准入门槛，代码内设下限 |
| `QUANT_DIPBUY_MIN_SCORE` | `99.00` | 首阴低吸准入门槛 |
| `QUANT_REVERSAL_MIN_SCORE` | `6.00` | 中线超跌反转准入门槛，代码内设下限 |
| `QUANT_MAIN_WAVE_MIN_SCORE` | `6.60` | 右侧主升浪准入门槛，代码内设下限 |
| `QUANT_GLOBAL_MIN_SCORE` | `0.90` | 全局动量狙击概率门槛，代码内设下限 |
| `QUANT_PRODUCTION_STRATEGIES` | `全局动量狙击,右侧主升浪,尾盘突破` | 生产策略白名单 |
| `QUANT_PAUSED_STRATEGIES` | `右侧主升浪,中线超跌反转` | 暂停策略名单，前端灰显且不进入默认账本 |
| `QUANT_PRODUCTION_TOTAL_PICK_LIMIT` | `2` | 当前启用策略总出票上限 |
| `QUANT_LATE_PULL_TRAP_THRESHOLD_PCT` | `4.00` | 14:30 到 14:50 尾盘拉升诱多阈值 |
| `QUANT_SHADOW_ACCOUNT_PATH` | `data/shadow_account.json` | V5.0 影子资金池 |
| `PUSHPLUS_TOKEN` | 空 | PushPlus 微信推送 token |
| `OLLAMA_API` | `http://127.0.0.1:11434/api/generate` | Ollama 生成接口 |
| `OLLAMA_MODEL` | `qwen2.5:14b` | 舆情风控模型名 |

## 6. 生产数据流

### 6.1 14:30 盘中快照

入口：

- `snapshot_1430.py`
- `run_snapshot_1430.sh`
- `launch_agents/com.eudis.quant.snapshot-1430.plist`

作用：

- 调用新浪实时行情接口。
- 保存全市场 `代码 -> 14:30 最新价` 到 `data/intraday/price_1430.json`。
- 给 14:50 预测端做“尾盘诱多”识别。

### 6.2 14:50 四策略雷达与 PushPlus 推送

入口：

- `quant_pushplus_tasks.py top-pick`
- `run_push_top_pick.sh`
- `launch_agents/com.eudis.quant.push-top-pick.plist`

核心调用链：

```text
quant_pushplus_tasks.top_pick()
  -> quant_core.predictor.scan_market()
  -> build_features()
  -> score_candidates()
  -> market_risk_gate()
  -> apply_production_filters()
  -> apply_strategy_score_gate()
  -> select_strategy_top_picks(limit_per_strategy=1)
  -> save_prediction_snapshot()
  -> save_pushed_top_picks()
  -> PushPlus 推送
```

当前生产逻辑是“每个启用策略独立分档出票”，不是全局唯一 Top1：

- `全局动量狙击`：启用，基准线 Top1 或动态下探 Top1。
- `尾盘突破`：启用，基准线 Top1 或动态下探 Top1。
- `右侧主升浪`：暂停，保留模型、代码和前端灰色卡片。
- `中线超跌反转`：暂停，保留模型、代码和前端灰色卡片。

因此当前正常交易日最多写入 2 条可行动标的：全局狙击 1 条、尾盘突破 1 条。

写入 `daily_picks` 时会封存：

- `snapshot_time`
- `snapshot_price`
- `snapshot_vol_ratio`
- `is_shadow_test`

这些字段用于前向影子测试，后续更新开盘价或收盘结果时不能覆盖原始快照。

### 6.3 09:26 早盘哨兵

入口：

- `quant_core/exit_sentinel.py`
- `run_exit_sentinel.sh`
- `launch_agents/com.eudis.quant.exit-sentinel.plist`

职责：

- 读取尚未完结的 `daily_picks`。
- 对 `尾盘突破` 执行 T+1 开盘审判：
  - 低开：核按钮警告。
  - 0% 到 3%：落袋为安。
  - 大于等于 3%：超预期锁仓。
- 对波段策略只回填 `open_price/open_premium`，不再用早盘开盘价触发卖出。
- 回填 `open_price`、`open_premium`、`open_checked_at`。

### 6.4 15:10 T+3 收盘结算器

入口：

- `quant_core/swing_patrol.py`
- `run_swing_patrol.sh`
- `launch_agents/com.eudis.quant.swing-patrol.plist`

职责：

- 只处理到达 `target_date` 的 T+3 波段持仓。
- 读取目标交易日 `stock_daily.close` 作为 15:00 结算价，不再用 14:45 盘中实时价触发止盈、止损或追踪卖出。
- 按 `close_price / snapshot_price - 1` 计算 `close_return_pct`。
- 调用存储层关闭交易，避免重复结算。

### 6.5 15:05 收盘同步

入口：

- `quant_market_sync.py`
- `quant_core/market_sync.py`
- `run_market_close_sync.sh`
- `launch_agents/com.eudis.quant.market-close-sync.plist`

职责：

- 在收盘后同步全市场最新日线/行情数据到 SQLite。
- 写入 `market_sync_runs`，前端展示同步时间、数量、状态。

## 7. 核心策略说明

### 7.1 尾盘突破

文件：

- `build_smart_overnight_dataset.py`
- `quant_train_premium_models.py`
- `overnight_premium_xgboost.json`
- `quant_core/predictor.py`

目标：

- 预测 `T+1` 开盘溢价。
- 用于隔夜套利。

主要过滤：

- 剔除创业板、科创板、北交所、ST/退市。
- 大盘雷暴/缩量下跌时空仓。
- 高位爆量、尾盘诱多、准涨停未封、长上影、断头铡刀等。

前端展示：

- `预测置信度`：展示策略评分。
- `模型预期`：展示预期开盘溢价。
- `收益口径`：展示 T+1 开盘溢价。

### 7.2 中线超跌反转

文件：

- `quant_core/strategy_lab_bottom_reversal.py`
- `build_reversal_dataset.py`
- `quant_train_reversal_models.py`
- `reversal_t3_xgboost.json`
- `quant_core/predictor.py`

目标：

- 预测 `T+1` 到 `T+3` 三个交易日内的最大涨幅。
- 用于波段观察，不用 T+1 开盘价作为唯一结果。

核心形态：

- T-1 收盘价在 60 日均线下方。
- 过去 60 日有足够深的回撤。
- T-5 到 T-1 出现极度缩量。
- T 日放量大阳线，一阳穿 5 日和 10 日均线。
- 防伪条件包含 30 日均线斜率和 20/30 日均线乖离限制。

前端展示：

- `预测置信度` 显示 `-`。
- `模型预期` 显示 `T+3预期涨幅`。
- `收益口径` 显示 `T+3最大涨幅`。
- `开盘卖出` 显示 `波段持仓`。

### 7.3 右侧主升浪

文件：

- `quant_core/strategy_lab_main_wave.py`
- `build_main_wave_dataset.py`
- `quant_train_main_wave_models.py`
- `main_wave_t3_xgboost.json`
- `quant_core/predictor.py`

目标：

- 预测 `T+1` 到 `T+3` 三个交易日内的最大涨幅。
- 捕获强势趋势下缩量蓄势后的放量接力。

当前生产条件：

- T-1 日 20 日均线 > 60 日均线。
- T-1 收盘价距离 60 日高点回撤不超过 15%。
- T-5 到 T-1 区间振幅不超过当前放宽后的上限。
- T-1 缩量。
- T 日突破平台高点。
- T 日实体攻击。
- T 日温和或有效放量。

前端展示：

- 紫/金色策略标签：`🚀 顺势主升浪`。
- 同中线反转一样走 T+3 波段展示。

### 7.4 全局动量狙击

文件：

- `quant_core/engine/daily_factor_factory.py`
- `quant_core/engine/daily_model_trainer.py`
- `quant_core/strategies/global_momentum.py`
- `models/xgboost_daily_swing_global_v1.json`
- `models/xgboost_daily_swing_global_v1.meta.json`

目标：

- 使用带 `theme_pct_chg_1`、`theme_pct_chg_3`、`theme_volatility_5`、`rs_stock_vs_theme`、`rs_theme_ema_5` 的全市场 XGBoost 概率模型。
- 预测未来 3 个交易日最高收益率是否超过目标阈值。
- 生产口径归属 T+3 波段观察。

生产硬风控：

- 只保留 `00`、`60` 开头主板股票。
- 剔除名称包含 `ST` / `*ST` 的股票。
- 剔除 14:50 实时涨幅 `>= 9.0%` 的准涨停/涨停票。

前端展示：

- 策略标签：`全局狙击`。
- 展示 `核心主题`、`主题3日动量`、`凯利仓位`、风偏提示和 T+3 观察结果。

### 7.5 首阴低吸

文件：

- `quant_train_dipbuy_models.py`
- `dipbuy_premium_xgboost.json`
- `quant_core/predictor.py`

状态：

- 代码与模型保留。
- 默认门槛 99，不参与正常生产出票。
- 未来若要恢复，先做失败归因与阈值扫频，不要直接降低生产门槛。

## 8. 数据库结构

SQLite 主库：

```text
data/core_db/quant_workstation.sqlite3
```

主要表：

### 8.1 `stock_daily`

由 `quant_core/storage.py` 创建。保存本地日线数据，支撑前端 K 线、回测、训练集修复等。

关键字段：

- `code`
- `name`
- `date`
- `open`
- `high`
- `low`
- `close`
- `pre_close`
- `volume`
- `amount`
- `turnover`
- `volume_ratio`
- `source`
- `created_at`

### 8.2 `daily_picks`

生产/影子测试核心表。当前唯一约束是 `(selection_date, strategy_type, code)`，允许同一天同一策略写入多只不同股票。

关键字段：

- `selection_date`：预测日。
- `target_date`：目标日期，短线是 T+1，波段是 T+3 观察日。
- `selected_at`：写入时间。
- `code` / `name`：股票。
- `strategy_type`：策略类型。
- `win_rate`：历史兼容字段；波段策略不应解读为真实胜率。
- `selection_price`：兼容旧字段。
- `snapshot_time`：14:50 实盘快照时间。
- `snapshot_price`：14:50 实盘快照价格。
- `snapshot_vol_ratio`：14:50 外推量比。
- `is_shadow_test`：是否前向影子测试记录。
- `open_price` / `open_premium`：T+1 开盘数据。
- `t3_max_gain_pct`：波段策略 T+3 最大涨幅。
- `is_closed`：是否完结。
- `close_date` / `close_price` / `close_return_pct` / `close_reason`：哨兵关闭结果。
- `raw_json`：原始候选 JSON。

V5.6 关键展示字段存于 `raw_json.winner`，并由 `quant_core/storage.py` 解码后透出给 `/api/daily-picks`：

- `theme_name` / `theme_pct_chg_3`：核心主题与主题 3 日动量。
- `selection_tier`：`base` 或 `dynamic_floor`。
- `risk_warning`：下探、历史导入等风偏提示。
- `dynamic_floor` / `score_floor`：当日合规池自适应底线。
- `suggested_position`：建议仓位比例。

`/api/daily-picks` 默认 `view=strategy_top1`，会过滤暂停策略并按 `selection_date + strategy_type` 折叠，解决历史 Top3 或旧逻辑导致同策略同日多条记录的问题。`view=all` 只用于审计原始落库记录，不作为前端生产账本口径。

旧全局狙击页面的锁定记录保存在 `v3_sniper_locks`。如果未进入新影子账本，原因通常是旧页面只写了锁表而没有写 `daily_picks`；迁移时必须保留原始 `snapshot_price` / `snapshot_time`，并在 `risk_warning` 中标记该记录未经过当前 V5.6 风控重筛。

### 8.3 `prediction_snapshots`

保存雷达扫描缓存，供前端“预测雷达”快速读取。

字段：

- `id`
- `created_at`
- `strategy`
- `rows_json`

### 8.4 `validation_reports`

保存数据校验报告。

### 8.5 `market_sync_runs`

保存 15:05 收盘同步执行记录。

## 9. 后端 API

入口文件：

```text
quant_dashboard/backend/main.py
```

核心接口：

| 方法 | 路径 | 作用 |
|---|---|---|
| `GET` | `/health` | 后端健康检查 |
| `GET` | `/` | API 入口与接口索引 |
| `GET` | `/api/overview` | 数据库和系统概览 |
| `GET` | `/api/ollama/status` | Ollama 可用性检查 |
| `POST` | `/api/data/sync` | 从 Parquet 导入/同步数据到 SQLite |
| `GET` | `/api/data/market-sync/latest` | 最近一次收盘同步 |
| `GET` | `/api/data/market-sync/history` | 收盘同步历史 |
| `POST` | `/api/data/market-sync/run` | 手动执行收盘同步 |
| `POST` | `/api/data/validate` | 数据完整性/真实性校验 |
| `GET` | `/api/data/reports` | 历史校验报告 |
| `GET` | `/api/data/history/{code}` | 单票 K 线数据 |
| `GET` | `/api/data/history_min/{code}?period=5` | 单票分钟 K 线数据 |
| `GET` | `/api/data/minute-fetch/status` | 聚宽冷数据与 Ashare/Tencent 热数据采集状态 |
| `GET` | `/api/radar/cache` | 读取最近雷达缓存 |
| `GET` | `/api/radar/scan?limit=10` | 实时扫描并更新缓存 |
| `GET` | `/api/daily-picks?view=strategy_top1&limit=20` | 前向影子测试记录，默认每策略 Top1 |
| `GET` | `/api/backtest/top-pick-open?months=12` | 生产策略复盘 |
| `GET` | `/api/strategy/lab?months=12` | 策略实验室 |
| `GET` | `/api/strategy/failure-analysis?months=12` | 失败归因 |
| `GET` | `/api/strategy/up-reason-analysis?months=12` | 上涨原因统计 |
| `GET` | `/api/sniper/status` | 读取 Mac Sniper 保险匣状态 |
| `POST` | `/api/sniper/toggle` | 更新 Mac Sniper 保险匣状态 |
| `POST` | `/api/sniper/test_fire` | 手动试射，只做跳转/拉起验证 |
| `GET` | `/api/shadow-account` | 读取 V5.0 影子资金池、锁定持仓和确认流水 |
| `POST` | `/api/shadow-account/cash` | 覆盖设置影子资金池可用资金 |
| `POST` | `/api/shadow-account/sync-broker` | 从同花顺交易页同步资金和持仓 |
| `POST` | `/api/shadow-account/test_order` | 算股预览或全自动休市试射 |
| `POST` | `/api/daily-picks/save-now` | 禁用：前端不允许手动保存 |
| `POST` | `/api/daily-picks/update-open` | 手动触发开盘回填 |
| `POST` | `/api/radar/analyze` | 单票 Ollama 舆情风控 |

缓存说明：

- 复盘和策略分析接口使用 `quant_core/cache_utils.py` 读写 JSON 缓存。
- 加 `refresh=true` 会强制重算。
- `/api/backtest/top-pick-open` 会补齐 `core_theme`、`theme_momentum_3d`、`theme_name`、`theme_pct_chg_3`，供 Shadow Test 月度复盘直接展示。
- `/api/shadow-account/test_order` 只有在同花顺持仓表确认成交后才写入 `broker_confirmed` 本地流水；休市试射、券商弹窗和资金不足不写成交记录。

## 10. 前端结构

目录：

```text
quant_dashboard/frontend
├── package.json
├── vite.config.js
├── index.html
├── public/
├── dist/
└── src/
    ├── main.js
    ├── App.vue
    ├── RadarView.vue
    └── style.css
```

文件说明：

- `src/main.js`：Vue 应用入口，挂载 `App.vue`。
- `src/App.vue`：当前主看板，包含预测、策略、数据、单票四个主区域。
- `src/RadarView.vue`：旧/辅助雷达视图组件，当前主入口主要使用 `App.vue`。
- `src/style.css`：全局基础样式。
- `dist/`：`npm run build` 后的构建产物。

主要页面区域：

1. `预测`：实时雷达、影子测试记录、09:25 早盘哨兵与 T+3 收盘结算观测。
2. `策略` / `Shadow Test`：真实影子账本、月度 Tabs、策略军团卡片、卖出策略口径与失败归因。
3. `数据` / `Validation`：数据同步、数据校验、同步历史，并展示聚宽冷数据和 Ashare/Tencent 热数据采集状态。
4. `资金池`：设置 `shadow_account.json.available_cash`、同步同花顺资金/持仓、算股预览、全自动休市试射和本地成交确认流水。
5. `单票行情库`：日 K、5m K 线验算，股票名称跳转默认展示日 K，鼠标悬停查看日期/开盘/收盘/成交量等。

前端展示口径：

- `尾盘突破` 显示 T+1 开盘溢价。
- `全局动量狙击`、`右侧主升浪` 和 `中线超跌反转` 显示 T+3 最大涨幅，不与 T+1 开盘溢价混用。
- `右侧主升浪` 与 `中线超跌反转` 当前暂停，顶部卡片灰显，默认账本和回放不展示这两条策略的记录。
- Shadow Test 默认请求 `/api/daily-picks?view=strategy_top1&limit=1000`，同一天可展示 `全局动量狙击` 与 `尾盘突破` 各一只。
- 顶部四策略卡片必须同时展示 T+1 胜率/T+1 均值和 T+3 胜率/T+3 均值；缺失维度显示 `-`。
- 影子账本表格必须展示 `核心主题`、`主题3日动量`、`凯利仓位`。主题 3 日动量超过 `3%` 时高亮，仓位低于 `10%` 时用警告色，仓位 `>=15%` 时用重仓高亮。
- 策略标签颜色固定：全局狙击红、顺势主升浪紫、中线超跌反转琥珀、尾盘突破蓝。
- `risk_warning` 不为空时必须直接在股票名下方或警告区域显示；`selection_tier=dynamic_floor` 时策略标签旁显示下探/逆势标记。
- T+3 波段票的“卖出策略”单元格优先展示 V5.6 Sentinel 统一口径：追踪止盈、尾盘结构止损、盘中防爆止损、T+3 强制平仓或日线兜底平仓。
- 聚宽冷数据卡片中“本次新增”是最近一次任务新增成功股票数 / 全市场股票池；“断点进度”来自 `jq_cold_5m_progress.json`，表示累计有断点记录的股票数和已完成的月切片数。
- Ashare/Tencent 热数据卡片的“今日覆盖”表示当日热数据归档成功数。
- 前端不能手动保存或修改 14:50 标的。

## 11. 文件逐项说明

### 11.1 `quant_core/`

| 文件 | 作用 |
|---|---|
| `__init__.py` | Python 包标记。 |
| `config.py` | 全局路径、模型路径、策略门槛、PushPlus/Ollama 配置。 |
| `engine/predictor.py` | 生产预测核心：实时快照清洗、特征工程、四策略评分、物理风控、动态底线、仓位计算、分档出票、API 行格式化。 |
| `storage.py` | SQLite 连接、建表、迁移、预测缓存、日线入库、daily_picks 写入/回填/关闭。 |
| `market.py` | 新浪行情与大盘指数数据抓取。 |
| `market_sync.py` | 收盘后市场数据同步和同步记录入库。 |
| `intraday_snapshot.py` | 14:30 盘中快照保存与 14:50 尾盘诱多计算。 |
| `daily_pick.py` | 14:50 推送标的批量锁定、T+1 开盘回填逻辑。 |
| `exit_sentinel.py` | 09:26 开盘哨兵，处理短线开盘指令和波段极端低开预警。 |
| `swing_patrol.py` | 15:10 T+3 收盘结算器，按目标日 15:00 close 闭环波段策略。 |
| `engine/backtest.py` | 生产复盘接口计算：按交易日和策略分组输出前端复盘统计，波段策略使用 T+3 口径。 |
| `strategy_lab.py` | 策略实验室，对不同规则、阈值、过滤条件做历史对比。 |
| `failure_analysis.py` | 失败样本归因、按策略分组统计、反转弱样本尸检。 |
| `up_reason_analysis.py` | 统计次日上涨股票，分析技术/情绪因素对上涨的影响。 |
| `validation.py` | 数据完整性、正确性、真实性校验。 |
| `threshold_sweep.py` | 尾盘突破/低吸阈值扫频工具。 |
| `reversal_threshold_sweep.py` | 波段策略阈值扫频工具，支持反转和主升浪。 |
| `strategy_lab_n_shape.py` | N 字反包历史探伤脚本。 |
| `strategy_lab_bottom_reversal.py` | 中线超跌反转历史探伤脚本。 |
| `strategy_lab_main_wave.py` | 右侧主升浪历史探伤脚本。 |

### 11.2 后端文件

| 文件 | 作用 |
|---|---|
| `quant_dashboard/backend/main.py` | FastAPI 后端入口，暴露雷达、数据、复盘、策略实验、失败归因、Ollama 分析接口。 |
| `quant_dashboard/backend/requirements.txt` | 后端 Python 依赖列表。 |
| `run_backend_api.sh` | 后端 Uvicorn 常驻启动脚本。 |
| `install_backend_launch_agent.sh` | 安装后端 LaunchAgent。 |

### 11.3 前端文件

| 文件 | 作用 |
|---|---|
| `quant_dashboard/frontend/package.json` | 前端依赖和 `dev/build/preview` 脚本。 |
| `quant_dashboard/frontend/src/App.vue` | 主看板组件，当前绝大多数前端逻辑在这里。 |
| `quant_dashboard/frontend/src/RadarView.vue` | 辅助/旧雷达视图。 |
| `quant_dashboard/frontend/src/main.js` | Vue 入口。 |
| `quant_dashboard/frontend/src/style.css` | 全局 CSS。 |
| `run_frontend_dev.sh` | 前端 Vite 常驻启动脚本。 |
| `install_frontend_launch_agent.sh` | 安装前端 LaunchAgent。 |

### 11.4 数据集构建与模型训练

| 文件 | 作用 |
|---|---|
| `build_smart_overnight_dataset.py` | 构建尾盘突破/低吸训练集，包含时序趋势、量价背离、断头铡刀等特征。 |
| `build_reversal_dataset.py` | 构建中线超跌反转训练集，目标为 `t3_max_gain_pct`。 |
| `build_main_wave_dataset.py` | 构建右侧主升浪训练集，目标为 `t3_max_gain_pct`。 |
| `build_overnight_dataset.py` | 早期隔夜模型训练集脚本，历史兼容。 |
| `build_precise_overnight_dataset.py` | 早期精细隔夜训练集脚本，历史兼容。 |
| `build_training_dataset.py` | 早期通用训练集脚本，历史兼容。 |
| `quant_train_premium_models.py` | 训练尾盘突破 XGBRegressor。 |
| `quant_train_dipbuy_models.py` | 训练首阴低吸 XGBRegressor。 |
| `quant_train_reversal_models.py` | 训练中线超跌反转 XGBRegressor。 |
| `quant_train_main_wave_models.py` | 训练右侧主升浪 XGBRegressor。 |
| `train_xgboost_model.py` | 早期 XGBoost 训练入口，历史兼容。 |
| `save_model.py` | 早期模型保存辅助脚本。 |

### 11.4.1 回测与回放脚本

| 文件 | 作用 |
|---|---|
| `scripts/backtest/simulate_sentinel_5m.py` | V5.6 5m Sentinel 离线回放器，读取前复权冷数据和本地热数据，按每策略 Top1 样本回放非对称风控、追踪止盈、尾盘结构止损和 T+3 兜底。 |

### 11.5 数据同步与采集脚本

| 文件 | 作用 |
|---|---|
| `download_all_baostock.py` | 早期 Baostock 全市场下载脚本。 |
| `download_all_robust.py` | 更稳健的全市场数据下载脚本。 |
| `fetch_kline.py` | 单票 K 线抓取。 |
| `fetch_limit_up.py` | 涨停数据抓取。 |
| `batch_kline_indicators.py` | 批量计算 K 线指标。 |
| `batch_limit_up.py` | 批量处理涨停相关数据。 |
| `data_recorder.py` | 数据记录辅助脚本。 |
| `data_validator.py` | 早期数据校验脚本。 |
| `check_data.py` | 数据检查脚本。 |
| `clean_db.py` | 清理数据库辅助脚本。 |
| `quant_db_sync.py` | Parquet 到 SQLite 同步入口。 |
| `quant_market_sync.py` | 收盘同步 CLI 包装。 |

### 11.6 推送与自动化脚本

| 文件 | 作用 |
|---|---|
| `quant_pushplus_tasks.py` | PushPlus 心跳和 14:50 Top pick 推送主入口。 |
| `heartbeat.py` | 旧心跳脚本。 |
| `test_push.py` | PushPlus 测试脚本。 |
| `quant_daily_pick.py` | 旧/兼容 daily pick CLI，支持 `save` 和 `update-open`。 |
| `realtime_sniper.py` | 早期实时狙击脚本，当前生产以 `quant_core/predictor.py` 为准。 |
| `snapshot_1430.py` | 14:30 盘中快照入口。 |
| `stock_xray.py` | 单票扫描/透视辅助脚本。 |
| `daily_analyzer.py` | 早期每日分析脚本。 |

### 11.7 回测、分析和实验脚本

| 文件 | 作用 |
|---|---|
| `rebuild_historical_picks.py` | 清空并重建 12 个月 `daily_picks` 历史账本，按当前模型与门槛回放。 |
| `analyze_overnight_strategy.py` | 早期隔夜策略分析。 |
| `analyze_precise_overnight.py` | 早期精细隔夜分析。 |
| `analyze_smart_overnight.py` | 智能隔夜策略分析。 |

### 11.8 模型文件

| 文件 | 作用 |
|---|---|
| `overnight_premium_xgboost.json` | 尾盘突破当前生产回归模型。 |
| `dipbuy_premium_xgboost.json` | 首阴低吸回归模型。 |
| `reversal_t3_xgboost.json` | 中线超跌反转 T+3 回归模型。 |
| `main_wave_t3_xgboost.json` | 右侧主升浪 T+3 回归模型。 |
| `overnight_xgboost.json` | 早期分类/兼容模型。 |
| `*.bak_*` | 训练或调参前自动/手动保留的模型备份。 |

### 11.9 文档和配置

| 文件 | 作用 |
|---|---|
| `.env.example` | 环境变量示例。 |
| `.gitignore` | Git 忽略规则。 |
| `README.md` | 项目简要说明，部分内容偏旧。 |
| `PROJECT_1.0_DOC.md` | 初始需求文档。 |
| `TECHNICAL_DOC.md` | 历史技术文档。 |
| `PROJECT_STRUCTURE_GUIDE.md` | 当前项目结构与二次开发指南。 |

### 11.10 日志文件

常见日志：

- `quant_dashboard_backend.log`
- `quant_dashboard_backend_err.log`
- `quant_dashboard/frontend/frontend_server.log`
- `quant_dashboard/frontend/frontend_server.err.log`
- `push_heartbeat.log`
- `push_top_pick.log`
- `exit_sentinel.log`
- `swing_patrol.log`
- `market_close_sync.log`
- `snapshot_1430.log`
- `datasync.log`

日志只用于本地排障，不应作为业务数据源。

## 12. LaunchAgent 定时任务

配置目录：

```text
launch_agents/
```

| Label | 时间 | 脚本 | 作用 |
|---|---:|---|---|
| `com.eudis.quant.backend-api` | 开机/登录常驻 | `run_backend_api.sh` | 后端 API 常驻 |
| `com.eudis.quant.frontend-dev` | 开机/登录常驻 | `run_frontend_dev.sh` | 前端 Vite 常驻 |
| `com.eudis.quant.jq-cold-5m` | 01:20 | `run_jq_cold_5m.sh` | 聚宽 5m 冷数据额度任务，按断点和滚动授权窗口继续 |
| `com.eudis.quant.push-heartbeat` | 09:00 | `run_push_heartbeat.sh` | PushPlus 心跳 |
| `com.eudis.quant.exit-sentinel` | 09:26 | `run_exit_sentinel.sh` | 早盘哨兵 |
| `com.eudis.quant.daily-pick-open` | 09:31 | `quant_daily_pick.py update-open` | 兼容开盘回填 |
| `com.eudis.quant.snapshot-1430` | 14:30 | `run_snapshot_1430.sh` | 盘中快照 |
| `com.eudis.quant.swing-patrol` | 15:10 | `run_swing_patrol.sh` | T+3 收盘结算 |
| `com.eudis.quant.push-top-pick` | 14:50 | `run_push_top_pick.sh` | 预测与推送 |
| `com.eudis.quant.market-close-sync` | 15:05 | `run_market_close_sync.sh` | 收盘同步 |
| `com.eudis.quant.daily-pick-save` | 15:30 | `quant_daily_pick.py save` | 兼容保存最高胜率标的 |

安装命令：

```bash
cd /Users/eudis/ths
./install_backend_launch_agent.sh
./install_frontend_launch_agent.sh
./install_pushplus_launch_agents.sh
./install_daily_pick_launch_agents.sh
./install_market_sync_launch_agent.sh
```

查看状态：

```bash
launchctl print gui/$(id -u)/com.eudis.quant.backend-api
launchctl print gui/$(id -u)/com.eudis.quant.frontend-dev
launchctl print gui/$(id -u)/com.eudis.quant.push-top-pick
```

## 13. 常用开发命令

启动后端：

```bash
cd /Users/eudis/ths
python3 -m uvicorn quant_dashboard.backend.main:app --host 127.0.0.1 --port 8000
```

启动前端：

```bash
cd /Users/eudis/ths/quant_dashboard/frontend
npm run dev -- --host 127.0.0.1 --port 5173
```

前端构建：

```bash
cd /Users/eudis/ths/quant_dashboard/frontend
npm run build
```

后端健康检查：

```bash
curl http://127.0.0.1:8000/health
```

读取雷达缓存：

```bash
curl http://127.0.0.1:8000/api/radar/cache
```

实时扫描：

```bash
curl http://127.0.0.1:8000/api/radar/scan?limit=10
```

强制刷新生产复盘：

```bash
curl "http://127.0.0.1:8000/api/backtest/top-pick-open?months=12&refresh=true"
```

重建 12 个月历史账本：

```bash
cd /Users/eudis/ths
python3 rebuild_historical_picks.py
```

运行数据校验：

```bash
curl -X POST "http://127.0.0.1:8000/api/data/validate?sample=200&source_check=false"
```

## 14. 二次开发建议

### 14.1 新增一个策略的标准步骤

1. 新建历史探伤脚本：
   - 放在 `quant_core/strategy_lab_xxx.py`。
   - 先验证自然胜率、样本数、T+1/T+3 收益分布。

2. 新建数据集脚本：
   - 放在根目录 `build_xxx_dataset.py`。
   - 明确样本物理条件、目标标签和特征列。
   - 必须清理 NaN/Inf。

3. 新建训练脚本：
   - 放在根目录 `quant_train_xxx_models.py`。
   - 输出模型到根目录 `xxx_xgboost.json`。
   - 打印 Top 10% 或 Top 5% 的验证集表现。

4. 接入预测核心：
   - 修改 `quant_core/config.py` 增加模型路径和门槛。
   - 修改 `quant_core/predictor.py`：
     - 增加策略常量。
     - 增加物理 mask。
     - 加载模型。
     - 在 `score_candidates()` 内打标和预测。
     - 在 `apply_strategy_score_gate()` 内加入独立门槛。
     - 在 `select_strategy_top_picks()` 的策略输出序列中加入新策略。

5. 接入回测：
   - 修改 `quant_core/backtest.py`。
   - 如果是 T+3 策略，必须使用 `t3_max_gain_pct`，不能用 T+1 开盘溢价评价。
   - 修改 `rebuild_historical_picks.py` 保存策略结果。

6. 接入前端：
   - 修改 `quant_dashboard/frontend/src/App.vue`。
   - 新增策略 badge、结果口径、表格列展示。
   - 跑 `npm run build`。

7. 接入哨兵：
   - 短线策略接入 `exit_sentinel.py`。
   - 波段策略接入 `swing_patrol.py`。

### 14.2 修改门槛前的检查

修改以下配置前，必须先跑扫频：

- `QUANT_BREAKOUT_MIN_SCORE`
- `QUANT_REVERSAL_MIN_SCORE`
- `QUANT_MAIN_WAVE_MIN_SCORE`
- `QUANT_DIPBUY_MIN_SCORE`

推荐流程：

```bash
python3 -m quant_core.threshold_sweep
python3 -m quant_core.reversal_threshold_sweep
python3 rebuild_historical_picks.py
npm run build
```

### 14.3 快照字段不可篡改原则

`daily_picks.snapshot_price` 和 `daily_picks.snapshot_time` 是前向影子测试可信度的核心，代表 14:50 当时真实看到的盘口数据。

禁止：

- 用盘后收盘价覆盖 `snapshot_price`。
- 用历史回测价格伪造前向影子测试记录。
- 前端手动写入或修改 `daily_picks`。

允许：

- 09:26 回填 `open_price`、`open_premium`。
- 15:10 在 T+3 目标日收盘同步完成后回填 `close_*` 和 `is_closed`。
- 历史重建脚本写入 `is_shadow_test=0` 的模拟记录。

## 15. 当前容易踩坑的地方

1. `README.md` 仍可能有历史描述；当前生产口径以 `TECHNICAL_DOC.md`、`PROJECT_STRUCTURE_GUIDE.md` 和源码为准。
2. 波段策略不能用 T+1 开盘溢价判断成败，要看 `t3_max_gain_pct` 或哨兵最终关闭结果。
3. `win_rate` 字段对波段策略是兼容字段，前端不应展示为“收益信号”。
4. `daily_picks` 允许同一天同策略多标的，不能再假设 `selection_date` 或 `(selection_date, strategy_type)` 唯一。
5. 前端 `5173` 需要 LaunchAgent 常驻，否则临时终端退出后页面会访问不了。
6. macOS LaunchAgent 默认 PATH 很短，NVM 下的 Node/npm 必须用绝对路径或显式 PATH。
7. `data/all_kline` 很大，不要在 Git 或文档中逐个列出每只股票文件。
8. 修改 `predictor.py` 后，需要同步检查：
   - `quant_core/engine/backtest.py`
   - `rebuild_historical_picks.py`
   - `quant_core/execution/pushplus_tasks.py`
   - `quant_core/execution/exit_sentinel.py`
   - `quant_core/execution/swing_patrol.py`
   - `quant_dashboard/frontend/src/App.vue`
   - `quant_dashboard/frontend/src/components/SelectionTable.vue`
9. 重建历史账本会清空并重写 `daily_picks`，不要在真实前向影子测试阶段随意执行。
10. Ollama 风控失败时系统会红灯降级，这是正确行为，不应让不可解析 JSON 参与交易判断。

## 16. 推荐交接顺序

新开发者接手时建议按以下顺序阅读：

1. `PROJECT_STRUCTURE_GUIDE.md`
2. `quant_core/config.py`
3. `quant_core/predictor.py`
4. `quant_core/storage.py`
5. `quant_core/backtest.py`
6. `rebuild_historical_picks.py`
7. `quant_pushplus_tasks.py`
8. `quant_core/exit_sentinel.py`
9. `quant_core/swing_patrol.py`
10. `quant_dashboard/backend/main.py`
11. `quant_dashboard/frontend/src/App.vue`

## 17. 最小可用启动清单

```bash
cd /Users/eudis/ths

# 后端依赖
python3 -m pip install -r quant_dashboard/backend/requirements.txt

# 前端依赖
cd quant_dashboard/frontend
npm install
npm run build

# 启动服务
cd /Users/eudis/ths
./install_backend_launch_agent.sh
./install_frontend_launch_agent.sh

# 自动任务
./install_pushplus_launch_agents.sh
./install_daily_pick_launch_agents.sh
./install_market_sync_launch_agent.sh

# 验证
curl http://127.0.0.1:8000/health
curl -I http://127.0.0.1:5173/
```

完成后打开：

```text
http://127.0.0.1:5173/
```
