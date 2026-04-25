# 离岸量化数据控制台技术文档

## 1. 项目目标

本项目是一个本地运行的 A 股微型量化工作站，核心目标是采集全市场股票数据，训练结构化模型，筛选尾盘买入、次日开盘卖出的短线候选股，并在前端展示数据质量、预测结果、策略复盘和失败归因。

当前生产策略采用高置信模式：

- 只使用 `XGBRegressor` 预测次日开盘预期溢价。
- 每日候选按 `预期溢价` 优先排序。
- 实时预测采用双轨模型：`尾盘突破` 与 `强趋势首阴低吸`。
- 默认生产门槛：`综合评分 >= 69.0`。
- 过滤创业板、北交所、科创板、ST/退市、高位爆量、尾盘诱多、近 3 日断头铡刀。
- 14:50 PushPlus 推送成功后自动锁定当日标的，前端不能手动修改。

## 2. 技术栈

### 后端

- Python 3
- FastAPI
- Uvicorn
- SQLite
- Pandas
- NumPy
- XGBoost
- Requests
- BeautifulSoup

### 前端

- Vue 3
- Vite
- 原生 CSS
- SVG K 线可视化

### 数据源

- 新浪行情接口：实时全市场快照、指数行情、指数 K 线。
- 本地 Parquet 日线文件：`/Users/eudis/ths/data/all_kline`
- 本地 SQLite 数据库：`/Users/eudis/ths/data/core_db/quant_workstation.sqlite3`

### 通知与自动化

- PushPlus 微信推送
- macOS LaunchAgent 定时任务

## 3. 重要目录与文件

```text
/Users/eudis/ths
├── quant_dashboard/backend/main.py        # FastAPI 后端入口
├── quant_dashboard/frontend/src/App.vue   # Vue 前端主界面
├── quant_core/config.py                   # 全局配置
├── quant_core/predictor.py                # 实时预测、生产过滤、评分排序
├── quant_core/daily_pick.py               # 14:50 标的锁定与次日开盘验证
├── quant_core/intraday_snapshot.py        # 14:30 快照与尾盘诱多识别
├── quant_core/storage.py                  # SQLite 存储层
├── quant_core/backtest.py                 # 生产策略复盘
├── quant_core/strategy_lab.py             # 策略实验室
├── quant_core/failure_analysis.py         # 失败归因
├── quant_core/up_reason_analysis.py       # 次日上涨原因分析
├── build_smart_overnight_dataset.py       # 构建训练集
├── quant_train_premium_models.py          # 训练 XGBRegressor
├── quant_pushplus_tasks.py                # PushPlus 心跳与 14:50 推送
├── snapshot_1430.py                       # 14:30 盘中快照脚本
├── run_snapshot_1430.sh                   # 14:30 快照执行脚本
├── run_push_heartbeat.sh                  # 9:00 心跳执行脚本
├── run_push_top_pick.sh                   # 14:50 推送执行脚本
├── launch_agents/                         # LaunchAgent 配置
├── overnight_premium_xgboost.json         # 当前生产回归模型
├── dipbuy_premium_xgboost.json            # 强趋势首阴低吸回归模型
└── TECHNICAL_DOC.md                       # 本文档
```

## 4. 核心策略逻辑

### 4.1 模型

生产模型为 `XGBRegressor`，模型文件：

```text
/Users/eudis/ths/overnight_premium_xgboost.json
/Users/eudis/ths/dipbuy_premium_xgboost.json
```

训练目标：

```text
预测 next_day_premium，即次日开盘相对当日收盘的溢价百分比。
```

训练脚本：

```bash
cd /Users/eudis/ths
python3 build_smart_overnight_dataset.py
python3 quant_train_premium_models.py
python3 quant_train_dipbuy_models.py
```

### 4.1.1 强趋势首阴低吸

低吸策略完全独立于尾盘突破模型，只有满足以下物理过滤时才调用低吸模型：

- `近5日最高涨幅 > 15.0`
- `今日急跌度 < -4.0`
- `10日均线乖离率` 在 `-3.0` 到 `+3.0` 之间
- `今日缩量比例 < 0`

低吸模型仍预测 `next_day_premium`。如果 `近3日断头铡刀标记 = 1`，训练目标上限压低到 `-1.0%`，生产预测中也会被直接剔除。

### 4.2 主要特征

基础 K 线：

- 换手率
- 量比
- 实体比例
- 上影线比例
- 下影线比例
- 日内振幅

趋势特征：

- 3 日累计涨幅
- 5 日累计涨幅
- 5 日均线乖离率
- 20 日均线乖离率
- 3 日平均换手率

量价行为：

- 5 日量能堆积
- 10 日量比
- 3 日红盘比例
- 5 日地量标记
- 缩量下跌标记

诱多/风险代理：

- 振幅换手比
- 缩量大涨标记
- 极端下影线标记
- 近 3 日断头铡刀标记
- 60 日高位比例
- 近5日最高涨幅
- 今日急跌度
- 10日均线乖离率
- 今日缩量比例

### 4.3 防大面惩罚

如果一只股票过去 3 个交易日内出现过单日跌幅 `<= -7%`，训练集中会标记：

```text
近3日断头铡刀标记 = 1
```

训练时对这类样本进行惩罚：

```text
如果原始 next_day_premium 较高，也会将训练目标上限压到 -1.0%。
```

生产过滤中直接剔除：

```text
近3日断头铡刀标记 >= 0.5
```

### 4.4 14:30 快照与尾盘诱多过滤

14:30 定时运行：

```bash
/Users/eudis/ths/run_snapshot_1430.sh
```

保存文件：

```text
/Users/eudis/ths/data/intraday/price_1430.json
```

14:50 实时预测时计算：

```text
尾盘拉升幅度 = (实时价 - 14:30 快照价) / 14:30 快照价 * 100
```

默认阈值：

```text
QUANT_LATE_PULL_TRAP_THRESHOLD_PCT=4.00
```

超过阈值后：

```text
尾盘诱多标记 = 1
```

生产池直接剔除。

## 5. 14:50 标的锁定规则

14:50 PushPlus 推送任务：

```bash
/Users/eudis/ths/run_push_top_pick.sh
```

执行逻辑：

1. 如果今日已经有锁定记录，不重新扫描，不换股，只推送已锁定股票。
2. 如果今日没有锁定记录，运行实时预测。
3. 若有候选，推送预期溢价最高的股票。
4. PushPlus 推送成功后，将同一只股票写入 `daily_picks`。
5. `daily_picks.selection_date` 唯一，冲突时 `DO NOTHING`，不覆盖修改。
6. 次日开盘后自动更新开盘价、开盘溢价和成功/失败结果。

前端只展示该记录，不提供手动修改入口。

## 6. 部署与启动

### 6.1 后端

```bash
cd /Users/eudis/ths
python3 -m uvicorn quant_dashboard.backend.main:app --host 127.0.0.1 --port 8000
```

后端地址：

```text
http://127.0.0.1:8000
```

接口文档：

```text
http://127.0.0.1:8000/docs
```

### 6.2 前端

```bash
cd /Users/eudis/ths/quant_dashboard/frontend
npm run dev -- --host 127.0.0.1 --port 5173
```

前端地址：

```text
http://127.0.0.1:5173
```

### 6.3 前端构建验证

```bash
cd /Users/eudis/ths/quant_dashboard/frontend
npm run build
```

## 7. LaunchAgent 定时任务

安装脚本：

```bash
cd /Users/eudis/ths
./install_pushplus_launch_agents.sh
```

已安装任务：

| 时间 | 任务 | 文件 |
|---|---|---|
| 09:00 | PushPlus 心跳 | `run_push_heartbeat.sh` |
| 14:30 | 保存全市场快照 | `run_snapshot_1430.sh` |
| 14:50 | 推送并锁定最高预期溢价股票 | `run_push_top_pick.sh` |
| 09:31 | 更新前一交易日锁定股票的开盘结果 | `quant_daily_pick.py update-open` |
| 15:05 | 盘后同步数据入库 | `run_market_close_sync.sh` |

## 8. 核心接口地址

### 状态

```text
GET /health
GET /api/overview
GET /api/ollama/status
```

### 预测雷达

```text
GET /api/radar/cache
GET /api/radar/scan?limit=10
POST /api/radar/analyze
```

说明：

- `/api/radar/cache` 返回最近一次预测缓存。
- `/api/radar/scan` 立即实时扫描并更新缓存。
- 高置信模式下如果无候选，前端显示“空仓避险”。

### 14:50 锁定标的

```text
GET /api/daily-picks?limit=10
POST /api/daily-picks/update-open?force=true
```

说明：

- 前端只读取 `GET /api/daily-picks`。
- `update-open` 主要用于自动任务或调试，前端不展示按钮。
- `POST /api/daily-picks/save-now` 已禁用手动保存，会返回 `409`。

### 数据

```text
POST /api/data/sync
POST /api/data/validate
GET /api/data/reports
GET /api/data/history/{code}?limit=120
GET /api/data/market-sync/latest
GET /api/data/market-sync/history
POST /api/data/market-sync/run
```

### 策略复盘

```text
GET /api/backtest/top-pick-open?months=12&refresh=false
GET /api/strategy/lab?months=12&refresh=false
GET /api/strategy/failure-analysis?months=12&refresh=false
GET /api/strategy/up-reason-analysis?months=12&refresh=false
```

前端策略卡片默认使用 12 个月数据。

说明：

- 默认 `refresh=false`，后端优先读取 `data/strategy_cache/` 中的结果缓存。
- 点击前端“刷新”按钮时会传 `refresh=true`，强制重新计算并覆盖缓存。
- 缓存签名包含数据库最大日期、K 线总行数、模型文件修改时间、月份数和 `QUANT_MIN_COMPOSITE_SCORE`，盘后同步新数据或重训模型后会自动失效。
- 复盘、策略实验、失败归因共享同一份“已评分历史候选池”缓存，避免每个卡片重复构建历史特征。

## 9. 环境变量

| 变量 | 默认值 | 说明 |
|---|---:|---|
| `QUANT_BASE_DIR` | `/Users/eudis/ths` | 项目根目录 |
| `QUANT_DATA_DIR` | `data/all_kline` | 日线 Parquet 目录 |
| `QUANT_SQLITE_PATH` | `data/core_db/quant_workstation.sqlite3` | SQLite 数据库 |
| `QUANT_PREMIUM_MODEL_PATH` | `overnight_premium_xgboost.json` | 尾盘突破 XGBRegressor 模型 |
| `QUANT_DIPBUY_PREMIUM_MODEL_PATH` | `dipbuy_premium_xgboost.json` | 首阴低吸 XGBRegressor 模型 |
| `QUANT_MIN_COMPOSITE_SCORE` | `69.00` | 高置信综合评分门槛 |
| `QUANT_PROFIT_TARGET_PCT` | `1.00` | 复盘成功阈值 |
| `QUANT_BREAKOUT_HIGH_TARGET_PCT` | `2.00` | 训练评估冲高阈值 |
| `QUANT_LATE_PULL_TRAP_THRESHOLD_PCT` | `4.00` | 14:30 后尾盘拉升拦截阈值 |
| `PUSHPLUS_TOKEN` | 本地配置值 | PushPlus 推送令牌 |
| `OLLAMA_API` | `http://127.0.0.1:11434/api/generate` | 本地大模型接口 |
| `OLLAMA_MODEL` | `qwen2.5:14b` | 本地大模型名称 |

## 10. 当前生产模式

当前默认是高置信模式：

```text
QUANT_MIN_COMPOSITE_SCORE=69.00
```

该模式特点：

- 出手频率较低。
- 无候选时前端显示“空仓避险”。
- 更重视胜率和大面规避。

如需提高资金利用率，可临时降低：

```bash
QUANT_MIN_COMPOSITE_SCORE=62 python3 -m uvicorn quant_dashboard.backend.main:app --host 127.0.0.1 --port 8000
```

## 11. 重要注意事项

1. 本项目输出只作为策略研究与数据观察，不应直接作为交易建议。
2. 高置信模式下空仓是正常结果，不是接口错误。
3. 14:50 锁定标的一旦写入，不会被后续扫描覆盖。
4. 如果 14:30 快照缺失，尾盘诱多过滤会降级为未启用，但其他生产过滤仍有效。
5. 如果 Ollama 不可用，舆情风控会自动红灯否决，不影响结构化模型扫描。
6. 修改模型特征后，必须重新运行训练集构建和模型训练。
7. 策略复盘排除周末和非完整交易日，停盘前最后一个交易日会对比停盘后第一个交易日开盘价。
