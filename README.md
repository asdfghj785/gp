# A 股尾盘量化工作站

这是一个面向 A 股尾盘买入、次日开盘验证的微型量化系统，包含数据同步、特征工程、XGBoost 回归预测、PushPlus 定时推送、FastAPI 后端和 Vite 前端看板。当前生产预测为“尾盘突破”和“强趋势首阴低吸”双轨策略。

## 快速启动

```bash
cd /Users/eudis/ths
cp .env.example .env
# 编辑 .env，填入 PUSHPLUS_TOKEN 等本地配置

python3 -m pip install -r quant_dashboard/backend/requirements.txt
cd quant_dashboard/frontend && npm install
```

后端：

```bash
cd /Users/eudis/ths
python3 -m uvicorn quant_dashboard.backend.main:app --host 127.0.0.1 --port 8000
```

前端：

```bash
cd /Users/eudis/ths/quant_dashboard/frontend
npm run dev
```

## 主要文档

- 技术文档：[TECHNICAL_DOC.md](TECHNICAL_DOC.md)
- 原始项目说明：[PROJECT_1.0_DOC.md](PROJECT_1.0_DOC.md)

## 注意事项

- `data/`、数据库、日志、前端构建产物和 `node_modules/` 不纳入 Git 管理。
- PushPlus token 只通过 `PUSHPLUS_TOKEN` 环境变量配置，禁止写入代码。
- 生产预测模型文件为 `overnight_premium_xgboost.json` 和 `dipbuy_premium_xgboost.json`。
