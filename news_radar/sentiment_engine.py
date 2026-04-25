import sqlite3
import requests
import json
import time
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# 锁定你的专属数据库目录
DB_PATH = "/Users/eudis/ths/news_radar/flash_news.db"
# Ollama 本地 API 地址
OLLAMA_API = "http://localhost:11434/api/generate"
# 你本地的模型名称
MODEL_NAME = "qwen2.5:14b"

def analyze_news_with_ollama(news_content):
    """调用本地 Qwen 模型，对新闻进行金融情感分析提取"""

    system_prompt = """你是一个顶级的华尔街宏观与地缘政治量化分析师。
你的任务是阅读一条极短的快讯，并迅速判断它对金融市场的影响。
请严格根据以下规则打分（范围 -100 到 100）：
- 负数：利空大盘、引发恐慌（如战争升级、制裁、经济衰退、加息预期）。数值越小越恐慌。
- 正数：利好大盘、情绪高涨（如停火、经济刺激、降息预期、重大利好）。数值越大越狂热。
- 0附近：中性消息或无关紧要。

关联资产识别：请识别这条消息最直接冲击的资产种类（如：A股, 黄金, 原油, 军工板块, 半导体等）。

【强制要求】：你只能返回一个标准的 JSON 格式，绝不允许输出任何其他解释性废话！格式如下：
{
    "score": 80,
    "assets": ["A股", "半导体"],
    "reason": "简短的一句话逻辑解释"
}
"""

    payload = {
        "model": MODEL_NAME,
        "prompt": f"{system_prompt}\n\n待分析快讯：\n{news_content}",
        "stream": False,
        "format": "json"
    }

    try:
        response = requests.post(OLLAMA_API, json=payload, timeout=30)
        result_text = response.json().get("response", "")
        return json.loads(result_text)
    except Exception as e:
        print(f"❌ 模型推理失败: {e}")
        return None

def run_sentiment_engine():
    print(f"🧠 AI 情感计算引擎启动！正在连接本地模型 {MODEL_NAME}...")

    while True:
        try:
            # 第一步：【极速读取】打开门，拿出没处理的新闻，立刻关门！
            # timeout=20 表示如果爬虫正在写，就耐心等 20 秒，别直接报错
            conn = sqlite3.connect(DB_PATH, timeout=20)
            cursor = conn.cursor()
            cursor.execute("SELECT news_id, publish_time, content FROM news_pool WHERE is_processed = 0 ORDER BY publish_time ASC")
            unprocessed_news = cursor.fetchall()
            conn.close() # 核心：立刻释放数据库锁！

            if unprocessed_news:
                print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 发现 {len(unprocessed_news)} 条新快讯，正在投喂给 AI...")

                for news in unprocessed_news:
                    news_id, pub_time, content = news
                    print(f"➡️ 正在思考: {content[:40]}...")

                    # 第二步：【耗时操作】让 AI 思考，此时数据库大门是敞开的，爬虫可以随便写
                    ai_result = analyze_news_with_ollama(content)

                    if ai_result:
                        score = ai_result.get("score", 0)
                        assets = ", ".join(ai_result.get("assets", []))
                        reason = ai_result.get("reason", "")

                        color = "🔴 利空" if score < -20 else "🟢 利好" if score > 20 else "⚪️ 中性"
                        print(f"   {color} | 恐慌指数: {score} | 关联资产: {assets} | 逻辑: {reason}")

                        # 第三步：【极速写入】AI算完了，再次瞬间开门写进去，然后立刻关门
                        conn_update = sqlite3.connect(DB_PATH, timeout=20)
                        cursor_update = conn_update.cursor()
                        cursor_update.execute('''
                            UPDATE news_pool
                            SET sentiment_score = ?, is_processed = 1
                            WHERE news_id = ?
                        ''', (score, news_id))
                        conn_update.commit()
                        conn_update.close() # 再次释放锁

        except sqlite3.OperationalError as e:
            print(f"⚠️ 数据库轻微拥堵，正在重试: {e}")

        time.sleep(10)

if __name__ == "__main__":
    run_sentiment_engine()