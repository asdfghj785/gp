import requests
from bs4 import BeautifulSoup
import json
import sys
import warnings
warnings.filterwarnings('ignore')

# 本地大模型配置
OLLAMA_API = "http://localhost:11434/api/generate"
MODEL_NAME = "qwen2.5:14b"

class StockMicroFetcher:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        }

    def _fetch_guba_tab(self, url, limit=10):
        """通用抓取引擎"""
        try:
            res = requests.get(url, headers=self.headers, timeout=10)
            res.encoding = 'utf-8'
            soup = BeautifulSoup(res.text, 'html.parser')
            titles = []
            for article in soup.find_all('div', class_='title'):
                a_tag = article.find('a')
                if a_tag and 'title' in a_tag.attrs:
                    title = a_tag['title']
                    titles.append(title)
                    if len(titles) >= limit:
                        break
            return titles if titles else ["暂无最新数据"]
        except Exception as e:
            return [f"抓取失败: {e}"]

    def get_stock_data(self, stock_code):
        # 东财股吧代码前缀
        prefix = "sh" if str(stock_code).startswith("6") else "sz"

        # 1. 抓取官方公告 (tab=2)
        ann_url = f"http://guba.eastmoney.com/list,{prefix}{stock_code},2,f.html"
        announcements = self._fetch_guba_tab(ann_url, limit=5)

        # 2. 抓取行业/个股资讯 (tab=1)
        news_url = f"http://guba.eastmoney.com/list,{prefix}{stock_code},1,f.html"
        news = self._fetch_guba_tab(news_url, limit=8)

        # 3. 抓取散户真实讨论 (去除公告和资讯的纯净版)
        guba_url = f"http://guba.eastmoney.com/list,{prefix}{stock_code}.html"
        raw_retail = self._fetch_guba_tab(guba_url, limit=20)
        retail = [t for t in raw_retail if "资讯" not in t and "公告" not in t][:10]

        return announcements, news, retail

def generate_sentiment_report(stock_code, stock_name):
    print(f"🔍 正在启动超短线个股 X 光机，扫描标的：{stock_name} ({stock_code})...")

    fetcher = StockMicroFetcher()

    print("📡 正在全网搜集该股的【官方公告】、【行业资讯】与【贴吧情绪】...")
    announcements, news, retail = fetcher.get_stock_data(stock_code)

    data_context = "【最新官方公告】:\n- " + "\n- ".join(announcements) + "\n\n"
    data_context += "【最新行业/个股资讯】:\n- " + "\n- ".join(news) + "\n\n"
    data_context += "【最新散户发帖讨论】:\n- " + "\n- ".join(retail)

    system_prompt = f"""你是一个顶级的A股短线游资操盘手。你的任务是分析【{stock_name} ({stock_code})】这只股票的微观基本面和情绪面，决定今天尾盘是否能买入，准备博弈明天的早盘溢价。

请阅读以下三大维度的实时情报：
{data_context}

【核心判断逻辑（极其重要）】：
1. 官方公告：是否有减持、退市风险、业绩暴雷？（一票否决）是否有重组、增持、中标？（加分）
2. 行业资讯：该股所处板块今天是否有明显利好驱动？
3. 散户情绪（反向指标）：如果散户极度亢奋喊涨停，通常是高位接盘信号；如果散户恐慌谩骂、绝望割肉，反而是短线博弈的绝佳安全垫。

【强制输出格式要求】（直接输出文字，不要任何多余废话）：
📝 消息面判定：(利空/中立/利好) + 核心理由总结
🔥 资金情绪判定：(一致看多危险/分歧博弈/恐慌冰点安全) + 核心理由总结

📌 核心佐证数据（必须从上述情报中，原封不动地摘录 3 条最能支撑你判定的原话，并标明来源）：
1. [来源：公告 / 资讯 / 散户] (摘录原文)
2. [来源：公告 / 资讯 / 散户] (摘录原文)
3. [来源：公告 / 资讯 / 散户] (摘录原文)

⚔️ 最终风控指令：(绿灯：允许执行买入 / 🔴 红灯：强制一票否决) + 致命逻辑
"""

    print(f"🧠 正在唤醒本地模型 {MODEL_NAME} 进行逻辑推演...\n")
    payload = {
        "model": MODEL_NAME,
        "prompt": system_prompt,
        "stream": False
    }

    try:
        response = requests.post(OLLAMA_API, json=payload, timeout=45)
        report = response.json().get("response", "")

        print("="*60)
        print(f"🎯 【{stock_name}】盘口微观情报与风控裁决书")
        print("="*60)
        print(report)
        print("="*60)

    except Exception as e:
        print(f"❌ 大模型推理异常，请检查 Ollama 是否运行: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("⚠️ 用法错误！请输入：python stock_xray.py <股票代码> <股票名称>")
        print("💡 例如：python stock_xray.py 000729 燕京啤酒")
        sys.exit(1)

    code = sys.argv[1]
    name = sys.argv[2]
    generate_sentiment_report(code, name)