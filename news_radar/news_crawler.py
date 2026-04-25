import os
import sqlite3
import requests
import time
from datetime import datetime
import json
import re
from bs4 import BeautifulSoup
import warnings
warnings.filterwarnings('ignore')

# 锁定你的专属数据库目录
DB_PATH = "/Users/eudis/ths/news_radar/flash_news.db"

def init_db():
    """初始化雷达数据库：自带排重机制与情感打分预留字段"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS news_pool (
            news_id TEXT PRIMARY KEY,
            source TEXT,
            publish_time TEXT,
            content TEXT,
            sentiment_score REAL DEFAULT 0,
            is_processed INTEGER DEFAULT 0
        )
    ''')
    conn.commit()
    return conn

class BaseCrawler:
    """爬虫基类：所有消息源都必须遵守这个标准"""
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        }

    def fetch(self):
        """核心抓取逻辑，由子类实现"""
        pass

class SinaCrawler(BaseCrawler):
    """新浪财经 7x24 小时：全球宏观、突发事件、地缘政治（极其稳定无拦截）"""
    def fetch(self):
        url = "https://zhibo.sina.com.cn/api/zhibo/feed?page=1&page_size=15&zhibo_id=152"
        try:
            res = requests.get(url, headers=self.headers, timeout=10)
            data = res.json()
            items = []

            for msg in data.get('result', {}).get('data', {}).get('feed', {}).get('list', []):
                news_id = f"sina_{msg.get('id')}"
                content = msg.get('rich_text', '')
                pub_time = msg.get('create_time', '')

                if content:
                    items.append((news_id, '新浪7x24', pub_time, content))
            return items
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 新浪抓取异常: {e}")
            return []

class CailianCrawler(BaseCrawler):
    """财联社：A股盘面异动、国内产业政策直击"""
    def fetch(self):
        url = "https://www.cls.cn/nodeapi/telegraphList?rn=15"
        try:
            res = requests.get(url, headers=self.headers, timeout=10)
            data = res.json()
            items = []
            for item in data.get('data', {}).get('roll_data', []):
                news_id = f"cls_{item.get('id')}"
                content = item.get('content', '') or item.get('title', '')
                ts = item.get('ctime')
                pub_time = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S') if ts else ''

                if content:
                    items.append((news_id, '财联社', pub_time, content))
            return items
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 财联社抓取异常: {e}")
            return []

class TelegramJin10Crawler(BaseCrawler):
    """金十数据 Telegram 网页版：完美的 API 替代方案"""
    def fetch(self):
        url = "https://t.me/s/jin10data"
        try:
            # 强制不使用代理，确保直连速度
            proxies = {"http": None, "https": None}
            res = requests.get(url, headers=self.headers, proxies=proxies, timeout=15)
            soup = BeautifulSoup(res.text, 'html.parser')

            messages = soup.find_all('div', class_='tgme_widget_message_wrap')
            items = []

            for msg in messages:
                msg_id_tag = msg.find('div', class_='tgme_widget_message')
                if not msg_id_tag or 'data-post' not in msg_id_tag.attrs:
                    continue
                news_id = f"tg_jin10_{msg_id_tag.attrs['data-post'].replace('/', '_')}"

                content_tag = msg.find('div', class_='tgme_widget_message_text')
                if not content_tag:
                    continue
                content = content_tag.get_text(separator=' ').strip()

                time_tag = msg.find('time', class_='time')
                pub_time = time_tag.attrs['datetime'] if time_tag else datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                if len(content) > 10:
                    content = re.sub(r'\s+', ' ', content)
                    items.append((news_id, '金十电报', pub_time, content))

            return items[::-1]
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 金十电报抓取异常: {e}")
            return []

def run_radar():
    print("📡 战略雷达启动！正在 24 小时监听全球市场异动...")
    conn = init_db()
    cursor = conn.cursor()

    # 挂载了新浪、财联社和金十电报三大巨头
    crawlers = [SinaCrawler(), CailianCrawler(), TelegramJin10Crawler()]

    while True:
        new_count = 0
        for crawler in crawlers:
            items = crawler.fetch()
            for item in items:
                try:
                    cursor.execute('''
                        INSERT OR IGNORE INTO news_pool (news_id, source, publish_time, content)
                        VALUES (?, ?, ?, ?)
                    ''', item)
                    if cursor.rowcount > 0:
                        new_count += 1
                        print(f"\n[{item[1]}] {item[2]}\n👉 {item[3][:80]}...")
                except sqlite3.Error as e:
                    print(f"数据库写入错误: {e}")

        conn.commit()

        if new_count > 0:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 雷达本轮新增 {new_count} 条快讯入库。")

        time.sleep(60)

if __name__ == "__main__":
    run_radar()