from __future__ import annotations

import html
import os
import random
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any
from urllib.parse import quote_plus

import requests


DEFAULT_TIMEOUT = float(os.getenv("AI_AGENT_NEWS_TIMEOUT", "6"))
DEFAULT_MAX_NEWS = int(os.getenv("AI_AGENT_MAX_NEWS_PER_STOCK", "5"))
FALLBACK_NEWS_TEXT = "暂无该股票今日最新重大新闻线索。"

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/124.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
]


@dataclass(frozen=True)
class NewsItem:
    title: str
    source: str = ""
    published_at: str = ""
    url: str = ""
    summary: str = ""

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


def fetch_stock_news(code: str, name: str, *, max_items: int = DEFAULT_MAX_NEWS) -> list[dict[str, str]]:
    """Fetch qualitative news snippets without using Eastmoney APIs.

    The function is deliberately defensive: every network or parsing failure returns
    a normal fallback text, never an exception object that would pollute the LLM prompt.
    """
    safe_code = str(code).strip()
    safe_name = str(name).strip() or safe_code
    candidates: list[NewsItem] = []

    for fetcher in (_fetch_baidu_results, _fetch_sina_search_results):
        try:
            candidates.extend(fetcher(safe_code, safe_name, max_items=max_items))
        except Exception:
            continue
        candidates = _dedupe(candidates)
        if candidates:
            break

    if not candidates:
        candidates = [_fallback_item()]
    return [item.to_dict() for item in _dedupe(candidates)[:max_items]]


def fetch_batch_news(stock_codes: list[str], stock_names: list[str], *, max_items: int = DEFAULT_MAX_NEWS) -> dict[str, list[dict[str, str]]]:
    output: dict[str, list[dict[str, str]]] = {}
    for code, name in zip(stock_codes, stock_names):
        safe_code = str(code).strip()
        safe_name = str(name).strip() or safe_code
        try:
            output[safe_code] = fetch_stock_news(safe_code, safe_name, max_items=max_items)
        except Exception:
            output[safe_code] = [_fallback_item().to_dict()]
    return output


def format_news_context(news_by_code: dict[str, list[dict[str, str]]]) -> str:
    blocks: list[str] = []
    for code, items in news_by_code.items():
        safe_items = items or [_fallback_item().to_dict()]
        lines = [f"{code} 新闻线索:"]
        for item in safe_items:
            title = _clean_text(item.get("title") or FALLBACK_NEWS_TEXT, max_len=120)
            source = _clean_text(item.get("source") or "local", max_len=40)
            published_at = _clean_text(item.get("published_at") or "", max_len=40)
            summary = _clean_text(item.get("summary") or "", max_len=180)
            suffix = f" {summary}" if summary and summary != title else ""
            lines.append(f"- [{source} {published_at}] {title}{suffix}".strip())
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


def _fetch_baidu_results(code: str, name: str, *, max_items: int) -> list[NewsItem]:
    query = f"{name} {code} 最新消息 股票 公告 风险"
    url = f"https://www.baidu.com/s?wd={quote_plus(query)}&rn={max_items}"
    response = _http_get(url)
    text = _decode_response(response)
    items: list[NewsItem] = []

    blocks = re.findall(r'<div[^>]+class="[^"]*(?:result|c-container)[^"]*"[^>]*>(.*?)</div>\s*</div>', text, flags=re.S)
    if not blocks:
        blocks = re.findall(r'<h3[^>]*class="[^"]*t[^"]*"[^>]*>(.*?)</h3>(.{0,1200})', text, flags=re.S)

    for block in blocks:
        if isinstance(block, tuple):
            block_html = " ".join(block)
        else:
            block_html = block
        title = _extract_first_title(block_html)
        if not title:
            continue
        summary = _extract_summary(block_html)
        href = _extract_first_href(block_html)
        item = NewsItem(
            title=title,
            source="百度搜索",
            published_at=datetime.now().strftime("%Y-%m-%d"),
            url=href,
            summary=summary,
        )
        if _is_noise_item(item):
            continue
        items.append(
            item
        )
        if len(items) >= max_items:
            break
    return _dedupe(items)


def _fetch_sina_search_results(code: str, name: str, *, max_items: int) -> list[NewsItem]:
    query = f"{name} {code} 股票 最新消息"
    url = f"https://search.sina.com.cn/?q={quote_plus(query)}&c=news&sort=time"
    response = _http_get(url)
    text = _decode_response(response)
    items: list[NewsItem] = []
    for match in re.finditer(r'<h2[^>]*>\s*<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>\s*</h2>(.{0,700})', text, flags=re.S):
        href = html.unescape(match.group(1))
        title = _clean_text(_strip_tags(match.group(2)), max_len=120)
        summary = _extract_summary(match.group(3))
        if not title:
            continue
        item = NewsItem(
            title=title,
            source="新浪搜索",
            published_at=datetime.now().strftime("%Y-%m-%d"),
            url=href,
            summary=summary,
        )
        if _is_noise_item(item):
            continue
        items.append(
            item
        )
        if len(items) >= max_items:
            break
    return _dedupe(items)


def _http_get(url: str) -> requests.Response:
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.7",
        "Cache-Control": "no-cache",
    }
    response = requests.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    return response


def _decode_response(response: requests.Response) -> str:
    if response.encoding is None or response.encoding.lower() == "iso-8859-1":
        response.encoding = response.apparent_encoding or "utf-8"
    return response.text or ""


def _extract_first_title(block_html: str) -> str:
    match = re.search(r"<a[^>]*>(.*?)</a>", block_html, flags=re.S)
    if match:
        return _clean_text(_strip_tags(match.group(1)), max_len=120)
    return _clean_text(_strip_tags(block_html), max_len=120)


def _extract_summary(block_html: str) -> str:
    text = _clean_text(_strip_tags(block_html), max_len=220)
    return text


def _extract_first_href(block_html: str) -> str:
    match = re.search(r'<a[^>]+href="([^"]+)"', block_html, flags=re.S)
    return html.unescape(match.group(1)) if match else ""


def _strip_tags(value: str) -> str:
    value = re.sub(r"<script.*?</script>", " ", value, flags=re.S | re.I)
    value = re.sub(r"<style.*?</style>", " ", value, flags=re.S | re.I)
    value = re.sub(r"<[^>]+>", " ", value)
    return html.unescape(value)


def _clean_text(value: Any, *, max_len: int) -> str:
    text = str(value or "")
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()
    text = re.sub(r"百度快照|网页链接|展开全部", "", text).strip()
    if len(text) > max_len:
        return text[: max_len - 3] + "..."
    return text


def _fallback_item() -> NewsItem:
    return NewsItem(
        title=FALLBACK_NEWS_TEXT,
        source="local",
        published_at=datetime.now().strftime("%Y-%m-%d"),
        url="",
        summary="新闻源暂不可用或未发现明确突发风险，AI 只能基于量化候选摘要做保守判断。",
    )


def _is_noise_item(item: NewsItem) -> bool:
    text = f"{item.title} {item.summary} {item.url}".lower()
    title = _clean_text(item.title, max_len=160)
    if not title or len(title) < 6:
        return True
    noise_keywords = (
        "相关搜索",
        "大家还在搜",
        "换一换",
        "百度为您找到",
        "时间不限",
        "所有网页和文件",
        "站点内检索",
        "最新相关信息",
        "cardtype",
        "logbase",
        "conttype",
        "东方财富",
        "股吧",
        "行情_走势图",
        "最新价格_行情",
    )
    if any(keyword.lower() in text for keyword in noise_keywords):
        return True
    visible_chars = re.sub(r"[\W_]+", "", title, flags=re.UNICODE)
    if len(visible_chars) < 4:
        return True
    return False


def _dedupe(items: list[NewsItem]) -> list[NewsItem]:
    seen: set[str] = set()
    out: list[NewsItem] = []
    for item in items:
        title = _clean_text(item.title, max_len=160)
        if not title:
            continue
        key = title.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out
