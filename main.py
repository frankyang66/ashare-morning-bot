import base64
import hashlib
import html
import hmac
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import feedparser
import requests

SH_TZ = timezone(timedelta(hours=8))
USER_AGENT = "ashare-morning-bot/1.0"


@dataclass
class NewsItem:
    title: str
    link: str
    source: str
    published: str
    published_dt: datetime
    summary: str
    excerpt: str


def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def read_feeds(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"RSS list not found: {path}")
    feeds = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        feeds.append(line)
    if not feeds:
        raise ValueError("No RSS feeds found in rss_feeds.txt")
    return feeds


def parse_entry_time(entry) -> datetime:
    raw = entry.get("published") or entry.get("updated") or entry.get("pubDate")
    if not raw:
        return datetime.now(SH_TZ)
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(SH_TZ)
    except Exception:
        return datetime.now(SH_TZ)


def normalize_url(url: str) -> str:
    try:
        p = urlparse(url)
        q = [
            (k, v)
            for k, v in parse_qsl(p.query, keep_blank_values=True)
            if not k.lower().startswith("utm_") and k.lower() not in {"spm", "from"}
        ]
        return urlunparse(p._replace(query=urlencode(q), fragment=""))
    except Exception:
        return url


def normalize_title(title: str) -> str:
    title = title.lower().strip()
    title = re.sub(r"\s+", "", title)
    title = re.sub(r"[|｜\-—_·•,:：;；!！?？\"'“”‘’\[\]【】()（）]", "", title)
    return title


def similar(a: str, b: str, threshold: float) -> bool:
    return SequenceMatcher(None, a, b).ratio() >= threshold


def is_ashare_related(text: str) -> bool:
    keys = [
        "a股",
        "沪指",
        "深成指",
        "创业板",
        "科创板",
        "上证",
        "深证",
        "北交所",
        "证监会",
        "涨停",
        "跌停",
        "业绩",
        "公告",
        "同花顺",
        "财新",
        "第一财经",
        "一财",
    ]
    t = text.lower()
    return any(k in t for k in keys)


def clean_excerpt(text: str) -> str:
    text = html.unescape(text or "")
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"图：[^。；;]*", "", text)
    text = re.sub(r"【[^】]*】", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_paragraph_length(text: str, min_len: int = 120, max_len: int = 220) -> str:
    text = clean_excerpt(text)
    if len(text) <= max_len:
        return text
    split_points = [m.start() + 1 for m in re.finditer(r"[。！？；;]", text)]
    for point in split_points:
        if min_len <= point <= max_len:
            return text[:point].strip()
    return text[:max_len].rstrip("，,、 ") + "。"


def extract_market_review_paragraph(items: List[NewsItem]) -> str:
    market_keywords = [
        "沪指", "深成指", "创业板指", "全A上涨", "成交额", "A股市场", "三大指数", "行业板块",
        "市场风格", "主题指数", "上涨个股", "下跌个股",
    ]
    candidates = []
    for item in items:
        base_text = clean_excerpt(f"{item.title}。{item.excerpt}")
        text = normalize_paragraph_length(base_text, min_len=100, max_len=320)
        score = sum(1 for keyword in market_keywords if keyword in text)
        if score >= 4:
            candidates.append((score, item.published_dt, text))
    if not candidates:
        return ""
    candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return candidates[0][2]


def extract_basis_review_paragraph(items: List[NewsItem]) -> str:
    basis_keywords = ["基差", "升水", "贴水", "IF", "IC", "IM", "IH", "股指期货", "近月", "远月"]
    candidates = []
    for item in items:
        base_text = clean_excerpt(f"{item.title}。{item.excerpt}")
        text = normalize_paragraph_length(base_text, min_len=80, max_len=240)
        score = sum(1 for keyword in basis_keywords if keyword.lower() in text.lower())
        if score >= 3:
            candidates.append((score, item.published_dt, text))
    if not candidates:
        return ""
    candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return candidates[0][2]


def is_market_snapshot_item(item: NewsItem) -> bool:
    text = f"{item.title}{item.excerpt}"
    market_keywords = ["沪指", "深成指", "创业板指", "成交额", "全A上涨", "板块", "主题指数", "市场风格"]
    return sum(1 for keyword in market_keywords if keyword in text) >= 4


def is_basis_item(item: NewsItem) -> bool:
    text = f"{item.title}{item.excerpt}".lower()
    basis_keywords = ["基差", "升水", "贴水", "if", "ic", "im", "ih", "股指期货", "近月", "远月"]
    return sum(1 for keyword in basis_keywords if keyword in text) >= 3


def build_source_candidates(items: List[NewsItem]) -> List[Dict[str, str]]:
    candidates = []
    for item in items:
        if len(item.excerpt) < 100 or is_low_signal_item(item) or is_market_snapshot_item(item) or is_basis_item(item):
            continue
        candidates.append(
            {
                "title": item.title,
                "excerpt": normalize_paragraph_length(item.excerpt, min_len=120, max_len=220),
                "published": item.published,
                "link": item.link,
            }
        )
    return candidates[:20]


def build_market_snapshot_candidates(items: List[NewsItem]) -> List[str]:
    keywords = ["沪指", "深成指", "创业板指", "成交额", "三大指数", "板块", "领涨", "市场风格"]
    results: List[str] = []
    for item in items:
        combined = clean_excerpt(f"{item.title}。{item.excerpt}")
        if sum(1 for keyword in keywords if keyword in combined) >= 2:
            results.append(normalize_paragraph_length(combined, min_len=40, max_len=120))
    return results[:6]


def is_low_signal_item(item: NewsItem) -> bool:
    title = item.title.lower()
    noise_keywords = [
        "成交额", "高开", "低开", "短线拉升", "直线涨停", "涨幅", "跌幅", "快讯", "#",
        "此时缩量", "开盘", "盘初", "午评", "收评", "视频", "直播",
    ]
    return any(keyword in title for keyword in noise_keywords)


def fetch_feed(url: str, timeout_sec: int) -> Tuple[str, feedparser.FeedParserDict]:
    try:
        r = requests.get(url, timeout=timeout_sec, headers={"User-Agent": USER_AGENT})
        r.raise_for_status()
        return url, feedparser.parse(r.content)
    except Exception:
        return url, feedparser.FeedParserDict(entries=[], feed={"title": url})


def collect_news(feeds: List[str], lookback_hours: int, max_items: int) -> List[NewsItem]:
    timeout_sec = int(os.getenv("RSS_FETCH_TIMEOUT", "12"))
    workers = int(os.getenv("RSS_FETCH_WORKERS", "8"))
    title_similarity = float(os.getenv("TITLE_SIMILARITY_THRESHOLD", "0.9"))
    cutoff = datetime.now(SH_TZ) - timedelta(hours=lookback_hours)

    fetched = []
    with ThreadPoolExecutor(max_workers=max(workers, 1)) as pool:
        futures = [pool.submit(fetch_feed, u, timeout_sec) for u in feeds]
        for f in as_completed(futures):
            fetched.append(f.result())

    items: List[NewsItem] = []
    seen_links = set()
    seen_titles: List[str] = []

    for url, parsed in fetched:
        source = parsed.feed.get("title", url)
        for entry in parsed.entries:
            title = (entry.get("title") or "").strip()
            link = normalize_url((entry.get("link") or "").strip())
            summary = (entry.get("summary") or entry.get("description") or "").strip()
            excerpt = clean_excerpt(summary)
            if not title or not link:
                continue

            if link in seen_links:
                continue
            norm = normalize_title(title)
            if any(similar(norm, x, title_similarity) for x in seen_titles):
                continue

            pub_dt = parse_entry_time(entry)
            if pub_dt < cutoff:
                continue
            if not is_ashare_related(f"{title}\n{summary}"):
                continue

            seen_links.add(link)
            seen_titles.append(norm)
            items.append(
                NewsItem(
                    title=title,
                    link=link,
                    source=source,
                    published=pub_dt.strftime("%Y-%m-%d %H:%M"),
                    published_dt=pub_dt,
                    summary=summary[:400],
                    excerpt=excerpt,
                )
            )

    items.sort(key=lambda x: x.published_dt, reverse=True)
    return items[:max_items]


def extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    text = text.strip()
    try:
        return json.loads(text)
    except Exception:
        pass
    s, e = text.find("{"), text.rfind("}")
    if s >= 0 and e > s:
        try:
            return json.loads(text[s : e + 1])
        except Exception:
            return None
    return None


def fallback_docx_style(items: List[NewsItem], title: str) -> Dict[str, Any]:
    eligible_items = [
        item
        for item in items
        if len(item.excerpt) >= 100 and not is_low_signal_item(item) and not is_market_snapshot_item(item) and not is_basis_item(item)
    ]
    market_review = extract_market_review_paragraph(items)
    basis_review = extract_basis_review_paragraph(items)
    points = []
    for item in eligible_items[:8]:
        excerpt = normalize_paragraph_length(item.excerpt, min_len=120, max_len=220)
        points.append(
            {
                "title": item.title,
                "excerpt": excerpt,
                "analysis": f"{excerpt} 这条信息对市场的影响更偏向预期修正与结构演绎，短期可继续观察消息催化是否扩散到相关板块和龙头品种，中期仍需结合政策兑现、行业景气和基本面变化判断持续性。",
            }
        )
    while len(points) < 8:
        points.append(
            {
                "title": "暂无补充要点",
                "excerpt": "暂无符合100字以上原文段落的资讯。",
                "analysis": "建议继续跟踪政策、产业和资金面的新增变化。",
            }
        )
    return {
        "doc_title": title,
        "market_review": market_review,
        "basis_review": basis_review,
        "key_points": points,
        "strategy": "建议围绕市场要点中反复出现的政策催化、产业趋势和外部扰动进行归纳，保持均衡配置，优先关注逻辑清晰、业绩确定性较高的方向，避免对单一题材做过度追涨。",
    }


def llm_docx_style(items: List[NewsItem], allow_fallback: bool = True) -> Dict[str, Any]:
    now = datetime.now(SH_TZ)
    title = f"{now.month}月{now.day}日行情复盘与重点信息梳理"
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        print("LLM disabled: OPENAI_API_KEY is empty, using fallback content.")
        return fallback_docx_style(items, title)

    model = os.getenv("OPENAI_MODEL", "deepseek-chat")
    base_url = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
    endpoint = f"{base_url}/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    market_review = extract_market_review_paragraph(items)
    basis_review = extract_basis_review_paragraph(items)
    market_snapshot_candidates = build_market_snapshot_candidates(items)
    source_items = build_source_candidates(items)
    if not source_items:
        msg = "LLM input error: no qualified source_items after filtering."
        if allow_fallback:
            print(msg + " Falling back to template content.")
            return fallback_docx_style(items, title)
        raise RuntimeError(msg)

    prompt = (
        "输出严格 JSON，不要额外文字。字段如下："
        "{doc_title, market_review, basis_review, key_points, strategy}。"
        "其中 key_points 必须 8 条，每条包含 title/excerpt/analysis。"
        "使用中文，风格参考历史A股晨报，但不要编造未给事实。"
        "market_review 必须优先使用我提供的A股行情段，保持其原意与结构，写成一整段。"
        "如果A股行情段为空，但我提供了若干市场线索，请你仅基于这些线索合成一段固定风格的行情复盘，顺序必须是：指数与点位/涨跌幅 -> 个股涨跌家数与成交额 -> 行业板块与风格变化 -> 简短总结。"
        "如果线索仍不足以支撑完整表述，再返回空字符串。"
        "basis_review 必须优先使用我提供的基差段，保持其原意与结构，写成一整段；如果我提供了空字符串，就返回空字符串。"
        "key_points 的要求："
        "1. 先从候选新闻中自行挑选最适合写入晨报的8条，不要机械按时间顺序挑选。"
        "2. 8条要尽量形成均衡结构，优先覆盖海外宏观或地缘、国内宏观政策或制度、国内行业主题、国内企业动态。"
        "3. 这是通用编排要求，不要依赖某几个特定事件词或固定关键词来判断题材。"
        "4. title 是你总结后的要点标题，不要带序号，不能泛泛而谈，要写出主体+事件+影响，建议控制在14到26字。"
        "5. excerpt 必须直接使用输入里的原文段落或在不改变原意前提下做极轻微压缩，长度尽量控制在120到220字。"
        "6. analysis 必须写成完整的一段点评，长度尽量与 excerpt 接近，建议控制在100到220字。"
        "7. analysis 要基于 excerpt 做延展总结，语气保持研判式，但不要写得过度绝对，也不要空泛套话。"
        "8. 不要在 title、excerpt、analysis 中出现日期、时间、媒体来源、链接等元数据。"
        "strategy 需要基于 key_points 做综合建议，但表达保持泛化，不要写得过细。"
        f"doc_title 使用：{title}。"
    )
    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": "你是严谨的A股复盘编辑。"},
            {
                "role": "user",
                "content": prompt
                + "\nA股行情段：\n"
                + json.dumps(market_review, ensure_ascii=False)
                + "\nA股市场线索：\n"
                + json.dumps(market_snapshot_candidates, ensure_ascii=False)
                + "\n基差段：\n"
                + json.dumps(basis_review, ensure_ascii=False)
                + "\n候选新闻：\n"
                + json.dumps(source_items, ensure_ascii=False),
            },
        ],
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
    }
    try:
        r = requests.post(endpoint, headers=headers, json=body, timeout=90)
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"]
        data = extract_json_object(content)
        if not data:
            msg = "LLM parse error: response is not a valid JSON object."
            if allow_fallback:
                print(msg + " Falling back to template content.")
                return fallback_docx_style(items, title)
            raise RuntimeError(msg)
        key_points = data.get("key_points")
        if not isinstance(key_points, list):
            key_points = []
        normalized_points = []
        for p in key_points[:8]:
            title_text = str(p.get("title", "")).strip()
            excerpt_text = normalize_paragraph_length(str(p.get("excerpt", "")).strip(), min_len=100, max_len=240)
            analysis_text = normalize_paragraph_length(str(p.get("analysis", "")).strip(), min_len=100, max_len=240)
            if not title_text or len(excerpt_text) < 100 or len(analysis_text) < 80:
                continue
            normalized_points.append(
                {
                    "title": title_text,
                    "excerpt": excerpt_text,
                    "analysis": analysis_text or "建议继续跟踪其对市场风格和板块轮动的影响。",
                }
            )
        key_points = normalized_points
        if not clean_excerpt(str(data.get("market_review") or market_review or "")) and not key_points:
            msg = "LLM validation error: market_review is empty and no valid key_points were generated."
            if allow_fallback:
                print(msg + " Falling back to template content.")
                return fallback_docx_style(items, title)
            raise RuntimeError(msg)
        while len(key_points) < 8:
            key_points.append(
                {
                    "title": "暂无补充要点",
                    "excerpt": "暂无符合100字以上原文段落的资讯。",
                    "analysis": "建议继续跟踪政策、产业和资金面的新增变化。",
                }
            )
        return {
            "doc_title": data.get("doc_title") or title,
            "market_review": clean_excerpt(str(data.get("market_review") or market_review or "")),
            "basis_review": clean_excerpt(str(data.get("basis_review") or basis_review or "")),
            "key_points": key_points,
            "strategy": data.get("strategy") or "建议围绕政策催化、产业景气和风险偏好变化保持均衡配置，优先关注确定性更高的方向。",
        }
    except Exception as exc:
        msg = f"LLM request failed: {exc}"
        if allow_fallback:
            print(msg + " Falling back to template content.")
            return fallback_docx_style(items, title)
        raise RuntimeError(msg) from exc


def render_doc_text(doc: Dict[str, Any], items: List[NewsItem]) -> str:
    lines = [
        f'<div align="center"><strong>{doc["doc_title"]}</strong></div>',
        "",
        "一. 行情复盘",
        doc["market_review"],
    ]
    if doc.get("basis_review", "").strip():
        lines.append(doc["basis_review"])
    lines.extend(["", "二. 市场要点"])
    for idx, p in enumerate(doc["key_points"][:8], start=1):
        lines.append(f"{idx}. {str(p.get('title', '')).strip()}")
        lines.append(str(p.get("excerpt", "")).strip())
        lines.append(str(p.get("analysis", "")))
    lines.extend(["", "三. 建议", doc["strategy"], "", "原文链接（前15条）："])
    for item in items[:15]:
        lines.append(f"- {item.title} | {item.link}")
    return "\n".join(lines)


def build_sign(secret: str, timestamp: str) -> str:
    s = f"{timestamp}\n{secret}"
    h = hmac.new(s.encode("utf-8"), digestmod=hashlib.sha256).digest()
    return base64.b64encode(h).decode("utf-8")


def build_flow_payload(items: List[NewsItem], text: str, doc: Dict[str, Any]) -> Dict[str, Any]:
    highlights = [p.get("title", "") for p in doc.get("key_points", [])[:3]]
    while len(highlights) < 3:
        highlights.append("暂无")
    return {
        "title": doc.get("doc_title", "A股行情复盘与重点信息梳理"),
        "report_date": datetime.now(SH_TZ).strftime("%Y-%m-%d"),
        "summary": doc.get("market_review", ""),
        "highlights": highlights,
        "watchlist": ["市场情绪与量能变化", "主线板块持续性", "重点公司公告与业绩"],
        "raw_markdown": text,
        "doc_title": doc.get("doc_title", ""),
        "market_review": doc.get("market_review", ""),
        "basis_review": doc.get("basis_review", ""),
        "key_points": doc.get("key_points", []),
        "strategy": doc.get("strategy", ""),
        "msg_type": "text",
        "content": {"text": text},
    }


def send_to_feishu(webhook_url: str, text: str, secret: Optional[str], payload: Dict[str, Any]) -> None:
    if "flow/api/trigger-webhook" in webhook_url:
        r = requests.post(webhook_url, json=payload, timeout=20)
        r.raise_for_status()
        return
    bot = {"msg_type": "text", "content": {"text": text[:28000]}}
    if secret:
        ts = str(int(time.time()))
        bot["timestamp"] = ts
        bot["sign"] = build_sign(secret, ts)
    r = requests.post(webhook_url, json=bot, timeout=20)
    r.raise_for_status()
    data = r.json()
    if data.get("code", 0) != 0:
        raise RuntimeError(f"Feishu webhook error: {data}")


def main() -> None:
    project_dir = Path(__file__).resolve().parent
    load_env_file(project_dir / ".env")

    webhook = os.getenv("FEISHU_WEBHOOK_URL", "").strip()
    if not webhook:
        raise ValueError("Missing FEISHU_WEBHOOK_URL")
    secret = os.getenv("FEISHU_BOT_SECRET", "").strip() or None
    lookback = int(os.getenv("LOOKBACK_HOURS", "24"))
    max_items = int(os.getenv("MAX_NEWS_ITEMS", "30"))
    strict_llm = os.getenv("STRICT_LLM", "1").strip().lower() not in {"0", "false", "no"}

    feeds = read_feeds(project_dir / "rss_feeds.txt")
    items = collect_news(feeds, lookback, max_items)
    doc = llm_docx_style(items, allow_fallback=not strict_llm)
    text = render_doc_text(doc, items)

    report_path = project_dir / f"report_{datetime.now(SH_TZ).strftime('%Y-%m-%d')}.md"
    report_path.write_text(text, encoding="utf-8")
    print(f"Local report saved: {report_path}")

    payload = build_flow_payload(items, text, doc)
    try:
        send_to_feishu(webhook, text, secret, payload)
        print("Morning report sent to Feishu.")
    except Exception as exc:
        print(f"Feishu send failed: {exc}")


if __name__ == "__main__":
    main()
