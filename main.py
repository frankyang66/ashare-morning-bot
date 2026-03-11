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
CATEGORY_ORDER = ["国内宏观政策", "国内行业", "国内企业", "海外宏观"]
CATEGORY_QUOTA = 2


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
    for line in path.read_text(encoding="utf-8-sig").splitlines():
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


def is_economic_relevant(item: NewsItem) -> bool:
    text = f"{item.title} {item.excerpt} {item.source} {item.link}".lower()

    strong_econ_keys = [
        "财政", "货币", "利率", "降息", "加息", "通胀", "cpi", "ppi", "汇率", "债券",
        "国债", "专项债", "预算", "赤字", "地方债", "化债", "gdp", "经济增长", "宏观",
        "央行", "美联储", "opec", "wti", "brent", "油价", "关税", "出口", "进口",
        "政策", "监管", "证监会", "发改", "工信", "财政部", "国务院",
    ]
    econ_section_keys = [
        "/finance", "/stock", "/regulation", "/economy", "/macro", "/opinion", "/database",
        "topics.caixin.com", "finance.caixin.com", "opinion.caixin.com",
    ]
    company_finance_keys = [
        "公司", "企业", "财报", "年报", "季报", "净利润", "营收", "订单", "增资", "并购",
        "ipo", "回购", "分红", "revenue", "earnings",
    ]
    market_keys = [
        "a股", "沪指", "深成指", "创业板", "板块", "行业", "产业", "赛道", "公司",
        "财报", "年报", "业绩", "净利润", "营收", "估值", "市值", "增资", "并购",
        "新能源", "半导体", "储能", "通信", "光伏", "算力", "电池", "黄金", "有色",
    ]
    non_econ_keys = [
        "社论", "剧", "影片", "电影", "综艺", "娱乐", "体育", "性侵", "法庭",
        "婚姻", "校园", "刑事", "小说", "艺术", "展览", "主播说", "一探", "快评",
        "信用卡先被刷爆", "还没养熟", "渐冻人", "同情用药", "医患纠纷",
    ]
    if any(k in text for k in non_econ_keys):
        return False
    if "龙虾" in text and not any(k in text for k in ["ai", "算力", "模型", "大模型", "智能体"]):
        return False

    score = 0
    strong_hits = sum(1 for k in strong_econ_keys if k in text)
    section_hits = sum(1 for k in econ_section_keys if k in text)
    company_hits = sum(1 for k in company_finance_keys if k in text)
    market_hits = sum(1 for k in market_keys if k in text)
    score += strong_hits * 2
    score += section_hits * 2
    score += company_hits
    score += market_hits
    # Avoid weakly related narrative pieces: require strong economic/section signals.
    return score >= 4 and (strong_hits > 0 or section_hits > 0 or company_hits >= 1 or market_hits >= 2)


def economic_score(item: NewsItem) -> int:
    text = f"{item.title} {item.excerpt} {item.source} {item.link}".lower()
    keys = [
        "财政", "货币", "利率", "降息", "加息", "通胀", "汇率", "债券", "国债", "专项债",
        "预算", "化债", "央行", "美联储", "油价", "出口", "进口", "政策", "监管",
        "a股", "沪指", "深成指", "创业板", "财报", "业绩", "净利润", "营收", "估值", "市值",
        "行业", "产业", "新能源", "半导体", "储能", "光伏", "算力",
    ]
    section_keys = ["/finance", "/stock", "/regulation", "/economy", "finance.caixin.com", "topics.caixin.com"]
    return sum(1 for k in keys if k in text) + 2 * sum(1 for k in section_keys if k in text)


def build_source_candidates(items: List[NewsItem]) -> List[Dict[str, str]]:
    def make_entry(item: NewsItem) -> Dict[str, str]:
        excerpt = normalize_paragraph_length(item.excerpt, min_len=120, max_len=220)
        return {
            "title": item.title,
            "excerpt": excerpt,
            "published": item.published,
            "link": item.link,
            "econ_score": economic_score(item),
            "category": categorize_candidate(item.title, excerpt),
        }

    candidates: List[Dict[str, str]] = []
    seen_links = set()

    # Pass 1: strict filtering
    for item in items:
        if (
            len(item.excerpt) < 100
            or is_low_signal_item(item)
            or is_market_snapshot_item(item)
            or is_basis_item(item)
            or not is_economic_relevant(item)
        ):
            continue
        if item.link in seen_links:
            continue
        candidates.append(make_entry(item))
        seen_links.add(item.link)
        if len(candidates) >= 20:
            return candidates

    # Pass 2: relax market snapshot exclusion if not enough items
    if len(candidates) < 8:
        for item in items:
            if (
                len(item.excerpt) < 100
                or is_low_signal_item(item)
                or is_basis_item(item)
                or not is_economic_relevant(item)
            ):
                continue
            if item.link in seen_links:
                continue
            candidates.append(make_entry(item))
            seen_links.add(item.link)
            if len(candidates) >= 20:
                return candidates

    # Pass 3: relax length threshold as last resort to avoid大量“暂无补充要点”
    if len(candidates) < 8:
        for item in items:
            if (
                len(item.excerpt) < 80
                or is_low_signal_item(item)
                or is_basis_item(item)
                or not is_economic_relevant(item)
            ):
                continue
            if item.link in seen_links:
                continue
            candidates.append(make_entry(item))
            seen_links.add(item.link)
            if len(candidates) >= 20:
                return candidates

    # Pass 4: minimal viable fallback to keep pipeline running on thin-news days
    if len(candidates) < 4:
        for item in items:
            if len(item.excerpt) < 70 or is_basis_item(item):
                continue
            if item.link in seen_links:
                continue
            candidates.append(make_entry(item))
            seen_links.add(item.link)
            if len(candidates) >= 20:
                return candidates

    candidates.sort(key=lambda x: x.get("econ_score", 0), reverse=True)
    return candidates[:20]


def categorize_candidate(title: str, excerpt: str) -> str:
    text = f"{title} {excerpt}".lower()

    macro_keys = [
        "国务院", "财政", "央行", "证监会", "政策", "预算", "专项债", "监管",
        "发改", "部委", "货币", "利率", "化债", "赤字", "地方债", "宏观", "两会",
        "人大", "政协", "工作报告", "国债", "财政经济委员会",
    ]
    industry_keys = [
        "板块", "行业", "产业", "赛道", "概念", "需求", "装机", "产能",
        "储能", "半导体", "光伏", "医疗器械", "机器人", "通信", "ai", "算力",
        "高端制造", "有色", "化工", "电池", "芯片",
    ]
    company_keys = [
        "股份", "集团", "公司", "财报", "年报", "净利润", "业绩", "公告", "增资", "收购",
        "合作", "发布", "推出", "股价", "总市值", "ipo", "分红", "私有化",
    ]
    overseas_keys = [
        "美国", "欧盟", "日本", "韩国", "中东", "伊朗", "俄", "乌克兰", "欧洲", "海外",
        "fed", "federal reserve", "美联储", "world bank", "imf", "opec", "brent", "wti",
        "特朗普", "关税", "停战", "能源危机",
    ]
    overseas_policy_keys = ["美国会", "美国国会", "白宫", "制裁", "停战", "地缘", "冲突", "军"]
    domestic_marker_keys = ["a股", "沪", "深", "上交所", "深交所", "北交所", "港股", "国内", "中国", "央企"]

    # negative hints to prevent category drift
    non_industry_hints = ["国债", "预算", "财政", "央行", "政策", "人大", "政协", "两会"]
    non_company_hints = ["代表团", "会议", "报告审议", "草案", "国常会"]

    scores = {
        "国内宏观政策": sum(1 for k in macro_keys if k in text),
        "国内行业": sum(1 for k in industry_keys if k in text),
        "国内企业": sum(1 for k in company_keys if k in text),
        "海外宏观": sum(1 for k in overseas_keys if k in text),
    }
    overseas_hits = scores["海外宏观"] + sum(1 for k in overseas_policy_keys if k in text)
    domestic_hits = sum(1 for k in domestic_marker_keys if k in text)

    # Priority rule: overseas policy/geopolitical events should not be dragged into domestic buckets.
    if overseas_hits >= 2 and domestic_hits <= 1:
        return "海外宏观"

    if any(k in text for k in non_industry_hints):
        scores["国内行业"] -= 2
    if any(k in text for k in non_company_hints):
        scores["国内企业"] -= 2
    if "港股" in text and ("市值" in text or "股价" in text):
        scores["国内企业"] += 1
    if ("油价" in text or "地缘" in text) and any(k in text for k in overseas_keys):
        scores["海外宏观"] += 1

    best_cat = max(scores, key=lambda x: scores[x])
    if scores[best_cat] <= 0:
        return "国内行业"
    return best_cat


def arrange_candidates_by_blocks(candidates: List[Dict[str, str]], limit: int = 8) -> List[Dict[str, str]]:
    buckets: Dict[str, List[Dict[str, str]]] = {"海外宏观": [], "国内宏观政策": [], "国内行业": [], "国内企业": []}
    for c in candidates:
        cat = str(c.get("category", "")).strip() or categorize_candidate(c.get("title", ""), c.get("excerpt", ""))
        c2 = dict(c)
        c2["category"] = cat
        buckets[cat].append(c2)

    for cat in buckets:
        buckets[cat].sort(key=lambda x: x.get("econ_score", 0), reverse=True)

    result: List[Dict[str, str]] = []
    # User-required display order: domestic first, then international.
    order = [(cat, CATEGORY_QUOTA) for cat in CATEGORY_ORDER]
    used_links = set()
    for cat, need in order:
        for c in buckets[cat]:
            if need <= 0:
                break
            link = c.get("link", "")
            if link in used_links:
                continue
            result.append(c)
            used_links.add(link)
            need -= 1

    if len(result) < limit:
        for c in candidates:
            link = c.get("link", "")
            if link in used_links:
                continue
            c2 = dict(c)
            c2["category"] = categorize_candidate(c.get("title", ""), c.get("excerpt", ""))
            result.append(c2)
            used_links.add(link)
            if len(result) >= limit:
                break
    return result[:limit]


def synthesize_missing_points(source_items: List[Dict[str, str]], used_titles: set, missing_count: int) -> List[Dict[str, str]]:
    supplements: List[Dict[str, str]] = []
    for src in source_items:
        if missing_count <= 0:
            break
        title = str(src.get("title", "")).strip()
        excerpt = normalize_paragraph_length(str(src.get("excerpt", "")).strip(), min_len=100, max_len=240)
        if not title or title in used_titles or len(excerpt) < 80:
            continue
        analysis = (
            "该信息反映了当日资金与情绪的边际变化，短期可观察相关板块是否形成联动，"
            "中期仍需结合政策兑现、产业景气与业绩验证来评估持续性。"
        )
        supplements.append(
            {
                "title": title,
                "excerpt": excerpt,
                "analysis": normalize_paragraph_length(analysis, min_len=100, max_len=240),
            }
        )
        used_titles.add(title)
        missing_count -= 1
    return supplements


def dedupe_analysis_against_excerpt(excerpt: str, analysis: str) -> str:
    ex = clean_excerpt(excerpt)
    an = clean_excerpt(analysis)
    if not an:
        return an
    # Remove direct full-prefix duplication
    if an.startswith(ex):
        an = an[len(ex) :].lstrip(" ，,。；;:：")
    # Remove long overlapping prefix by characters
    max_check = min(len(ex), len(an), 120)
    overlap = 0
    for n in range(max_check, 39, -1):
        if ex[:n] == an[:n]:
            overlap = n
            break
    if overlap > 0:
        an = an[overlap:].lstrip(" ，,。；;:：")
    return an


def finalize_point(title: str, excerpt: str, analysis: str) -> str:
    cleaned = dedupe_analysis_against_excerpt(excerpt, analysis)
    cleaned = normalize_paragraph_length(cleaned, min_len=80, max_len=240)
    if len(cleaned) < 60:
        hint = normalize_paragraph_length(excerpt, min_len=50, max_len=90)
        cleaned = (
            f"围绕“{title}”这一线索，市场更关注其对风险偏好、资金风格和行业估值的传导。"
            f"结合原文可见，{hint}。短期关注催化扩散节奏，中期仍需观察政策兑现与业绩验证。"
        )
    return cleaned


def matches_any_source_excerpt(excerpt: str, source_items: List[Dict[str, str]]) -> bool:
    ex = clean_excerpt(excerpt)
    if len(ex) < 70:
        return False
    for src in source_items:
        s = clean_excerpt(str(src.get("excerpt", "")))
        if not s:
            continue
        if ex in s or s in ex:
            return True
        if len(ex) >= 40 and len(s) >= 40 and ex[:40] == s[:40]:
            return True
    return False


def looks_like_repeated_template(text: str) -> bool:
    t = clean_excerpt(text)
    bad_patterns = [
        "该信息反映了当日资金与情绪的边际变化",
        "短期可观察相关板块是否形成联动",
        "中期仍需结合政策兑现",
    ]
    return sum(1 for p in bad_patterns if p in t) >= 2


def extract_title_keywords(text: str) -> List[str]:
    t = clean_excerpt(text).lower()
    parts = re.findall(r"[a-z]{2,}|[\u4e00-\u9fff]{2,}", t)
    stop = {
        "国内", "海外", "宏观", "政策", "行业", "企业", "市场", "公司", "集团", "股份", "今日",
        "最新", "继续", "相关", "影响", "走势", "变化", "出现", "推动", "提升", "方案", "公告",
        "发布", "指出", "表示", "关于", "以及", "其中", "全国", "中国",
    }
    kws = []
    for p in parts:
        if p in stop:
            continue
        if p not in kws:
            kws.append(p)
    return kws[:12]


def source_match_score(title: str, excerpt: str, source: Dict[str, str]) -> int:
    src_title = clean_excerpt(str(source.get("title", "")))
    src_excerpt = clean_excerpt(str(source.get("excerpt", "")))
    tt = clean_excerpt(title)
    ex = clean_excerpt(excerpt)
    score = 0
    if ex and src_excerpt:
        if ex in src_excerpt or src_excerpt in ex:
            score += 10
        elif ex[:60] and src_excerpt[:60] and ex[:60] == src_excerpt[:60]:
            score += 7
    if tt and src_title:
        if tt in src_title or src_title in tt:
            score += 6
    title_kws = extract_title_keywords(tt)
    source_text = f"{src_title} {src_excerpt}".lower()
    overlap = sum(1 for k in title_kws if k in source_text)
    score += overlap * 2
    return score


def find_best_source_for_point(title: str, excerpt: str, source_items: List[Dict[str, str]], used_links: set) -> Optional[Dict[str, str]]:
    best = None
    best_score = -1
    for src in source_items:
        link = str(src.get("link", ""))
        if link in used_links:
            continue
        s = source_match_score(title, excerpt, src)
        if s > best_score:
            best_score = s
            best = src
    if best is None:
        return None
    # Hard threshold to enforce title-excerpt-source consistency
    if best_score < 8:
        return None
    return best


def title_source_keyword_overlap(title: str, source: Dict[str, str]) -> int:
    t_kws = extract_title_keywords(title)
    source_text = clean_excerpt(f"{source.get('title', '')} {source.get('excerpt', '')}").lower()
    return sum(1 for k in t_kws if k in source_text)


def enforce_point_from_source(title: str, excerpt: str, analysis: str, source: Dict[str, str]) -> Dict[str, str]:
    source_title = clean_excerpt(str(source.get("title", "")).strip())
    source_excerpt = normalize_paragraph_length(str(source.get("excerpt", "")).strip(), min_len=100, max_len=240)
    final_title = clean_excerpt(title).strip() or source_title
    # Hard binding: weakly aligned titles are replaced by source title.
    if not final_title or title_source_keyword_overlap(final_title, source) < 2:
        final_title = source_title
    final_excerpt = source_excerpt
    final_analysis = finalize_point(final_title, final_excerpt, analysis)
    return {"title": final_title, "excerpt": final_excerpt, "analysis": final_analysis}


def enforce_category_quota(points: List[Dict[str, str]], source_items: List[Dict[str, str]]) -> List[Dict[str, str]]:
    buckets: Dict[str, List[Dict[str, str]]] = {cat: [] for cat in CATEGORY_ORDER}
    for p in points:
        cat = str(p.get("category", "")).strip() or categorize_candidate(str(p.get("title", "")), str(p.get("excerpt", "")))
        p2 = dict(p)
        p2["category"] = cat
        buckets.setdefault(cat, []).append(p2)

    used_links = {str(p.get("link", "")) for p in points if p.get("link")}
    for src in source_items:
        cat = str(src.get("category", "")).strip() or categorize_candidate(str(src.get("title", "")), str(src.get("excerpt", "")))
        if cat not in buckets:
            continue
        if len(buckets[cat]) >= CATEGORY_QUOTA:
            continue
        link = str(src.get("link", ""))
        if link in used_links:
            continue
        buckets[cat].append(
            {
                "title": clean_excerpt(str(src.get("title", "")).strip()),
                "excerpt": normalize_paragraph_length(str(src.get("excerpt", "")).strip(), min_len=100, max_len=240),
                "analysis": finalize_point(
                    str(src.get("title", "")).strip(),
                    str(src.get("excerpt", "")).strip(),
                    "该信息对应的政策或产业线索较明确，短期看资金与情绪传导，中期仍需观察兑现节奏与业绩验证。",
                ),
                "category": cat,
                "link": link,
            }
        )
        used_links.add(link)

    result: List[Dict[str, str]] = []
    for cat in CATEGORY_ORDER:
        selected = buckets.get(cat, [])[:CATEGORY_QUOTA]
        for p in selected:
            p.pop("category", None)
            p.pop("link", None)
            result.append(p)
    return result[:8]


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
    retries = max(int(os.getenv("RSS_FETCH_RETRIES", "2")), 0)
    for i in range(retries + 1):
        try:
            r = requests.get(url, timeout=timeout_sec, headers={"User-Agent": USER_AGENT})
            r.raise_for_status()
            return url, feedparser.parse(r.content)
        except Exception:
            if i < retries:
                time.sleep(0.8 * (i + 1))
                continue
    return url, feedparser.FeedParserDict(entries=[], feed={"title": url})


def collect_news(feeds: List[str], lookback_hours: int, max_items: int) -> List[NewsItem]:
    timeout_sec = int(os.getenv("RSS_FETCH_TIMEOUT", "12"))
    workers = int(os.getenv("RSS_FETCH_WORKERS", "8"))
    title_similarity = float(os.getenv("TITLE_SIMILARITY_THRESHOLD", "0.9"))
    def fetch_all() -> List[Tuple[str, feedparser.FeedParserDict]]:
        fetched: List[Tuple[str, feedparser.FeedParserDict]] = []
        with ThreadPoolExecutor(max_workers=max(workers, 1)) as pool:
            futures = [pool.submit(fetch_feed, u, timeout_sec) for u in feeds]
            for f in as_completed(futures):
                fetched.append(f.result())
        return fetched

    def filter_items(
        fetched_data: List[Tuple[str, feedparser.FeedParserDict]],
        cutoff_hours: int,
        ashare_gate: bool,
    ) -> List[NewsItem]:
        cutoff = datetime.now(SH_TZ) - timedelta(hours=cutoff_hours)
        items: List[NewsItem] = []
        seen_links = set()
        seen_titles: List[str] = []
        for url, parsed in fetched_data:
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
                if ashare_gate and not is_ashare_related(f"{title}\n{summary}"):
                    continue
                if not is_economic_relevant(
                    NewsItem(
                        title=title,
                        link=link,
                        source=source,
                        published=pub_dt.strftime("%Y-%m-%d %H:%M"),
                        published_dt=pub_dt,
                        summary=summary[:400],
                        excerpt=excerpt,
                    )
                ):
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

    fetched = fetch_all()
    total_entries = sum(len(parsed.entries) for _, parsed in fetched)
    print(
        f"RSS fetch done: feeds={len(feeds)}, responses={len(fetched)}, entries={total_entries}, lookback_hours={lookback_hours}"
    )
    if total_entries < 20:
        refill: List[Tuple[str, feedparser.FeedParserDict]] = []
        failed_urls = [u for u, parsed in fetched if len(parsed.entries) == 0]
        retry_timeout = max(timeout_sec * 2, 20)
        for u in failed_urls:
            refill.append(fetch_feed(u, retry_timeout))
        if refill:
            refill_map = {u: p for u, p in refill}
            merged: List[Tuple[str, feedparser.FeedParserDict]] = []
            for u, parsed in fetched:
                merged.append((u, refill_map.get(u, parsed) if len(parsed.entries) == 0 else parsed))
            fetched = merged
            total_entries = sum(len(parsed.entries) for _, parsed in fetched)
            print(
                f"RSS retry done: retried_feeds={len(failed_urls)}, entries={total_entries}, timeout={retry_timeout}s"
            )
    selected = filter_items(fetched, cutoff_hours=lookback_hours, ashare_gate=True)
    if len(selected) < 8:
        extended_hours = max(lookback_hours * 2, 48)
        selected = filter_items(fetched, cutoff_hours=extended_hours, ashare_gate=True)
        print(f"RSS fallback #1: extended lookback to {extended_hours}h, related_items={len(selected)}")
    if len(selected) < 8:
        extended_hours = max(lookback_hours * 3, 72)
        selected = filter_items(fetched, cutoff_hours=extended_hours, ashare_gate=False)
        print(f"RSS fallback #2: disabled ashare gate, lookback={extended_hours}h, related_items={len(selected)}")
    print(f"RSS filter done: related_items={len(selected)}, max_items={max_items}")
    return selected


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

    model = os.getenv("OPENAI_MODEL", "deepseek-chat").strip() or "deepseek-chat"
    raw_base_url = os.getenv("OPENAI_BASE_URL", "").strip()
    base_url = (raw_base_url or "https://api.openai.com/v1").rstrip("/")
    if not re.match(r"^https?://", base_url, flags=re.I):
        print(f"LLM config warning: invalid OPENAI_BASE_URL={raw_base_url!r}, fallback to default.")
        base_url = "https://api.openai.com/v1"
    endpoint = f"{base_url}/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    market_review = extract_market_review_paragraph(items)
    basis_review = extract_basis_review_paragraph(items)
    market_snapshot_candidates = build_market_snapshot_candidates(items)
    source_items = arrange_candidates_by_blocks(build_source_candidates(items), limit=8)
    print(
        "LLM input prepared:",
        f"market_review={'yes' if market_review else 'no'}",
        f"basis_review={'yes' if basis_review else 'no'}",
        f"source_items={len(source_items)}",
    )
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
        "1. 严格输出8条，且顺序必须是：1-2国内宏观政策，3-4国内行业，5-6国内企业，7-8海外宏观。"
        "2. 每条都必须使用候选新闻中的1条，不允许重复同一新闻。"
        "3. title 要写成晨报风格小标题，不要带序号，控制在14到26字。"
        "4. title 前必须带分类前缀，格式为【国内宏观政策】/【国内行业】/【国内企业】/【海外宏观】。"
        "5. excerpt 必须直接使用输入里的原文段落或在不改变原意前提下做极轻微压缩，长度尽量控制在120到220字。"
        "6. analysis 必须写成完整的一段点评，长度尽量与 excerpt 接近，建议控制在100到220字。"
        "7. analysis 不要复读 excerpt，不要以 excerpt 原句开头。8条analysis不得套用同一句模板，必须逐条针对该新闻写差异化判断。"
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
        template_like_count = 0
        used_source_links = set()
        for p in key_points[:8]:
            title_text = str(p.get("title", "")).strip()
            excerpt_text = normalize_paragraph_length(str(p.get("excerpt", "")).strip(), min_len=100, max_len=240)
            analysis_text = finalize_point(title_text, excerpt_text, str(p.get("analysis", "")).strip())
            if (
                not title_text
                or len(excerpt_text) < 100
                or len(analysis_text) < 80
            ):
                continue
            matched_source = find_best_source_for_point(title_text, excerpt_text, source_items, used_source_links)
            if not matched_source:
                continue
            used_source_links.add(str(matched_source.get("link", "")))
            enforced = enforce_point_from_source(title_text, excerpt_text, analysis_text, matched_source)
            title_text = enforced["title"]
            excerpt_text = enforced["excerpt"]
            analysis_text = enforced["analysis"]
            if len(excerpt_text) < 100:
                continue
            if looks_like_repeated_template(analysis_text):
                template_like_count += 1
            normalized_points.append(
                {
                    "title": title_text,
                    "excerpt": excerpt_text,
                    "analysis": analysis_text or "建议继续跟踪其对市场风格和板块轮动的影响。",
                    "link": str(matched_source.get("link", "")),
                    "category": str(matched_source.get("category", "")),
                }
            )
        key_points = normalized_points
        if len(key_points) < 4:
            msg = f"LLM validation error: only {len(key_points)} high-quality key points passed checks."
            if allow_fallback:
                print(msg + " Falling back to template content.")
                return fallback_docx_style(items, title)
            raise RuntimeError(msg)
        if len(key_points) >= 6 and template_like_count >= 4:
            msg = "LLM validation error: analysis paragraphs are overly templated."
            if allow_fallback:
                print(msg + " Falling back to template content.")
                return fallback_docx_style(items, title)
            raise RuntimeError(msg)
        if not clean_excerpt(str(data.get("market_review") or market_review or "")) and not key_points:
            msg = "LLM validation error: market_review is empty and no valid key_points were generated."
            if allow_fallback:
                print(msg + " Falling back to template content.")
                return fallback_docx_style(items, title)
            raise RuntimeError(msg)
        if len(key_points) < 8:
            used_titles = {str(x.get("title", "")).strip() for x in key_points}
            key_points.extend(synthesize_missing_points(source_items, used_titles, 8 - len(key_points)))

        key_points = enforce_category_quota(key_points, source_items)

        while len(key_points) < 8:
            key_points.append(
                {
                    "title": "暂无补充要点",
                    "excerpt": "暂无符合100字以上原文段落的资讯。",
                    "analysis": "建议继续跟踪政策、产业和资金面的新增变化。",
                }
            )
        print(f"LLM output accepted: key_points={len(key_points)}")
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
    block_headers = {1: "国内宏观政策", 3: "国内行业", 5: "国内企业", 7: "海外宏观"}
    for idx, p in enumerate(doc["key_points"][:8], start=1):
        if idx in block_headers:
            lines.append(f"{block_headers[idx]}")
        raw_title = str(p.get("title", "")).strip()
        clean_title = re.sub(r"^【[^】]+】\s*", "", raw_title).strip()
        lines.append(f"{idx}. {clean_title or raw_title}")
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
        try:
            data = r.json()
            if isinstance(data, dict) and data.get("code", 0) not in {0, "0", None}:
                raise RuntimeError(f"Feishu flow webhook error: {data}")
            print(f"Feishu flow response: {data}")
        except ValueError:
            print("Feishu flow response: non-JSON body")
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
    max_items = int(os.getenv("MAX_NEWS_ITEMS", "100"))
    strict_llm = os.getenv("STRICT_LLM", "1").strip().lower() not in {"0", "false", "no"}

    feeds = read_feeds(project_dir / "rss_feeds.txt")
    items = collect_news(feeds, lookback, max_items)
    doc = llm_docx_style(items, allow_fallback=not strict_llm)
    text = render_doc_text(doc, items)
    print(f"Render done: report_chars={len(text)}, key_points={len(doc.get('key_points', []))}")

    now = datetime.now(SH_TZ)
    report_daily_path = project_dir / f"report_{now.strftime('%Y-%m-%d')}.md"
    report_snapshot_path = project_dir / f"report_{now.strftime('%Y-%m-%d_%H%M%S')}.md"
    tmp_path = report_daily_path.with_suffix(".tmp")
    tmp_path.write_text(text, encoding="utf-8")
    os.replace(tmp_path, report_daily_path)
    report_snapshot_path.write_text(text, encoding="utf-8")
    print(f"Local report saved: {report_daily_path}")
    print(f"Local report snapshot: {report_snapshot_path}")

    payload = build_flow_payload(items, text, doc)
    try:
        send_to_feishu(webhook, text, secret, payload)
        print("Morning report sent to Feishu.")
    except Exception as exc:
        print(f"Feishu send failed: {exc}")


if __name__ == "__main__":
    main()
