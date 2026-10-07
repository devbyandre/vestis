"""
news_sources.py — headline sources besides Yahoo's ticker search.

- Google News RSS search by company name: covers European listings and ETFs
  that Yahoo's US-centric search misses.
- General market RSS feeds (CNBC, MarketWatch, ...): every headline is kept as
  market news, and headlines naming a held/watched company are attached to it.
"""
import email.utils
import hashlib
import html
import re
import xml.etree.ElementTree as ET
from typing import Dict, Iterable, List, Optional
from urllib.parse import quote_plus

import pandas as pd
import requests

from sentiment import score_article

DEFAULT_FEEDS = {
    "CNBC Markets": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114",
    "MarketWatch Top Stories": "https://feeds.content.dowjones.io/public/rss/mw_topstories",
    "MarketWatch MarketPulse": "https://feeds.content.dowjones.io/public/rss/mw_marketpulse",
    "Investing.com": "https://www.investing.com/rss/news.rss",
    "Nasdaq Stocks": "https://www.nasdaq.com/feed/rssoutbound?category=Stocks",
    "FT Markets": "https://www.ft.com/markets?format=rss",
}
GOOGLE_NEWS = "https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
MAX_BYTES = 3_000_000
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; Vestis news reader)"}

# Legal-form and share-class noise that never appears in headlines.
_SUFFIXES = re.compile(
    r"[,.]?\s+(inc|incorporated|corp|corporation|co|company|ag|se|sa|nv|n\.v|plc|ltd|limited|"
    r"holding|holdings|group|kgaa|& co|aktiengesellschaft|asa|a/s|ab|oyj|spa|s\.p\.a|class [a-c]|"
    r"adr|ord|reg|registered|vz|st)\.?$", re.I)


class RateLimited(Exception):
    pass


def clean_name(name: Optional[str]) -> Optional[str]:
    """'Allianz SE' -> 'Allianz', 'Novo Nordisk A/S' -> 'Novo Nordisk'."""
    if not name:
        return None
    n = re.sub(r"\s*\(.*?\)", "", str(name)).strip()
    n = re.sub(r"\s+(EUR|USD|GBP|CHF|USDT)$", "", n)          # crypto pairs: "Bitcoin EUR"
    for _ in range(4):
        stripped = _SUFFIXES.sub("", n).strip(" ,.-")
        if stripped == n:
            break
        n = stripped
    return n if len(n) >= 3 else None


def _safe_url(url: Optional[str]) -> Optional[str]:
    """Only plain web links ever reach the page (no javascript:, data:, ...)."""
    url = (url or "").strip()
    return url if re.match(r"^https?://", url, re.I) else None


def fetch(url: str, timeout: float = 15.0) -> bytes:
    resp = requests.get(url, headers=HEADERS, timeout=timeout, stream=True)
    if resp.status_code in (429, 503):
        raise RateLimited(f"{resp.status_code} from {url.split('?')[0]}")
    resp.raise_for_status()
    body = b""
    for chunk in resp.iter_content(65536):
        body += chunk
        if len(body) > MAX_BYTES:
            break
    return body


def parse_rss(body: bytes, default_publisher: Optional[str] = None) -> List[dict]:
    """RSS 2.0 items -> normalised headline dicts (uid, title, publisher, url, published_at, sentiment)."""
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return []
    out = []
    for item in root.iter("item"):
        title = (item.findtext("title") or "").strip()
        link = _safe_url(item.findtext("link"))
        guid = (item.findtext("guid") or link or "").strip()
        pub = item.findtext("pubDate")
        if not title or not guid or not pub:
            continue
        try:
            published = pd.Timestamp(email.utils.parsedate_to_datetime(pub))
        except (TypeError, ValueError):
            continue
        if published.tzinfo is not None:
            published = published.tz_convert("UTC").tz_localize(None)
        source = item.find("source")
        publisher = (source.text.strip() if source is not None and source.text else None) or default_publisher
        if publisher and title.endswith(f" - {publisher}"):
            title = title[: -len(publisher) - 3].strip()
        summary = clean_abstract(item.findtext("description"), title)
        score, _ = score_article(title, summary)
        out.append({
            "uid": "rss:" + hashlib.sha1(guid.encode("utf-8")).hexdigest(),
            "title": title,
            "publisher": publisher,
            "url": link,
            "published_at": published.strftime("%Y-%m-%dT%H:%M:%S"),
            "sentiment": score,
            "summary": summary,
        })
    return out


ABSTRACT_MAX = 400


def clean_abstract(raw: Optional[str], title: str = "") -> Optional[str]:
    """Plain-text abstract from a feed description or page meta tag, or None.

    Google News descriptions are only a link back to the headline, so anything
    that is just markup, repeats the title or is too short is dropped.
    """
    if not raw:
        return None
    text = html.unescape(re.sub(r"<[^>]+>", " ", raw))
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) < 30 or text.lower().startswith(title.lower()[:40]) and len(text) < len(title) + 40:
        return None
    return text[:ABSTRACT_MAX].rsplit(" ", 1)[0] + "…" if len(text) > ABSTRACT_MAX else text


_META = re.compile(r'<meta[^>]+(?:property|name)=["\'](?:og:description|description)["\'][^>]*>', re.I)
_CONTENT = re.compile(r'content=["\']([^"\']*)["\']', re.I)


def page_abstract(url: Optional[str], title: str = "") -> Optional[str]:
    """The description an article page publishes for link previews (og:description)."""
    url = _safe_url(url)
    if not url or "news.google.com" in url:
        return None
    body = fetch(url, timeout=8.0)[:400_000].decode("utf-8", errors="ignore")
    for tag in _META.findall(body):
        m = _CONTENT.search(tag)
        abstract = clean_abstract(m.group(1), title) if m else None
        if abstract:
            return abstract
    return None


def _name_pattern(name: str) -> re.Pattern:
    words = name.split()
    # Long fund names rarely appear in full; their first two words still identify them.
    key = " ".join(words[:2]) if len(words) > 2 else name
    return re.compile(r"(?<![\w-])" + re.escape(key) + r"(?![\w-])", re.I)


_FINANCE_TERMS = " OR ".join(["stock", "shares", "earnings", "investors", "analyst", "market", "revenue", "profit"])
# Quote/profile pages that search engines index like articles.
_QUOTE_PAGE = re.compile(r"stock (price|quote)|price,? news|quote (and|&) history|share price (today|live)", re.I)


def google_news(name: str, days: int = 7, count: int = 10) -> List[dict]:
    """Recent headlines that name the company (search by cleaned company name)."""
    q = quote_plus(f'"{name}" ({_FINANCE_TERMS}) when:{max(1, days)}d')
    items = parse_rss(fetch(GOOGLE_NEWS.format(q=q)))
    pattern = _name_pattern(name)
    kept = [i for i in items if pattern.search(i["title"]) and not _QUOTE_PAGE.search(i["title"])]
    kept.sort(key=lambda i: i["published_at"], reverse=True)
    return kept[:count]


def match_targets(title: str, targets: Iterable[dict]) -> List[int]:
    """Security ids whose company name (or US-style ticker) appears in a headline."""
    hits = []
    for t in targets:
        name = t.get("clean_name")
        sym = str(t.get("symbol") or "")
        if name and _name_pattern(name).search(title):
            hits.append(int(t["security_id"]))
        elif re.fullmatch(r"[A-Z]{3,5}", sym) and re.search(rf"(?<![\w$])\$?{sym}(?![\w])", title):
            hits.append(int(t["security_id"]))
    return hits


def market_feeds(feeds: Optional[Dict[str, str]] = None) -> List[dict]:
    """All items of the configured market feeds; a failing feed is skipped."""
    out = []
    for name, url in (feeds or DEFAULT_FEEDS).items():
        if not _safe_url(url):
            continue
        try:
            items = parse_rss(fetch(url), default_publisher=name)
        except Exception:
            continue
        for it in items:
            it["feed"] = name
        out.extend(items)
    return out
