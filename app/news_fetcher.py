#!/usr/bin/env python3
"""
news_fetcher.py — pull recent headlines for holdings and watchlist entries from
Yahoo Finance, score their sentiment and store them. Cron entry point; the API
also calls refresh_news() for the manual refresh button.

Sources: Yahoo's ticker search (US-centric), a Google News search by company
name (covers European listings) and general market RSS feeds (see
news_sources.py). Symbols with no coverage are recorded as "checked, nothing
found" so they are not hammered on every run.
"""
import argparse
import json
import logging
import re
import threading
import time
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd

import db_utils as db
from config_utils import get_config
import news_sources
from sentiment import score_article, score_headline

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

RETENTION_DAYS = 30
ABSTRACT_BUDGET = 60            # article pages fetched per run for missing abstracts
EMPTY_RECHECK_MINUTES = 360     # symbols with no coverage are only re-checked every 6h
FETCH_COUNT = 10
MAX_CONSECUTIVE_ERRORS = 3      # stop the run early when Yahoo starts refusing us

_run_lock = threading.Lock()

Fetch = Callable[[str, Optional[str], Optional[str]], Tuple[List[dict], str]]


def _iso(ts: pd.Timestamp) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S")


def _utcnow() -> pd.Timestamp:
    return pd.Timestamp.utcnow().replace(tzinfo=None)


def _is_relevant(raw: dict, symbol: str, security_type: Optional[str]) -> bool:
    """Search also returns loosely related stories; keep those Yahoo tags with this symbol."""
    related = {str(t).upper() for t in (raw.get("relatedTickers") or [])}
    sym = symbol.upper()
    if sym in related:
        return True
    if (security_type or "").upper() == "CRYPTOCURRENCY" and "-" in sym:
        base = sym.split("-")[0]
        return any(t.split("-")[0] == base for t in related)
    return False


def normalize(raw: dict) -> Optional[dict]:
    title = (raw.get("title") or "").strip()
    published = raw.get("providerPublishTime")
    uid = raw.get("uuid") or raw.get("link")
    if not title or not published or not uid:
        return None
    score, _label = score_headline(title)
    return {
        "uid": str(uid),
        "title": title,
        "publisher": raw.get("publisher"),
        "url": raw.get("link"),
        "published_at": _iso(pd.Timestamp(int(published), unit="s")),
        "sentiment": score,
    }


_google_blocked = False


def _title_key(title: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", title.lower())[:80]


def fetch_headlines(symbol: str, name: Optional[str], security_type: Optional[str]) -> Tuple[List[dict], str]:
    """Headlines from Yahoo's ticker search plus a Google News search by company name.

    Returns (items, which sources found something: 'ticker', 'name', 'ticker+name' or 'none').
    Raises only if every source failed.
    """
    global _google_blocked
    import yfinance as yf

    found, errors, items = [], [], []
    try:
        news = yf.Search(symbol, max_results=1, news_count=FETCH_COUNT).news or []
        yahoo = [k for k in (normalize(n) for n in news if _is_relevant(n, symbol, security_type)) if k]
        if yahoo:
            found.append("ticker")
            items += yahoo
    except Exception as exc:
        errors.append(exc)

    company = news_sources.clean_name(name)
    if company and not _google_blocked:
        try:
            google = news_sources.google_news(company, days=7, count=FETCH_COUNT)
            if google:
                found.append("name")
                items += google
        except news_sources.RateLimited as exc:
            logging.warning("Google News is rate limiting (%s); skipping it for the rest of this run", exc)
            _google_blocked = True
        except Exception as exc:
            errors.append(exc)

    if not items and errors and len(errors) == (2 if company and not _google_blocked else 1):
        raise errors[0]
    seen, unique = set(), []
    for it in items:
        key = _title_key(it["title"])
        if key not in seen:
            seen.add(key)
            unique.append(it)
    return unique, "+".join(found) or "none"


def refresh_market_news(targets: pd.DataFrame, stamp: str) -> dict:
    """Pull the market feeds, keep everything as market news and attach
    headlines that name a held/watched company to that security."""
    feeds = get_config("news_rss_feeds") or None
    if isinstance(feeds, str):
        try:
            feeds = json.loads(feeds)
        except ValueError:
            feeds = None
    items = news_sources.market_feeds(feeds)
    named = [{"security_id": int(t.security_id), "symbol": t.symbol, "clean_name": news_sources.clean_name(t.name)}
             for t in targets.itertuples()]
    attached = 0
    by_security: dict = {}
    for it in items:
        for sid in news_sources.match_targets(it["title"], named):
            by_security.setdefault(sid, []).append(it)
    for sid, rows in by_security.items():
        attached += db.upsert_news(sid, rows, stamp)
    db.upsert_market_news(items, stamp)
    return {"market": len(items), "attached": attached}


def _add_abstracts(security_id: int, items: List[dict], budget: int) -> int:
    """Give new articles without an abstract the one their page publishes and
    re-score them on headline + abstract. Returns the remaining page budget."""
    if budget <= 0:
        return budget
    known = db.get_news_uids(security_id)
    for it in items:
        if budget <= 0:
            break
        if it.get("summary") or it["uid"] in known or not it.get("url"):
            continue
        budget -= 1
        try:
            it["summary"] = news_sources.page_abstract(it["url"], it["title"])
        except Exception as exc:
            logging.debug("No abstract for %s: %s", it["url"], exc)
            continue
        if it["summary"]:
            it["sentiment"], _ = score_article(it["title"], it["summary"])
    return budget


def _last_fetches() -> Dict[int, Tuple[pd.Timestamp, int]]:
    log = db.get_news_fetch_log()
    return {int(r.security_id): (pd.Timestamp(r.fetched_at), int(r.items or 0))
            for r in log.itertuples()}


def _due(last: Optional[Tuple[pd.Timestamp, int]], now: pd.Timestamp, min_minutes: float) -> bool:
    if last is None:
        return True
    fetched_at, items = last
    wait = min_minutes if items else max(min_minutes, EMPTY_RECHECK_MINUTES)
    return (now - fetched_at).total_seconds() >= wait * 60


def refresh_news(symbol: Optional[str] = None, force: bool = False,
                 fetch: Optional[Fetch] = None, sleep: Callable[[float], None] = time.sleep,
                 market: Optional[Callable] = None) -> dict:
    """Fetch news for holdings + watchlist (or one `symbol`), honouring per-symbol throttling.

    force=True ignores the throttle (used for an explicit refresh of one symbol).
    """
    if not _run_lock.acquire(blocking=False):
        logging.info("News refresh already running in this process — skipping")
        return {"checked": 0, "skipped": 0, "stored": 0, "errors": 0, "pruned": 0, "busy": True}
    try:
        return _refresh(symbol, force, fetch or fetch_headlines, sleep, market)
    finally:
        _run_lock.release()


def _refresh(symbol, force, fetch, sleep, market=None) -> dict:
    global _google_blocked
    _google_blocked = False
    now = _utcnow()
    min_minutes = float(get_config("news_min_fetch_minutes") or 30)
    pause = float(get_config("yf_base_sleep_sec") or 0.8)

    targets = db.list_news_targets()
    if symbol:
        targets = targets[targets["symbol"].str.upper() == symbol.upper()]
    last = _last_fetches()

    stats = {"checked": 0, "skipped": 0, "stored": 0, "errors": 0, "pruned": 0}
    budget = ABSTRACT_BUDGET
    consecutive_errors = 0
    for t in targets.itertuples():
        sid = int(t.security_id)
        if not force and not _due(last.get(sid), now, min_minutes):
            stats["skipped"] += 1
            continue
        if stats["checked"]:
            sleep(pause)
        try:
            items, query = fetch(t.symbol, t.name, t.security_type)
        except Exception as exc:
            logging.warning("News fetch failed for %s: %s", t.symbol, exc)
            stats["errors"] += 1
            consecutive_errors += 1
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                logging.error("Aborting news run after %d consecutive failures", consecutive_errors)
                break
            continue
        consecutive_errors = 0
        budget = _add_abstracts(sid, items, budget)
        stamp = _iso(_utcnow())
        db.upsert_news(sid, items, stamp)
        db.record_news_fetch(sid, len(items), query, stamp)
        stats["checked"] += 1
        stats["stored"] += len(items)

    if not symbol:
        try:
            stats.update((market or refresh_market_news)(targets, _iso(_utcnow())))
        except Exception as exc:
            logging.warning("Market news refresh failed: %s", exc)
    cutoff = _iso(now - pd.Timedelta(days=RETENTION_DAYS))
    stats["pruned"] = db.delete_news_older_than(cutoff) + db.delete_market_news_older_than(cutoff)
    logging.info("News refresh: %s", stats)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch and score news for holdings and watchlist")
    parser.add_argument("--symbol", help="only this symbol")
    parser.add_argument("--force", action="store_true", help="ignore the per-symbol throttle")
    args = parser.parse_args()
    refresh_news(symbol=args.symbol, force=args.force)


if __name__ == "__main__":
    main()
