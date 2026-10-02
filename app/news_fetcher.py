#!/usr/bin/env python3
"""
news_fetcher.py — pull recent headlines for holdings and watchlist entries from
Yahoo Finance, score their sentiment and store them. Cron entry point; the API
also calls refresh_news() for the manual refresh button.

Yahoo's news search is US-centric: XETRA tickers and ETFs often return nothing.
For those we retry by company name (single stocks only) and otherwise record
"checked, nothing found" so the symbol is not hammered on every run.
"""
import argparse
import logging
import threading
import time
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd

import db_utils as db
from config_utils import get_config
from sentiment import score_headline

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

RETENTION_DAYS = 30
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


def fetch_headlines(symbol: str, name: Optional[str], security_type: Optional[str]) -> Tuple[List[dict], str]:
    """Return (normalized items, which query found them: 'ticker' | 'name' | 'none')."""
    import yfinance as yf

    def search(query: str) -> List[dict]:
        news = yf.Search(query, max_results=1, news_count=FETCH_COUNT).news or []
        kept = [normalize(n) for n in news if _is_relevant(n, symbol, security_type)]
        return [k for k in kept if k]

    items = search(symbol)
    if items:
        return items, "ticker"
    if name and (security_type or "EQUITY").upper() in ("EQUITY", "CRYPTOCURRENCY"):
        time.sleep(float(get_config("yf_base_sleep_sec") or 0.8))
        items = search(name)
        if items:
            return items, "name"
    return [], "none"


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
                 fetch: Optional[Fetch] = None, sleep: Callable[[float], None] = time.sleep) -> dict:
    """Fetch news for holdings + watchlist (or one `symbol`), honouring per-symbol throttling.

    force=True ignores the throttle (used for an explicit refresh of one symbol).
    """
    if not _run_lock.acquire(blocking=False):
        logging.info("News refresh already running in this process — skipping")
        return {"checked": 0, "skipped": 0, "stored": 0, "errors": 0, "pruned": 0, "busy": True}
    try:
        return _refresh(symbol, force, fetch or fetch_headlines, sleep)
    finally:
        _run_lock.release()


def _refresh(symbol, force, fetch, sleep) -> dict:
    now = _utcnow()
    min_minutes = float(get_config("news_min_fetch_minutes") or 30)
    pause = float(get_config("yf_base_sleep_sec") or 0.8)

    targets = db.list_news_targets()
    if symbol:
        targets = targets[targets["symbol"].str.upper() == symbol.upper()]
    last = _last_fetches()

    stats = {"checked": 0, "skipped": 0, "stored": 0, "errors": 0, "pruned": 0}
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
        stamp = _iso(_utcnow())
        db.upsert_news(sid, items, stamp)
        db.record_news_fetch(sid, len(items), query, stamp)
        stats["checked"] += 1
        stats["stored"] += len(items)

    stats["pruned"] = db.delete_news_older_than(_iso(now - pd.Timedelta(days=RETENTION_DAYS)))
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
