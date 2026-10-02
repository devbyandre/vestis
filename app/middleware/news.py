from typing import Optional

import pandas as pd

import db_utils as db
from config_utils import get_config
from sentiment import label_for

SCOPES = ("all", "holdings", "watchlist")


def _counts(scores: pd.Series) -> dict:
    labels = scores.map(label_for)
    return {
        "count": int(len(scores)),
        "avg_sentiment": round(float(scores.mean()), 3) if len(scores) else None,
        "positive": int((labels == "positive").sum()),
        "negative": int((labels == "negative").sum()),
        "neutral": int((labels == "neutral").sum()),
    }


def _z(iso: Optional[str]) -> Optional[str]:
    return f"{iso}Z" if iso else None


def get_news_feed(scope: str = "all", symbol: Optional[str] = None,
                  days: int = 7, limit: Optional[int] = None) -> dict:
    """Headlines with sentiment for holdings/watchlist.

    Returns {items, summary, overall}. `summary` has one row per security in
    scope (also those without coverage) and ignores `symbol`; `items` and
    `overall` honour it. An article tagged to several securities appears once.
    """
    if scope not in SCOPES:
        raise ValueError(f"scope must be one of {SCOPES}")
    targets = db.list_news_targets()
    if scope != "all":
        targets = targets[targets["scope"] == scope]
    since = (pd.Timestamp.utcnow().replace(tzinfo=None) - pd.Timedelta(days=max(1, int(days)))
             ).strftime("%Y-%m-%dT%H:%M:%S")
    news = db.get_news(since, [int(i) for i in targets["security_id"]])

    log = db.get_news_fetch_log()
    checked = dict(zip(log["security_id"].astype(int), log["fetched_at"]))

    summary = []
    for t in targets.itertuples():
        rows = news[news["security_id"] == t.security_id]
        row = {
            "symbol": t.symbol, "name": t.name, "scope": t.scope,
            "security_type": t.security_type,
            "latest": _z(rows["published_at"].max()) if len(rows) else None,
            "checked_at": _z(checked.get(int(t.security_id))),
            **_counts(rows["sentiment"].dropna()),
        }
        summary.append(row)
    summary.sort(key=lambda r: (-r["count"], r["symbol"]))

    feed = news
    if symbol:
        feed = feed[feed["symbol"].str.upper() == symbol.upper()]
    items = []
    for uid, grp in feed.groupby("uid", sort=False):
        first = grp.iloc[0]
        sentiment = None if pd.isna(first["sentiment"]) else float(first["sentiment"])
        items.append({
            "uid": uid, "title": first["title"], "publisher": first["publisher"],
            "url": first["url"], "published_at": _z(first["published_at"]),
            "sentiment": sentiment, "label": label_for(sentiment),
            "symbols": sorted(grp["symbol"].unique().tolist()),
        })
    items.sort(key=lambda i: i["published_at"], reverse=True)
    unique_scores = pd.Series([i["sentiment"] for i in items if i["sentiment"] is not None], dtype=float)
    cap = int(limit or get_config("news_max_items") or 50)
    return {"items": items[:cap], "summary": summary, "overall": _counts(unique_scores)}


def refresh_news(symbol: Optional[str] = None, force: bool = False) -> dict:
    import news_fetcher  # lazy: pulls in yfinance only when a refresh is requested
    return news_fetcher.refresh_news(symbol=symbol, force=force)
