from typing import Dict, List, Optional

import pandas as pd

from .core import get_conn, _adapt_sql, _read_sql
from .transactions import get_net_quantities


def list_news_targets() -> pd.DataFrame:
    """Securities worth fetching news for: current holdings and watchlist entries.

    `scope` is 'holdings' (net quantity > 0 in some portfolio) or 'watchlist'
    (no transactions at all). Fully sold positions are left out.
    """
    df = _read_sql("""
        SELECT s.id AS security_id, s.yahoo_ticker AS symbol,
               COALESCE(sc.longName, sc.shortName) AS name,
               sc.security_type AS security_type,
               (SELECT COUNT(*) FROM transactions t WHERE t.security_id = s.id) AS n_tx
        FROM securities s
        LEFT JOIN securities_cache sc ON sc.security_id = s.id
        ORDER BY s.yahoo_ticker
    """)
    if df.empty:
        return df.assign(scope=pd.Series(dtype=object)).drop(columns=["n_tx"])
    held: Dict[int, float] = {}
    for (_pid, sid), qty in get_net_quantities().items():
        held[sid] = held.get(sid, 0.0) + qty
    scope = []
    for sid, n_tx in zip(df["security_id"], df["n_tx"]):
        if held.get(sid, 0.0) > 1e-8:
            scope.append("holdings")
        elif n_tx == 0:
            scope.append("watchlist")
        else:
            scope.append(None)
    df["scope"] = scope
    return df[df["scope"].notna()].drop(columns=["n_tx"]).reset_index(drop=True)


def upsert_news(security_id: int, items: List[dict], fetched_at: str) -> int:
    """Store headlines for a security; re-seen uids just refresh their sentiment."""
    if not items:
        return 0
    sql = _adapt_sql("""
        INSERT INTO news (security_id, uid, title, publisher, url, published_at, sentiment, fetched_at, summary)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (security_id, uid) DO UPDATE SET
            title = EXCLUDED.title,
            summary = COALESCE(EXCLUDED.summary, news.summary),
            sentiment = CASE WHEN EXCLUDED.summary IS NULL AND news.summary IS NOT NULL
                             THEN news.sentiment ELSE EXCLUDED.sentiment END
    """)
    rows = [(security_id, it["uid"], it["title"], it.get("publisher"), it.get("url"),
             it["published_at"], it.get("sentiment"), fetched_at, it.get("summary")) for it in items]
    with get_conn() as conn:
        cur = conn.cursor()
        cur.executemany(sql, rows)
    return len(rows)


def record_news_fetch(security_id: int, items: int, query: Optional[str], fetched_at: str) -> None:
    sql = _adapt_sql("""
        INSERT INTO news_fetch_log (security_id, fetched_at, items, query)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (security_id) DO UPDATE SET
            fetched_at = EXCLUDED.fetched_at, items = EXCLUDED.items, query = EXCLUDED.query
    """)
    with get_conn() as conn:
        conn.cursor().execute(sql, (security_id, fetched_at, items, query))


def get_news_fetch_log() -> pd.DataFrame:
    return _read_sql("SELECT security_id, fetched_at, items, query FROM news_fetch_log")


def get_news(since: str, security_ids: Optional[List[int]] = None, limit: int = 5000) -> pd.DataFrame:
    """Headlines published at/after `since` (ISO), newest first, one row per (security, article)."""
    where, params = ["n.published_at >= ?"], [since]
    if security_ids is not None:
        if not security_ids:
            return pd.DataFrame(columns=["security_id", "symbol", "uid", "title", "publisher",
                                         "url", "published_at", "sentiment", "summary"])
        where.append("n.security_id IN (%s)" % ",".join("?" * len(security_ids)))
        params += [int(i) for i in security_ids]
    params.append(int(limit))
    return _read_sql(f"""
        SELECT n.security_id, s.yahoo_ticker AS symbol, n.uid, n.title, n.publisher,
               n.url, n.published_at, n.sentiment, n.summary
        FROM news n JOIN securities s ON s.id = n.security_id
        WHERE {' AND '.join(where)}
        ORDER BY n.published_at DESC
        LIMIT ?
    """, tuple(params))


def delete_news_older_than(before: str) -> int:
    sql = _adapt_sql("DELETE FROM news WHERE published_at < ?")
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, (before,))
        return cur.rowcount


def upsert_market_news(items: List[dict], fetched_at: str) -> int:
    """Store general market headlines (not tied to one security)."""
    if not items:
        return 0
    sql = _adapt_sql("""
        INSERT INTO market_news (uid, title, publisher, url, published_at, sentiment, feed, fetched_at, summary)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (uid) DO UPDATE SET title = EXCLUDED.title, sentiment = EXCLUDED.sentiment,
            summary = COALESCE(EXCLUDED.summary, market_news.summary)
    """)
    rows = [(it["uid"], it["title"], it.get("publisher"), it.get("url"), it["published_at"],
             it.get("sentiment"), it.get("feed"), fetched_at, it.get("summary")) for it in items]
    with get_conn() as conn:
        conn.cursor().executemany(sql, rows)
    return len(rows)


def get_market_news(since: str, limit: int = 500) -> pd.DataFrame:
    return _read_sql("""
        SELECT uid, title, publisher, url, published_at, sentiment, feed, summary
        FROM market_news WHERE published_at >= ?
        ORDER BY published_at DESC LIMIT ?
    """, (since, int(limit)))


def delete_market_news_older_than(before: str) -> int:
    sql = _adapt_sql("DELETE FROM market_news WHERE published_at < ?")
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, (before,))
        return cur.rowcount


def get_news_fetched_since(fetched_since: str, security_ids: List[int]) -> pd.DataFrame:
    """Headlines stored after `fetched_since` (ISO) for the given securities."""
    if not security_ids:
        return pd.DataFrame(columns=["security_id", "symbol", "uid", "title", "publisher",
                                     "url", "published_at", "sentiment", "fetched_at"])
    ph = ",".join("?" * len(security_ids))
    return _read_sql(f"""
        SELECT n.security_id, s.yahoo_ticker AS symbol, n.uid, n.title, n.publisher,
               n.url, n.published_at, n.sentiment, n.fetched_at
        FROM news n JOIN securities s ON s.id = n.security_id
        WHERE n.fetched_at > ? AND n.security_id IN ({ph})
        ORDER BY n.fetched_at
    """, tuple([fetched_since] + [int(i) for i in security_ids]))


def get_news_uids(security_id: int) -> set:
    """uids already stored for a security (to skip re-fetching their abstracts)."""
    df = _read_sql("SELECT uid FROM news WHERE security_id = ? AND summary IS NOT NULL", (int(security_id),))
    return set(df["uid"])
