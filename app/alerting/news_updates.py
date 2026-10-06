"""News on Telegram: a section for the digest and optional instant alerts for
strongly worded headlines about current holdings.

Sentiment is a rough headline-only score, so instant alerts are opt-in
(setting news_alerts) and only fire beyond news_alert_threshold.
"""
import logging
import re
from typing import Callable, List, Optional

import pandas as pd

import db_utils as db
from config_utils import get_config, set_config
from .messages import md, vestis_link
from .quiet_hours import quiet_hours_active

Notify = Callable[[str, Optional[tuple]], bool]

DIGEST_MIN_STRENGTH = 0.3
DIGEST_MAX_ITEMS = 6
ALERT_MAX_ITEMS = 5
_SHORT = re.compile(r"[,.]?\s+(inc|corp|corporation|co|company|ag|se|sa|nv|plc|ltd|limited|holding|holdings|"
                    r"group|kgaa|& co|aktiengesellschaft|a/s|asa|ab)\.?$", re.I)


def _short(name: Optional[str], symbol: str) -> str:
    if not name:
        return symbol
    n = re.sub(r"\s*\(.*?\)", "", name).strip()
    for _ in range(4):
        m = _SHORT.sub("", n).strip(" ,.-")
        if m == n:
            break
        n = m
    return n or symbol


def _holdings() -> pd.DataFrame:
    targets = db.list_news_targets()
    return targets[targets["scope"] == "holdings"] if not targets.empty else targets


def _strongest(news: pd.DataFrame, holdings: pd.DataFrame, min_strength: float, one_per_security: bool) -> List[dict]:
    if news.empty:
        return []
    news = news.dropna(subset=["sentiment"])
    news = news[news["sentiment"].abs() >= min_strength].copy()
    if news.empty:
        return []
    names = dict(zip(holdings["security_id"].astype(int), holdings["name"]))
    news["key"] = news["title"].str.lower().str.replace(r"[^a-z0-9]+", "", regex=True).str[:80]
    news["strength"] = news["sentiment"].abs()
    news = news.sort_values("strength", ascending=False).drop_duplicates("key")
    if one_per_security:
        news = news.drop_duplicates("security_id")
    return [{**r, "name": _short(names.get(int(r["security_id"])), r["symbol"])} for r in news.to_dict("records")]


def _line(item: dict) -> str:
    icon = "📈" if item["sentiment"] > 0 else "📉"
    title = md(str(item["title"]).replace("]", ")"))
    url = str(item.get("url") or "")
    head = f"[{title}]({url.replace(')', '%29')})" if url.startswith(("http://", "https://")) else title
    publisher = f" — _{md(item['publisher'])}_" if item.get("publisher") else ""
    return f"  • {icon} *{md(item['name'])}*: {head}{publisher}"


def news_digest_lines(since: pd.Timestamp) -> List[str]:
    """Digest section: the strongest headlines per holding since `since` (UTC)."""
    holdings = _holdings()
    if holdings.empty:
        return []
    news = db.get_news(since.strftime("%Y-%m-%dT%H:%M:%S"), [int(i) for i in holdings["security_id"]])
    items = _strongest(news, holdings, DIGEST_MIN_STRENGTH, one_per_security=True)[:DIGEST_MAX_ITEMS]
    if not items:
        return []
    return ["📰 *News on your holdings:*"] + [_line(i) for i in items] + [""]


def _enabled() -> bool:
    return str(get_config("news_alerts") or "false").lower() in ("1", "true", "yes")


def send_news_alerts(notify: Notify, now: Optional[pd.Timestamp] = None) -> int:
    """Send newly fetched, strongly worded headlines about holdings. Returns how many."""
    if not _enabled():
        return 0
    now = now if now is not None else pd.Timestamp.utcnow().replace(tzinfo=None)
    mark = get_config("news_alerts_sent_until")
    if not mark:
        # First run after switching on: start from now instead of sending the backlog.
        set_config("news_alerts_sent_until", now.strftime("%Y-%m-%dT%H:%M:%S"))
        return 0
    if quiet_hours_active():
        return 0   # keep the mark; they go out after quiet hours
    holdings = _holdings()
    if holdings.empty:
        return 0
    news = db.get_news_fetched_since(str(mark), [int(i) for i in holdings["security_id"]])
    if news.empty:
        return 0
    try:
        threshold = float(get_config("news_alert_threshold") or 0.5)
    except (TypeError, ValueError):
        threshold = 0.5
    items = _strongest(news, holdings, threshold, one_per_security=False)
    new_mark = str(news["fetched_at"].max())
    if items:
        shown = items[:ALERT_MAX_ITEMS]
        lines = ["📰 *News on your holdings*"] + [_line(i) for i in shown]
        if len(items) > len(shown):
            lines.append(f"  _+{len(items) - len(shown)} more in Vestis_")
        if not notify("\n".join(lines), vestis_link("📰 Open News", tab="news")):
            logging.warning("News alert could not be sent; will retry next run")
            return 0
    set_config("news_alerts_sent_until", new_mark)
    return len(items)
