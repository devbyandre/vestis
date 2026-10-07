"""News on Telegram, grouped by holding: a digest section and optional instant
alerts for strongly worded headlines.

Each holding gets a verdict (critical / negative / mixed / positive /
neutral), its positive/negative/neutral mix and average score, and its
strongest headlines; the most worrying holdings come first. Sentiment is a
rough headline + abstract score, so instant alerts are opt-in (setting
news_alerts) and only fire beyond news_alert_threshold.
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

DIGEST_MAX_SECURITIES = 8
ALERT_MAX_SECURITIES = 5
HEADLINES_PER_SECURITY = 2
LABEL_MIN = 0.2              # a headline counts as positive/negative from here (as label_for)
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


def _verdict(scores: pd.Series) -> tuple:
    """(rank, icon, word) for a security's headline scores; lower rank = more urgent."""
    avg = float(scores.mean())
    pos, neg = int((scores >= LABEL_MIN).sum()), int((scores <= -LABEL_MIN).sum())
    if avg <= -0.4 or int((scores <= -0.5).sum()) >= 2:
        return 0, "🔴", "critical"
    if avg <= -0.15:
        return 1, "🟠", "negative"
    if pos and neg and abs(avg) < 0.15:
        return 2, "⚪", "mixed"
    if avg >= 0.15:
        return 3, "🟢", "positive"
    return 4, "⚪", "neutral"


def _grouped(news: pd.DataFrame, holdings: pd.DataFrame) -> List[dict]:
    """One entry per security: verdict, mix of its headlines and the strongest ones."""
    if news.empty:
        return []
    news = news.dropna(subset=["sentiment"]).copy()
    if news.empty:
        return []
    names = dict(zip(holdings["security_id"].astype(int), holdings["name"]))
    news["key"] = news["title"].str.lower().str.replace(r"[^a-z0-9]+", "", regex=True).str[:80]
    news["strength"] = news["sentiment"].abs()
    groups = []
    for sid, g in news.drop_duplicates(["security_id", "key"]).groupby("security_id"):
        scores = g["sentiment"].astype(float)
        rank, icon, word = _verdict(scores)
        top = g.sort_values("strength", ascending=False)
        groups.append({
            "security_id": int(sid), "symbol": g["symbol"].iloc[0],
            "name": _short(names.get(int(sid)), g["symbol"].iloc[0]),
            "rank": rank, "icon": icon, "verdict": word, "avg": float(scores.mean()),
            "pos": int((scores >= LABEL_MIN).sum()), "neg": int((scores <= -LABEL_MIN).sum()),
            "neutral": int((scores.abs() < LABEL_MIN).sum()), "count": len(g),
            "headlines": top[top["strength"] >= LABEL_MIN].to_dict("records"),
        })
    groups.sort(key=lambda x: (x["rank"], x["avg"] if x["rank"] < 2 else -x["avg"], -x["count"]))
    return groups


def _headline(item: dict) -> str:
    icon = "📈" if item["sentiment"] > 0 else "📉"
    title = md(str(item["title"]).replace("]", ")"))
    url = str(item.get("url") or "")
    head = f"[{title}]({url.replace(')', '%29')})" if url.startswith(("http://", "https://")) else title
    publisher = f" — _{md(item['publisher'])}_" if item.get("publisher") else ""
    return f"     • {icon} {head}{publisher}"


def _group_lines(group: dict, headlines: List[dict]) -> List[str]:
    mix = f"{group['pos']}↑ {group['neg']}↓ {group['neutral']}→"
    head = (f"{group['icon']} *{md(group['name'])}* ({md(group['symbol'])}) — {group['verdict']} · "
            f"avg {group['avg']:+.2f} · {mix}")
    return [head] + [_headline(h) for h in headlines[:HEADLINES_PER_SECURITY]]


def news_digest_lines(since: pd.Timestamp) -> List[str]:
    """Digest section: per holding with news since `since` (UTC), its verdict,
    the positive/negative/neutral mix and its strongest headlines. Holdings with
    only neutral news are just counted."""
    holdings = _holdings()
    if holdings.empty:
        return []
    news = db.get_news(since.strftime("%Y-%m-%dT%H:%M:%S"), [int(i) for i in holdings["security_id"]])
    groups = _grouped(news, holdings)
    notable = [g for g in groups if g["verdict"] != "neutral"]
    quiet = len(groups) - len(notable)
    if not notable and not quiet:
        return []
    lines = ["📰 *News on your holdings:*"]
    for g in notable[:DIGEST_MAX_SECURITIES]:
        lines += _group_lines(g, g["headlines"])
    extra = len(notable) - DIGEST_MAX_SECURITIES
    if extra > 0:
        lines.append(f"  _+{extra} more holdings with news in Vestis_")
    if quiet:
        lines.append(f"  _{quiet} more holding{'s' if quiet != 1 else ''} with neutral news only_")
    return lines + [""]


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
    new_mark = str(news["fetched_at"].max())
    # Securities with at least one strongly worded new headline; the verdict and
    # mix cover all of that security's new headlines for context.
    groups = []
    for g in _grouped(news, holdings):
        strong = [h for h in g["headlines"] if abs(h["sentiment"]) >= threshold]
        if strong:
            groups.append((g, strong))
    sent = sum(len(strong) for _, strong in groups)
    if groups:
        lines = ["📰 *News on your holdings*"]
        for g, strong in groups[:ALERT_MAX_SECURITIES]:
            lines += _group_lines(g, strong)
        if len(groups) > ALERT_MAX_SECURITIES:
            lines.append(f"  _+{len(groups) - ALERT_MAX_SECURITIES} more holdings in Vestis_")
        if not notify("\n".join(lines), vestis_link("📰 Open News", tab="news")):
            logging.warning("News alert could not be sent; will retry next run")
            return 0
    set_config("news_alerts_sent_until", new_mark)
    return sent
