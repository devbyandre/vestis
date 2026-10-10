"""News on Telegram, grouped by holding: a digest section and optional instant
alerts for urgent news.

Digest: each holding gets a verdict (critical / negative / mixed / positive /
neutral), its positive/negative/neutral mix and average score, and its
strongest headlines; the most worrying holdings come first.

Instant alerts (opt-in, setting news_alerts) are for news that should not wait
for the digest. Sentiment is only a rough headline score, so a strongly worded
headline alone is not enough. A holding is urgent when
- a new headline reports an urgent material event (profit warning, guidance
  change, earnings beat/miss, takeover, fraud, insolvency, trading halt,
  dividend cut, management exit — see sentiment.material_event), or
- COVERAGE_SOURCES different publishers carried negative headlines on it
  (score <= -news_alert_threshold) within COVERAGE_HOURS.
Roundups ("Disney, Netflix, Visa and more") never count. A holding is not
alerted twice for the same reason within COOLDOWN_HOURS, all urgent holdings of
a run go into one message, and at most news_alert_daily_max messages go out
per 24 hours (critical events — CAP_EXEMPT — always go out). Everything else
waits for the digest.
"""
import json
import logging
import re
from typing import Callable, List, Optional

import pandas as pd

import db_utils as db
from config_utils import get_config, set_config
from .messages import md, vestis_link
from .quiet_hours import quiet_hours_active
from sentiment import material_event

Notify = Callable[[str, Optional[tuple]], bool]

DIGEST_MAX_SECURITIES = 8
ALERT_MAX_SECURITIES = 5
HEADLINES_PER_SECURITY = 2
LABEL_MIN = 0.2              # a headline counts as positive/negative from here (as label_for)
COVERAGE_SOURCES = 3
COVERAGE_HOURS = 6
COOLDOWN_HOURS = 24
DAILY_MAX = 3
CAP_EXEMPT = {"profit_warning", "insolvency", "fraud", "halt", "takeover"}
# Lists and market wraps that merely name a holding among others.
_ROUNDUP = re.compile(
    r"\band more\b|final trades|stocks making|biggest (?:moves|movers)|(?:pre-?market|midday|after-?hours) movers|"
    r"by market cap|stocks? to (?:buy|watch|own|target)|\b(?:top|best|\d+) (?:[\w-]+ ){0,3}(?:stocks|companies|picks|names|etfs)\b|"
    r"\b[A-Z][\w.&'’-]+(?:, [A-Z][\w.&'’-]+){2,}", re.I)
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


def is_roundup(title: str) -> bool:
    return bool(_ROUNDUP.search(str(title or "")))


_OTHER_PARTY = r"(?:partners?|suppliers?|rivals?|peers?|customers?|competitors?|clients?|vendors?)"


def _names_holding(title: str, name: str, symbol: str) -> bool:
    """The headline is about the holding itself, not its partner, supplier or rival."""
    keys = [re.escape(k) for k in {name, re.sub(r"[.\-].*$", "", symbol)} if len(k) >= 2]
    rx = "(?:%s)" % "|".join(keys)
    if not re.search(rf"(?<![\w$]){rx}(?!\w)", title, re.I):
        return False
    return not re.search(rf"(?<!\w){rx}(?:['’]s)? {_OTHER_PARTY}\b|\b{_OTHER_PARTY} (?:of|to) {rx}(?!\w)", title, re.I)


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
    news = news.dropna(subset=["sentiment"])
    news = news[~news["title"].map(is_roundup)].copy()
    if news.empty:
        return []
    names = dict(zip(holdings["security_id"].astype(int), holdings["name"]))
    news["key"] = news["title"].str.lower().str.replace(r"[^a-z0-9]+", "", regex=True).str[:80]
    news["strength"] = news["sentiment"].abs()
    news["event"] = news["title"].map(material_event)
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
            "headlines": top[(top["strength"] >= LABEL_MIN) | top["event"].notna()].to_dict("records"),
        })
    groups.sort(key=lambda x: (x["rank"], x["avg"] if x["rank"] < 2 else -x["avg"], -x["count"]))
    return groups


def _headline(item: dict) -> str:
    event = item.get("event")
    direction = event["direction"] if event else item["sentiment"]
    icon = "📈" if direction > 0 else "📉" if direction < 0 else "•"
    title = md(str(item["title"]).replace("]", ")"))
    url = str(item.get("url") or "")
    head = f"[{title}]({url.replace(')', '%29')})" if url.startswith(("http://", "https://")) else title
    publisher = f" — _{md(item['publisher'])}_" if item.get("publisher") else ""
    tag = f" · *{event['label']}*" if event else ""
    return f"     • {icon} {head}{publisher}{tag}"


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
    notable = [g for g in groups if g["verdict"] != "neutral" or any(h["event"] for h in g["headlines"])]
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


def _iso(ts: pd.Timestamp) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S")


def _setting(key: str, default: float) -> float:
    try:
        return float(get_config(key))
    except (TypeError, ValueError):
        return default


def _state() -> dict:
    raw = get_config("news_alert_state")
    try:
        state = json.loads(raw) if isinstance(raw, str) else (raw or {})
    except ValueError:
        state = {}
    return {"sent": list(state.get("sent") or []), "cooldown": dict(state.get("cooldown") or {})}


def _urgent(new: pd.DataFrame, recent: pd.DataFrame, holdings: pd.DataFrame,
            threshold: float, cooldown: dict, now: pd.Timestamp) -> List[dict]:
    """Holdings with urgent news among the `new` headlines: one entry each with
    its reasons [(key, label, direction)] and the headlines behind them."""
    since = _iso(now - pd.Timedelta(hours=COOLDOWN_HOURS))
    groups = {g["security_id"]: g for g in _grouped(new, holdings)}
    coverage = {g["security_id"]: g for g in _grouped(recent, holdings)}
    out = []
    for sid, g in groups.items():
        reasons, headlines = [], []
        for h in g["headlines"]:
            ev = h["event"]
            # an event worded against its own direction ("avoids insolvency") is not trusted
            if not ev or not ev["urgent"] or ev["direction"] * h["sentiment"] <= -LABEL_MIN \
                    or not _names_holding(h["title"], g["name"], g["symbol"]):
                continue
            if ev["key"] not in [r[0] for r in reasons]:
                reasons.append((ev["key"], ev["label"], ev["direction"]))
            headlines.append(h)
        c = coverage.get(sid)
        if c:
            neg = [h for h in c["headlines"] if h["sentiment"] <= -threshold]
            sources = {str(h.get("publisher") or h.get("url")) for h in neg}
            if len(sources) >= COVERAGE_SOURCES and any(h["uid"] in set(new["uid"]) for h in neg):
                reasons.append(("coverage", f"negative coverage from {len(sources)} sources", -1))
                headlines += [h for h in neg if h not in headlines]
        reasons = [r for r in reasons if cooldown.get(f"{sid}:{r[0]}", "") < since]
        if reasons:
            keys = {r[0] for r in reasons}
            shown = [h for h in headlines if (h["event"] or {}).get("key") in keys
                     or ("coverage" in keys and h["sentiment"] <= -threshold)]
            out.append({**g, "reasons": reasons, "urgent_headlines": shown})
    out.sort(key=lambda u: (min(r[2] for r in u["reasons"]), u["avg"]))   # bad news first
    return out


def _urgent_lines(u: dict) -> List[str]:
    icon = "🔴" if any(r[2] < 0 for r in u["reasons"]) else "🟢"
    why = ", ".join(r[1] for r in u["reasons"])
    return ([f"{icon} *{md(u['name'])}* ({md(u['symbol'])}) — {md(why)}"]
            + [_headline(h) for h in u["urgent_headlines"][:HEADLINES_PER_SECURITY]])


def send_news_alerts(notify: Notify, now: Optional[pd.Timestamp] = None) -> int:
    """Send urgent news on holdings fetched since the last run, bundled into one
    message. Returns how many headlines went out."""
    if not _enabled():
        return 0
    now = now if now is not None else pd.Timestamp.utcnow().replace(tzinfo=None)
    mark = get_config("news_alerts_sent_until")
    if not mark:
        # First run after switching on: start from now instead of sending the backlog.
        set_config("news_alerts_sent_until", _iso(now))
        return 0
    if quiet_hours_active():
        return 0   # keep the mark; they go out after quiet hours
    holdings = _holdings()
    if holdings.empty:
        return 0
    new = db.get_news_fetched_since(str(mark), [int(i) for i in holdings["security_id"]])
    if new.empty:
        return 0
    new_mark = str(new["fetched_at"].max())
    threshold = _setting("news_alert_threshold", 0.5)
    recent = db.get_news(_iso(now - pd.Timedelta(hours=COVERAGE_HOURS)),
                         sorted({int(i) for i in new["security_id"]}))
    state = _state()
    urgent = _urgent(new, recent, holdings, threshold, state["cooldown"], now)

    day_ago = _iso(now - pd.Timedelta(hours=24))
    state["sent"] = [t for t in state["sent"] if t >= day_ago]
    if urgent and len(state["sent"]) >= _setting("news_alert_daily_max", DAILY_MAX):
        held_back = len(urgent)
        urgent = [u for u in urgent if any(r[0] in CAP_EXEMPT for r in u["reasons"])]
        if held_back > len(urgent):
            logging.info("News alerts: daily maximum reached, %d holding(s) wait for the digest",
                         held_back - len(urgent))
    sent = 0
    if urgent:
        lines = ["📰 *Urgent news on your holdings*"]
        for u in urgent[:ALERT_MAX_SECURITIES]:
            lines += _urgent_lines(u)
        if len(urgent) > ALERT_MAX_SECURITIES:
            lines.append(f"  _+{len(urgent) - ALERT_MAX_SECURITIES} more holdings in Vestis_")
        lines.append("_Other news waits for the daily digest._")
        if not notify("\n".join(lines), vestis_link("📰 Open News", tab="news")):
            logging.warning("News alert could not be sent; will retry next run")
            return 0
        stamp = _iso(now)
        state["sent"].append(stamp)
        for u in urgent:
            for key, _label, _dir in u["reasons"]:
                state["cooldown"][f"{u['security_id']}:{key}"] = stamp
        sent = sum(len(u["urgent_headlines"][:HEADLINES_PER_SECURITY]) for u in urgent[:ALERT_MAX_SECURITIES])
    state["cooldown"] = {k: v for k, v in state["cooldown"].items() if v >= day_ago}
    set_config("news_alert_state", json.dumps(state))
    set_config("news_alerts_sent_until", new_mark)
    return sent
