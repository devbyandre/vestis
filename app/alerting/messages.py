"""Message text helpers shared by the alert engine and the digest (pure, no DB)."""
import json
from urllib.parse import urlencode

import telegram_client as tg


def md(text: str) -> str:
    return tg.md_escape(text)


def vestis_link(label: str = "🔎 Open in Vestis", **params) -> tuple | None:
    """(label, url) for the 'open Vestis' button, or None if no URL is configured.
    params become the UI's query string, e.g. tab="technical", symbol="TSM"."""
    url = tg.get_vestis_url()
    if not url:
        return None
    query = urlencode({k: v for k, v in params.items() if v})
    return (label, f"{url}/?{query}" if query else url)


def describe_alert(alert_type: str, params) -> str:
    try:
        p = params if isinstance(params, dict) else json.loads(params or "{}")
    except Exception:
        p = {}
    if alert_type == "price":
        direction = p.get("direction", "")
        threshold = p.get("threshold")
        mode = p.get("mode", "absolute")
        arrow = "above" if direction == "above" else "below"
        icon = "📈" if direction == "above" else "📉"
        if threshold and mode == "absolute":
            return f"{icon} Price crossed {arrow} {float(threshold):.2f}"
        elif threshold and mode == "relative":
            return f"{icon} Price crossed {arrow} {float(threshold)*100:.1f}% threshold"
        return "Price crossing alert"
    if alert_type == "pct_change":
        pct = p.get("pct", 5)
        days = p.get("days", 1)
        direction = p.get("direction", "down")
        icon = "📉" if direction == "down" else "📈"
        period = f"over {days} trading days" if int(days) > 1 else "vs previous close"
        verb = "drop" if direction == "down" else "rise"
        return f"{icon} Sudden {verb} >{pct}% {period}"
    if alert_type == "earnings_soon":
        days = p.get("days", 3)
        return f"📅 Earnings in ≤{days} days"
    if alert_type == "rsi":
        if "direction" in p:
            direction = p.get("direction", "above")
            thr = p.get("threshold", 70)
            if direction == "above":
                return f"📈 RSI overbought — crossed above {thr}"
            else:
                return f"📉 RSI oversold — crossed below {thr}"
        # Legacy combined format
        ob = p.get("overbought", 70)
        os_ = p.get("underbought", 30)
        return f"RSI zone crossed (overbought >{ob} / oversold <{os_})"
    if alert_type in ("ma_crossover", "golden_cross", "death_cross"):
        short = p.get("short", 50)
        long_ = p.get("long", 200)
        ct = p.get("crossover_type", "golden")
        icon = "☀️" if ct == "golden" else "💀"
        return f"{icon} {ct.capitalize()} cross ({short}/{long_} MA)"
    if alert_type == "52w":
        typ = p.get("type", "high")
        return "🏔️ New 52-week high" if typ == "high" else "🕳️ New 52-week low"
    if alert_type == "volume_spike":
        mult = p.get("multiplier", 2)
        return f"📊 Volume spike >{mult}x average"
    if alert_type == "split_pending":
        split_date = p.get("split_date", "?")
        ratio = p.get("ratio", "?")
        return f"🔀 Unrecorded stock split: ratio {ratio} on {split_date} — please add a split transaction in Vestis"
    return alert_type.replace("_", " ").title()
