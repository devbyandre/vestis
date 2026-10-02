"""Alert engine: evaluate active alerts, deliver or hold what fired.

Delivery is injected as `notify(text, link) -> bool`, so nothing here knows
about Telegram.
"""
import logging
from typing import Callable, Optional

import pandas as pd

import middleware as mw
from config_utils import get_config, set_config
from .messages import md, vestis_link, describe_alert
from .quiet_hours import quiet_hours_active

Notify = Callable[[str, Optional[tuple]], bool]


def run_immediate(notify: Notify) -> None:
    """Evaluate all active alerts once; deliver what fired through `notify`."""
    quiet = quiet_hours_active()
    if quiet:
        logging.info("Quiet hours — alerts are evaluated and held until they end")
    else:
        flush_held(notify)
    alerts_df = mw.get_alerts()
    if alerts_df.empty:
        logging.info("No active alerts")
        return
    sec_df = mw.get_all_securities()[['id', 'symbol', 'name']].rename(columns={'id': 'security_id'})
    alerts_df = alerts_df.merge(sec_df, on="security_id", how="left", suffixes=('_alert', '_sec'))
    alerts_df['symbol'] = alerts_df.get('symbol_sec', alerts_df.get('symbol_alert', '??')).fillna('??')
    alerts_df['name']   = alerts_df.get('name', pd.Series(dtype=str)).fillna('')
    now = pd.Timestamp.utcnow().replace(tzinfo=None)
    fired_count = digest_count = held_count = 0
    for symbol, group in alerts_df.groupby('symbol'):
        sec_name = group['name'].iloc[0]
        fired_lines = []
        for alert in group.to_dict(orient='records'):
            alert_id = alert['id']
            notify_mode = alert.get('notify_mode') or 'immediate'
            alert_type_val = alert.get('alert_type', '')
            # split_pending alerts repeat every 24h until user records the split
            if alert_type_val == 'split_pending':
                cooldown = 86400  # remind once per day
            else:
                cooldown = int(alert.get('cooldown_seconds') or 14400)
            last_trigger = mw.last_trigger(alert_id)
            on_cooldown = bool(last_trigger) and \
                (now - last_trigger.replace(tzinfo=None)).total_seconds() < cooldown
            # split_pending alerts are always triggered (fire until split is recorded)
            if alert_type_val == 'split_pending':
                if on_cooldown or quiet:
                    continue
                triggered = True
            else:
                # Evaluate even on cooldown so edge state (side / armed / last
                # bar) keeps tracking the market between notifications.
                try:
                    triggered = mw.evaluate_alert(alert)
                except Exception:
                    logging.exception("Error evaluating alert %s", alert_id)
                    continue
                if not triggered:
                    mw.save_alert_state(alert)
                    continue
                if on_cooldown:
                    # Keep the old state so this edge is still seen once the cooldown ends
                    logging.debug("Alert %s triggered but on cooldown", alert_id)
                    continue
            if notify_mode.startswith('digest'):
                # Logged now, reported by send_digest() for this notify_mode
                mw.log_trigger(alert_id, log_payload(alert, notify_mode))
                mw.save_alert_state(alert)
                digest_count += 1
                continue
            if quiet:
                # Triggered inside quiet hours: logged now, delivered by flush_held()
                mw.log_trigger(alert_id, log_payload(alert, "held"))
                mw.save_alert_state(alert)
                held_count += 1
                continue
            description = describe_alert(alert_type_val, alert.get('params', ''))
            line = f"  • {description}"
            if alert.get('_detail'):
                line += f"\n      {md(alert['_detail'])}"
            if alert.get('note'):
                line += f"\n      _{md(alert['note'])}_"
            fired_lines.append((alert_id, line, alert))
        if not fired_lines:
            continue
        label = f"*{md(symbol)}*"
        if sec_name:
            label += f" ({md(sec_name)})"
        header = f"⚡ *Alert* — {label}"
        body = "\n".join([header] + [l for _, l, _ in fired_lines])
        body += f"\n\n_{now.strftime('%Y-%m-%d %H:%M UTC')}_"
        link = vestis_link(f"🔎 Open {symbol} in Vestis", tab="technical", symbol=symbol)
        if notify(body, link):
            for alert_id, _, al in fired_lines:
                mw.log_trigger(alert_id, log_payload(al, "immediate"))
                mw.save_alert_state(al)
            fired_count += len(fired_lines)
    logging.info("Immediate run complete — %d alerts sent, %d logged for digest, %d held",
                 fired_count, digest_count, held_count)

def flush_held(notify: Notify) -> int:
    """Send one summary of the alerts held during quiet hours. Returns how many."""
    key = "last_held_flush"
    last = get_config(key)
    since = pd.Timestamp(last) if last else pd.Timestamp.min
    if since.tzinfo is not None:
        since = since.tz_convert("UTC").tz_localize(None)
    held = []
    for r in mw.get_alert_history(limit=500):
        if r.get("delivery") != "held":
            continue
        ts = pd.Timestamp(r["triggered_at"])
        if ts.tzinfo is not None:
            ts = ts.tz_convert("UTC").tz_localize(None)
        if ts > since:
            held.append((ts, r))
    if not held:
        return 0
    held.sort(key=lambda x: x[0])
    lines = [f"🌙 *Held during quiet hours* ({len(held)})"]
    for ts, r in held[:25]:
        desc = describe_alert(r.get("alert_type", ""), r.get("params", ""))
        line = f"  • *{md(r.get('symbol') or '??')}* — {desc}"
        if r.get("detail"):
            line += f"\n      {md(r['detail'])}"
        lines.append(line + f"\n      _{ts.strftime('%a %H:%M')} UTC_")
    if len(held) > 25:
        lines.append(f"  … and {len(held) - 25} more")
    if notify("\n".join(lines), vestis_link("🔎 Open alert history", tab="alerts", mode="history")):
        set_config(key, pd.Timestamp.utcnow().replace(tzinfo=None).isoformat())
        return len(held)
    return 0


def log_payload(alert: dict, note: str) -> dict:
    payload = {"note": note}
    if alert.get("_curr_side"):
        payload["side"] = alert["_curr_side"]
    if alert.get("_detail"):
        payload["detail"] = alert["_detail"]
    return payload
