#!/usr/bin/env python3
"""
telegram_worker.py — Vestis alert engine
"""
import argparse
import logging
import time
import re
import json
import pandas as pd
import requests
import middleware as mw
from config_utils import get_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

def _md(text: str) -> str:
    if not isinstance(text, str):
        text = str(text)
    return re.sub(r'([_*\[\]()~`>#+\-=|{}.!])', r'\\\1', text)

def _get_creds(cli_token=None, cli_chat=None):
    token = cli_token or get_config("telegram_bot_token")
    chat  = cli_chat  or get_config("telegram_chat_id")
    return token, chat

def send_telegram(token: str, chat_id: str, text: str, max_retries: int = 3) -> bool:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.post(url, json=payload, timeout=10)
            if r.status_code == 200:
                logging.info("Telegram message sent")
                return True
            logging.warning("Telegram send failed %s: %s", r.status_code, r.text[:200])
        except Exception:
            logging.exception("Telegram send exception")
        time.sleep(backoff)
        backoff *= 2
    return False

def _describe_alert(alert_type: str, params) -> str:
    try:
        p = params if isinstance(params, dict) else json.loads(params or "{}")
    except Exception:
        p = {}
    if alert_type == "price":
        direction = p.get("direction", "")
        threshold = p.get("threshold")
        arrow = "above" if direction == "above" else "below"
        icon = "📈" if direction == "above" else "📉"
        if threshold:
            return f"{icon} Price {arrow} {float(threshold):.2f}"
        return "Price alert"
    if alert_type == "rsi":
        ob = p.get("overbought", 70)
        os_ = p.get("underbought", 30)
        return f"RSI overbought >{ob} or oversold <{os_}"
    if alert_type in ("ma_crossover", "golden_cross", "death_cross"):
        short = p.get("short", 20)
        long_ = p.get("long", 50)
        ct = p.get("crossover_type", "golden")
        icon = "☀️" if ct == "golden" else "💀"
        return f"{icon} {ct.capitalize()} cross ({short}/{long_} MA)"
    if alert_type == "52w":
        typ = p.get("type", "high")
        return "🏔️ New 52-week high" if typ == "high" else "🕳️ New 52-week low"
    if alert_type == "volume_spike":
        mult = p.get("multiplier", 2)
        return f"📊 Volume spike >{mult}x average"
    return alert_type.replace("_", " ").title()

def run_immediate(cli_token=None, cli_chat=None):
    token, chat = _get_creds(cli_token, cli_chat)
    if not token or not chat:
        logging.error("Telegram token/chat not configured.")
        return
    dnd_enabled = str(get_config("dnd") or "false").lower() in ("1", "true", "yes")
    if dnd_enabled:
        logging.info("DND enabled — skipping immediate alerts")
        return
    alerts_df = mw.get_alerts()
    if alerts_df.empty:
        logging.info("No active alerts")
        return
    sec_df = mw.get_all_securities()[['id', 'symbol', 'name']].rename(columns={'id': 'security_id'})
    alerts_df = alerts_df.merge(sec_df, on="security_id", how="left", suffixes=('_alert', '_sec'))
    alerts_df['symbol'] = alerts_df.get('symbol_sec', alerts_df.get('symbol_alert', '??')).fillna('??')
    alerts_df['name']   = alerts_df.get('name', pd.Series(dtype=str)).fillna('')
    now = pd.Timestamp.utcnow()
    fired_count = 0
    for symbol, group in alerts_df.groupby('symbol'):
        sec_name = group['name'].iloc[0]
        fired_lines = []
        for alert in group.to_dict(orient='records'):
            alert_id = alert['id']
            notify_mode = alert.get('notify_mode') or 'immediate'
            if notify_mode.startswith('digest'):
                continue
            cooldown = int(alert.get('cooldown_seconds') or 14400)
            last_trigger = mw.last_trigger(alert_id)
            if last_trigger and (now - last_trigger).total_seconds() < cooldown:
                logging.debug("Alert %s on cooldown", alert_id)
                continue
            try:
                triggered = mw.evaluate_alert(alert)
            except Exception:
                logging.exception("Error evaluating alert %s", alert_id)
                continue
            if not triggered:
                continue
            description = _describe_alert(alert.get('alert_type', ''), alert.get('params', ''))
            line = f"  • {description}"
            if alert.get('note'):
                line += f" — {alert['note']}"
            fired_lines.append((alert_id, line))
        if not fired_lines:
            continue
        label = f"*{_md(symbol)}*"
        if sec_name:
            label += f" ({_md(sec_name)})"
        header = f"⚡ *Alert* — {label}"
        body = "\n".join([header] + [l for _, l in fired_lines])
        body += f"\n\n_{now.strftime('%Y-%m-%d %H:%M UTC')}_"
        if send_telegram(token, chat, body):
            for alert_id, _ in fired_lines:
                mw.log_trigger(alert_id, {"note": "immediate"})
            fired_count += len(fired_lines)
    logging.info("Immediate run complete — %d alerts fired", fired_count)

def send_digest(cli_token=None, cli_chat=None, freq="daily"):
    token, chat = _get_creds(cli_token, cli_chat)
    if not token or not chat:
        logging.error("Telegram token/chat not configured.")
        return
    now = pd.Timestamp.utcnow()
    key = f"last_digest_sent_{freq}"
    last_sent_raw = get_config(key)
    since_ts = pd.Timestamp(last_sent_raw) if last_sent_raw else now - pd.Timedelta(days=1)
    parts = []
    try:
        snap = mw.get_latest_holdings_snapshot(aggregate=True)
        if not snap.empty:
            total_mv   = snap['market_value'].sum()
            total_cost = snap['cost_basis'].sum()
            total_pnl  = total_mv - total_cost
            pnl_pct    = (total_pnl / total_cost * 100) if total_cost else 0
            pnl_icon   = "📈" if total_pnl >= 0 else "📉"
            parts.append("📊 *Daily Portfolio Digest*")
            parts.append("")
            parts.append(f"💰 Total value: *{total_mv:,.0f}*")
            parts.append(f"{pnl_icon} P&L: *{total_pnl:+,.0f}* ({pnl_pct:+.1f}%)")
            parts.append("")
            snap['pnl_pct'] = (snap['market_value'] - snap['cost_basis']) / snap['cost_basis'].replace(0, float('nan')) * 100
            snap = snap.dropna(subset=['pnl_pct'])
            if not snap.empty:
                top3 = snap.nlargest(3, 'pnl_pct')
                bot3 = snap.nsmallest(3, 'pnl_pct')
                parts.append("🏆 *Top gainers:*")
                for _, r in top3.iterrows():
                    parts.append(f"  • {_md(str(r.get('symbol','?')))}: {r['pnl_pct']:+.1f}%")
                parts.append("⚠️ *Underperformers:*")
                for _, r in bot3.iterrows():
                    parts.append(f"  • {_md(str(r.get('symbol','?')))}: {r['pnl_pct']:+.1f}%")
                parts.append("")
    except Exception:
        logging.exception("Error building portfolio snapshot for digest")
    try:
        notify_mode = f"digest_{freq}"
        rows = mw.get_alert_log_entries(since_ts.isoformat(), notify_mode)
        if rows:
            parts.append("🔔 *Alerts since last digest:*")
            for r in rows:
                try:
                    info = mw.get_security_basic(r.get('security_id'))
                    sym  = info.get('symbol', '?')
                except Exception:
                    sym = '?'
                desc = _describe_alert(r.get('alert_type', ''), r.get('params', '{}'))
                ts   = str(r.get('triggered_at', ''))[:16].replace('T', ' ')
                line = f"  • *{_md(sym)}* — {desc}"
                if r.get('note'):
                    line += f" — {r['note']}"
                if ts:
                    line += f" ({ts})"
                parts.append(line)
            parts.append("")
        else:
            parts.append("🔔 *No alerts fired since last digest*")
            parts.append("")
    except Exception:
        logging.exception("Error building alert digest")
    parts.append(f"_{now.strftime('%Y-%m-%d %H:%M UTC')}_")
    body = "\n".join(parts)
    if send_telegram(token, chat, body):
        mw.set_config(key, now.isoformat())
        logging.info("Digest (%s) sent", freq)

def test_telegram(cli_token=None, cli_chat=None):
    token, chat = _get_creds(cli_token, cli_chat)
    if not token or not chat:
        logging.error("Telegram token/chat not configured.")
        return
    send_telegram(token, chat, "✅ *Vestis* — Telegram connection test successful!")

def _ensure_alert(security_id, alert_type, params, note="",
                  notify_mode="immediate", cooldown_seconds=14400):
    alerts = mw.get_automatic_alerts()
    existing = alerts[
        (alerts['security_id'] == int(security_id)) &
        (alerts['alert_type'] == alert_type)
    ]
    params_str = json.dumps(params, sort_keys=True)
    if not existing.empty:
        alert = existing.iloc[0]
        current_params = json.loads(alert.get('params') or '{}')
        if json.dumps(current_params, sort_keys=True) != params_str or alert.get('note', '') != note:
            mw.edit_alert(alert_id=int(alert['id']), params=params, note=note, automatic=True)
    else:
        mw.create_alert(security_id=int(security_id), alert_type=alert_type,
                        params=params, note=note, notify_mode=notify_mode,
                        cooldown_seconds=cooldown_seconds, automatic=True)

def maintain_alerts():
    PROFIT_BUFFER = 0.05
    try:
        holdings = mw.get_holdings()
    except Exception:
        logging.exception("Could not load holdings for maintain_alerts")
        return
    for _, row in holdings.iterrows():
        symbol      = row['symbol']
        security_id = row['security_id']
        data = mw.fetch_symbol_data(symbol)
        if not data or 'last_price' not in data:
            logging.warning("fetch_symbol_data: no price history for %s", symbol)
            continue
        current_price = float(data['last_price'])
        buy_price = float(row.get('avg_cost') or row.get('buy_price') or current_price)
        if current_price > buy_price * 1.05:
            threshold = round(current_price * (1 - PROFIT_BUFFER), 4)
            _ensure_alert(security_id, "price",
                          {"threshold": threshold, "mode": "absolute", "direction": "below"},
                          note="Trailing stop — protect profits", cooldown_seconds=14400)
        elif current_price < buy_price * 0.95:
            _ensure_alert(security_id, "price",
                          {"threshold": buy_price, "mode": "absolute", "direction": "above"},
                          note="Recovery to buy price", notify_mode="digest_daily",
                          cooldown_seconds=86400)
    try:
        watchlist = mw.get_watchlist_symbols()
        for symbol in watchlist:
            sec = mw.get_security(symbol)
            if not sec:
                continue
            security_id = sec['id']
            _ensure_alert(security_id, "ma_crossover",
                          {"short": 20, "long": 50, "ma_type": "SMA", "crossover_type": "golden"},
                          note="Golden cross — potential entry", notify_mode="digest_daily",
                          cooldown_seconds=86400)
    except Exception:
        logging.exception("Error maintaining watchlist alerts")

def main():
    ap = argparse.ArgumentParser(description="Vestis Telegram alert worker")
    ap.add_argument("--digest", choices=["hourly", "daily", "weekly"], default=None)
    ap.add_argument("--token", default=None)
    ap.add_argument("--chat",  default=None)
    ap.add_argument("--test",  action="store_true")
    args = ap.parse_args()
    if args.test:
        test_telegram(args.token, args.chat)
        return
    maintain_alerts()
    run_immediate(args.token, args.chat)
    if args.digest:
        send_digest(args.token, args.chat, args.digest)

if __name__ == "__main__":
    main()
