#!/usr/bin/env python3
import argparse
import logging
import time
import pandas as pd
import requests
import middleware as mw
from config_utils import get_config
import json
import re

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# ---------- Telegram helper ----------
def escape_md(text: str) -> str:
    """Escape special MarkdownV2 characters for Telegram."""
    if not text:
        return ""
    # escape: _ * [ ] ( ) ~ ` > # + - = | { } . !
    return re.sub(r'([_*\[\]()~`>#+\-=|{}.!])', r'\\\1', str(text))

def _get_creds(cli_token=None, cli_chat=None):
    token = cli_token or get_config("telegram_bot_token")
    chat = cli_chat or get_config("telegram_chat_id")
    return token, chat

def send_telegram(token: str, chat_id: str, text: str, max_retries: int = 3) -> bool:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.post(url, json=payload, timeout=10)
            if r.status_code == 200:
                logging.info("Telegram message sent ✅")
                return True
            logging.warning("Telegram send failed ❌ %s: %s", r.status_code, r.text)
        except Exception:
            logging.exception("Telegram send exception ❌")
        time.sleep(backoff)
        backoff *= 2
    return False

# ---------- Immediate alerts ----------
def run_immediate(cli_token=None, cli_chat=None):
    token, chat = _get_creds(cli_token, cli_chat)
    if not token or not chat:
        logging.error("Telegram token/chat not configured.")
        return

    dnd_raw = get_config("dnd")
    dnd_enabled = str(dnd_raw or "false").lower() in ("1", "true", "yes")

    alerts_df = mw.get_alerts()
    if alerts_df.empty:
        logging.info("No active alerts found")
        return

    # Merge with securities and normalize columns
    sec_df = mw.get_all_securities()[['id', 'symbol', 'name']].rename(columns={'id': 'security_id'})
    alerts_df = alerts_df.merge(sec_df, on="security_id", how="left", suffixes=('_alert', '_sec'))

    # Create final clean columns
    alerts_df['symbol'] = alerts_df['symbol_sec'].fillna(alerts_df['symbol_alert']).fillna('??')
    alerts_df['name'] = alerts_df['name'].fillna(alerts_df['security_name']).fillna('')
    alerts_df = alerts_df[['id','security_id','symbol','name','alert_type','params',
                           'cooldown_seconds','notify_mode','note']]  # drop dupes
    alerts_df = alerts_df.sort_values('symbol')

    # Group by symbol
    for symbol, group in alerts_df.groupby('symbol'):
        sec_name = group['name'].iloc[0]
        lines = [f"⚡ *Alert* — {escape_md(symbol)} {f'({escape_md(sec_name)})' if sec_name else ''}"]

        sent_any = False
        for alert in group.to_dict(orient="records"):
            try:
                # --- cooldown check ---
                last_trigger = mw.last_trigger(alert["id"])
                cooldown = int(alert.get("cooldown_seconds") or 3600)
                now = pd.Timestamp.utcnow()
                if last_trigger and (now - last_trigger).total_seconds() < cooldown:
                    logging.debug("Skipping alert %s due to cooldown", alert["id"])
                    continue

                # evaluate alert
                if not mw.evaluate_alert(alert):
                    continue

                # DND check
                if dnd_enabled and (alert.get("notify_mode") or "immediate") == "immediate":
                    logging.info("DND enabled; skipping immediate alert %s", alert["id"])
                    continue

                params = alert.get("params", "")
                lines.append(f"▫️ *Type:* {escape_md(alert['alert_type'])} — Params: `{escape_md(params)}`")
                if alert.get("note"):
                    lines.append(f"   📝 {escape_md(alert['note'])}")

                # NEW: add timestamp
                if last_trigger:
                    lines.append(f"   ⏰ Last triggered: {escape_md(last_trigger.strftime('%Y-%m-%d %H:%M UTC'))}")
                else:
                    lines.append("   ⏰ First time trigger")

                # log trigger AFTER sending message
                sent_any = True

            except Exception:
                logging.exception("Error processing alert %s", alert["id"])

        # send message if at least one alert fired
        if sent_any:
            if send_telegram(token, chat, "\n".join(lines)):
                # log triggers only AFTER successful send
                for alert in group.to_dict(orient="records"):
                    last_trigger = mw.last_trigger(alert["id"])
                    cooldown = int(alert.get("cooldown_seconds") or 3600)
                    now = pd.Timestamp.utcnow()
                    if not last_trigger or (now - last_trigger).total_seconds() >= cooldown:
                        mw.log_trigger(alert["id"], {"note": "sent_immediate"})



# ---------- Test alerts ----------
def test_telegram_alerts(cli_token=None, cli_chat=None):
    token, chat = _get_creds(cli_token, cli_chat)
    if not token or not chat:
        logging.error("Telegram token/chat not configured.")
        return

    logging.info("=== TESTING TELEGRAM ALERTS ===")
    alerts_df = mw.get_alerts()
    if alerts_df.empty:
        logging.warning("No alerts found to test")
        return

    # Merge with securities and normalize columns
    sec_df = mw.get_all_securities()[['id', 'symbol', 'name']].rename(columns={'id': 'security_id'})
    alerts_df = alerts_df.merge(sec_df, on="security_id", how="left", suffixes=('_alert', '_sec'))

    # Clean columns
    alerts_df['symbol'] = alerts_df['symbol_sec'].fillna(alerts_df['symbol_alert']).fillna('??')
    alerts_df['name'] = alerts_df['name'].fillna(alerts_df['security_name']).fillna('')
    alerts_df = alerts_df[['id','security_id','symbol','name','alert_type','params','note']].drop_duplicates()
    alerts_df = alerts_df.sort_values('symbol')

    # Group by symbol
    for symbol, group in alerts_df.groupby('symbol'):
        sec_name = group['name'].iloc[0]
        lines = [f"⚡ *TEST ALERT* — {escape_md(symbol)} {f'({escape_md(sec_name)})' if sec_name else ''}"]

        sent_any = False
        for alert in group.to_dict(orient="records"):
            try:
                if not mw.evaluate_alert(alert):
                    continue

                params = alert.get("params", "")
                lines.append(f"▫️ *Type:* {escape_md(alert['alert_type'])} — Params: `{escape_md(params)}`")
                if alert.get("note"):
                    lines.append(f"   📝 {escape_md(alert['note'])}")

                sent_any = True

            except Exception:
                logging.exception("Error testing alert %s", alert["id"])

        if sent_any:
            logging.info("Sending Telegram message for %s...", symbol)
            send_telegram(token, chat, "\n".join(lines))

    logging.info("=== TESTING COMPLETE ===")





# ---------- Digest notifications ----------
def _gather_digest_entries(since_ts, notify_mode):
    return mw.get_alert_log_entries(since_ts, notify_mode)

def send_digest(cli_token=None, cli_chat=None, freq="hourly"):
    token, chat = _get_creds(cli_token, cli_chat)
    if not token or not chat:
        logging.error("Telegram token/chat not configured.")
        return

    key = f"last_digest_sent_{freq}"
    last_sent = get_config(key)
    since_ts = last_sent or (pd.Timestamp.utcnow() - pd.Timedelta(days=7)).isoformat()
    notify_mode = f"digest_{freq}"

    rows = _gather_digest_entries(since_ts, notify_mode)
    if not rows:
        logging.info("No digest entries for %s since %s", notify_mode, since_ts)
        mw.set_config(key, pd.Timestamp.utcnow().isoformat())
        return

    # Group entries by symbol (security_id)
    grouped = {}
    for r in rows:
        sec_id = r.get("security_id")
        grouped.setdefault(sec_id, []).append(r)

    parts = [f"📊 Digest — {freq.capitalize()}", ""]

    for sec_id, alerts in grouped.items():
        sec_info = mw.get_security_basic(sec_id)
        sec_label = sec_info.get("symbol", f"ID {sec_id}")
        sec_name = sec_info.get("name", "")

        header = f"*{escape_md(sec_label)}*"
        if sec_name:
            header += f" ({escape_md(sec_name)})"

        lines = [f"⚡ Digest — {header}"]

        # Group by alert_type within this symbol
        by_type = {}
        for alert in alerts:
            t = alert["alert_type"]
            by_type.setdefault(t, {"count": 0, "notes": set()})
            by_type[t]["count"] += 1
            if alert.get("note"):
                by_type[t]["notes"].add(alert["note"])

        # Format each alert type
        for t, info in by_type.items():
            line = f"▫️ *Type:* {escape_md(t)} — Count: {info['count']}"
            if info["notes"]:
                line += " — 📝 " + "; ".join(info["notes"])
            lines.append(line)

        parts.extend(lines)
        parts.append("")  # empty line between symbols

    body = "\n".join(parts)
    if send_telegram(token, chat, body):
        mw.set_config(key, pd.Timestamp.utcnow().isoformat())
        logging.info("Digest %s sent", freq)




# ---------- Automatic alert management ----------
def ensure_alert(security_id, alert_type, params, note="", notify_mode="immediate", cooldown_seconds=3600):
    """
    Create or update an automatic alert if needed.
    """
    # Get only automatic alerts
    alerts = mw.get_automatic_alerts()  # should return alerts with automatic=True
    existing = alerts[
        (alerts['security_id'] == security_id) &
        (alerts['alert_type'] == alert_type)
    ]

    params_json = json.dumps(params, sort_keys=True)

    if not existing.empty:
        # Only take the first match (should be unique)
        alert = existing.iloc[0]
        existing_params = json.loads(alert.get('params') or "{}")
        if existing_params != params or (alert.get('note', '') != note):
            logging.info("Updating automatic alert %s (%s)", security_id, alert_type)
            mw.edit_alert(
                alert_id=alert['id'],
                params=params,
                note=note,
                automatic=True
            )
        else:
            logging.debug("Automatic alert %s (%s) already up-to-date", security_id, alert_type)
    else:
        logging.info("Creating automatic alert %s (%s)", security_id, alert_type)
        mw.create_alert(
            security_id=security_id,
            alert_type=alert_type,
            params=params,
            note=note,
            notify_mode=notify_mode,
            cooldown_seconds=cooldown_seconds,
            automatic=True
        )



def maintain_alerts():
    PROFIT_OFFSET = 0.05  # 5% below current price
    holdings = mw.get_holdings()
    watchlist = mw.get_watchlist_symbols()

    for idx, row in holdings.iterrows():
        symbol = row['symbol']
        security_id = row['security_id']

        data = mw.fetch_symbol_data(symbol)
        if not data or 'last_price' not in data:
            continue
        current_price = data['last_price']

        # Determine buy price (fallback)
        buy_price = row.get('buy_price', current_price)

        # Positive holdings: protect profits
        if current_price > buy_price:
            threshold = max(current_price * (1-PROFIT_OFFSET), buy_price)
            params = {"threshold": threshold, "mode":"absolute", "direction":"below"}
            ensure_alert(security_id, "price", params, note="Secure profits")

        # Negative holdings: recovery alert
        elif current_price < buy_price:
            params = {"threshold": buy_price, "mode":"absolute", "direction":"above"}
            ensure_alert(security_id, "price", params, note="Recovery alert")

    # Watchlist alerts
    for symbol in watchlist:
        security_id = mw.get_security(symbol)['id']
        data = mw.fetch_symbol_data(symbol)
        if not data or 'last_price' not in data:
            continue

        # RSI oversold
        # params = {"window":14,"overbought":70,"underbought":30,"trigger_on":"Cross below underbought"}
        # ensure_alert(security_id, "rsi", params, note="RSI oversold - potential buy", notify_mode="digest_daily")

        # Golden cross
        # params = {"short":50,"long":200,"ma_type":"SMA","crossover_type":"golden"}
        params = {"short":20,"long":50,"ma_type":"SMA","crossover_type":"golden"}
        ensure_alert(security_id, "ma_crossover", params, note="Golden cross - potential buy", notify_mode="digest_daily")

        # Price dip
        threshold = data['last_price'] * 0.95
        params = {"threshold": threshold, "mode":"absolute", "direction":"below"}
        ensure_alert(security_id, "price", params, note="Price dip - potential buy", notify_mode="digest_daily")




# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--digest", choices=["hourly", "daily", "weekly"], default=None)
    ap.add_argument("--token", default=None)
    ap.add_argument("--chat", default=None)
    ap.add_argument("--test", action="store_true", help="Run Telegram test alerts")
    args = ap.parse_args()

    if args.test:
        test_telegram_alerts(args.token, args.chat)
        return

    # Maintain automatic alerts only
    maintain_alerts()

    # Evaluate all alerts (manual + automatic)
    run_immediate(args.token, args.chat)

    # Send digest if requested
    if args.digest:
        send_digest(args.token, args.chat, args.digest)

if __name__ == "__main__":
    main()

