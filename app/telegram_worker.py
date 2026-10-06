#!/usr/bin/env python3
"""
telegram_worker.py — cron entry point: maintain automatic alerts, evaluate and
deliver them over Telegram, optionally send a digest. The logic lives in the
`alerting` package; this file only wires it to Telegram.
"""
import argparse
import logging

import telegram_client as tg
from alerting.digest import send_digest
from alerting.engine import run_immediate
from alerting.maintenance import maintain_alerts
from alerting.news_updates import send_news_alerts
from alerting.messages import vestis_link

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def make_notifier(cli_token=None, cli_chat=None):
    """notify(text, link) -> bool over Telegram, or None if not configured."""
    token, chat = tg.get_creds(cli_token, cli_chat)
    if not token or not chat:
        logging.error("Telegram token/chat not configured.")
        return None
    return lambda text, link=None: tg.send_message(token, chat, text, link=link)


def test_telegram(cli_token=None, cli_chat=None):
    notify = make_notifier(cli_token, cli_chat)
    if notify:
        return notify("✅ *Vestis* — Telegram connection test successful!", vestis_link())


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
    try:
        maintain_alerts()
        notify = make_notifier(args.token, args.chat)
        if notify:
            run_immediate(notify)
            try:
                send_news_alerts(notify)
            except Exception:
                logging.exception("News alerts failed")
            if args.digest:
                send_digest(notify, args.digest)
    except Exception as e:
        logging.exception("Telegram worker crashed")
        try:
            tg.notify_error("telegram_worker", error=e,
                            message="The Telegram alert worker crashed.")
        except Exception:
            logging.exception("Failed to send crash notification")
        raise

if __name__ == "__main__":
    main()
