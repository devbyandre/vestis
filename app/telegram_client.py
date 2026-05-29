#!/usr/bin/env python3
"""
telegram_client.py — pure Telegram messaging layer.

Extracted so both telegram_worker.py and data_fetcher.py can send messages
and error notifications without duplicating the send/retry/escape logic.

This module contains NO business logic — only message formatting and delivery.
"""
import re
import time
import logging
import traceback
import requests
from config_utils import get_config

logger = logging.getLogger(__name__)


def md_escape(text) -> str:
    """Escape Markdown special characters for Telegram."""
    if not isinstance(text, str):
        text = str(text)
    return re.sub(r'([_*\[\]()~`>#+\-=|{}.!])', r'\\\1', text)


def get_creds(cli_token=None, cli_chat=None):
    """Resolve bot token and chat id from CLI args or config."""
    token = cli_token or get_config("telegram_bot_token")
    chat = cli_chat or get_config("telegram_chat_id")
    return token, chat


def send_message(token: str, chat_id: str, text: str,
                 parse_mode: str = "Markdown", max_retries: int = 3) -> bool:
    """Send a Telegram message with exponential backoff retry."""
    if not token or not chat_id:
        logger.warning("Telegram credentials missing — cannot send message")
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.post(url, json=payload, timeout=10)
            if r.status_code == 200:
                logger.info("Telegram message sent")
                return True
            logger.warning("Telegram send failed %s: %s", r.status_code, r.text[:200])
        except Exception:
            logger.exception("Telegram send exception")
        time.sleep(backoff)
        backoff *= 2
    return False


def notify_error(source: str, error: Exception = None, message: str = None,
                 cli_token=None, cli_chat=None) -> bool:
    """
    Send an error/crash notification to Telegram.

    source:  short label for where the error came from, e.g. "data_fetcher", "telegram_worker"
    error:   optional exception object — its traceback will be included (truncated)
    message: optional custom message instead of / in addition to the exception
    """
    token, chat = get_creds(cli_token, cli_chat)
    if not token or not chat:
        logger.warning("Cannot send error notification — Telegram not configured")
        return False

    lines = [f"🚨 *Vestis Error* — `{md_escape(source)}`", ""]
    if message:
        lines.append(md_escape(message))
        lines.append("")
    if error is not None:
        err_type = type(error).__name__
        err_msg = str(error)[:300]
        lines.append(f"*{md_escape(err_type)}*: {md_escape(err_msg)}")
        # Include a short traceback tail
        tb = traceback.format_exc()
        if tb and tb.strip() != "NoneType: None":
            tail = tb.strip().splitlines()[-4:]
            tb_text = "\n".join(tail)
            lines.append("")
            lines.append("```")
            lines.append(tb_text[:500])
            lines.append("```")
    body = "\n".join(lines)

    # Send WITHOUT markdown parse_mode for the code block reliability —
    # fall back to plain text if markdown send fails
    if send_message(token, chat, body, parse_mode="Markdown"):
        return True
    plain = body.replace("*", "").replace("`", "")
    return send_message(token, chat, plain, parse_mode=None)
