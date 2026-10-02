from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from config_utils import get_all_config, set_config
import telegram_client as tg

router = APIRouter(tags=["settings"])


@router.get("/settings")
def get_settings():
    cfg = get_all_config()
    # Never expose secrets in GET — mask them
    safe = {k: v for k, v in cfg.items() if k not in ("telegram_bot_token", "telegram_chat_id")}
    safe["telegram_bot_token_set"] = bool(cfg.get("telegram_bot_token"))
    safe["telegram_chat_id_set"] = bool(cfg.get("telegram_chat_id"))
    return safe

class SettingsUpdate(BaseModel):
    settings: dict

@router.put("/settings")
def update_settings(body: SettingsUpdate):
    for key, value in body.settings.items():
        set_config(key, value)
    return {"ok": True}


@router.post("/settings/telegram-test")
def telegram_test():
    """Send a test message (with the 'Open in Vestis' button if vestis_url is set)."""
    token, chat = tg.get_creds()
    if not token or not chat:
        raise HTTPException(400, "Telegram bot token / chat ID not configured")
    url = tg.get_vestis_url()
    ok = tg.send_message(token, chat, "✅ *Vestis* — Telegram test message",
                         link=("🔎 Open Vestis", url) if url else None, max_retries=2)
    if not ok:
        raise HTTPException(502, "Telegram did not accept the message — check bot token and chat ID")
    return {"ok": True, "button": bool(url)}
