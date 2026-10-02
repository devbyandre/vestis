"""Quiet hours: a daily local-time window in which immediate alerts are held."""
import logging
from datetime import datetime, time
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from config_utils import get_config

DEFAULT_START = "22:00"
DEFAULT_END = "07:00"
DEFAULT_TZ = "Europe/Berlin"


def parse_hhmm(value, default: str) -> time:
    try:
        h, m = str(value).strip().split(":")
        return time(int(h), int(m))
    except Exception:
        h, m = default.split(":")
        return time(int(h), int(m))


def is_valid_hhmm(value) -> bool:
    try:
        h, m = str(value).strip().split(":")
        time(int(h), int(m))
        return True
    except Exception:
        return False


def is_valid_tz(name) -> bool:
    try:
        ZoneInfo(str(name))
        return True
    except (ZoneInfoNotFoundError, ValueError, KeyError, OSError):
        return False


def in_quiet_hours(now_utc: datetime, start: time, end: time, tz_name: str) -> bool:
    """True if now (UTC) falls inside [start, end) local time; the window may
    wrap past midnight. start == end means no window."""
    if start == end:
        return False
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        logging.warning("Unknown timezone %r for quiet hours — using UTC", tz_name)
        tz = ZoneInfo("UTC")
    local = now_utc.replace(tzinfo=ZoneInfo("UTC")).astimezone(tz).time()
    if start < end:
        return start <= local < end
    return local >= start or local < end


def quiet_hours_active(now_utc: Optional[datetime] = None) -> bool:
    """Read the settings (dnd on/off, quiet_hours_start/end, timezone)."""
    if str(get_config("dnd") or "false").lower() not in ("1", "true", "yes"):
        return False
    now_utc = now_utc or datetime.utcnow()
    return in_quiet_hours(
        now_utc,
        parse_hhmm(get_config("quiet_hours_start"), DEFAULT_START),
        parse_hhmm(get_config("quiet_hours_end"), DEFAULT_END),
        get_config("timezone") or DEFAULT_TZ,
    )
