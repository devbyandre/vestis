"""Helpers for reading the securities_cache row (db returns a one-row DataFrame)."""
import logging
from typing import Any, Dict

import pandas as pd

import db_utils as db


def get_security_cache_row(security_id) -> Dict[str, Any]:
    """The cached info row for a security id as a plain dict ({} if missing)."""
    try:
        df = db.get_security_cache(security_id)
    except Exception:
        logging.exception("get_security_cache failed for %s", security_id)
        return {}
    if df is None:
        return {}
    if isinstance(df, dict):
        return df
    return {} if df.empty else df.iloc[0].to_dict()


def earnings_date(cache: Dict[str, Any]):
    """Next earnings date (naive UTC Timestamp) from a cache row, or None.
    Accepts ISO strings and epoch seconds (older rows were stored as ints)."""
    ts = cache.get("earningsTimestamp")
    if ts is None or pd.isna(ts):
        return None
    try:
        if isinstance(ts, (int, float)) or (isinstance(ts, str) and ts.strip().isdigit()):
            out = pd.Timestamp(int(float(ts)), unit="s", tz="UTC")
        else:
            out = pd.Timestamp(ts)
        if pd.isna(out):
            return None
        return (out.tz_convert("UTC") if out.tzinfo else out).replace(tzinfo=None)
    except (ValueError, TypeError, OverflowError):
        return None
