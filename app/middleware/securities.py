import logging
import threading
from typing import Dict, Any
import pandas as pd

import db_utils as db
from data_fetcher import fetch_and_store_lazy


def get_all_securities() -> pd.DataFrame:
    """Return all securities as a DataFrame."""
    return db.list_securities()

def get_security(symbol: str) -> dict | None:
    """Return security metadata for a given symbol."""
    df = get_all_securities()
    row = df[df['symbol'] == symbol]
    return row.iloc[0].to_dict() if not row.empty else None

def add_new_security(symbol: str, name: str | None = None, isin: str | None = None) -> int:
    sec = get_security(symbol)
    if sec:
        return sec['id']
    return add_security(symbol, name=name, isin=isin)


def add_security(symbol: str, name: str | None = None, isin: str | None = None) -> int:
    """
    Add a security to the database if it does not exist.
    Returns the security_id.
    Fetches data immediately for new securities.
    """
    security_id = db.get_security_id(symbol)
    if security_id is None:
        security_id = db.insert_security(symbol)

        if name is not None or isin is not None:
            db.update_security(security_id, name=name, isin=isin)

        # Fetch data in background
        thread = threading.Thread(target=fetch_and_store_lazy, args=(symbol,), daemon=True)
        thread.start()

    return security_id

def get_security_basic(symbol: str, cache_hours: float = 24) -> Dict[str, Any]:
    """
    Get basic cached info for a security by symbol.
    Always returns a dict (may be empty if not found).
    """

    """Return basic info for a security given its ID."""
    try:
        sec_id = db.get_security_id(symbol)
        df = db.get_security_cache(sec_id)
        if df is not None and not df.empty:
            row = df.iloc[0].to_dict()
            return row
    except Exception:
        logging.exception("get_security_basic failed for %s", sec_id)

    return {}  # always return a dict, even if nothing found
