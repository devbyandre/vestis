from fastapi import APIRouter, HTTPException
from typing import Optional

import middleware as mw
import db_utils as db

from ._helpers import _df

router = APIRouter(tags=["securities"])


@router.get("/securities")
def get_securities():
    return _df(db.list_securities())

@router.get("/securities/all")
def get_all_securities():
    return _df(mw.get_all_securities() if hasattr(mw, 'get_all_securities') else db.list_securities())

@router.get("/securities/symbols")
def get_all_symbols():
    return {"symbols": mw.get_all_symbols()}

@router.get("/securities/{symbol}")
def get_security(symbol: str):
    sec = mw.get_security(symbol)
    if not sec:
        raise HTTPException(404, f"Security {symbol} not found")
    return sec

@router.get("/securities/{symbol}/basic")
def get_security_basic(symbol: str):
    return mw.get_security_basic(symbol) or {}

@router.get("/securities/{symbol}/price/latest")
def get_latest_price(symbol: str):
    price = db.get_latest_price(symbol)
    return {"price": price}

@router.get("/securities/{symbol}/price/series")
def get_price_series(
    symbol: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    df = db.get_price_series(symbol, start, end)
    return _df(df)
