from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional

import middleware as mw

from ._helpers import _df

router = APIRouter(tags=["watchlist"])


@router.get("/watchlist")
def get_watchlist():
    return _df(mw.get_watchlist())

@router.get("/watchlist/symbols")
def get_watchlist_symbols():
    return {"symbols": mw.get_watchlist_symbols()}

class WatchlistAdd(BaseModel):
    symbol: str
    name: Optional[str] = None
    isin: Optional[str] = None

class WatchlistDelete(BaseModel):
    symbol: str

@router.post("/watchlist")
def add_to_watchlist(body: WatchlistAdd):
    mw.add_new_security(body.symbol, body.name, body.isin)
    return {"ok": True}

@router.delete("/watchlist")
def remove_from_watchlist(body: WatchlistDelete):
    mw.delete_security_from_watchlist(body.symbol)
    return {"ok": True}
