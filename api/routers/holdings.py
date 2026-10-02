from fastapi import APIRouter, Query
from typing import Optional
import pandas as pd

import middleware as mw

from ._helpers import _df, _safe_float

router = APIRouter(tags=["holdings"])


@router.get("/holdings/snapshot")
def get_holdings_snapshot(
    portfolio_ids: Optional[str] = Query(None),
    aggregate: bool = True,
):
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    snap = mw.get_latest_holdings_snapshot(portfolio_ids=ids, aggregate=aggregate)
    return _df(snap)

@router.get("/holdings/timeseries")
def get_holdings_timeseries(
    portfolio_ids: Optional[str] = Query(None),
    sectors: Optional[str] = Query(None),
    security_types: Optional[str] = Query(None),
    aggregate: bool = True,
):
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    secs = sectors.split(",") if sectors else None
    types = security_types.split(",") if security_types else None
    df = mw.holdings_timeseries(
        portfolio_ids=ids,
        sectors=secs,
        security_types=types,
        aggregate=aggregate,
    )
    return _df(df)

@router.get("/holdings/risk-timeseries")
def get_portfolio_risk_timeseries(
    portfolio_ids: Optional[str] = Query(None),
):
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    df = mw.fetch_portfolio_risk_timeseries(portfolio_ids=ids)
    return _df(df)


@router.get("/holdings/metrics")
def get_holdings_metrics(portfolio_ids: Optional[str] = Query(None)):
    """Portfolio-level volatility + Sharpe from the aggregated value timeseries."""
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    ts = mw.holdings_timeseries(portfolio_ids=ids, aggregate=True)
    if ts is None or ts.empty:
        return {"volatility": None, "sharpe": None, "max_drawdown": None}
    ts = ts.copy()
    # Aggregate market_value by date
    by_date = ts.groupby("date")["market_value"].sum().reset_index().sort_values("date")
    if len(by_date) < 2:
        return {"volatility": None, "sharpe": None, "max_drawdown": None}
    price_df = pd.DataFrame({"close": pd.to_numeric(by_date["market_value"], errors="coerce").ffill().values})
    try:
        return {
            "volatility": _safe_float(mw.volatility(price_df, "close")),
            "sharpe": _safe_float(mw.sharpe_ratio(price_df, "close")),
            "max_drawdown": _safe_float(mw.max_drawdown(price_df["close"])),
        }
    except Exception:
        return {"volatility": None, "sharpe": None, "max_drawdown": None}
