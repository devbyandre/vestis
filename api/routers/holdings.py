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


@router.get("/holdings/performance")
def get_holdings_performance(portfolio_ids: Optional[str] = Query(None)):
    """Daily market value, cost basis, net flow and time-weighted return index."""
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    perf = mw.performance_series(portfolio_ids=ids)
    if perf.empty:
        return []
    perf["date"] = perf["date"].dt.strftime("%Y-%m-%d")
    return _df(perf.round({"market_value": 2, "cost_basis": 2, "flow": 2, "twr": 6}))


@router.get("/holdings/metrics")
def get_holdings_metrics(portfolio_ids: Optional[str] = Query(None)):
    """Volatility, Sharpe and max drawdown of the time-weighted return (deposits
    and withdrawals don't count as gains or losses)."""
    empty = {"volatility": None, "sharpe": None, "max_drawdown": None, "twr": None, "cagr": None}
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    perf = mw.performance_series(portfolio_ids=ids)
    if len(perf) < 3:
        return empty
    twr = perf.set_index("date")["twr"]
    # Holdings are valued on calendar days; returns are measured on trading days.
    bd = twr[twr.index.dayofweek < 5]
    rets = bd.pct_change().dropna()
    if rets.empty or rets.std() == 0:
        return empty
    years = max((twr.index[-1] - twr.index[0]).days / 365.25, 1 / 365.25)
    total = float(twr.iloc[-1] / twr.iloc[0] - 1)
    return {
        "volatility": _safe_float(rets.std() * 252 ** 0.5),
        "sharpe": _safe_float(rets.mean() * 252 / (rets.std() * 252 ** 0.5)),
        "max_drawdown": _safe_float((twr / twr.cummax() - 1).min()),
        "twr": _safe_float(total),
        "cagr": _safe_float((1 + total) ** (1 / years) - 1) if total > -1 else None,
    }

