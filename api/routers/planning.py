from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import pandas as pd

import middleware as mw

from ._helpers import _df, _safe_float

router = APIRouter(tags=["planning"])


def _weekly(frame: pd.DataFrame) -> pd.DataFrame:
    """Keep the last row of each calendar week (and so the latest day).

    Daily history over many years is several thousand points per series; a
    weekly resolution draws the same picture at a fraction of the payload.
    """
    idx = pd.to_datetime(frame.index)
    return frame[pd.Series(idx.to_period("W"), index=frame.index).duplicated(keep="last") == False]  # noqa: E712


def _series_payload(frac: pd.DataFrame) -> dict:
    frac = _weekly(frac.sort_index()).round(5)
    return {
        "dates": [pd.Timestamp(d).strftime("%Y-%m-%d") for d in frac.index],
        "series": {str(col): frac[col].tolist() for col in frac.columns},
    }


def _grouped_series(ts: pd.DataFrame, group_col: str) -> dict:
    # Was a per-(date, group) Python loop doing a fresh DataFrame filter on
    # every iteration — O(dates * groups) pandas operations, which dominated
    # the Planning tab's load time (each grouping took ~1s standalone, worse
    # once several ran concurrently). A pivot_table does the same aggregation
    # in one vectorized pass.
    pivot = ts.pivot_table(index="date", columns=group_col, values="market_value",
                            aggfunc="sum", fill_value=0.0)
    totals = pivot.sum(axis=1).replace(0, 1.0)
    return _series_payload(pivot.div(totals, axis=0))


def _prep_allocation_ts(portfolio_ids: Optional[str]):
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    ts = mw.holdings_timeseries(portfolio_ids=ids, aggregate=False)
    if ts is None or ts.empty:
        return None
    ts = ts.copy()
    ts["date"] = pd.to_datetime(ts["date"]).dt.strftime("%Y-%m-%d")
    ts["market_value"] = pd.to_numeric(ts.get("market_value", 0), errors="coerce").fillna(0)
    latest_date = ts["date"].max()
    top10 = (
        ts[ts["date"] == latest_date]
        .groupby("symbol")["market_value"].sum()
        .sort_values(ascending=False).head(10).index
    )
    ts["symbol_group"] = ts["symbol"].where(ts["symbol"].isin(top10), "Other")
    return ts


@router.get("/planning/allocation-over-time")
def get_allocation_over_time(
    portfolio_ids: Optional[str] = Query(None),
    group_by: str = "security_type",
):
    """Allocation (fraction of market value) over time, for the area chart.
    group_by: security_type (default) | sector | industry | symbol.
    For symbol, only the top 10 securities by latest market value keep
    their own series — the rest are bucketed into "Other" (matches
    app_streamlit.py's "Top 10 Securities" chart)."""
    if group_by not in ("security_type", "sector", "industry", "symbol"):
        raise HTTPException(400, "group_by must be one of: security_type, sector, industry, symbol")
    ts = _prep_allocation_ts(portfolio_ids)
    if ts is None:
        return {"dates": [], "series": {}}
    group_col = "symbol_group" if group_by == "symbol" else group_by
    return _grouped_series(ts, group_col)


@router.get("/planning/allocation-over-time-all")
def get_allocation_over_time_all(portfolio_ids: Optional[str] = Query(None)):
    """
    All 4 allocation-over-time groupings (security_type, sector, industry,
    symbol) in one response, computed from a single shared holdings
    timeseries fetch. The Planning tab needs all 4 for its charts —
    requesting them as 4 separate HTTP calls means re-fetching and
    re-processing the same underlying data 4 times, and 4x the round trips.
    """
    empty = {"dates": [], "series": {}}
    ts = _prep_allocation_ts(portfolio_ids)
    if ts is None:
        return {"security_type": empty, "sector": empty, "industry": empty, "symbol": empty}
    return {
        "security_type": _grouped_series(ts, "security_type"),
        "sector": _grouped_series(ts, "sector"),
        "industry": _grouped_series(ts, "industry"),
        "symbol": _grouped_series(ts, "symbol_group"),
    }


@router.get("/planning/risk-breakdown")
def get_risk_breakdown(
    portfolio_ids: Optional[str] = Query(None),
    current_only: bool = False,
    types: Optional[str] = Query(None),
):
    """Risk by asset type / sector / industry / security.

    share: each category's share of the portfolio's value-weighted volatility
    over time (weekly; securities: top 10 by current contribution).
    current_vol: each category's current value-weighted annualised volatility.
    """
    import db_utils as db
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    groups = ("security_type", "sector", "industry", "symbol")
    empty = {"share": {g: {"dates": [], "series": {}} for g in groups}, "current_vol": {g: {} for g in groups}}
    hold = db.get_holdings_timeseries(portfolio_ids=ids)
    if hold is None or hold.empty:
        return empty
    if current_only:
        held = set(mw.get_latest_holdings_snapshot(portfolio_ids=ids, aggregate=True).get("symbol", []))
        hold = hold[hold["symbol"].isin(held)]
    if types:
        hold = hold[hold["security_type"].isin(types.split(","))]
    if hold.empty:
        return empty
    df = db.holdings_with_risk(hold)
    if df.empty:
        return empty
    for col in ("security_type", "sector", "industry", "symbol"):
        df[col] = df[col].fillna("Unknown")
    latest = df[df["date"] == df["date"].max()]
    out = {"share": {}, "current_vol": {}}
    for g in groups:
        pivot = df.pivot_table(index="date", columns=g, values="weighted_risk", aggfunc="sum", fill_value=0.0)
        if g == "symbol":
            top = latest.groupby("symbol")["weighted_risk"].sum().nlargest(10).index
            pivot = pivot[[c for c in pivot.columns if c in top]]
        totals = df.groupby("date")["weighted_risk"].sum().reindex(pivot.index).replace(0, 1.0)
        out["share"][g] = _series_payload(pivot.div(totals, axis=0))
        cur = latest.groupby(g).apply(
            lambda x: float((x["risk_score"] * x["market_value"]).sum() / x["market_value"].sum())
            if x["market_value"].sum() > 0 else None, include_groups=False)
        out["current_vol"][g] = {str(k): round(v, 5) for k, v in cur.items() if v is not None}
    return out


@router.get("/planning/risk-over-time")
def get_risk_over_time(portfolio_ids: Optional[str] = Query(None), aggregate: bool = True):
    """Portfolio risk over time. aggregate=True (default): date+weighted_risk
    only. aggregate=False: per-security rows (symbol/sector/industry/
    security_type/weighted_risk) for the risk-by-category and
    top-10-by-risk-contribution charts."""
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    df = mw.fetch_portfolio_risk_timeseries(portfolio_ids=ids, aggregate=aggregate)
    if df is None or df.empty:
        return []
    df = df.copy()
    if aggregate:
        df = _weekly(df.set_index(pd.to_datetime(df["date"]))[["weighted_risk"]].sort_index()).round(5)
        return [{"date": d.strftime("%Y-%m-%d"), "weighted_risk": v} for d, v in df["weighted_risk"].items()]
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    if not aggregate:
        # The per-security frame carries every holdings column for every day
        # (25 MB for ~60k rows); the charts only need these, summed over
        # portfolios.
        keys = ["date", "symbol", "security_type", "sector", "industry"]
        df = (df.groupby(keys, as_index=False, sort=True, dropna=False)["weighted_risk"].sum()
                .round({"weighted_risk": 6}))
    return _df(df)


@router.get("/planning/kpis")
def get_kpis(portfolio_ids: Optional[str] = Query(None)):
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    snap = mw.get_latest_holdings_snapshot(portfolio_ids=ids, aggregate=False)
    rows = _df(snap)
    # Enrich each holding with cached fundamentals (beta, P/E, dividend yield, RSI, etc.)
    import db_utils as db
    cache = db.get_security_cache_many({r.get("symbol") for r in rows})
    enrich_fields = ["regularMarketPrice", "bookValue", "beta", "trailingPE", "forwardPE", "trailingEps",
                     "dividendRate", "dividendYield", "marketCap", "rsi",
                     "fiftyTwoWeekHigh", "fiftyTwoWeekLow", "profitMargins"]
    for r in rows:
        sym = r.get("symbol")
        if not sym:
            continue
        basic = cache.get(sym, {})
        for f in enrich_fields:
            if f not in r or r.get(f) is None:
                r[f] = _safe_float(basic.get(f)) if isinstance(basic.get(f), (int, float)) else basic.get(f)

    # pb_ratio/Temperature — same scoring the Watchlist tab uses, so both
    # tables are consistent.
    if rows:
        rows = _df(mw.calc_security_KPIs(pd.DataFrame(rows)))
    return rows

@router.get("/planning/taxonomy")
def get_taxonomy():
    holdings = mw.get_latest_holdings_snapshot()
    t = mw.get_complete_taxonomy(holdings)
    return t if isinstance(t, dict) else {}

@router.get("/planning/portfolio-symbols")
def get_portfolio_symbols(portfolio_ids: Optional[str] = Query(None)):
    ids = {int(x) for x in portfolio_ids.split(",")} if portfolio_ids else None
    portfolios = mw.list_portfolios()
    names = [r["name"] for _, r in portfolios.iterrows() if ids is None or int(r["id"]) in ids]
    return sorted({s for name in names for s in mw.get_portfolio_symbols(name)})
