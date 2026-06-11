"""
Vestis FastAPI — exposes all middleware functions as REST endpoints.
Runs on port 8503, sits alongside the Streamlit app on 8502.
Do not modify middleware.py or db_utils.py — this layer only wraps them.
"""
import sys, os
sys.path.insert(0, "/app")

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List, Any
import pandas as pd
import numpy as np
import json

import middleware as mw
import db_utils as db
from config_utils import get_all_config, set_config

app = FastAPI(title="Vestis API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _df(df: pd.DataFrame) -> list:
    """Convert DataFrame to JSON-serialisable list, handling NaN/Inf/Timestamps."""
    if df is None or df.empty:
        return []
    df = df.copy()
    for col in df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns:
        df[col] = df[col].astype(str)
    df = df.where(pd.notnull(df), None)
    records = df.to_dict(orient="records")
    def clean(v):
        if isinstance(v, float) and (v != v or v == float("inf") or v == float("-inf")):
            return None
        if isinstance(v, (np.integer,)):
            return int(v)
        if isinstance(v, (np.floating,)):
            return float(v) if (v == v and v != float("inf") and v != float("-inf")) else None
        if isinstance(v, pd.Timestamp):
            return str(v)
        return v
    return [{k: clean(val) for k, val in row.items()} for row in records]


def _safe_float(v):
    try:
        if v is None:
            return None
        f = float(v)
        if f != f or f in (float("inf"), float("-inf")):
            return None
        return f
    except Exception:
        return None


def _series(s: pd.Series) -> list:
    return _df(s.reset_index()) if isinstance(s, pd.Series) else []


# ── Portfolios ────────────────────────────────────────────────────────────────

@app.get("/portfolios")
def get_portfolios():
    return _df(mw.list_portfolios())


class PortfolioCreate(BaseModel):
    name: str

class PortfolioRename(BaseModel):
    old_name: str
    new_name: str

class PortfolioDelete(BaseModel):
    name: str
    reassign_to: Optional[str] = None

@app.post("/portfolios")
def create_portfolio(body: PortfolioCreate):
    pid = mw.create_portfolio(body.name)
    return {"id": pid, "name": body.name}

@app.put("/portfolios/rename")
def rename_portfolio(body: PortfolioRename):
    mw.rename_portfolio(body.old_name, body.new_name)
    return {"ok": True}

@app.delete("/portfolios")
def delete_portfolio(body: PortfolioDelete):
    mw.delete_and_reassign_portfolio(body.name, body.reassign_to)
    return {"ok": True}


# ── Securities ────────────────────────────────────────────────────────────────

@app.get("/securities")
def get_securities():
    return _df(db.list_securities())

@app.get("/securities/all")
def get_all_securities():
    return _df(mw.get_all_securities() if hasattr(mw, 'get_all_securities') else db.list_securities())

@app.get("/securities/symbols")
def get_all_symbols():
    return {"symbols": mw.get_all_symbols()}

@app.get("/securities/{symbol}")
def get_security(symbol: str):
    sec = mw.get_security(symbol)
    if not sec:
        raise HTTPException(404, f"Security {symbol} not found")
    return sec

@app.get("/securities/{symbol}/basic")
def get_security_basic(symbol: str):
    return mw.get_security_basic(symbol) or {}

@app.get("/securities/{symbol}/price/latest")
def get_latest_price(symbol: str):
    price = db.get_latest_price(symbol)
    return {"price": price}

@app.get("/securities/{symbol}/price/series")
def get_price_series(
    symbol: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    df = db.get_price_series(symbol, start, end)
    return _df(df)


# ── Holdings ─────────────────────────────────────────────────────────────────

@app.get("/holdings/snapshot")
def get_holdings_snapshot(
    portfolio_ids: Optional[str] = Query(None),
    aggregate: bool = True,
):
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    snap = mw.get_latest_holdings_snapshot(portfolio_ids=ids, aggregate=aggregate)
    return _df(snap)

@app.get("/holdings/timeseries")
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

@app.get("/holdings/risk-timeseries")
def get_portfolio_risk_timeseries(
    portfolio_ids: Optional[str] = Query(None),
):
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    df = mw.fetch_portfolio_risk_timeseries(portfolio_ids=ids)
    return _df(df)


# ── Transactions ──────────────────────────────────────────────────────────────

@app.get("/transactions")
def get_transactions(portfolio_ids: Optional[str] = Query(None)):
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    df = db.list_transactions_detailed() if ids is None else db.list_transactions(ids)
    if df is None or df.empty:
        return []
    df = df.copy()
    # Normalise column names across the two query shapes
    if "tx_date" not in df.columns and "date" in df.columns:
        df["tx_date"] = df["date"]
    if "tx_type" not in df.columns and "type" in df.columns:
        df["tx_type"] = df["type"]
    if "tx_cost" not in df.columns and "fees" in df.columns:
        df["tx_cost"] = df["fees"]
    if "portfolio" not in df.columns and "portfolio_name" in df.columns:
        df["portfolio"] = df["portfolio_name"]
    if "security_name" not in df.columns and "name" in df.columns:
        df["security_name"] = df["name"]
    # Ensure numeric
    for c in ["quantity", "price", "tx_cost"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    # Compute total_cost = quantity*price + fees (splits have price=0 → total 0)
    df["total_cost"] = df["quantity"] * df["price"] + df.get("tx_cost", 0.0)
    # security_label for display
    if "security_label" not in df.columns:
        df["security_label"] = df.apply(
            lambda r: (r.get("security_name") or r.get("symbol") or "?"), axis=1
        )
    return _df(df)


@app.get("/transactions/summary")
def get_transactions_summary(portfolio_ids: Optional[str] = Query(None)):
    """Pre-computed summary metrics for the transactions tab."""
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    df = db.list_transactions_detailed() if ids is None else db.list_transactions(ids)
    if df is None or df.empty:
        return {"count": 0, "total_quantity": 0, "total_buys": 0, "num_buys": 0,
                "total_sells": 0, "num_sells": 0, "total_fees": 0}
    df = df.copy()
    if "tx_type" not in df.columns and "type" in df.columns:
        df["tx_type"] = df["type"]
    if "tx_cost" not in df.columns and "fees" in df.columns:
        df["tx_cost"] = df["fees"]
    for c in ["quantity", "price", "tx_cost"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df["total_cost"] = df["quantity"] * df["price"] + df.get("tx_cost", 0.0)
    buys = df[df["tx_type"] == "buy"]
    sells = df[df["tx_type"] == "sell"]
    return {
        "count": int(len(df)),
        "total_quantity": float(df["quantity"].sum()),
        "total_buys": float(buys["total_cost"].sum()),
        "num_buys": int(len(buys)),
        "total_sells": float(sells["total_cost"].sum()),
        "num_sells": int(len(sells)),
        "total_fees": float(df["tx_cost"].sum()),
    }

class TransactionCreate(BaseModel):
    portfolio_id: int
    symbol: str
    tx_date: str
    tx_type: str
    quantity: float
    price: float
    fees: float = 0.0

class TransactionEdit(BaseModel):
    tx_id: int
    portfolio_id: int
    symbol: str
    tx_date: str
    tx_type: str
    quantity: float
    price: float
    fees: float = 0.0

class SplitCreate(BaseModel):
    symbol: str
    split_date: str
    ratio: float
    portfolio_id: Optional[int] = None

@app.post("/transactions")
def add_transaction(body: TransactionCreate):
    mw.add_transaction(
        portfolio_id=body.portfolio_id,
        symbol=body.symbol,
        tx_date=body.tx_date,
        tx_type=body.tx_type,
        quantity=body.quantity,
        price=body.price,
        fees=body.fees,
    )
    return {"ok": True}

@app.put("/transactions/{tx_id}")
def edit_transaction(tx_id: int, body: TransactionEdit):
    mw.edit_transaction(
        tx_id=tx_id,
        portfolio_id=body.portfolio_id,
        symbol=body.symbol,
        tx_date=body.tx_date,
        tx_type=body.tx_type,
        quantity=body.quantity,
        price=body.price,
        fees=body.fees,
    )
    return {"ok": True}

@app.delete("/transactions/{tx_id}")
def delete_transaction(tx_id: int):
    mw.remove_transaction(tx_id)
    return {"ok": True}

@app.post("/transactions/split")
def add_split(body: SplitCreate):
    applied = mw.add_split_transaction(
        symbol=body.symbol,
        split_date=body.split_date,
        ratio=body.ratio,
        portfolio_id=body.portfolio_id,
    )
    return {"ok": True, "portfolios_applied": applied}


# ── Analytics ─────────────────────────────────────────────────────────────────

@app.get("/analytics/capital-gains")
def get_capital_gains(
    portfolio_ids: Optional[str] = Query(None),
    year: Optional[int] = None,
):
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    df = mw.calc_capital_gains_fifo(portfolio_ids=ids, year=year)
    return _df(df)

@app.get("/analytics/dividends")
def get_dividends(
    portfolio_ids: Optional[str] = Query(None),
    year: Optional[int] = None,
):
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    df = mw.calc_dividends_for_portfolio(portfolio_ids=ids, year=year)
    return _df(df)

@app.get("/analytics/revenues-summary")
def get_revenues_summary(portfolio_ids: Optional[str] = Query(None), year: Optional[int] = None):
    """Aggregated capital gains + dividends for the Revenues tab charts."""
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    cg = mw.calc_capital_gains_fifo(portfolio_ids=ids, year=year)
    dv = mw.calc_dividends_for_portfolio(portfolio_ids=ids, year=year)

    tax_rate = 0.26375
    try:
        from config_utils import get_config
        tr = get_config("tax_rate")
        if tr:
            tax_rate = float(tr)
    except Exception:
        pass

    # Normalise
    if cg is None or cg.empty:
        cg = pd.DataFrame(columns=["symbol", "profit", "year", "portfolio_id"])
    if dv is None or dv.empty:
        dv = pd.DataFrame(columns=["symbol", "total", "year", "portfolio_id"])

    total_gains = float(cg["profit"].sum()) if "profit" in cg.columns else 0.0
    total_divs = float(dv["total"].sum()) if "total" in dv.columns else 0.0
    taxable = max(0.0, total_gains) + total_divs

    # By year
    by_year = {}
    if "profit" in cg.columns and "year" in cg.columns:
        for y, g in cg.groupby("year"):
            by_year.setdefault(int(y), {"year": int(y), "gains": 0.0, "dividends": 0.0})
            by_year[int(y)]["gains"] = float(g["profit"].sum())
    if "total" in dv.columns and "year" in dv.columns:
        for y, g in dv.groupby("year"):
            by_year.setdefault(int(y), {"year": int(y), "gains": 0.0, "dividends": 0.0})
            by_year[int(y)]["dividends"] = float(g["total"].sum())

    # By security
    by_sec = {}
    if "profit" in cg.columns and "symbol" in cg.columns:
        for s, g in cg.groupby("symbol"):
            by_sec.setdefault(s, {"symbol": s, "gains": 0.0, "dividends": 0.0})
            by_sec[s]["gains"] = float(g["profit"].sum())
    if "total" in dv.columns and "symbol" in dv.columns:
        for s, g in dv.groupby("symbol"):
            by_sec.setdefault(s, {"symbol": s, "gains": 0.0, "dividends": 0.0})
            by_sec[s]["dividends"] = float(g["total"].sum())

    # Tax-loss harvesting candidates (negative gains)
    tax_loss = []
    if "profit" in cg.columns:
        losses = cg[cg["profit"] < 0]
        if not losses.empty:
            agg = losses.groupby("symbol")["profit"].sum().reset_index()
            tax_loss = [{"symbol": r["symbol"], "loss": float(r["profit"])}
                        for _, r in agg.iterrows()]

    return {
        "total_gains": total_gains,
        "total_dividends": total_divs,
        "estimated_tax": taxable * tax_rate,
        "tax_rate": tax_rate,
        "by_year": sorted(by_year.values(), key=lambda x: x["year"]),
        "by_security": sorted(by_sec.values(), key=lambda x: -(x["gains"] + x["dividends"])),
        "tax_loss_candidates": sorted(tax_loss, key=lambda x: x["loss"]),
        "capital_gains": _df(cg),
        "dividends": _df(dv),
    }


@app.get("/holdings/metrics")
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


@app.get("/analytics/rebalancing")
def get_rebalancing(portfolio_ids: Optional[str] = Query(None), retirement_year: int = 2047):
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    result = mw.suggest_rebalancing(portfolio_ids=ids, retirement_year=retirement_year)
    if isinstance(result, pd.DataFrame):
        return _df(result)
    return result


@app.get("/planning/allocation-over-time")
def get_allocation_over_time(portfolio_ids: Optional[str] = Query(None)):
    """Asset-type allocation (fraction of market value) over time, for the area chart."""
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    ts = mw.holdings_timeseries(portfolio_ids=ids, aggregate=False)
    if ts is None or ts.empty:
        return {"dates": [], "series": {}}
    ts = ts.copy()
    if "security_type" not in ts.columns:
        # Join security_type from snapshot
        snap = mw.get_latest_holdings_snapshot(portfolio_ids=ids, aggregate=False)
        type_map = {}
        if snap is not None and not snap.empty and "security_id" in snap.columns:
            for _, r in snap.iterrows():
                type_map[r.get("security_id")] = r.get("security_type") or "Other"
        ts["security_type"] = ts.get("security_id", pd.Series([None]*len(ts))).map(type_map).fillna("Other")
    ts["date"] = pd.to_datetime(ts["date"]).dt.strftime("%Y-%m-%d")
    ts["market_value"] = pd.to_numeric(ts.get("market_value", 0), errors="coerce").fillna(0)
    # Pivot: by date and type
    grouped = ts.groupby(["date", "security_type"])["market_value"].sum().reset_index()
    dates = sorted(grouped["date"].unique().tolist())
    types = sorted(grouped["security_type"].unique().tolist())
    # Build fraction series
    series = {t: [] for t in types}
    for d in dates:
        day = grouped[grouped["date"] == d]
        total = day["market_value"].sum() or 1.0
        for t in types:
            row = day[day["security_type"] == t]
            val = float(row["market_value"].sum()) / total if not row.empty else 0.0
            series[t].append(val)
    return {"dates": dates, "series": series}


@app.get("/planning/risk-over-time")
def get_risk_over_time(portfolio_ids: Optional[str] = Query(None)):
    """Aggregated portfolio risk over time."""
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    df = mw.fetch_portfolio_risk_timeseries(portfolio_ids=ids, aggregate=True)
    if df is None or df.empty:
        return []
    df = df.copy()
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    return _df(df)

@app.get("/analytics/indicators/{symbol}")
def get_indicators(
    symbol: str,
    sma_periods: str = "50,200",
    ema_periods: str = "",
    window_rsi: int = 14,
    bb_window: int = 20,
    show_bb: bool = True,
    show_crossovers: bool = True,
    show_extrema: bool = False,
    lookback_days: int = 400,
):
    """Returns price series + indicators (SMA/EMA/RSI/Bollinger), crossover markers,
    local extrema, and risk metrics for a symbol."""
    prices = db.get_price_history(symbol, lookback_days=lookback_days)
    if prices is None or prices.empty:
        raise HTTPException(404, f"No price data for {symbol}")

    prices = prices.reset_index(drop=True)
    close = pd.to_numeric(
        prices["adj_close"] if "adj_close" in prices.columns else prices["close"],
        errors="coerce"
    ).ffill().bfill()

    result = _df(prices)
    smas = [int(x) for x in sma_periods.split(",") if x.strip()] if sma_periods else []
    emas = [int(x) for x in ema_periods.split(",") if x.strip()] if ema_periods else []

    # SMAs
    for w in smas:
        try:
            s = mw.sma(close, w)
            for i, row in enumerate(result):
                row[f"sma_{w}"] = float(s.iloc[i]) if i < len(s) and pd.notna(s.iloc[i]) else None
        except Exception:
            pass

    # EMAs
    for e in emas:
        try:
            s = mw.ema(close, e)
            for i, row in enumerate(result):
                row[f"ema_{e}"] = float(s.iloc[i]) if i < len(s) and pd.notna(s.iloc[i]) else None
        except Exception:
            pass

    # RSI
    try:
        r = mw.rsi(close, window_rsi)
        for i, row in enumerate(result):
            row[f"rsi"] = float(r.iloc[i]) if i < len(r) and pd.notna(r.iloc[i]) else None
    except Exception:
        pass

    # Bollinger Bands — mw.bollinger returns (mid, upper, lower) tuple of Series
    if show_bb:
        try:
            mid, upper, lower = mw.bollinger(close, bb_window, 2.0)
            for i, row in enumerate(result):
                row["bb_mid"] = float(mid.iloc[i]) if i < len(mid) and pd.notna(mid.iloc[i]) else None
                row["bb_upper"] = float(upper.iloc[i]) if i < len(upper) and pd.notna(upper.iloc[i]) else None
                row["bb_lower"] = float(lower.iloc[i]) if i < len(lower) and pd.notna(lower.iloc[i]) else None
        except Exception:
            pass

    # Crossover markers (SMA x EMA, or first two SMAs)
    crossovers = {"buy": [], "sell": []}
    if show_crossovers and len(smas) >= 1 and (len(emas) >= 1 or len(smas) >= 2):
        try:
            if len(emas) >= 1:
                short_ma = mw.sma(close, smas[0])
                long_ma = mw.ema(close, emas[0])
            else:
                short_ma = mw.sma(close, smas[0])
                long_ma = mw.sma(close, smas[1])
            buy_idx, sell_idx = mw.find_crossovers(short_ma, long_ma)
            dates = [r.get("date") for r in result]
            closes = [r.get("adj_close") or r.get("close") for r in result]
            crossovers["buy"] = [{"date": dates[i], "price": closes[i]} for i in buy_idx if i < len(dates)]
            crossovers["sell"] = [{"date": dates[i], "price": closes[i]} for i in sell_idx if i < len(dates)]
        except Exception:
            pass

    # Local extrema
    extrema = {"min": [], "max": []}
    if show_extrema:
        try:
            idx_min, idx_max = mw.local_min_max(close)
            dates = [r.get("date") for r in result]
            closes = [r.get("adj_close") or r.get("close") for r in result]
            extrema["min"] = [{"date": dates[i], "price": closes[i]} for i in idx_min if i < len(dates)]
            extrema["max"] = [{"date": dates[i], "price": closes[i]} for i in idx_max if i < len(dates)]
        except Exception:
            pass

    # Risk metrics — these take a DataFrame with a price column
    metrics = {}
    try:
        price_df = pd.DataFrame({"close": close.values})
        metrics = {
            "volatility": _safe_float(mw.volatility(price_df, "close")),
            "sharpe": _safe_float(mw.sharpe_ratio(price_df, "close")),
            "max_drawdown": _safe_float(mw.max_drawdown(close)),
        }
        if hasattr(mw, "sortino_ratio"):
            metrics["sortino"] = _safe_float(mw.sortino_ratio(price_df, "close"))
    except Exception:
        metrics = {}

    return {"series": result, "metrics": metrics, "crossovers": crossovers, "extrema": extrema}

@app.get("/analytics/crossovers/{symbol}")
def get_crossovers(
    symbol: str,
    short: int = 20,
    long_: int = 50,
    lookback_days: int = 400,
):
    prices = db.get_price_history(symbol, lookback_days=lookback_days)
    if prices is None or prices.empty:
        raise HTTPException(404, f"No price data for {symbol}")
    adj = pd.to_numeric(prices["adj_close"] if "adj_close" in prices.columns else prices["close"], errors="coerce").ffill()
    result = mw.find_crossovers(adj, short, long_)
    if isinstance(result, pd.DataFrame):
        return _df(result)
    return []

@app.get("/analytics/local-extrema/{symbol}")
def get_local_extrema(symbol: str, lookback_days: int = 200):
    prices = db.get_price_history(symbol, lookback_days=lookback_days)
    if prices is None or prices.empty:
        raise HTTPException(404, f"No price data for {symbol}")
    adj = pd.to_numeric(prices["adj_close"] if "adj_close" in prices.columns else prices["close"], errors="coerce").ffill()
    result = mw.local_min_max(adj)
    if isinstance(result, pd.DataFrame):
        return _df(result)
    return []


# ── Watchlist ─────────────────────────────────────────────────────────────────

@app.get("/watchlist")
def get_watchlist():
    return _df(mw.get_watchlist())

@app.get("/watchlist/symbols")
def get_watchlist_symbols():
    return {"symbols": mw.get_watchlist_symbols()}

class WatchlistAdd(BaseModel):
    symbol: str
    name: Optional[str] = None
    isin: Optional[str] = None

class WatchlistDelete(BaseModel):
    symbol: str

@app.post("/watchlist")
def add_to_watchlist(body: WatchlistAdd):
    mw.add_new_security(body.symbol, body.name, body.isin)
    return {"ok": True}

@app.delete("/watchlist")
def remove_from_watchlist(body: WatchlistDelete):
    mw.delete_security_from_watchlist(body.symbol)
    return {"ok": True}


# ── Alerts ────────────────────────────────────────────────────────────────────

@app.get("/alerts")
def get_alerts(active_only: bool = False):
    df = mw.get_all_alerts_for_ui()
    if active_only:
        df = df[df["active"] == 1] if not df.empty else df
    return _df(df)

class AlertCreate(BaseModel):
    security_id: int
    alert_type: str
    params: dict
    note: Optional[str] = ""
    notify_mode: str = "immediate"
    cooldown_seconds: int = 14400
    active: bool = True

class AlertEdit(BaseModel):
    params: Optional[dict] = None
    note: Optional[str] = None
    active: Optional[bool] = None
    cooldown_seconds: Optional[int] = None
    notify_mode: Optional[str] = None

@app.post("/alerts")
def create_alert(body: AlertCreate):
    alert_id = mw.create_alert(
        security_id=body.security_id,
        alert_type=body.alert_type,
        params=body.params,
        note=body.note,
        notify_mode=body.notify_mode,
        cooldown_seconds=body.cooldown_seconds,
    )
    return {"id": alert_id}

@app.put("/alerts/{alert_id}")
def edit_alert(alert_id: int, body: AlertEdit):
    mw.edit_alert(
        alert_id=alert_id,
        params=body.params,
        note=body.note,
        active=body.active,
        cooldown_seconds=body.cooldown_seconds,
        notify_mode=body.notify_mode,
    )
    return {"ok": True}

@app.delete("/alerts/{alert_id}")
def delete_alert(alert_id: int):
    mw.delete_alert(alert_id)
    return {"ok": True}


# ── Planning / KPI ────────────────────────────────────────────────────────────

@app.get("/planning/kpis")
def get_kpis(portfolio_ids: Optional[str] = Query(None)):
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    snap = mw.get_latest_holdings_snapshot(portfolio_ids=ids, aggregate=False)
    rows = _df(snap)
    # Enrich each holding with cached fundamentals (beta, P/E, dividend yield, RSI, etc.)
    cache = {}
    enrich_fields = ["beta", "trailingPE", "forwardPE", "trailingEps",
                     "dividendRate", "dividendYield", "marketCap", "rsi",
                     "fiftyTwoWeekHigh", "fiftyTwoWeekLow", "profitMargins"]
    for r in rows:
        sym = r.get("symbol")
        if not sym:
            continue
        if sym not in cache:
            try:
                cache[sym] = mw.get_security_basic(sym) or {}
            except Exception:
                cache[sym] = {}
        basic = cache[sym]
        for f in enrich_fields:
            if f not in r or r.get(f) is None:
                r[f] = _safe_float(basic.get(f)) if isinstance(basic.get(f), (int, float)) else basic.get(f)
    return rows

@app.get("/planning/taxonomy")
def get_taxonomy():
    t = mw.get_complete_taxonomy()
    return t if isinstance(t, dict) else {}

@app.get("/planning/portfolio-symbols")
def get_portfolio_symbols(portfolio_ids: Optional[str] = Query(None)):
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    result = mw.get_portfolio_symbols(portfolio_ids=ids)
    if isinstance(result, pd.DataFrame):
        return _df(result)
    return result or []


# ── Settings ──────────────────────────────────────────────────────────────────

@app.get("/settings")
def get_settings():
    cfg = get_all_config()
    # Never expose secrets in GET — mask them
    safe = {k: v for k, v in cfg.items() if k not in ("telegram_bot_token", "telegram_chat_id")}
    safe["telegram_bot_token_set"] = bool(cfg.get("telegram_bot_token"))
    safe["telegram_chat_id_set"] = bool(cfg.get("telegram_chat_id"))
    return safe

class SettingsUpdate(BaseModel):
    settings: dict

@app.put("/settings")
def update_settings(body: SettingsUpdate):
    for key, value in body.settings.items():
        set_config(key, value)
    return {"ok": True}


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}
