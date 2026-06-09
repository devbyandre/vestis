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
    return _df(df)

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

@app.get("/analytics/rebalancing")
def get_rebalancing(portfolio_ids: Optional[str] = Query(None)):
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    result = mw.suggest_rebalancing(portfolio_ids=ids)
    if isinstance(result, pd.DataFrame):
        return _df(result)
    return result

@app.get("/analytics/indicators/{symbol}")
def get_indicators(
    symbol: str,
    window_sma: int = 20,
    window_ema: int = 20,
    window_rsi: int = 14,
    window_bb: int = 20,
    lookback_days: int = 400,
):
    """Returns price series plus all technical indicators for a symbol."""
    prices = db.get_price_history(symbol, lookback_days=lookback_days)
    if prices is None or prices.empty:
        raise HTTPException(404, f"No price data for {symbol}")

    adj = prices["adj_close"] if "adj_close" in prices.columns else prices["close"]
    adj = pd.to_numeric(adj, errors="coerce").ffill().bfill()

    result = _df(prices)

    # SMA
    try:
        sma = mw.sma(adj, window_sma)
        for i, row in enumerate(result):
            row[f"sma_{window_sma}"] = float(sma.iloc[i]) if i < len(sma) and pd.notna(sma.iloc[i]) else None
    except Exception:
        pass

    # EMA
    try:
        ema = mw.ema(adj, window_ema)
        for i, row in enumerate(result):
            row[f"ema_{window_ema}"] = float(ema.iloc[i]) if i < len(ema) and pd.notna(ema.iloc[i]) else None
    except Exception:
        pass

    # RSI
    try:
        rsi = mw.rsi(adj, window_rsi)
        for i, row in enumerate(result):
            row[f"rsi_{window_rsi}"] = float(rsi.iloc[i]) if i < len(rsi) and pd.notna(rsi.iloc[i]) else None
    except Exception:
        pass

    # Bollinger Bands
    try:
        bb = mw.bollinger(adj, window_bb)
        if isinstance(bb, pd.DataFrame):
            for i, row in enumerate(result):
                if i < len(bb):
                    for col in bb.columns:
                        v = bb.iloc[i][col]
                        row[f"bb_{col}"] = float(v) if pd.notna(v) else None
    except Exception:
        pass

    # Risk metrics
    try:
        row_metrics = {
            "sharpe": mw.sharpe_ratio(adj),
            "sortino": mw.sortino_ratio(adj),
            "calmar": mw.calmar_ratio(adj),
            "max_drawdown": mw.max_drawdown(adj),
            "volatility": mw.volatility(adj),
            "cagr": mw.cagr(adj),
        }
        return {"series": result, "metrics": {k: (float(v) if v is not None and pd.notna(v) else None) for k, v in row_metrics.items()}}
    except Exception:
        return {"series": result, "metrics": {}}

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
    return _df(snap)

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
