from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import pandas as pd

import middleware as mw
import db_utils as db

from ._helpers import _df, _safe_float

router = APIRouter(tags=["analytics"])


@router.get("/analytics/capital-gains")
def get_capital_gains(
    portfolio_ids: Optional[str] = Query(None),
    year: Optional[int] = None,
):
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    df = mw.calc_capital_gains_fifo(portfolio_ids=ids, year=year)
    return _df(df)

@router.get("/analytics/dividends")
def get_dividends(
    portfolio_ids: Optional[str] = Query(None),
    year: Optional[int] = None,
):
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    df = mw.calc_dividends_for_portfolio(portfolio_ids=ids, year=year)
    return _df(df)

@router.get("/analytics/revenues-summary")
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


@router.get("/analytics/rebalancing")
def get_rebalancing(portfolio_ids: Optional[str] = Query(None), retirement_year: int = 2047):
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    result = mw.suggest_rebalancing(portfolio_ids=ids, retirement_year=retirement_year)
    if isinstance(result, pd.DataFrame):
        return _df(result)
    if isinstance(result, dict) and isinstance(result.get("industry_weights"), dict):
        # industry_weights is keyed by (sector, industry) tuples — FastAPI's
        # jsonable_encoder turns each tuple key into a list to encode it,
        # then crashes trying to use that (unhashable) list as a dict key.
        result = {
            **result,
            "industry_weights": {
                f"{sec} → {ind}": v for (sec, ind), v in result["industry_weights"].items()
            },
        }
    return result


@router.get("/analytics/indicators/{symbol}")
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
        # cagr/calmar_ratio annualise using (df.index[-1] - df.index[0]).days,
        # so they need a real DatetimeIndex — the plain RangeIndex above won't do.
        dated_price_df = price_df.copy()
        dated_price_df.index = pd.to_datetime([r.get("date") for r in result])
        if hasattr(mw, "cagr"):
            metrics["cagr"] = _safe_float(mw.cagr(dated_price_df, "close"))
        if hasattr(mw, "calmar_ratio"):
            metrics["calmar"] = _safe_float(mw.calmar_ratio(dated_price_df, "close"))
        if hasattr(mw, "treynor_ratio"):
            # Real per-security beta rather than a hardcoded 1.0 (which is what
            # app_streamlit.py does — it never actually computes beta for this).
            try:
                basic = mw.get_security_basic(symbol) or {}
                beta = _safe_float(basic.get("beta")) or 1.0
            except Exception:
                beta = 1.0
            metrics["treynor"] = _safe_float(mw.treynor_ratio(price_df, "close", beta=beta))
    except Exception:
        metrics = {}

    return {"series": result, "metrics": metrics, "crossovers": crossovers, "extrema": extrema}

@router.get("/analytics/crossovers/{symbol}")
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

@router.get("/analytics/local-extrema/{symbol}")
def get_local_extrema(symbol: str, lookback_days: int = 200):
    prices = db.get_price_history(symbol, lookback_days=lookback_days)
    if prices is None or prices.empty:
        raise HTTPException(404, f"No price data for {symbol}")
    adj = pd.to_numeric(prices["adj_close"] if "adj_close" in prices.columns else prices["close"], errors="coerce").ffill()
    result = mw.local_min_max(adj)
    if isinstance(result, pd.DataFrame):
        return _df(result)
    return []
