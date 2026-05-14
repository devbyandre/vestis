#!/usr/bin/env python3
# middleware.py
import os, json, logging, datetime as dt
from typing import Optional, Dict, Any, List
import sqlite3
import pandas as pd
import numpy as np
import yfinance as yf
from scipy.signal import argrelextrema
import threading
from collections import deque

import db_utils as db

from config_utils import safe_json_load, get_config

from data_fetcher import fetch_and_store_lazy

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# SCHEMA = r"""
# PRAGMA foreign_keys=ON;
# CREATE TABLE IF NOT EXISTS portfolios(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE);
# CREATE TABLE IF NOT EXISTS transactions(id INTEGER PRIMARY KEY AUTOINCREMENT, portfolio_id INTEGER, symbol TEXT, tx_date TEXT, tx_type TEXT, quantity REAL, price REAL, tx_cost REAL DEFAULT 0, notes TEXT, recurring_id INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP);
# CREATE TABLE IF NOT EXISTS recurring_transactions(id INTEGER PRIMARY KEY AUTOINCREMENT, portfolio_id INTEGER, symbol TEXT, tx_type TEXT, start_date TEXT, end_date TEXT, frequency TEXT, quantity REAL, amount_money REAL, price REAL, tx_cost REAL, next_run TEXT, active INTEGER DEFAULT 1, notes TEXT);
# CREATE TABLE IF NOT EXISTS securities(symbol TEXT PRIMARY KEY, yahoo_symbol TEXT, isin TEXT, wkn TEXT, ticker TEXT, security_type TEXT, country TEXT, exchange TEXT, market TEXT, sector TEXT, industry TEXT, watchlist INTEGER DEFAULT 0, in_portfolio INTEGER DEFAULT 0, beta REAL, pb REAL, eps REAL, name TEXT, market_cap REAL, last_updated TEXT);
# CREATE TABLE IF NOT EXISTS prices(symbol TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL, adj_close REAL, volume REAL, PRIMARY KEY(symbol,date));
# CREATE TABLE IF NOT EXISTS dividends(symbol TEXT, date TEXT, dividend REAL, PRIMARY KEY(symbol,date));
# CREATE TABLE IF NOT EXISTS financials(symbol TEXT, statement TEXT, period TEXT, as_of_date TEXT, payload TEXT, PRIMARY KEY(symbol,statement,period,as_of_date));
# CREATE TABLE IF NOT EXISTS news(symbol TEXT, id TEXT, title TEXT, publisher TEXT, link TEXT, published_at TEXT, related_tickers TEXT, sentiment_score REAL, sentiment_label TEXT, payload TEXT, article_text TEXT, PRIMARY KEY(symbol,id));
# CREATE TABLE IF NOT EXISTS analyst_price_targets(symbol TEXT, as_of_date TEXT, target_low REAL, target_mean REAL, target_high REAL, target_median REAL, number_of_analysts INTEGER, PRIMARY KEY(symbol,as_of_date));
# CREATE TABLE IF NOT EXISTS alerts(id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL, alert_type TEXT NOT NULL, params TEXT, active INTEGER DEFAULT 1, snooze_until TEXT, cooldown_seconds INTEGER DEFAULT 3600, notify_mode TEXT DEFAULT 'immediate', last_evaluated TEXT, last_triggered TEXT);
# CREATE TABLE IF NOT EXISTS alerts_log(id INTEGER PRIMARY KEY AUTOINCREMENT, alert_id INTEGER NOT NULL, triggered_at TEXT NOT NULL, payload TEXT, FOREIGN KEY(alert_id) REFERENCES alerts(id));
# CREATE TABLE IF NOT EXISTS valuations(symbol TEXT PRIMARY KEY, intrinsic_per_share REAL, total_pv REAL, terminal_value REAL, shares_outstanding REAL, market_price REAL, margin_of_safety REAL, rating TEXT, computed_at TEXT, assumptions TEXT);
# CREATE TABLE IF NOT EXISTS kpi_cache(symbol TEXT PRIMARY KEY, payload TEXT, fetched_at TEXT);
# CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
# """


# ---------------------------
# Portfolio services
# ---------------------------
def create_portfolio(name: str) -> int:
    return db.insert_portfolio(name)


def rename_portfolio(old_name: str, new_name: str):
    db.rename_portfolio(old_name, new_name)


def list_portfolios() -> pd.DataFrame:
    return db.list_portfolios()


# ---------------------------
# Portfolio helpers
# ---------------------------
def delete_and_reassign_portfolio(del_name: str, target_name: str):
    """Delete portfolio by name; reassign its transactions to target."""

    if del_name == target_name:
        raise ValueError("Deleted portfolio and target portfolio cannot be the same.")

    # resolve IDs for names
    del_row = db.get_portfolio_by_name(del_name)
    if not del_row:
        raise ValueError(f"Portfolio '{del_name}' not found.")
    del_id = del_row["id"]

    target_row = db.get_portfolio_by_name(target_name)
    if not target_row:
        raise ValueError(f"Target portfolio '{target_name}' not found.")
    target_id = target_row["id"]

    # reassign transactions and delete portfolio
    db.reassign_transactions(del_id, target_id)
    db.delete_portfolio(del_id)


def get_all_symbols() -> List[str]:
    return db.get_all_symbols()


# ---------------------------
# Securities helpers
# ---------------------------
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

# ---------------------------
# Transactions services
# ---------------------------
def add_transaction(portfolio_id: int, symbol: str, tx_date: str,
                    tx_type: str, quantity: float, price: float, fees: float = 0.0):
    sec_id = add_security(symbol)
    db.insert_transaction(portfolio_id, sec_id, tx_date, tx_type, quantity, price, fees)
    db.recompute_holdings_timeseries(portfolio_id, sec_id)

# def edit_transaction(tx_id: int, tx_date: str, tx_type: str,
#                      quantity: float, price: float, fees: float) -> None:
#     db.update_transaction(tx_id, tx_date, tx_type, quantity, price, fees)
#     row = db.get_transaction_by_id(tx_id)
#     if row:
#         db.recompute_holdings_timeseries(row["portfolio_id"], row["security_id"])

def edit_transaction(tx_id: int, tx_date: str, tx_type: str,
                     quantity: float, price: float, fees: float,
                     security_id: int | None = None,
                     security_name: str | None = None,
                     security_isin: str | None = None):
    """
    Edit a transaction and optionally update the related security.
    """
    # Update transaction
    db.update_transaction(tx_id, tx_date, tx_type, quantity, price, fees)

    # Update security info if provided
    if security_id and (security_name is not None or security_isin is not None):
        db.update_security(security_id, name=security_name, isin=security_isin)

    # Recompute holdings timeseries if needed
    row = db.get_transaction_by_id(tx_id)
    if row:
        db.recompute_holdings_timeseries(row["portfolio_id"], row["security_id"])

def remove_transaction(tx_id: int) -> None:
    row = db.get_transaction_by_id(tx_id)
    db.delete_transaction(tx_id)
    if row:
        db.recompute_holdings_timeseries(row["portfolio_id"], row["security_id"])

def list_transactions(portfolio_ids: Optional[List[int]] = None) -> pd.DataFrame:
    return db.list_transactions(portfolio_ids)


def list_transactions_detailed() -> pd.DataFrame:
    return db.list_transactions_detailed()

def apply_splits_to_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Retroactively adjust buy/sell quantities and prices for any stock splits
    recorded as tx_type='split'.

    A split row encodes:
        quantity = new_shares / old_shares (e.g. 3.0 for a 3-for-1 split)
        price    = 0
        tx_date  = effective date of the split

    For each split on symbol S with ratio R on date D:
      - All buy/sell rows for S with tx_date < D get:
          quantity *= R
          price    /= R
      - The split row itself is excluded from the returned DataFrame

    Call this before any FIFO, dividends, or holdings calculation.
    """
    if df.empty:
        return df

    df = df.copy()
    date_col = 'tx_date' if 'tx_date' in df.columns else 'date'
    if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

    type_col = 'type' if 'type' in df.columns else 'tx_type'
    splits = df[df[type_col].str.lower() == 'split'].copy()

    if splits.empty:
        return df[df[type_col].str.lower() != 'split'].copy()

    for _, split_row in splits.iterrows():
        sym        = split_row.get('symbol')
        split_date = pd.to_datetime(split_row[date_col])
        try:
            ratio = float(split_row['quantity'])
        except (ValueError, TypeError):
            logging.warning("apply_splits: invalid ratio for %s on %s", sym, split_date)
            continue
        if ratio <= 0:
            continue

        mask = (
            (df['symbol'] == sym) &
            (df[type_col].str.lower().isin(['buy', 'sell'])) &
            (pd.to_datetime(df[date_col]) < split_date)
        )
        df.loc[mask, 'quantity'] = df.loc[mask, 'quantity'] * ratio
        df.loc[mask, 'price']    = df.loc[mask, 'price']    / ratio
        logging.info(
            "apply_splits: adjusted %d rows for %s (ratio %.4f, effective %s)",
            mask.sum(), sym, ratio, split_date.date()
        )

    return df[df[type_col].str.lower() != 'split'].copy()


def add_split_transaction(portfolio_id: int, symbol: str, split_date: str, ratio: float) -> None:
    """
    Record a stock split. ratio = new_shares / old_shares (e.g. 3.0 for 3-for-1).
    Use 0.5 for a 1-for-2 reverse split.
    All historical buy/sell quantities and prices are adjusted automatically
    when holdings, FIFO gains, and dividends are calculated.
    """
    if ratio <= 0:
        raise ValueError(f"Split ratio must be positive, got {ratio}")
    sec_id = add_security(symbol)
    db.insert_transaction(
        portfolio_id, sec_id, split_date,
        tx_type='split',
        quantity=ratio,
        price=0.0,
        fees=0.0,
    )
    db.recompute_holdings_timeseries(portfolio_id, sec_id)
    logging.info("Recorded split for %s: ratio=%.4f on %s", symbol, ratio, split_date)


def get_watchlist()-> pd.DataFrame:
    """
    Add derived metrics, scores, and temperature labels to the raw watchlist.
    """
    df = db.get_watchlist()

    return calc_security_KPIs(df)

def calc_security_KPIs(df: pd.DataFrame)-> pd.DataFrame:
    """
    Add derived metrics, scores, and temperature labels to the raw watchlist.
    """
    df = df.copy()

    # Derived metrics
    df['pb_ratio'] = df.apply(lambda r: r['regularMarketPrice']/r['bookValue'] if r['bookValue'] else None, axis=1)
    df['from_52w_low'] = df.apply(
        lambda r: (r['regularMarketPrice'] - r['fiftyTwoWeekLow']) / 
                  (r['fiftyTwoWeekHigh'] - r['fiftyTwoWeekLow'])
        if r['fiftyTwoWeekHigh'] and r['fiftyTwoWeekLow'] and r['fiftyTwoWeekHigh'] != r['fiftyTwoWeekLow'] else None,
        axis=1
    )

    # Scoring functions
    def score_beta(b): return 1.0 if b is not None and b < 1 else (0.5 if b is not None and b <= 1.2 else 0)
    def score_pe(pe): return 1.0 if pe is not None and pe < 15 else (0.5 if pe is not None and pe <= 30 else 0)
    def score_pb(pb): return 1.0 if pb is not None and pb < 1.5 else (0.5 if pb is not None and pb <= 3 else 0)
    def score_div(y): return 1.0 if y is not None and y > 0.03 else (0.5 if y is not None and y > 0.01 else 0)
    def score_52w(low_pct): return 1.0 if low_pct is not None and low_pct < 0.2 else (0.5 if low_pct is not None and low_pct <= 0.8 else 0)
    def score_profit(pm): return 1.0 if pm is not None and pm > 0.2 else (0.5 if pm is not None and pm > 0.1 else 0)

    df['beta_score'] = df['beta'].apply(score_beta)
    df['pe_score'] = df['trailingPE'].apply(score_pe)
    df['pb_score'] = df['pb_ratio'].apply(score_pb)
    df['div_yield_score'] = df['dividendYield'].apply(score_div)
    df['from_52w_low_score'] = df['from_52w_low'].apply(score_52w)
    df['profit_margin_score'] = df['profitMargins'].apply(score_profit)

    # Overall temperature
    df['temperature_score'] = df[[
        'beta_score', 'pe_score', 'pb_score', 'div_yield_score', 'from_52w_low_score', 'profit_margin_score'
    ]].mean(axis=1)

    def temperature_label(score):
        if score is None:
            return "N/A"
        elif score <= 0.33:
            return "Cold"
        elif score <= 0.66:
            return "Warm"
        else:
            return "Hot"

    df['Temperature'] = df['temperature_score'].apply(temperature_label)
    return df


def delete_security_from_watchlist(symbol: str)-> None:
    db.delete_watchlist_item(symbol)


def add_security(symbol: str) -> int:
    """
    Add a security to the database if it does not exist.
    Returns the security_id.
    Fetches data immediately for new securities.
    """
    security_id = db.get_security_id(symbol)
    if security_id is None:
        security_id = db.insert_security(symbol)

        # Fetch data in background
        thread = threading.Thread(target=fetch_and_store_lazy, args=(symbol,), daemon=True)
        thread.start()

    return security_id

# def get_security_basic(symbol: str, cache_hours: float = 24) -> Dict[str, Any]:
#     """
#     Get basic cached info for a security by symbol.
#     Always returns a dict (may be empty if not found).
#     """
#     sec_id = db.get_security_id(symbol)

#     if sec_id:
#         row = db.get_security_cache(sec_id)
#     return row if row else {}


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



# # recurring
# def add_recurring(conn, portfolio_id:int, symbol:str, type:str, start_date,
#                   frequency:str, quantity:Optional[float]=None, amount_money:Optional[float]=None,
#                   price:Optional[float]=None, tx_cost:float=0.0, end_date:Optional[str]=None,
#                   notes:Optional[str]=None):
#     security_id = get_security_id(conn, symbol)
#     if not security_id:
#         raise ValueError(f"Security {symbol} not found in DB")

#     start_s = _iso(start_date)
#     end_s = _iso(end_date) if end_date else None
#     conn.execute("""INSERT INTO recurring_transactions(
#                         portfolio_id, security_id, type, start_date, end_date,
#                         frequency, quantity, amount_money, price, tx_cost, next_run, active, notes
#                     ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
#                  (portfolio_id, security_id, type, start_s, end_s, frequency,
#                   quantity, amount_money, price, tx_cost, start_s, 1, notes))
#     conn.commit()


# def list_recurring(conn):
#     return pd.read_sql_query("""
#         SELECT r.*, p.name as portfolio_name, s.yahoo_ticker
#         FROM recurring_transactions r
#         LEFT JOIN portfolios p ON p.id=r.portfolio_id
#         LEFT JOIN securities s ON s.id=r.security_id
#         ORDER BY next_run
#     """, conn)


# def _advance_date_str(date_str, freq):
#     d = pd.to_datetime(date_str)
#     if freq == 'monthly': d += pd.DateOffset(months=1)
#     elif freq == 'quarterly': d += pd.DateOffset(months=3)
#     elif freq in ('annually','annual','yearly'): d += pd.DateOffset(years=1)
#     elif freq == 'weekly': d += pd.DateOffset(weeks=1)
#     else: d += pd.DateOffset(months=1)
#     return d.date().isoformat()


# def _lookup_price(conn, security_id, on_date):
#     cur = conn.execute("SELECT close FROM prices WHERE security_id=? AND date<=? ORDER BY date DESC LIMIT 1",
#                        (security_id, on_date))
#     r = cur.fetchone()
#     return float(r[0]) if r else None


# def run_due_recurring(conn, as_of=None):
#     as_of_date = pd.to_datetime(as_of).date() if as_of else pd.Timestamp.today().date()
#     cur = conn.execute("SELECT * FROM recurring_transactions WHERE active=1 AND (next_run IS NULL OR next_run<=?)",
#                        (as_of_date.isoformat(),))
#     for r in cur.fetchall():
#         r = dict(r)
#         security_id = r['security_id']
#         pid = r['portfolio_id']
#         run_date = pd.to_datetime(r['next_run']).date() if r['next_run'] else as_of_date
#         exec_price = r['price'] if r['price'] else _lookup_price(conn, security_id, run_date.isoformat())
#         if exec_price is None:
#             next_run = _advance_date_str(run_date.isoformat(), r['frequency'])
#             conn.execute("UPDATE recurring_transactions SET next_run=? WHERE id=?", (next_run, r['id']))
#             conn.commit()
#             continue
#         qty = 0
#         if r['amount_money'] and r['amount_money'] > 0:
#             qty = int((r['amount_money']) // exec_price)
#         elif r['quantity'] and r['quantity'] != 0:
#             qty = int(r['quantity'])
#         if qty > 0:
#             # insert transaction
#             symbol = conn.execute("SELECT yahoo_ticker FROM securities WHERE id=?", (security_id,)).fetchone()[0]
#             add_transaction(conn, pid, symbol, run_date.isoformat(), r['type'], qty, exec_price, r['tx_cost'], r['notes'])
#         next_run = _advance_date_str(run_date.isoformat(), r['frequency'])
#         if r['end_date'] and pd.to_datetime(next_run).date() > pd.to_datetime(r['end_date']).date():
#             conn.execute("UPDATE recurring_transactions SET active=0 WHERE id=?", (r['id'],))
#         else:
#             conn.execute("UPDATE recurring_transactions SET next_run=? WHERE id=?", (next_run, r['id']))
#         conn.commit()


# ---------------------------
# technical indicators
# ---------------------------

def sma(series, window):
    return series.rolling(window).mean()

def ema(series, span):
    return series.ewm(span=span, adjust=False).mean()

def bollinger(series, window=20, n_std=2.0):
    m = sma(series, window); s = series.rolling(window).std()
    return m, m + n_std*s, m - n_std*s

def rsi(series, window=14):
    delta = series.diff()
    up = delta.clip(lower=0); down = -delta.clip(upper=0)
    ma_up = up.ewm(com=window-1, adjust=False).mean(); ma_down = down.ewm(com=window-1, adjust=False).mean()
    rs = ma_up / ma_down
    return 100 - (100 / (1 + rs))

def local_min_max(series, order=5):
    try:
        arr = series.values
        idx_max = argrelextrema(arr, np.greater, order=order)[0]
        idx_min = argrelextrema(arr, np.less, order=order)[0]
    except Exception:
        idx_min = np.array([], dtype=int); idx_max = np.array([], dtype=int)
    return idx_min, idx_max


def calc_returns(prices: pd.Series) -> pd.Series:
    """
    Calculate daily returns from a price series.
    Returns NaN for the first value.
    """
    prices = prices.astype(float)
    returns = prices.pct_change()
    return returns


def find_crossovers(short_ma: pd.Series, long_ma: pd.Series):
    """
    Identify crossover points between two moving averages.
    Returns buy and sell indices (buy when short MA crosses above long MA).
    """
    if len(short_ma) != len(long_ma):
        raise ValueError("MA series must be the same length")
    
    buy_signals = []
    sell_signals = []
    prev_diff = short_ma.iloc[0] - long_ma.iloc[0]
    
    for i in range(1, len(short_ma)):
        diff = short_ma.iloc[i] - long_ma.iloc[i]
        if prev_diff <= 0 and diff > 0:
            buy_signals.append(i)
        elif prev_diff >= 0 and diff < 0:
            sell_signals.append(i)
        prev_diff = diff
    
    return buy_signals, sell_signals

def volatility(df, price_col='close'):
    """
    Calculate annualized volatility from daily returns.
    df: DataFrame with a column price_col
    """
    df = df.copy()
    df['returns'] = df[price_col].pct_change()
    vol = df['returns'].std() * (252 ** 0.5)  # annualized
    return vol

def max_drawdown(series: pd.Series) -> float:
    """
    Calculate the maximum drawdown of a price series.
    
    Parameters:
        series (pd.Series): Price series
    
    Returns:
        float: Max drawdown as a fraction (e.g., 0.25 = 25%)
    """
    if series.empty:
        return 0.0
    roll_max = series.cummax()
    drawdown = (series - roll_max) / roll_max
    max_dd = drawdown.min()
    return abs(max_dd)

def sharpe_ratio(df, price_col='close', risk_free_rate=0.0):
    """
    Sharpe Ratio = Excess return / Total volatility
    Higher is better. >1.0 is good, >2.0 is very good.
    """
    df = df.copy()
    df['returns'] = df[price_col].pct_change().dropna()
    excess_ret = df['returns'] - (risk_free_rate / 252)
    avg_excess_ret = excess_ret.mean() * 252
    vol = excess_ret.std() * np.sqrt(252)
    return avg_excess_ret / vol if vol != 0 else np.nan


def sortino_ratio(df, price_col='close', risk_free_rate=0.0):
    """
    Sortino Ratio = Excess return / Downside volatility
    Focuses only on downside risk. >1.0 is good, >2.0 is very good.
    """
    df = df.copy()
    df['returns'] = df[price_col].pct_change().dropna()
    excess_ret = df['returns'] - (risk_free_rate / 252)
    avg_excess_ret = excess_ret.mean() * 252
    downside = excess_ret[excess_ret < 0]
    downside_vol = downside.std() * np.sqrt(252)
    return avg_excess_ret / downside_vol if downside_vol != 0 else np.nan


def cagr(df, price_col='close'):
    """
    CAGR = Annualized return
    Positive CAGR is required. 5–10% is typical for equities.
    """
    if df.empty:
        return np.nan
    start_val = df[price_col].iloc[0]
    end_val = df[price_col].iloc[-1]
    n_days = (df.index[-1] - df.index[0]).days
    if n_days <= 0:
        return np.nan
    years = n_days / 365.25
    return (end_val / start_val) ** (1 / years) - 1


def calmar_ratio(df, price_col='close'):
    """
    Calmar Ratio = CAGR / Max Drawdown
    Higher is better. >0.5 is decent, >1.0 is strong.
    """
    cagr_val = cagr(df, price_col)
    max_dd_val = max_drawdown(df[price_col])
    return cagr_val / max_dd_val if max_dd_val != 0 else np.nan


def treynor_ratio(df, price_col='close', beta=1.0, risk_free_rate=0.0):
    """
    Treynor Ratio = Excess return / Beta
    Measures return per unit of systematic risk.
    Higher is better.
    """
    df = df.copy()
    df['returns'] = df[price_col].pct_change().dropna()
    avg_excess_ret = (df['returns'].mean() - (risk_free_rate / 252)) * 252
    return avg_excess_ret / beta if beta != 0 else np.nan


def information_ratio(df, benchmark, price_col='close'):
    """
    Information Ratio = Active return / Tracking error
    >0.5 good, >1.0 very good.
    """
    df = df.copy()
    df['returns'] = df[price_col].pct_change().dropna()
    benchmark = benchmark.copy()
    benchmark['returns'] = benchmark[price_col].pct_change().dropna()
    
    active_ret = df['returns'].align(benchmark['returns'], join='inner')[0] - \
                 df['returns'].align(benchmark['returns'], join='inner')[1]
    
    avg_active_ret = active_ret.mean() * 252
    tracking_err = active_ret.std() * np.sqrt(252)
    return avg_active_ret / tracking_err if tracking_err != 0 else np.nan


# -----------------------------
# Free Cash Flow extraction
# -----------------------------
def extract_fcf_from_cashflow_payloads(symbol: str, lookback_years: int = 8) -> pd.Series:
    """
    Extract Free Cash Flow (FCF) from cashflow financial statements.
    Returns a pandas Series indexed by year (most recent first).
    """
    df = db.get_cashflow_payloads(symbol)
    if df.empty:
        return pd.Series(dtype=float)

    rows = []

    for _, r in df.iterrows():
        try:
            asof = r['as_of_date']
            payload = json.loads(r['payload']) if isinstance(r['payload'], str) else r['payload']
            fcf = None

            if isinstance(payload, dict):
                # Direct freeCashflow
                for k, v in payload.items():
                    if 'free cash' in k.lower() or 'freecash' in k.lower():
                        try:
                            fcf = float(v)
                            break
                        except Exception:
                            continue

                # If not found, compute FCF = Operating Cashflow - CapEx
                if fcf is None:
                    oc = None
                    capex = None
                    for k, v in payload.items():
                        kl = k.lower()
                        if 'operat' in kl and 'cash' in kl:
                            try: oc = float(v)
                            except Exception: oc = None
                        if 'capitalexpend' in kl or 'capex' in kl:
                            try: capex = float(v)
                            except Exception: capex = None
                    if oc is not None:
                        fcf = oc - abs(capex) if capex is not None else oc

            if fcf is not None:
                year = pd.to_datetime(asof).year
                rows.append((year, float(fcf)))

        except Exception:
            continue

    if not rows:
        return pd.Series(dtype=float)

    # Keep only the most recent FCF per year
    seen = {}
    for y, v in rows:
        if y not in seen:
            seen[y] = v

    ser = pd.Series({y: seen[y] for y in sorted(seen.keys(), reverse=True)})
    if len(ser) > lookback_years:
        ser = ser.iloc[:lookback_years]

    return ser




# -----------------------------
# Compute DCF
# -----------------------------
def compute_dcf_raw(symbol):
    projection_years = int(get_config('dcf_projection_years'))
    discount_rate = float(get_config('dcf_discount_rate'))
    terminal_growth = float(get_config('dcf_terminal_growth'))
    conservative = bool(get_config('dcf_conservative'))

    # Skip financials / REITs
    try:
        meta = db.list_securities_metadata()
        sector = meta['sector'] if meta else None
        if sector and any(x in sector.lower() for x in ['bank','financial','insurance','real estate','reit']):
            return {'note': 'DCF not meaningful for financial / REIT business models', 'intrinsic_per_share': None}
    except Exception:
        pass

    fcf_series = extract_fcf_from_cashflow_payloads(symbol, lookback_years=8)
    if fcf_series.empty:
        return {'note': 'Insufficient FCF data for DCF', 'intrinsic_per_share': None}

    # Median growth
    growth_rates = fcf_series.pct_change().dropna()
    g = float(growth_rates.median()) if not growth_rates.empty else 0.05
    if conservative:
        g *= 0.6

    # Project FCF
    last_fcf = float(fcf_series.iloc[-1])
    projected = []
    f = last_fcf
    for _ in range(projection_years):
        f *= (1 + g)
        projected.append(f)

    # Terminal value
    terminal = projected[-1] * (1 + terminal_growth) / (discount_rate - terminal_growth) if discount_rate > terminal_growth else 0.0

    # Present value
    pv = sum(cf / ((1 + discount_rate) ** (i + 1)) for i, cf in enumerate(projected))
    pv_terminal = terminal / ((1 + discount_rate) ** projection_years)
    total_pv = pv + pv_terminal

    # Shares & market price
    so = db.get_shares_outstanding(symbol)
    market_price = db.get_latest_price(symbol)
    intrinsic_per_share = total_pv / so if so and so > 0 else None

    # Margin of safety
    margin_of_safety = None
    rating = 'N/A'
    if intrinsic_per_share and market_price:
        margin_of_safety = (intrinsic_per_share - market_price) / intrinsic_per_share
        mos_pct = margin_of_safety * 100
        if mos_pct >= 20:
            rating = 'Buy'
        elif mos_pct >= 0:
            rating = 'Hold'
        else:
            rating = 'Sell'

    return {
        'intrinsic_per_share': intrinsic_per_share,
        'total_pv': total_pv,
        'terminal_value': terminal,
        'shares_outstanding': so,
        'market_price': market_price,
        'margin_of_safety': margin_of_safety,
        'rating': rating,
        'note': None
    }


# -----------------------------
# Compute DCF with caching
# -----------------------------
def compute_dcf_cached(symbol: str, assumptions: dict = {}, max_age_hours: float = 24, force_refresh: bool = False):
    """
    Compute DCF for a security, using cached values if available and valid.
    """
    # Try fetching cached DCF
    cached = db.get_cached_dcf(symbol)
    if cached and not force_refresh:
        try:
            computed_at = pd.to_datetime(cached['valuation_computed_at'])
            age_hours = (pd.Timestamp.utcnow() - computed_at).total_seconds() / 3600.0
            cached_assumptions = json.loads(cached['assumptions']) if cached.get('assumptions') else {}
            if age_hours < max_age_hours and cached_assumptions == assumptions:
                cached['cached'] = True
                cached['computed_at'] = str(cached['valuation_computed_at'])
                return cached
        except Exception:
            pass

    # Compute raw DCF
    res = compute_dcf_raw(symbol)

    # Get security_id
    security_id = db.get_security_id(symbol)
    if not security_id:
        raise ValueError(f"Security {symbol} not found in DB")

    # Store in DB if DCF is valid
    if res.get('intrinsic_per_share') is not None:
        db.store_dcf(security_id, res)

    res['cached'] = False
    res['computed_at'] = pd.Timestamp.utcnow().isoformat()
    return res



# ---------------------------
# dividends and capital gains
# ---------------------------
def calc_dividends_for_portfolio(portfolio_ids: Optional[list] = None, year: Optional[int] = None):
    tx = list_transactions(portfolio_ids)
    if tx.empty:
        return pd.DataFrame(columns=['symbol','portfolio_id','date','dividend_per_share','shares','total','year'])

    tx['tx_date'] = pd.to_datetime(tx['date']).dt.date

    # Apply split adjustments before FIFO — splits are not taxable events
    tx = apply_splits_to_transactions(tx)

    rows = []

    tickers = tx['symbol'].dropna().unique()

    for sym in tickers:
        df_divs = db.get_dividends(sym)
        if df_divs.empty:
            continue

        for _, d in df_divs.iterrows():
            d_date = pd.to_datetime(d['date']).date()
            amt = float(d['dividend'])
            yr = d_date.year
            if year and yr != int(year):
                continue

            df_sym = tx[tx['symbol']==sym]
            if df_sym.empty:
                continue

            df_sym = df_sym.copy()
            df_sym['qty_signed'] = df_sym.apply(lambda r: r['quantity'] if r['type'].lower()=='buy' else -r['quantity'], axis=1)
            df_sym = df_sym[df_sym['tx_date'] <= d_date]
            if df_sym.empty:
                continue

            holdings_by_port = df_sym.groupby('portfolio_id').qty_signed.sum().to_dict()
            for pid, qty in holdings_by_port.items():
                if qty <= 0:
                    continue
                rows.append({
                    'symbol': sym,
                    'portfolio_id': pid,
                    'date': d_date.isoformat(),
                    'dividend_per_share': amt,
                    'shares': qty,
                    'total': amt*qty,
                    'year': yr
                })

    return pd.DataFrame(rows)


def calc_capital_gains_fifo(portfolio_ids: Optional[list] = None, year: Optional[int] = None):
    df_tx = list_transactions(portfolio_ids)
    if df_tx.empty:
        return pd.DataFrame(columns=['portfolio_id','symbol','sell_date','quantity','proceeds','cost_basis','profit','year'])

    df_tx['tx_date'] = pd.to_datetime(df_tx['date'])
    # Apply split adjustments before FIFO — splits are not taxable events
    df_tx = apply_splits_to_transactions(df_tx)
    rows = []

    for pid, g in df_tx.groupby('portfolio_id'):
        buys = {}
        for _, r in g.sort_values(['tx_date','id']).iterrows():
            sym = r.get('symbol')
            if not sym:
                continue  # skip if no symbol

            typ = r['type'].lower()


            qty = float(r['quantity'])
            price = float(r['price']) if r['price'] else 0.0
            fees = float(r.get('fees') or 0.0)

            if typ == 'buy':
                buys.setdefault(sym, []).append({'qty': qty, 'price': price, 'fees': fees, 'date': r['tx_date']})
            elif typ == 'sell':
                remaining = qty
                proceeds = qty * price - fees
                cost_basis = 0.0
                while remaining > 0 and buys.get(sym):
                    b = buys[sym][0]
                    take = min(b['qty'], remaining)
                    cost_basis += take * b['price'] + (b['fees'] * (take / (b['qty'] if b['qty'] else 1)))
                    b['qty'] -= take
                    remaining -= take
                    if b['qty'] <= 1e-12:
                        buys[sym].pop(0)
                profit = proceeds - cost_basis
                yr = int(pd.to_datetime(r['tx_date']).year)
                if year and yr != int(year):
                    continue
                rows.append({
                    'portfolio_id': pid,
                    'symbol': sym,
                    'sell_date': r['tx_date'].isoformat(),
                    'quantity': qty,
                    'proceeds': proceeds,
                    'cost_basis': cost_basis,
                    'profit': profit,
                    'year': yr
                })
    return pd.DataFrame(rows)



# -----------------------------
# Alerts Middleware
# -----------------------------

def get_watchlist_symbols() -> list:
    """Return all symbols currently on the watchlist."""
    df = get_watchlist()
    return df['symbol'].dropna().tolist() if not df.empty else []


def get_holdings():
    return db.get_holdings_from_transactions(None)


def get_portfolio_symbols(portfolio_name: str) -> list:
    portfolios = list_portfolios()
    row = portfolios[portfolios['name'] == portfolio_name]
    if row.empty:
        return []

    portfolio_id = int(row.iloc[0]['id'])
    tx = list_transactions([portfolio_id])
    if tx.empty:
        return []

    # Compute signed quantities: buy = +quantity, sell = -quantity
    tx['signed_qty'] = tx.apply(lambda r: r['quantity'] if r['type'] == 'buy' else -r['quantity'], axis=1)

    # Sum by symbol
    net_quantities = tx.groupby('symbol')['signed_qty'].sum()

    # Keep only symbols with net quantity > 0
    active_symbols = net_quantities[net_quantities > 0].index.tolist()

    return active_symbols


def get_alerts() -> pd.DataFrame:
    """Return all alerts with security name for UI."""
    df = db.get_all_alerts()
    if df.empty:
        return pd.DataFrame(columns=[
            "id", "security_id", "alert_type", "params",
            "active", "notify_mode", "cooldown_seconds",
            "last_evaluated", "last_triggered", "note", "auto_managed"
        ])
    return df

def get_automatic_alerts() -> pd.DataFrame:
    """Return only alerts that are automatically managed."""
    df = get_alerts()
    if 'auto_managed' not in df.columns:
        return pd.DataFrame(columns=df.columns)  # no automatic alerts if column missing
    return df[df['auto_managed'] == True]

def create_alert(security_id: int, alert_type: str, params: dict,
                 notify_mode: str = "immediate", cooldown_seconds: int = 3600,
                 active: bool = True, note: str | None = None,
                 automatic: bool = False) -> int:
    """Create a new alert, optionally marking it as automatic."""
    params_json = json.dumps(params)
    return db.create_alert(
        security_id=security_id,
        alert_type=alert_type,
        params_json=params_json,
        notify_mode=notify_mode,
        cooldown_seconds=cooldown_seconds,
        active=active,
        note=note,
        auto_managed=automatic
    )


def edit_alert(alert_id: int, alert_type: str = None, params: dict = None,
               notify_mode: str = None, cooldown_seconds: int = None,
               active: bool = None, note: str = None, automatic=False) -> None:
    """
    Middleware function to update an alert.
    """
    db.update_alert(
        alert_id=alert_id,
        alert_type=alert_type,
        params=params,
        notify_mode=notify_mode,
        cooldown_seconds=cooldown_seconds,
        active=active,
        note=note, 
        automatic=automatic
    )

def toggle_alert(alert_id: int, active: bool) -> None:
    """Activate or deactivate an alert."""
    db.toggle_alert_active(alert_id, active)


def delete_alert(alert_id: int) -> None:
    """Delete an alert."""
    db.delete_alert(alert_id)


def log_trigger(alert_id: int, payload: Dict) -> None:
    """Log alert trigger."""
    db.log_alert_trigger(alert_id, payload)


def last_trigger(alert_id: int) -> Optional[pd.Timestamp]:
    """Return last trigger time."""
    return db.last_trigger_time(alert_id)



def fetch_symbol_data(symbol: str) -> dict:
    """
    Gather latest market data and indicators for a symbol.
    Returns a dict, or {} if no data available.

    Ensures all datetime values are tz-naive to avoid tz-aware/tz-naive comparison errors.
    """
    try:
        prices = db.get_price_history(symbol, lookback_days=400)
        if prices is None or prices.empty:
            logging.warning("fetch_symbol_data: no price history for %s", symbol)
            return {}

        # --- Ensure date column is timezone-naive (robust) ---
        prices = prices.copy()
        prices['date'] = pd.to_datetime(prices['date'], errors='coerce')

        # convert each Timestamp to a tz-naive python datetime to be 100% safe
        def _to_naive(ts):
            if pd.isna(ts):
                return pd.NaT
            # pd.Timestamp -> to_pydatetime, remove tzinfo
            try:
                py = pd.Timestamp(ts).to_pydatetime()
                return py.replace(tzinfo=None)
            except Exception:
                return pd.NaT

        prices['date'] = prices['date'].apply(_to_naive)
        prices = prices.sort_values('date').reset_index(drop=True)

        # --- last price & last volume ---
        last_row = prices.iloc[-1]
        last_price = None
        # prefer adj_close -> close
        for col in ('adj_close', 'close'):
            if col in last_row and pd.notna(last_row[col]):
                try:
                    last_price = float(last_row[col])
                    break
                except Exception:
                    pass
        if last_price is None:
            logging.warning("fetch_symbol_data: last price missing for %s", symbol)
            return {}

        last_vol = 0.0
        if 'volume' in last_row and pd.notna(last_row['volume']):
            try:
                last_vol = float(last_row['volume'])
            except Exception:
                last_vol = 0.0

        # --- 52-week high / low (use tz-naive cutoff) ---
        cutoff = pd.Timestamp(pd.Timestamp.utcnow().to_pydatetime().replace(tzinfo=None)) - pd.Timedelta(days=365)
        last_52w = prices[prices['date'] >= cutoff]
        high_52w = None
        low_52w = None
        if not last_52w.empty and 'adj_close' in last_52w.columns:
            adj_52 = pd.to_numeric(last_52w['adj_close'], errors='coerce').dropna()
            if not adj_52.empty:
                high_52w = float(adj_52.max())
                low_52w = float(adj_52.min())

        # --- Prepare adj_close series for indicators ---
        if 'adj_close' in prices.columns:
            adj = pd.to_numeric(prices['adj_close'], errors='coerce')
        elif 'close' in prices.columns:
            adj = pd.to_numeric(prices['close'], errors='coerce')
        else:
            adj = pd.Series(dtype=float)

        adj = adj.ffill().bfill()  # best-effort fill


        # --- SMA / EMA ---
        sma = {}
        ema = {}
        for win in (5, 10, 50, 100, 200):
            if len(adj) >= win:
                try:
                    sma_val = adj.rolling(window=win).mean().iloc[-1]
                    ema_val = adj.ewm(span=win, adjust=False).mean().iloc[-1]
                    sma[win] = float(sma_val) if pd.notna(sma_val) else None
                    ema[win] = float(ema_val) if pd.notna(ema_val) else None
                except Exception:
                    sma[win] = None
                    ema[win] = None
            else:
                sma[win] = None
                ema[win] = None

        # --- RSI (uses rsi() defined elsewhere in middleware) ---
        rsi_val = None
        try:
            if len(adj) >= 15:  # need at least window+1 for a meaningful RSI
                rsi_series = rsi(adj, window=14)
                if not rsi_series.empty and pd.notna(rsi_series.iloc[-1]):
                    rsi_val = float(rsi_series.iloc[-1])
        except Exception:
            logging.exception("fetch_symbol_data: RSI failed for %s", symbol)
            rsi_val = None

        # --- Volume averages ---
        avg_volume = {}
        if 'volume' in prices.columns:
            vol = pd.to_numeric(prices['volume'], errors='coerce').fillna(0.0)
            for look in (10, 20, 50):
                if len(vol) >= look:
                    try:
                        v = vol.rolling(window=look).mean().iloc[-1]
                        avg_volume[look] = float(v) if pd.notna(v) else None
                    except Exception:
                        avg_volume[look] = None
                else:
                    avg_volume[look] = None

        # --- Fundamentals / MOS if available ---
        mos_val = None
        try:
            # db.get_fundamentals may return dict or None; guard it
            fundamentals = getattr(db, "get_fundamentals", lambda s: None)(symbol)
            if fundamentals and isinstance(fundamentals, dict) and "mos" in fundamentals:
                mos_val = fundamentals.get("mos")
        except Exception:
            logging.exception("fetch_symbol_data: fundamentals failed for %s", symbol)

        return {
            "last_price": last_price,
            "rsi": rsi_val,
            "sma": sma,
            "ema": ema,
            "52w_high": high_52w,
            "52w_low": low_52w,
            "volume": last_vol,
            "avg_volume": avg_volume,
            "mos": mos_val
        }

    except Exception:
        logging.exception("fetch_symbol_data failed for %s", symbol)
        return {}




def evaluate_alert(alert: dict) -> bool:
    symbol = alert["symbol"]
    a_type = alert["alert_type"]
    params = json.loads(alert.get("params") or "{}")

    try:
        data = fetch_symbol_data(symbol)
        if not data:
            logging.info("evaluate_alert: %s (%s) — skipped (no data)", symbol, a_type)
            return False

        result = False

        # Price alert — crossing detection only
        # Fires once when price crosses the threshold, not continuously while below/above.
        # Stores the last "side" in the alert log to detect transitions.
        if a_type == "price":
            lp = data.get("last_price")
            if lp is not None:
                threshold = float(params.get("threshold"))
                mode = params.get("mode", "absolute")
                direction = params.get("direction", "above")
                target = threshold if mode == "absolute" else lp * (1 + threshold)
                curr_side = "above" if lp > target else "below"
                # Get last recorded side from alert log
                last_log = db.get_last_alert_log(alert.get("id"))
                prev_side = (last_log.get("side") if last_log else None)
                # Fire only on transition to the alert direction
                if curr_side == direction and prev_side != direction:
                    result = True
                # Always update side so next run knows where we are
                alert["_curr_side"] = curr_side

        elif a_type == "rsi":
            rsi_val = data.get("rsi")
            if rsi_val is not None:
                thr = float(params.get("threshold", 70))
                direction = params.get("direction", "above")
                result = (direction == "above" and rsi_val > thr) or \
                         (direction == "below" and rsi_val < thr)

        elif a_type == "ma_crossover":
            # Only fire if the cross happened within the last `lookback` bars
            # (not just that fast MA is currently above slow MA — that would fire forever)
            short = int(params.get("short", 50))
            long_ = int(params.get("long", 200))
            crossover_type = params.get("crossover_type", params.get("direction", "golden"))
            lookback = int(params.get("lookback_bars", 3))
            try:
                prices = db.get_price_history(symbol, lookback_days=400)
                if prices is not None and not prices.empty and len(prices) > long_ + lookback:
                    if 'adj_close' in prices.columns:
                        adj = pd.to_numeric(prices['adj_close'], errors='coerce').ffill().bfill()
                    else:
                        adj = pd.to_numeric(prices['close'], errors='coerce').ffill().bfill()
                    fast = adj.rolling(short).mean()
                    slow = adj.rolling(long_).mean()
                    # Check if cross occurred in the last `lookback` bars
                    for i in range(-lookback, 0):
                        prev_fast = fast.iloc[i - 1]
                        prev_slow = slow.iloc[i - 1]
                        curr_fast = fast.iloc[i]
                        curr_slow = slow.iloc[i]
                        if pd.isna(prev_fast) or pd.isna(prev_slow):
                            continue
                        if crossover_type == "golden" and prev_fast <= prev_slow and curr_fast > curr_slow:
                            result = True
                            break
                        if crossover_type == "death" and prev_fast >= prev_slow and curr_fast < curr_slow:
                            result = True
                            break
            except Exception:
                logging.exception("evaluate_alert: ma_crossover failed for %s", symbol)

        elif a_type == "52w":
            # Only fire if the 52w high/low was broken in the last `lookback` bars
            typ = params.get("type", "high")
            lookback = int(params.get("lookback_bars", 3))
            try:
                prices = db.get_price_history(symbol, lookback_days=400)
                if prices is not None and not prices.empty and len(prices) > lookback + 1:
                    if 'adj_close' in prices.columns:
                        adj = pd.to_numeric(prices['adj_close'], errors='coerce').ffill().bfill()
                    else:
                        adj = pd.to_numeric(prices['close'], errors='coerce').ffill().bfill()
                    cutoff = pd.Timestamp.utcnow().replace(tzinfo=None) - pd.Timedelta(days=365)
                    prices_copy = prices.copy()
                    prices_copy['date'] = pd.to_datetime(prices_copy['date'], errors='coerce').apply(
                        lambda t: t.replace(tzinfo=None) if pd.notna(t) else pd.NaT)
                    for i in range(-lookback, 0):
                        curr_price = adj.iloc[i]
                        # compute 52w high/low up to bar i-1 (not including current bar)
                        hist = adj.iloc[:len(adj)+i]
                        hist_dates = prices_copy['date'].iloc[:len(adj)+i]
                        hist_52w = hist[hist_dates >= cutoff]
                        if hist_52w.empty:
                            continue
                        if typ == "high" and curr_price >= hist_52w.max():
                            result = True
                            break
                        elif typ == "low" and curr_price <= hist_52w.min():
                            result = True
                            break
            except Exception:
                logging.exception("evaluate_alert: 52w failed for %s", symbol)

        elif a_type == "volume_spike":
            mult = float(params.get("multiplier", 2.0))
            look = int(params.get("lookback", 20))
            vol = data.get("volume")
            avg = data.get("avg_volume", {}).get(look)
            if vol is not None and avg is not None and avg > 0:
                result = vol >= mult * avg

        elif a_type == "pct_change":
            # Fire when price has changed by more than pct% within the last `days` calendar days
            # e.g. {"pct": 5, "days": 1, "direction": "down"} = dropped >5% in last 24h
            pct = float(params.get("pct", 5)) / 100.0
            days = int(params.get("days", 1))
            direction = params.get("direction", "down")
            try:
                prices = db.get_price_history(symbol, lookback_days=days + 5)
                if prices is not None and not prices.empty:
                    if 'adj_close' in prices.columns:
                        adj = pd.to_numeric(prices['adj_close'], errors='coerce').ffill()
                    else:
                        adj = pd.to_numeric(prices['close'], errors='coerce').ffill()
                    prices_copy = prices.copy()
                    prices_copy['date'] = pd.to_datetime(prices_copy['date'], errors='coerce').apply(
                        lambda t: t.replace(tzinfo=None) if pd.notna(t) else pd.NaT)
                    cutoff = pd.Timestamp.utcnow().replace(tzinfo=None) - pd.Timedelta(days=days)
                    past = adj[prices_copy['date'] <= cutoff]
                    if not past.empty:
                        ref_price = float(past.iloc[-1])
                        curr_price = float(adj.iloc[-1])
                        change = (curr_price - ref_price) / ref_price
                        if direction == "down":
                            result = change <= -pct
                        else:
                            result = change >= pct
            except Exception:
                logging.exception("evaluate_alert: pct_change failed for %s", symbol)

        elif a_type == "earnings_soon":
            # Fire when earnings date is within `days` calendar days
            days = int(params.get("days", 3))
            try:
                cache = db.get_security_cache(alert.get("security_id"))
                if cache is not None:
                    ts = cache.get("earningsTimestamp")
                    if ts:
                        earnings_dt = pd.Timestamp(ts).replace(tzinfo=None)
                        now = pd.Timestamp.utcnow().replace(tzinfo=None)
                        diff = (earnings_dt - now).days
                        result = 0 <= diff <= days
            except Exception:
                logging.exception("evaluate_alert: earnings_soon failed for %s", symbol)

        elif a_type == "mos":
            mos_val = data.get("mos")
            thr = float(params.get("threshold_pct", 0.25))
            if mos_val is not None:
                result = mos_val >= thr

        # TODO: dividend/earnings alerts can be added with event calendar logic

        logging.info("evaluate_alert: %s (%s) => %s [params=%s]",
                     symbol, a_type, result, params)
        return result

    except Exception:
        logging.exception("evaluate_alert: error for %s (%s)", symbol, a_type)
        return False



# -----------------------------
# Holdings Middleware
# -----------------------------

def get_latest_holdings_snapshot(
    portfolio_ids: Optional[List[int]] = None,
    sectors: Optional[List[str]] = None,
    industries: Optional[List[str]] = None,
    security_types: Optional[List[str]] = None,
    symbols: Optional[List[str]] = None,
    exchanges: Optional[List[str]] = None,
    aggregate: bool = True
) -> pd.DataFrame:
    """
    Snapshot (latest global date across all holdings).
    Returns only open holdings (quantity>0).
    If aggregate=True, aggregates across portfolios (1 row per security).
    Guaranteed columns: portfolio_id (if aggregate=False), security_id, symbol, name,
    sector, industry, security_type, exchange, quantity, market_value, cost_basis,
    abs_perf, rel_perf, security_label
    """
    df = holdings_timeseries(
        portfolio_ids=portfolio_ids,
        sectors=sectors,
        industries=industries,
        security_types=security_types,
        symbols=symbols,
        exchanges=exchanges,
        aggregate=False   # 🔑 get raw rows
    )
    if df.empty:
        return pd.DataFrame()

    df['date'] = pd.to_datetime(df['date'])

    # restrict to the global latest date
    latest_date = df['date'].max()
    latest = df[df['date'] == latest_date].copy()

    # compute performance columns
    latest['cost_basis'] = latest.get('cost_basis', 0.0)
    latest['abs_perf'] = latest['market_value'] - latest['cost_basis']
    latest['rel_perf'] = latest.apply(
        lambda r: (r['abs_perf'] / r['cost_basis']) if r['cost_basis'] else 0.0,
        axis=1
    )
    latest['security_label'] = latest.apply(
        lambda r: f"{r.get('name','Unknown')} ({r.get('symbol','Unknown')})",
        axis=1
    )

    # fill metadata if missing
    for col in ['sector','industry','security_type','exchange','symbol','name']:
        if col in latest.columns:
            latest[col] = latest[col].fillna('Unknown')

    if aggregate:
        agg_cols = {
            'quantity': 'sum',
            'market_value': 'sum',
            'cost_basis': 'sum',
            'abs_perf': 'sum'
        }
        grouped = (
            latest.groupby(
                ['security_id','symbol','name','sector','industry',
                 'security_type','exchange','security_label'],
                as_index=False
            ).agg(agg_cols)
        )
        grouped['rel_perf'] = grouped.apply(
            lambda r: (r['abs_perf']/r['cost_basis']) if r['cost_basis'] else 0.0,
            axis=1
        )
        return grouped
    else:
        return latest.reset_index(drop=True)


def holdings_timeseries(
    portfolio_ids: Optional[List[int]] = None,
    sectors: Optional[List[str]] = None,
    industries: Optional[List[str]] = None,
    security_types: Optional[List[str]] = None,
    symbols: Optional[List[str]] = None,
    exchanges: Optional[List[str]] = None,
    aggregate: bool = True
) -> pd.DataFrame:
    """
    Returns holdings timeseries.
    If aggregate=True: daily aggregated timeseries (suitable for charts).
    If aggregate=False: raw detailed timeseries rows (per portfolio+security+date).
    """
    df = db.get_holdings_timeseries(
        portfolio_ids=portfolio_ids,
        sectors=sectors,
        industries=industries,
        security_types=security_types,
        symbols=symbols,
        exchanges=exchanges
    )
    if df.empty:
        return pd.DataFrame()

    df['date'] = pd.to_datetime(df['date'])
    # normalize numeric columns
    df['market_value'] = pd.to_numeric(df['market_value'], errors='coerce').fillna(0.0)
    df['cost_basis'] = pd.to_numeric(df.get('cost_basis', 0.0), errors='coerce').fillna(0.0)

    if not aggregate:
        return df

    # aggregate per date (all portfolios/securities combined)
    df_grouped = (
        df.groupby('date')
          .agg({'market_value': 'sum', 'cost_basis': 'sum'})
          .reset_index()
          .sort_values('date')
    )
    df_grouped['net_value'] = df_grouped['market_value'] - df_grouped['cost_basis']

    df_grouped['rel_perf'] = 0.0
    mask = df_grouped['cost_basis'] != 0
    df_grouped.loc[mask, 'rel_perf'] = (
        (df_grouped.loc[mask, 'market_value'] - df_grouped.loc[mask, 'cost_basis'])
        / df_grouped.loc[mask, 'cost_basis']
    )

    return df_grouped



def add_annual_performance(df_ts: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates annual CAGR based on net portfolio value.
    Uses first and last value of each full calendar year.
    """
    df_ts = df_ts.copy()
    df_ts['year'] = df_ts['date'].dt.year

    df_cagr = []
    for year, grp in df_ts.groupby('year'):
        grp = grp.sort_values('date')
        start_val = grp.iloc[0]['net_value']
        end_val = grp.iloc[-1]['net_value']
        days = (grp.iloc[-1]['date'] - grp.iloc[0]['date']).days

        if start_val <= 0 or days < 365:  # only full years, skip partials
            continue
        cagr = (end_val / start_val) ** (365/days) - 1
        df_cagr.append({'year': year, 'annual_cagr': cagr})

    return pd.DataFrame(df_cagr)


def compute_cagr(df: pd.DataFrame, years_list=[1, 3, 5]) -> dict:
    """
    Compute CAGR over multiple horizons plus since inception.
    
    df: DataFrame with 'date' (datetime) and 'net_value'
    years_list: list of horizons in years
    """
    df = df.sort_values("date")
    latest_date = df['date'].max()
    latest_value = df['net_value'].iloc[-1]
    
    cagr_dict = {}
    
    # Since inception
    start_value = df['net_value'].iloc[0]
    total_years = (latest_date - df['date'].iloc[0]).days / 365
    cagr_dict['since_inception'] = (latest_value / start_value) ** (1 / total_years) - 1
    
    # Specific horizons
    for horizon in years_list:
        past_date = latest_date - pd.Timedelta(days=int(horizon*365))
        # find the closest date in df before or equal to past_date
        df_past = df[df['date'] <= past_date]
        if df_past.empty:
            cagr_dict[f'{horizon}y'] = None  # not enough history
        else:
            start_val = df_past['net_value'].iloc[-1]
            cagr_dict[f'{horizon}y'] = (latest_value / start_val) ** (1 / horizon) - 1
    
    return cagr_dict

def add_rolling_cagr(df_ts: pd.DataFrame, years: int = 3) -> pd.DataFrame:
    """
    Calculates rolling CAGR over a given horizon (default: 3 years).
    """
    df_ts = df_ts.copy().sort_values("date")
    df_ts.set_index("date", inplace=True)

    window_days = int(365 * years)
    results = []

    for i in range(window_days, len(df_ts)):
        start_val = df_ts['net_value'].iloc[i - window_days]
        end_val = df_ts['net_value'].iloc[i]
        if start_val > 0:
            cagr = (end_val / start_val) ** (1 / years) - 1
            results.append({"date": df_ts.index[i], "cagr": cagr})

    return pd.DataFrame(results)


def recompute_all_holdings_timeseries():
    """
    Recompute holdings timeseries for all portfolios and all securities,
    including equities, ETFs, and crypto.
    """
    # portfolios = list_portfolios()
    # for pf_id in portfolios['id']:
    #     securities = list_transactions([pf_id])
    #     for sec_id in securities['id']:
    #         db.recompute_holdings_timeseries(pf_id, sec_id)

    holdings = db.get_holdings_from_transactions()
    for _, row in holdings.iterrows():
        pf_id = row['portfolio_id']
        sec_id = row['security_id']
        # print(pf_id, sec_id)
        db.recompute_holdings_timeseries(pf_id, sec_id)

# -----------------------------
# Price Middleware
# -----------------------------

# --- price wrapper ---
def store_prices(security_id: int, df: pd.DataFrame) -> None:
    db.store_prices(security_id, df)
    portfolios = db.list_portfolios_holding_security(security_id)
    for pf_id in portfolios:
        db.recompute_holdings_timeseries(pf_id, security_id)



# ----------------------------
# RISK CALCULATION
# ----------------------------
# def compute_security_risk(security_id: int, symbol: str) -> Dict[str, Any]:
#     """
#     Estimate risk metrics for a single security.
#     Falls back gracefully if data is missing.
#     """
#     try:
#         cache_df = db.get_security_cache(security_id)  # now returns a DataFrame
#         if cache_df is not None and not cache_df.empty:
#             cache = cache_df.iloc[0].to_dict()  # convert first row to dict
#         else:
#             cache = None

#         beta = cache.get("beta") if cache else None

#         # If we have beta, we use it directly
#         if beta is not None and not pd.isna(beta):
#             return {"symbol": symbol, "beta": beta, "risk_score": float(beta)}

#         # Otherwise, fallback to volatility from price series
#         prices = db.get_price_series(symbol)
#         if prices.empty or "adj_close" not in prices:
#             return {"symbol": symbol, "beta": None, "risk_score": None}

#         returns = prices["adj_close"].pct_change().dropna()
#         vol = returns.std() * np.sqrt(252)  # annualized vol
#         return {"symbol": symbol, "beta": None, "risk_score": float(vol)}

#     except Exception as e:
#         logging.exception(f"compute_security_risk failed for {symbol}")
#         return {"symbol": symbol, "beta": None, "risk_score": None}



# def compute_portfolio_risk_timeseries(
#     portfolio_ids: Optional[List[int]] = None
# ) -> pd.DataFrame:
#     """
#     Compute daily weighted risk profile for the portfolio(s) over time.
#     Returns a DataFrame with columns: date, weighted_risk
#     """
#     # Pull the detailed holdings timeseries
#     df_timeseries = holdings_timeseries(portfolio_ids=portfolio_ids, aggregate=False)
#     if df_timeseries.empty:
#         return pd.DataFrame(columns=['date', 'weighted_risk'])

#     risks_over_time = []

#     # Compute weighted risk per day
#     for date, df_day in df_timeseries.groupby('date'):
#         df_day = df_day.copy()
#         day_risks = []
#         for _, row in df_day.iterrows():
#             r = compute_security_risk(row['security_id'], row['symbol'])
#             r['market_value'] = row['market_value']
#             day_risks.append(r)
#         df_day_risk = pd.DataFrame(day_risks).dropna(subset=['risk_score'])
#         if not df_day_risk.empty:
#             total_value = df_day_risk['market_value'].sum()
#             df_day_risk['weight'] = df_day_risk['market_value'] / total_value
#             df_day_risk['weighted_risk'] = df_day_risk['weight'] * df_day_risk['risk_score']
#             risks_over_time.append({'date': date, 'weighted_risk': df_day_risk['weighted_risk'].sum()})

#     df_risk_time = pd.DataFrame(risks_over_time).sort_values('date')
#     return df_risk_time


# ----------------------------
# SECURITY RISK TIMESERIES
# ----------------------------

def fetch_security_risk_timeseries(security_id: int, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
    """
    Wrapper to fetch precomputed risk timeseries for a security.
    """
    return db.get_security_risk_timeseries(security_id, start_date=start_date, end_date=end_date)


# ----------------------------
# PORTFOLIO RISK TIMESERIES
# ----------------------------

def fetch_portfolio_risk_timeseries(portfolio_ids: Optional[List[int]] = None, aggregate=True) -> pd.DataFrame:
    """
    Wrapper to fetch aggregated portfolio-level risk timeseries.
    """
    if aggregate:
        return db.get_portfolio_risk_timeseries(portfolio_ids)
    else:
        return db.get_portfolio_risk_timeseries_detailed(portfolio_ids)


def update_portfolio_risk_timeseries_for_portfolios(portfolio_ids: Optional[List[int]] = None):
    """
    Ensure that all security risk timeseries for the selected portfolios are up to date.
    """
    df_hold = holdings_timeseries(portfolio_ids=portfolio_ids, aggregate=False)
    if df_hold.empty:
        return pd.DataFrame(columns=["date", "portfolio_id", "weighted_risk"])

    # Only compute risk for securities that are actually held
    security_ids = df_hold["security_id"].unique()
    for sec_id in security_ids:
        db.update_security_risk_timeseries(sec_id, portfolio_ids=portfolio_ids)

    # Return aggregated portfolio-level risk timeseries
    return db.get_portfolio_risk_timeseries(portfolio_ids)


# ----------------------------
# TARGET CURVE (retirement glidepath)
# ----------------------------
def target_risk_curve(current_year: int, retirement_year: int, start_risk: float = 0.4, end_risk: float = 0.2) -> float:
    """
    Returns target risk for a given year on the glidepath.
    Linear decline from start_risk to end_risk over the period.
    """
    total_years = retirement_year - current_year
    if total_years <= 0:
        return end_risk
    def curve(year: int) -> float:
        year = min(year, retirement_year)
        return start_risk + (end_risk - start_risk) * ((year - current_year) / total_years)
    return curve


# ----------------------------
# REBALANCING SUGGESTIONS
# ----------------------------
def suggest_rebalancing(
    portfolio_ids: Optional[List[int]] = None,
    retirement_year: int = 2047,
    threshold: float = 0.05,
    max_watchlist_suggestions: int = 3
) -> Dict[str, Any]:
    """
    Suggest rebalancing steps combining:
    1. Allocation targets (asset type, sector, industry).
    2. Risk-aware adjustments.
    3. Aggregated, prioritized recommendations with expected impact.
    """

    # --- Fetch current holdings snapshot ---
    df = get_latest_holdings_snapshot(portfolio_ids=portfolio_ids, aggregate=False)
    if df.empty:
        return {"message": "No holdings available", "suggestions": []}

    # --- Fetch watchlist ---
    watchlist = get_watchlist()
    current_syms = set(df['symbol'].unique())
    watchlist_syms = set(watchlist['symbol'].unique())
    valid_syms = current_syms | watchlist_syms

    # --- Compute weights and risk ---
    df['market_value'] = df.get('market_value', 1.0)
    total_value = df['market_value'].sum() or 1.0
    df['weight'] = df['market_value'] / total_value

    # Merge in weighted_risk from historical risk timeseries if available
    df_risk = fetch_portfolio_risk_timeseries(portfolio_ids=portfolio_ids, aggregate=False)
    if not df_risk.empty:
        latest_risk = df_risk.groupby('symbol')['weighted_risk'].last().to_dict()
        df['weighted_risk'] = df['symbol'].map(latest_risk).fillna(df['market_value'])
    else:
        df['weighted_risk'] = df['market_value']

    df['risk_pct'] = df['weighted_risk'] / df['weighted_risk'].sum()

    # --- Only current holdings for reductions ---
    df_current = df[df['symbol'].isin(current_syms)]

    # --- Load targets ---
    asset_targets_raw = safe_json_load(get_config("asset_allocation_targets"), {})
    asset_targets = {
        "pre_retirement": asset_targets_raw.get("pre_retirement") or asset_targets_raw.get("current") or {},
    }
    sectors_config = safe_json_load(get_config("target_sector_allocation"), {})
    industries_config = safe_json_load(get_config("target_industry_allocation"), {})

    # --- Aggregate current weights ---
    asset_weights = df.groupby('security_type')['weight'].sum()
    sector_weights = df.groupby('sector')['weight'].sum()
    industry_weights = df.groupby(['sector','industry'])['weight'].sum()
    security_weights = df.groupby('symbol')['weight'].sum()
    security_risk = df.groupby('symbol')['risk_pct'].sum()

    actions_dict = {}

    def add_action(sym, pct_delta, market_value_delta, reason, impact_allocation=0.0, impact_risk=0.0):
        if sym not in actions_dict:
            actions_dict[sym] = {"pct": 0.0, "market_value": 0.0, "reasons": [], "impact_allocation": 0.0, "impact_risk": 0.0}
        actions_dict[sym]["pct"] += pct_delta
        actions_dict[sym]["market_value"] += market_value_delta
        actions_dict[sym]["reasons"].append(reason)
        actions_dict[sym]["impact_allocation"] += impact_allocation
        actions_dict[sym]["impact_risk"] += impact_risk

    def prioritize_low_risk(candidates: pd.DataFrame):
        return candidates.sort_values(by='beta').head(max_watchlist_suggestions)

    # --- Compute deltas ---
    def compute_delta(target_dict, actual_series):
        return {k: target_dict.get(k,0)-actual for k, actual in actual_series.items() if abs(target_dict.get(k,0)-actual) > threshold}

    asset_deltas = compute_delta(asset_targets.get("pre_retirement", {}), asset_weights)
    sector_deltas = compute_delta(sectors_config, sector_weights)
    industry_deltas = {}
    for (sec, ind), actual in industry_weights.items():
        target = industries_config.get(sec, {}).get(ind, 0)
        delta = target - actual
        if abs(delta) > threshold:
            industry_deltas[(sec, ind)] = delta

    # --- Allocation adjustments ---
    for t, delta in asset_deltas.items():
        syms = df[df['security_type']==t]['symbol'].unique()
        for sym in syms:
            if sym not in valid_syms:
                continue
            adj_pct = delta * (security_weights[sym]/asset_weights[t])
            reason = f"Adjust {t} allocation ({'increase' if adj_pct>0 else 'reduce'})"
            if abs(adj_pct) > 0.01:
                add_action(sym, adj_pct, adj_pct*total_value, reason, impact_allocation=abs(adj_pct))

    for s, delta in sector_deltas.items():
        syms = df[df['sector']==s]['symbol'].unique()
        if delta > 0 and len(syms)==0 and watchlist is not None:
            # underweight, buy from watchlist
            candidates = prioritize_low_risk(watchlist[watchlist['sector']==s])
            for idx, row in candidates.iterrows():
                adj_pct = delta / len(candidates)
                reason = f"Buy from watchlist to increase sector '{s}'"
                add_action(row['symbol'], adj_pct, adj_pct*total_value, reason, impact_allocation=abs(adj_pct))
        else:
            # adjust existing holdings
            for sym in syms:
                if sym not in current_syms:
                    continue
                adj_pct = delta * (security_weights[sym]/sector_weights[s])
                reason = f"Adjust sector '{s}' ({'increase' if adj_pct>0 else 'reduce'})"
                if abs(adj_pct) > 0.01:
                    add_action(sym, adj_pct, adj_pct*total_value, reason, impact_allocation=abs(adj_pct))

    for (sec, ind), delta in industry_deltas.items():
        syms = df[(df['sector']==sec) & (df['industry']==ind)]['symbol'].unique()
        if delta > 0 and len(syms)==0 and watchlist is not None:
            candidates = prioritize_low_risk(watchlist[(watchlist['sector']==sec) & (watchlist['industry']==ind)])
            for idx, row in candidates.iterrows():
                adj_pct = delta / len(candidates)
                reason = f"Buy from watchlist to increase industry '{sec} → {ind}'"
                add_action(row['symbol'], adj_pct, adj_pct*total_value, reason, impact_allocation=abs(adj_pct))
        else:
            for sym in syms:
                if sym not in current_syms:
                    continue
                adj_pct = delta * (security_weights[sym]/industry_weights[(sec,ind)])
                reason = f"Adjust industry '{sec} → {ind}' ({'increase' if adj_pct>0 else 'reduce'})"
                if abs(adj_pct) > 0.01:
                    add_action(sym, adj_pct, adj_pct*total_value, reason, impact_allocation=abs(adj_pct))

    # --- Risk adjustments ---
    avg_risk = df['risk_pct'].mean()
    risk_threshold = avg_risk * 1.5
    for sym in df_current['symbol'].unique():
        if security_risk[sym] > risk_threshold:
            adj_pct = -min(security_weights[sym], 0.1)
            reason = f"Reduce '{sym}' due to high risk ({security_risk[sym]:.1%})"
            add_action(sym, adj_pct, adj_pct*total_value, reason, impact_risk=security_risk[sym])

    # --- Aggregate actions ---
    actions = []
    for sym, info in actions_dict.items():
        priority_score = info["impact_allocation"] + info["impact_risk"]
        actions.append({
            "symbol": sym,
            "pct_change": info["pct"],
            "market_value_change": info["market_value"],
            "reasons": info["reasons"],
            "impact_allocation": info["impact_allocation"],
            "impact_risk": info["impact_risk"],
            "priority_score": priority_score
        })

    actions.sort(key=lambda x: x["priority_score"], reverse=True)

    return {
        "asset_weights": asset_weights.to_dict(),
        "sector_weights": sector_weights.to_dict(),
        "industry_weights": industry_weights.to_dict(),
        "security_weights": security_weights.to_dict(),
        "security_risk": security_risk.to_dict(),
        "suggestions": actions,
        "details": df.to_dict(orient="records")
    }




def get_complete_taxonomy(holdings_df):
    """
    Returns taxonomy dict with Yahoo baseline + any extra sectors/industries found in holdings.
    """
    baseline = get_config("taxonomy")
    if isinstance(baseline, str): baseline = json.loads(baseline)
    taxonomy = baseline.copy()

    for _, row in holdings_df.iterrows():
        sec = str(row.get("sector", "")).strip().lower()
        ind = str(row.get("industry", "")).strip().lower()
        if not sec:
            continue
        if sec not in taxonomy:
            taxonomy[sec] = []
        if ind and ind not in taxonomy[sec]:
            taxonomy[sec].append(ind)

    return taxonomy


def store_fx_rates(df: pd.DataFrame):
    """Pass-through middleware function for FX rates."""
    db.store_fx_rates(df)