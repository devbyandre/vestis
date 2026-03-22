#!/usr/bin/env python3
# db_utils.py  –  SQLite  ➜  PostgreSQL drop-in replacement
#
# Connection source priority:
#   1. DATABASE_URL env var  (Docker / production)
#   2. config.json "db_url"  (optional override)
#   3. config.json "db_path" (legacy SQLite path – still works for local dev)

import os
import logging
import json
from contextlib import contextmanager
from typing import Optional, List, Any, Dict, Tuple

import numpy as np
import pandas as pd
from collections import deque

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# ── connection bootstrap ──────────────────────────────────────────────────────

def _build_url() -> str:
    """Return the database URL to use, with PostgreSQL preferred."""
    # 1. Docker / environment variable (highest priority)
    url = os.environ.get("DATABASE_URL", "").strip()
    if url:
        return url

    # 2. config.json explicit url
    try:
        from config_utils import get_config
        url = get_config("db_url") or ""
        if url:
            return url
    except Exception:
        pass

    # 3. Fallback: SQLite from config "db_path"
    try:
        from config_utils import get_config
        db_path = os.path.expanduser(get_config("db_path") or "portfolio.db")
    except Exception:
        db_path = "portfolio.db"
    return f"sqlite:///{db_path}"


_DB_URL: str = _build_url()
_IS_POSTGRES: bool = _DB_URL.startswith("postgresql") or _DB_URL.startswith("postgres")

logging.info(f"db_utils: using {'PostgreSQL' if _IS_POSTGRES else 'SQLite'} → {_DB_URL[:60]}...")

# ── SQLAlchemy engine (used for pd.read_sql_query) ────────────────────────────
from sqlalchemy import create_engine, text as sa_text, event
from sqlalchemy.pool import NullPool

_engine = create_engine(
    _DB_URL,
    poolclass=NullPool,   # each call gets a fresh connection → thread-safe without extra work
    future=True,
)


def get_engine():
    return _engine


# ── Low-level connection context manager ─────────────────────────────────────
# Returns a DBAPI2-compatible connection so existing code (conn.execute,
# conn.executemany, conn.commit) keeps working unchanged.

@contextmanager
def _conn_ctx():
    """Yield a raw DBAPI connection and commit/rollback automatically."""
    raw = _engine.raw_connection()
    try:
        yield raw
        raw.commit()
    except Exception:
        raw.rollback()
        raise
    finally:
        raw.close()


def get_conn():
    """
    Return a new DBAPI connection.

    Callers that use  `with get_conn() as conn:`  still work because psycopg2
    and sqlite3 connections both support the context-manager protocol
    (commit on __exit__ success, rollback on exception).
    """
    return _engine.raw_connection()


# ── SQL dialect helper ────────────────────────────────────────────────────────

def _ph() -> str:
    """Return the correct placeholder for the active dialect."""
    return "%s" if _IS_POSTGRES else "?"


def _adapt_sql(sql: str) -> str:
    """Convert SQLite-style ? placeholders to %s for PostgreSQL."""
    if _IS_POSTGRES:
        return sql.replace("?", "%s")
    return sql


def _adapt_params(params):
    """Convert tuple/list params – no-op for now, hook for future type coercion."""
    return params


# ── Pandas read helper ────────────────────────────────────────────────────────

def _read_sql(sql: str, params=None) -> pd.DataFrame:
    """Execute a SELECT and return a DataFrame, dialect-agnostic."""
    adapted = _adapt_sql(sql)
    with _engine.connect() as con:
        if params:
            # SQLAlchemy 2.x requires named params as dicts with sa_text.
            # Works for both SQLite (?) and Postgres (%s) — convert to :p0, :p1...
            named_sql = adapted
            named_params = {}
            i = 0
            placeholder = "%s" if _IS_POSTGRES else "?"
            while placeholder in named_sql:
                named_sql = named_sql.replace(placeholder, f":p{i}", 1)
                named_params[f"p{i}"] = params[i]
                i += 1
            return pd.read_sql_query(sa_text(named_sql), con, params=named_params)
        return pd.read_sql_query(sa_text(adapted), con)


# ── Date helpers ──────────────────────────────────────────────────────────────

def to_iso_date(val: Optional[str]) -> Optional[str]:
    if val is None:
        return None
    return pd.to_datetime(val, utc=True).date().isoformat()


def to_iso_ts(val) -> Optional[str]:
    if val is None:
        return None
    return pd.to_datetime(val, utc=True).isoformat()


# ═════════════════════════════════════════════════════════════════════════════
# Portfolio queries
# ═════════════════════════════════════════════════════════════════════════════

def insert_holdings_timeseries(records: list, conn=None) -> None:
    if not records:
        return

    sql = _adapt_sql("""
        INSERT INTO holdings_timeseries
            (date, portfolio_id, security_id, quantity, market_value, cost_basis)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (date, portfolio_id, security_id) DO UPDATE SET
            quantity     = EXCLUDED.quantity,
            market_value = EXCLUDED.market_value,
            cost_basis   = EXCLUDED.cost_basis
    """) if _IS_POSTGRES else """
        INSERT OR REPLACE INTO holdings_timeseries
            (date, portfolio_id, security_id, quantity, market_value, cost_basis)
        VALUES (?, ?, ?, ?, ?, ?)
    """

    own = conn is None
    if own:
        conn = get_conn()
    try:
        cur = conn.cursor()
        cur.executemany(sql, records)
        if own:
            conn.commit()
    finally:
        if own:
            conn.close()


def get_portfolio_by_name(name: str) -> Optional[dict]:
    df = _read_sql("SELECT id, name FROM portfolios WHERE name=?", (name,))
    return df.iloc[0].to_dict() if not df.empty else None


def list_portfolios() -> pd.DataFrame:
    return _read_sql("SELECT id, name FROM portfolios ORDER BY name")


def rename_portfolio(old_name: str, new_name: str) -> None:
    with get_conn() as conn:
        conn.cursor().execute(
            _adapt_sql("UPDATE portfolios SET name=? WHERE name=?"), (new_name, old_name)
        )
        conn.commit()


def delete_portfolio(portfolio_id: int) -> Dict[str, Any]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(_adapt_sql("DELETE FROM portfolios WHERE id=?"), (portfolio_id,))
        conn.commit()

        remaining = _read_sql("SELECT id, name FROM portfolios LIMIT 1")
        if remaining.empty:
            cur.execute(_adapt_sql("INSERT INTO portfolios(name) VALUES(?)"), ("Default",))
            conn.commit()
            new_id = cur.fetchone()
            # fetch the just-inserted row
            row = _read_sql("SELECT id, name FROM portfolios ORDER BY id DESC LIMIT 1")
            return row.iloc[0].to_dict()
        return remaining.iloc[0].to_dict()


def reassign_transactions(old_id: int, new_id: int) -> None:
    with get_conn() as conn:
        conn.cursor().execute(
            _adapt_sql("UPDATE transactions SET portfolio_id=? WHERE portfolio_id=?"),
            (new_id, old_id),
        )
        conn.commit()


# ═════════════════════════════════════════════════════════════════════════════
# Securities queries
# ═════════════════════════════════════════════════════════════════════════════

def get_security_id(symbol: str, conn=None) -> Optional[int]:
    df = _read_sql("SELECT id FROM securities WHERE yahoo_ticker=?", (symbol,))
    return int(df.iloc[0]["id"]) if not df.empty else None


def list_securities() -> pd.DataFrame:
    return _read_sql("""
        SELECT s.id,
               s.yahoo_ticker AS symbol,
               COALESCE(sc."longName", sc."shortName") AS name,
               s.isin
        FROM securities s
        LEFT JOIN securities_cache sc ON sc.security_id = s.id
        ORDER BY name
    """)


def get_all_symbols() -> List[str]:
    df = _read_sql("""
        SELECT yahoo_ticker AS symbol FROM securities
        UNION
        SELECT DISTINCT s.yahoo_ticker AS symbol
        FROM transactions t
        JOIN securities s ON s.id = t.security_id
        WHERE s.yahoo_ticker IS NOT NULL
    """)
    return sorted(df["symbol"].dropna().unique())


def insert_security(symbol: str) -> int:
    if _IS_POSTGRES:
        sql = _adapt_sql("""
            INSERT INTO securities (yahoo_ticker) VALUES (?)
            ON CONFLICT (yahoo_ticker) DO NOTHING
        """)
    else:
        sql = "INSERT OR IGNORE INTO securities (yahoo_ticker) VALUES (?)"
    with get_conn() as conn:
        conn.cursor().execute(sql, (symbol,))
        conn.commit()
    return get_security_id(symbol)


def get_security_by_symbol(symbol: str) -> Optional[dict]:
    df = _read_sql("SELECT * FROM securities WHERE yahoo_ticker=?", (symbol,))
    return df.iloc[0].to_dict() if not df.empty else None


def get_security_by_id(sec_id: int) -> Optional[dict]:
    df = _read_sql(
        "SELECT id, yahoo_ticker AS symbol, isin FROM securities WHERE id=?", (sec_id,)
    )
    return df.iloc[0].to_dict() if not df.empty else None


def list_securities_metadata() -> pd.DataFrame:
    return _read_sql("""
        SELECT s.id,
               s.yahoo_ticker AS symbol,
               sc."longName" AS name,
               sc.security_type,
               sc.sector,
               sc.industry,
               sc.country,
               sc.exchange
        FROM securities s
        LEFT JOIN securities_cache sc ON sc.security_id = s.id
    """)


def update_security(sec_id: int, name: Optional[str] = None, isin: Optional[str] = None):
    with get_conn() as conn:
        cur = conn.cursor()
        if isin is not None:
            cur.execute(_adapt_sql("UPDATE securities SET isin=? WHERE id=?"), (isin, sec_id))
        if name is not None:
            cur.execute(
                _adapt_sql("""UPDATE securities_cache SET "longName"=? WHERE security_id=?"""),
                (name, sec_id),
            )
        conn.commit()


# ═════════════════════════════════════════════════════════════════════════════
# Security cache
# ═════════════════════════════════════════════════════════════════════════════

def store_security_cache(security_id: int, info: dict) -> None:
    security_type = info.get("quoteType")

    keys = [
        "country", "exchange", "sector", "industry", "shortName", "longName",
        "regularMarketPrice", "fiftyTwoWeekHigh", "fiftyTwoWeekLow", "volume", "averageVolume",
        "marketCap", "beta", "trailingPE", "forwardPE", "trailingEps", "earningsTimestamp",
        "dividendRate", "dividendYield", "enterpriseValue", "profitMargins", "operatingMargins",
        "returnOnAssets", "returnOnEquity", "totalRevenue", "revenuePerShare", "grossProfits",
        "ebitda", "totalCash", "totalDebt", "currentRatio", "bookValue", "operatingCashflow",
        "freeCashflow", "sharesOutstanding", "currency",
    ]
    numeric_keys = [
        "regularMarketPrice", "fiftyTwoWeekHigh", "fiftyTwoWeekLow", "volume", "averageVolume",
        "marketCap", "beta", "trailingPE", "forwardPE", "trailingEps", "dividendRate",
        "dividendYield", "enterpriseValue", "profitMargins", "operatingMargins",
        "returnOnAssets", "returnOnEquity", "totalRevenue", "revenuePerShare", "grossProfits",
        "ebitda", "totalCash", "totalDebt", "currentRatio", "bookValue",
        "operatingCashflow", "freeCashflow", "sharesOutstanding",
    ]

    data_values = []
    for k in keys:
        v = info.get(k)
        if k in numeric_keys:
            try:
                v = float(v)
            except (TypeError, ValueError):
                v = None
        data_values.append(v)

    ts_now = pd.Timestamp.utcnow().isoformat()
    cols = ["security_id", "security_type"] + keys + ["kpis_updated_at"]
    data = [security_id, security_type] + data_values + [ts_now]

    # quoted column names for reserved words (e.g. "longName")
    quoted_cols = [f'"{c}"' if c[0].islower() and c != "security_id" and c != "security_type"
                   else c for c in cols]

    ph = ", ".join([_ph()] * len(data))

    update_cols = [c for c in cols if c not in ("security_id",)]
    update_set = ",\n".join([
        f'"{c}" = EXCLUDED."{c}"'
        if c not in ("shortName", "longName")
        else f'"{c}" = COALESCE(securities_cache."{c}", EXCLUDED."{c}")'
        for c in update_cols
    ])

    if _IS_POSTGRES:
        col_list = ", ".join([f'"{c}"' for c in cols])
        sql = f"""
            INSERT INTO securities_cache ({col_list})
            VALUES ({ph})
            ON CONFLICT (security_id) DO UPDATE SET
            {update_set}
        """
    else:
        col_list = ", ".join(cols)
        sql = f"""
            INSERT INTO securities_cache ({col_list})
            VALUES ({ph})
            ON CONFLICT(security_id) DO UPDATE SET
            {update_set}
        """

    with get_conn() as conn:
        conn.cursor().execute(sql, data)
        conn.commit()


def store_lazy_security(security_id: int, data: dict) -> None:
    if not data:
        return
    if _IS_POSTGRES:
        sql = _adapt_sql("""
            INSERT INTO securities_cache (
                security_id, security_type, country, exchange, sector, industry,
                "shortName", "longName", "regularMarketPrice"
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (security_id) DO UPDATE SET
                security_type = EXCLUDED.security_type,
                country = EXCLUDED.country,
                exchange = EXCLUDED.exchange,
                sector = EXCLUDED.sector,
                industry = EXCLUDED.industry,
                "regularMarketPrice" = EXCLUDED."regularMarketPrice",
                "shortName" = COALESCE(securities_cache."shortName", EXCLUDED."shortName"),
                "longName"  = COALESCE(securities_cache."longName",  EXCLUDED."longName")
        """)
    else:
        sql = """
            INSERT INTO securities_cache (
                security_id, security_type, country, exchange, sector, industry,
                shortName, longName, regularMarketPrice
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(security_id) DO UPDATE SET
                security_type=excluded.security_type,
                country=excluded.country,
                exchange=excluded.exchange,
                sector=excluded.sector,
                industry=excluded.industry,
                regularMarketPrice=excluded.regularMarketPrice,
                shortName = COALESCE(securities_cache.shortName, excluded.shortName),
                longName  = COALESCE(securities_cache.longName,  excluded.longName)
        """
    with get_conn() as conn:
        conn.cursor().execute(sql, (
            security_id, data.get("security_type"), data.get("country"),
            data.get("exchange"), data.get("sector"), data.get("industry"),
            data.get("shortName"), data.get("longName"), data.get("regularMarketPrice"),
        ))
        conn.commit()


def get_security_cache(id: int) -> Optional[pd.DataFrame]:
    df = _read_sql("""
        SELECT s.id, s.yahoo_ticker AS symbol, sc.*
        FROM securities s
        LEFT JOIN securities_cache sc ON sc.security_id = s.id
        WHERE s.id = ?
    """, (id,))
    if df.empty:
        return None

    data = df.iloc[0].to_dict()
    currency = data.get("currency", "EUR") or "EUR"

    kpi_keys = [
        "security_type", "country", "exchange", "sector", "industry", "shortName", "longName",
        "regularMarketPrice", "fiftyTwoWeekHigh", "fiftyTwoWeekLow", "volume", "averageVolume",
        "marketCap", "beta", "trailingPE", "forwardPE", "trailingEps", "earningsTimestamp",
        "dividendRate", "dividendYield", "enterpriseValue", "profitMargins", "operatingMargins",
        "returnOnAssets", "returnOnEquity", "totalRevenue", "revenuePerShare", "grossProfits",
        "ebitda", "totalCash", "totalDebt", "currentRatio", "bookValue",
        "operatingCashflow", "freeCashflow", "sharesOutstanding",
    ]
    for k in kpi_keys:
        data.setdefault(k, None)

    monetary_fields = [
        "regularMarketPrice", "fiftyTwoWeekHigh", "fiftyTwoWeekLow", "marketCap", "dividendRate",
        "enterpriseValue", "totalRevenue", "revenuePerShare", "grossProfits", "ebitda",
        "totalCash", "totalDebt", "bookValue", "operatingCashflow", "freeCashflow",
    ]
    if currency.upper() != "EUR":
        fx_df = _read_sql("""
            SELECT rate FROM fx_rates
            WHERE base_currency=? AND target_currency='EUR'
            ORDER BY date DESC LIMIT 1
        """, (currency.upper(),))
        fx = float(fx_df.iloc[0]["rate"]) if not fx_df.empty else 1.0
        for field in monetary_fields:
            if data.get(field) is not None:
                data[field] = data[field] * fx

    return pd.DataFrame([data])


def get_last_info_update(security_id: int) -> Optional[str]:
    df = _read_sql(
        "SELECT kpis_updated_at FROM securities_cache WHERE security_id=?", (security_id,)
    )
    return str(df.iloc[0]["kpis_updated_at"]) if not df.empty else None


def get_last_prices_update(security_id: int) -> Optional[str]:
    df = _read_sql(
        "SELECT prices_updated_at FROM prices WHERE security_id=? LIMIT 1", (security_id,)
    )
    return str(df.iloc[0]["prices_updated_at"]) if not df.empty else None


# ═════════════════════════════════════════════════════════════════════════════
# Transactions
# ═════════════════════════════════════════════════════════════════════════════

def insert_transaction(portfolio_id, security_id, tx_date, tx_type, quantity, price, fees=0.0):
    sql = _adapt_sql("""
        INSERT INTO transactions(portfolio_id, security_id, date, type, quantity, price, fees)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)
    with get_conn() as conn:
        conn.cursor().execute(sql, (portfolio_id, security_id, to_iso_date(tx_date),
                                    tx_type, quantity, price, fees))
        conn.commit()


def update_transaction(tx_id, tx_date, tx_type, quantity, price, fees):
    sql = _adapt_sql("""
        UPDATE transactions
        SET date=?, type=?, quantity=?, price=?, fees=?
        WHERE id=?
    """)
    with get_conn() as conn:
        conn.cursor().execute(sql, (tx_date, tx_type, quantity, price, fees, tx_id))
        conn.commit()


def list_transactions(portfolio_ids: Optional[List[int]] = None) -> pd.DataFrame:
    base = """
        SELECT t.*, p.name AS portfolio_name,
               s.yahoo_ticker AS symbol,
               COALESCE(sc."longName", 'N/A') AS name
        FROM transactions t
        LEFT JOIN portfolios p ON p.id = t.portfolio_id
        LEFT JOIN securities s ON s.id = t.security_id
        LEFT JOIN securities_cache sc ON sc.security_id = s.id
    """
    if portfolio_ids:
        ph = ", ".join([_ph()] * len(portfolio_ids))
        return _read_sql(base + f" WHERE t.portfolio_id IN ({ph}) ORDER BY t.date",
                         tuple(portfolio_ids))
    return _read_sql(base + " ORDER BY t.date")


def list_transactions_detailed() -> pd.DataFrame:
    return _read_sql("""
        SELECT t.id, t.portfolio_id, p.name AS portfolio,
               s.yahoo_ticker AS symbol, s.isin,
               COALESCE(sc."longName", NULL) AS security_name,
               t.date AS tx_date, t.type AS tx_type,
               t.quantity, t.price, t.fees AS tx_cost
        FROM transactions t
        LEFT JOIN portfolios p ON p.id = t.portfolio_id
        LEFT JOIN securities s ON s.id = t.security_id
        LEFT JOIN securities_cache sc ON sc.security_id = s.id
        ORDER BY t.date DESC
    """)


def list_transactions_for_security(portfolio_id: int, security_id: int, conn=None) -> pd.DataFrame:
    sql = _adapt_sql("""
        SELECT t.*, p.name AS portfolio_name, s.yahoo_ticker AS symbol
        FROM transactions t
        LEFT JOIN portfolios p ON t.portfolio_id = p.id
        LEFT JOIN securities s ON t.security_id = s.id
        WHERE t.portfolio_id=? AND t.security_id=?
        ORDER BY t.date
    """)
    # use SQLAlchemy for pandas compatibility
    return _read_sql(
        """
        SELECT t.*, p.name AS portfolio_name, s.yahoo_ticker AS symbol
        FROM transactions t
        LEFT JOIN portfolios p ON t.portfolio_id = p.id
        LEFT JOIN securities s ON t.security_id = s.id
        WHERE t.portfolio_id=? AND t.security_id=?
        ORDER BY t.date
        """,
        (portfolio_id, security_id),
    )


def delete_transaction(tx_id: int) -> None:
    row = _read_sql(
        "SELECT security_id AS sid, portfolio_id AS pid FROM transactions WHERE id=?", (tx_id,)
    )
    if row.empty:
        return
    security_id = int(row.iloc[0]["sid"])
    portfolio_id = int(row.iloc[0]["pid"])

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(_adapt_sql("DELETE FROM transactions WHERE id=?"), (tx_id,))
        delete_security_if_no_transactions(security_id, conn)
        recompute_holdings_timeseries(portfolio_id, security_id, conn)
        conn.commit()


def delete_security_if_no_transactions(security_id: int, conn) -> None:
    cur = conn.cursor()
    cur.execute(
        _adapt_sql("SELECT COUNT(*) AS cnt FROM transactions WHERE security_id=?"), (security_id,)
    )
    row = cur.fetchone()
    cnt = row[0] if row else 0
    if cnt == 0:
        for tbl in ("securities_cache", "valuations", "prices", "financials",
                    "dividends", "alerts", "holdings_timeseries",
                    "security_risk_timeseries", "securities"):
            col = "security_id" if tbl != "securities" else "id"
            cur.execute(_adapt_sql(f"DELETE FROM {tbl} WHERE {col}=?"), (security_id,))


def get_transaction_by_id(tx_id: int) -> Optional[dict]:
    df = _read_sql("SELECT * FROM transactions WHERE id=?", (tx_id,))
    return df.iloc[0].to_dict() if not df.empty else None


# ═════════════════════════════════════════════════════════════════════════════
# Watchlist
# ═════════════════════════════════════════════════════════════════════════════

def get_watchlist() -> pd.DataFrame:
    monetary_fields = [
        "regularMarketPrice", "fiftyTwoWeekHigh", "fiftyTwoWeekLow",
        "marketCap", "enterpriseValue", "totalRevenue", "revenuePerShare",
        "grossProfits", "ebitda", "totalCash", "totalDebt",
        "operatingCashflow", "freeCashflow", "bookValue", "dividendRate",
    ]
    df = _read_sql("""
        SELECT s.id AS security_id, s.yahoo_ticker AS symbol,
               sc."longName" AS security_name, sc."shortName",
               sc.security_type, sc.country, sc.exchange,
               sc.sector, sc.industry, sc.currency,
               sc."regularMarketPrice", sc."fiftyTwoWeekHigh", sc."fiftyTwoWeekLow",
               sc.volume, sc."averageVolume", sc."marketCap", sc.beta,
               sc."trailingPE", sc."forwardPE", sc."trailingEps" AS eps,
               sc."earningsTimestamp" AS earnings_date, sc."dividendRate", sc."dividendYield",
               sc."enterpriseValue", sc."profitMargins", sc."operatingMargins",
               sc."returnOnAssets", sc."returnOnEquity", sc."totalRevenue",
               sc."revenuePerShare", sc."grossProfits", sc.ebitda,
               sc."totalCash", sc."totalDebt", sc."currentRatio", sc."bookValue",
               sc."operatingCashflow", sc."freeCashflow", sc."sharesOutstanding"
        FROM securities s
        LEFT JOIN securities_cache sc ON sc.security_id = s.id
        WHERE s.id NOT IN (
            SELECT t.security_id FROM transactions t
            GROUP BY t.security_id
            HAVING SUM(
                CASE WHEN LOWER(t.type)='buy'  THEN t.quantity
                     WHEN LOWER(t.type)='sell' THEN -t.quantity
                     ELSE 0 END
            ) > 0
        )
        ORDER BY s.yahoo_ticker
    """)

    currencies = df["currency"].dropna().unique() if "currency" in df.columns else []
    fx_map: Dict[str, float] = {}
    for cur in currencies:
        if cur.upper() == "EUR":
            fx_map[cur] = 1.0
        else:
            r = _read_sql(
                "SELECT rate FROM fx_rates WHERE base_currency=? AND target_currency='EUR' ORDER BY date DESC LIMIT 1",
                (cur.upper(),),
            )
            fx_map[cur] = float(r.iloc[0]["rate"]) if not r.empty else 1.0

    for field in monetary_fields:
        if field in df.columns:
            df[field] = df.apply(
                lambda row: row[field] * fx_map.get(row["currency"], 1.0)
                if pd.notna(row[field]) else None,
                axis=1,
            )
    return df


def delete_watchlist_item(symbol: str) -> None:
    sec_id = get_security_id(symbol)
    if not sec_id:
        return
    with get_conn() as conn:
        delete_security_if_no_transactions(sec_id, conn)
        conn.commit()


# ═════════════════════════════════════════════════════════════════════════════
# Prices
# ═════════════════════════════════════════════════════════════════════════════

def get_latest_price(symbol: str) -> Optional[float]:
    df = _read_sql("""
        SELECT p.adj_close, s.id AS security_id
        FROM prices p
        JOIN securities s ON p.security_id = s.id
        WHERE s.yahoo_ticker=?
        ORDER BY p.date DESC LIMIT 1
    """, (symbol,))
    if df.empty or df.iloc[0]["adj_close"] is None:
        return None

    price = float(df.iloc[0]["adj_close"])
    sec_id = int(df.iloc[0]["security_id"])
    cur_df = _read_sql(
        "SELECT currency FROM securities_cache WHERE security_id=?", (sec_id,)
    )
    currency = str(cur_df.iloc[0]["currency"]) if not cur_df.empty else "EUR"
    if currency.upper() != "EUR":
        fx = _read_sql(
            "SELECT rate FROM fx_rates WHERE base_currency=? AND target_currency='EUR' ORDER BY date DESC LIMIT 1",
            (currency.upper(),),
        )
        price *= float(fx.iloc[0]["rate"]) if not fx.empty else 1.0
    return price


def get_price_series(symbol: str, start_date=None, end_date=None) -> pd.DataFrame:
    sec_df = _read_sql("""
        SELECT s.id, sc.currency
        FROM securities s
        JOIN securities_cache sc ON sc.security_id = s.id
        WHERE s.yahoo_ticker=?
    """, (symbol,))
    if sec_df.empty:
        return pd.DataFrame()
    security_id = int(sec_df.iloc[0]["id"])
    currency = str(sec_df.iloc[0]["currency"] or "EUR")

    if start_date is None:
        r = _read_sql("SELECT MIN(date) AS min_date FROM prices WHERE security_id=?", (security_id,))
        start_date = r.iloc[0]["min_date"]
    if end_date is None:
        r = _read_sql("SELECT MAX(date) AS max_date FROM prices WHERE security_id=?", (security_id,))
        end_date = r.iloc[0]["max_date"]

    df = _read_sql("""
        SELECT date, open, high, low, close, adj_close, volume
        FROM prices
        WHERE security_id=? AND date BETWEEN ? AND ?
        ORDER BY date
    """, (security_id, start_date, end_date))

    if df.empty:
        return pd.DataFrame()

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    all_dates = pd.date_range(start=df["date"].min(), end=df["date"].max())
    df = df.set_index("date").reindex(all_dates).ffill().reset_index().rename(columns={"index": "date"})
    df["price"] = df["adj_close"].combine_first(df["close"])

    fx_series = get_fx_series(currency, df["date"].min().strftime("%Y-%m-%d"),
                              df["date"].max().strftime("%Y-%m-%d"))
    for col in ["open", "high", "low", "close", "adj_close", "price"]:
        df[col] = df[col] * fx_series.values
    return df


def get_price_history(symbol: str, lookback_days: int = 400) -> pd.DataFrame:
    end_date = pd.Timestamp.utcnow().normalize()
    start_date = end_date - pd.Timedelta(days=lookback_days)
    df = get_price_series(symbol, start_date.isoformat(), end_date.isoformat())
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
    return df


def store_prices(security_id: int, df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return

    df = df.reset_index().rename(columns={
        "Date": "date", "Open": "open", "High": "high", "Low": "low",
        "Close": "close", "Adj Close": "adj_close", "Volume": "volume",
    })
    for col in ["date", "open", "high", "low", "close", "adj_close", "volume"]:
        if col not in df.columns:
            df[col] = None
    for col in ["open", "high", "low", "close", "adj_close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["close"] = df["close"].combine_first(df["adj_close"])

    ts = to_iso_ts(pd.Timestamp.utcnow())
    recs = [
        (security_id, r.date,
         r.open if pd.notna(r.open) else 0.0,
         r.high if pd.notna(r.high) else 0.0,
         r.low if pd.notna(r.low) else 0.0,
         r.close if pd.notna(r.close) else 0.0,
         r.adj_close if pd.notna(r.adj_close) else 0.0,
         r.volume if pd.notna(r.volume) else 0.0,
         ts)
        for r in df.to_records(index=False)
    ]

    if _IS_POSTGRES:
        sql = _adapt_sql("""
            INSERT INTO prices (security_id, date, open, high, low, close, adj_close, volume, prices_updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (security_id, date) DO UPDATE SET
                open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                close=EXCLUDED.close, adj_close=EXCLUDED.adj_close,
                volume=EXCLUDED.volume, prices_updated_at=EXCLUDED.prices_updated_at
        """)
    else:
        sql = """
            INSERT OR REPLACE INTO prices
                (security_id, date, open, high, low, close, adj_close, volume, prices_updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    with get_conn() as conn:
        conn.cursor().executemany(sql, recs)
        conn.commit()


# ═════════════════════════════════════════════════════════════════════════════
# Dividends
# ═════════════════════════════════════════════════════════════════════════════

def get_dividends(symbol: str) -> pd.DataFrame:
    return _read_sql("""
        SELECT d.date, d.dividend
        FROM dividends d
        JOIN securities s ON d.security_id = s.id
        WHERE s.yahoo_ticker=?
        ORDER BY d.date DESC
    """, (symbol,))


def store_dividends(security_id: int, ser: pd.Series) -> None:
    if ser is None or ser.empty:
        return
    df = ser.reset_index()
    df.columns = ["date", "dividend"]
    df["date"] = df["date"].apply(to_iso_date)
    ts = to_iso_ts(pd.Timestamp.utcnow())
    recs = [(security_id, r.date, r.dividend, ts) for r in df.to_records(index=False)]

    if _IS_POSTGRES:
        sql = _adapt_sql("""
            INSERT INTO dividends (security_id, date, dividend, dividends_updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (security_id, date) DO UPDATE SET
                dividend=EXCLUDED.dividend, dividends_updated_at=EXCLUDED.dividends_updated_at
        """)
    else:
        sql = """
            INSERT OR REPLACE INTO dividends (security_id, date, dividend, dividends_updated_at)
            VALUES (?, ?, ?, ?)
        """
    with get_conn() as conn:
        conn.cursor().executemany(sql, recs)
        conn.commit()


# ═════════════════════════════════════════════════════════════════════════════
# Financials
# ═════════════════════════════════════════════════════════════════════════════

def get_financials(symbol: str, statement: str) -> pd.DataFrame:
    return _read_sql("""
        SELECT f.as_of_date, f.payload
        FROM financials f
        JOIN securities s ON f.security_id = s.id
        WHERE s.yahoo_ticker=? AND f.statement=?
        ORDER BY f.as_of_date DESC
    """, (symbol, statement))


def store_financials(security_id: int, t) -> None:
    import yfinance as yf
    statements = {
        "income_statement": getattr(t, "financials", None),
        "balance_sheet": getattr(t, "balance_sheet", None),
        "cashflow": getattr(t, "cashflow", None),
    }
    ts = to_iso_ts(pd.Timestamp.utcnow())
    if _IS_POSTGRES:
        sql = _adapt_sql("""
            INSERT INTO financials (security_id, statement, as_of_date, payload, kpis_updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (security_id, statement, as_of_date) DO UPDATE SET
                payload=EXCLUDED.payload, kpis_updated_at=EXCLUDED.kpis_updated_at
        """)
    else:
        sql = """
            INSERT OR REPLACE INTO financials (security_id, statement, as_of_date, payload, kpis_updated_at)
            VALUES (?, ?, ?, ?, ?)
        """
    with get_conn() as conn:
        cur = conn.cursor()
        for stmt_name, df in statements.items():
            if df is None or df.empty:
                continue
            df = df.fillna(0)
            for col in df.columns:
                cur.execute(sql, (
                    security_id, stmt_name, to_iso_date(col),
                    json.dumps({str(k): v for k, v in df[col].to_dict().items()}), ts,
                ))
        conn.commit()


# ═════════════════════════════════════════════════════════════════════════════
# Valuations
# ═════════════════════════════════════════════════════════════════════════════

def store_dcf(security_id: int, dcf_raw: dict) -> None:
    now = to_iso_ts(pd.Timestamp.utcnow())
    if _IS_POSTGRES:
        sql = _adapt_sql("""
            INSERT INTO valuations
                (security_id, intrinsic_per_share, total_pv, terminal_value,
                 shares_outstanding, market_price, margin_of_safety, rating, valuation_computed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (security_id) DO UPDATE SET
                intrinsic_per_share=EXCLUDED.intrinsic_per_share,
                total_pv=EXCLUDED.total_pv,
                terminal_value=EXCLUDED.terminal_value,
                shares_outstanding=EXCLUDED.shares_outstanding,
                market_price=EXCLUDED.market_price,
                margin_of_safety=EXCLUDED.margin_of_safety,
                rating=EXCLUDED.rating,
                valuation_computed_at=EXCLUDED.valuation_computed_at
        """)
    else:
        sql = """
            INSERT OR REPLACE INTO valuations
                (security_id, intrinsic_per_share, total_pv, terminal_value,
                 shares_outstanding, market_price, margin_of_safety, rating, valuation_computed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    with get_conn() as conn:
        conn.cursor().execute(sql, (
            security_id, dcf_raw.get("intrinsic_per_share"), dcf_raw.get("total_pv"),
            dcf_raw.get("terminal_value"), dcf_raw.get("shares_outstanding"),
            dcf_raw.get("market_price"), dcf_raw.get("margin_of_safety"),
            dcf_raw.get("rating"), now,
        ))
        conn.commit()


def get_cached_dcf(symbol: str) -> Optional[dict]:
    df = _read_sql("""
        SELECT v.*, s.yahoo_ticker AS symbol
        FROM valuations v
        JOIN securities s ON s.id = v.security_id
        WHERE s.yahoo_ticker=?
        ORDER BY v.valuation_computed_at DESC LIMIT 1
    """, (symbol,))
    return df.iloc[0].to_dict() if not df.empty else None


def get_cashflow_payloads(symbol: str) -> pd.DataFrame:
    return _read_sql("""
        SELECT f.as_of_date, f.payload
        FROM financials f
        JOIN securities s ON s.id = f.security_id
        WHERE s.yahoo_ticker=? AND f.statement LIKE '%cash%'
        ORDER BY f.as_of_date DESC
    """, (symbol,))


# ═════════════════════════════════════════════════════════════════════════════
# Alerts
# ═════════════════════════════════════════════════════════════════════════════

def get_all_alerts() -> pd.DataFrame:
    return _read_sql("""
        SELECT a.id, a.security_id,
               COALESCE(sc."longName", sc."shortName") AS security_name,
               s.yahoo_ticker AS symbol,
               a.alert_type, a.params, a.active, a.notify_mode,
               a.cooldown_seconds, a.last_evaluated, a.last_triggered,
               a.note, a.auto_managed
        FROM alerts a
        LEFT JOIN securities_cache sc ON sc.security_id = a.security_id
        LEFT JOIN securities s ON s.id = a.security_id
        ORDER BY a.id DESC
    """)


def create_alert(security_id, alert_type, params_json, notify_mode,
                 cooldown_seconds, active=True, note=None, auto_managed=False) -> int:
    sql = _adapt_sql("""
        INSERT INTO alerts(security_id, alert_type, params, active,
                           notify_mode, cooldown_seconds, note, auto_managed)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, (security_id, alert_type, params_json, int(active),
                          notify_mode, cooldown_seconds, note, int(auto_managed)))
        conn.commit()
        if _IS_POSTGRES:
            cur.execute("SELECT lastval()")
        else:
            cur.execute("SELECT last_insert_rowid()")
        return cur.fetchone()[0]


def update_alert(alert_id, alert_type=None, params=None, notify_mode=None,
                 cooldown_seconds=None, active=None, note=None, automatic=False):
    updates, values = [], []
    if alert_type is not None:
        updates.append("alert_type = " + _ph()); values.append(alert_type)
    if params is not None:
        updates.append("params = " + _ph()); values.append(json.dumps(params))
    if notify_mode is not None:
        updates.append("notify_mode = " + _ph()); values.append(notify_mode)
    if cooldown_seconds is not None:
        updates.append("cooldown_seconds = " + _ph()); values.append(cooldown_seconds)
    if active is not None:
        updates.append("active = " + _ph()); values.append(int(active))
    if note is not None:
        updates.append("note = " + _ph()); values.append(note)
    updates.append("auto_managed = " + _ph()); values.append(int(automatic))
    if not updates:
        return
    values.append(alert_id)
    with get_conn() as conn:
        conn.cursor().execute(f"UPDATE alerts SET {', '.join(updates)} WHERE id = {_ph()}", values)
        conn.commit()


def toggle_alert_active(alert_id: int, active: bool) -> None:
    with get_conn() as conn:
        conn.cursor().execute(
            _adapt_sql("UPDATE alerts SET active=? WHERE id=?"), (int(active), alert_id)
        )
        conn.commit()


def delete_alert(alert_id: int) -> None:
    with get_conn() as conn:
        conn.cursor().execute(_adapt_sql("DELETE FROM alerts WHERE id=?"), (alert_id,))
        conn.commit()


def log_alert_trigger(alert_id: int, payload: dict) -> None:
    now = pd.Timestamp.utcnow().isoformat()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            _adapt_sql("INSERT INTO alerts_log(alert_id, triggered_at, payload) VALUES (?, ?, ?)"),
            (alert_id, now, json.dumps(payload)),
        )
        cur.execute(
            _adapt_sql("UPDATE alerts SET last_triggered=?, last_evaluated=? WHERE id=?"),
            (now, now, alert_id),
        )
        conn.commit()


def last_trigger_time(alert_id: int) -> Optional[pd.Timestamp]:
    df = _read_sql(
        "SELECT triggered_at FROM alerts_log WHERE alert_id=? ORDER BY triggered_at DESC LIMIT 1",
        (alert_id,),
    )
    return pd.to_datetime(df.iloc[0]["triggered_at"]) if not df.empty else None


def get_active_alerts() -> List[Dict]:
    df = _read_sql("SELECT * FROM alerts WHERE active=1")
    return df.to_dict("records")


def get_alert_by_id(alert_id: int) -> Optional[Dict]:
    df = _read_sql("SELECT * FROM alerts WHERE id=?", (alert_id,))
    return df.iloc[0].to_dict() if not df.empty else None


def get_alerts_for_digest(since_ts: str, notify_mode: str) -> List[Dict]:
    df = _read_sql("""
        SELECT al.alert_id, al.triggered_at, al.payload,
               a.alert_type, a.params
        FROM alerts_log al
        JOIN alerts a ON a.id = al.alert_id
        WHERE a.notify_mode=? AND al.triggered_at>?
        ORDER BY al.triggered_at
    """, (notify_mode, since_ts))
    return df.to_dict("records")


def get_last_alert_trigger(alert_id: int) -> Optional[pd.Timestamp]:
    return last_trigger_time(alert_id)


# ═════════════════════════════════════════════════════════════════════════════
# Holdings timeseries
# ═════════════════════════════════════════════════════════════════════════════

def list_portfolios_holding_security(security_id: int) -> List[int]:
    df = _read_sql(
        "SELECT DISTINCT portfolio_id FROM transactions WHERE security_id=?", (security_id,)
    )
    return df["portfolio_id"].tolist()


def get_holdings_from_transactions(portfolio_ids: Optional[List[int]] = None) -> pd.DataFrame:
    sql = """
        SELECT DISTINCT p.id AS portfolio_id, p.name AS portfolio_name,
               s.id AS security_id, s.yahoo_ticker AS symbol,
               sc."longName" AS security_name
        FROM transactions t
        INNER JOIN portfolios p ON p.id = t.portfolio_id
        INNER JOIN securities s ON s.id = t.security_id
        LEFT JOIN securities_cache sc ON sc.security_id = s.id
        WHERE 1=1
    """
    params: list = []
    if portfolio_ids:
        ph = ", ".join([_ph()] * len(portfolio_ids))
        sql += f" AND p.id IN ({ph})"
        params.extend(portfolio_ids)
    sql += " ORDER BY p.id, s.yahoo_ticker"
    return _read_sql(sql, tuple(params) if params else None)


def list_prices_for_security(security_id: int, start_date: str, end_date: str) -> pd.DataFrame:
    df = _read_sql("""
        SELECT date, open, high, low, close, adj_close, volume
        FROM prices
        WHERE security_id=? AND date BETWEEN ? AND ?
        ORDER BY date
    """, (security_id, start_date, end_date))

    cur_df = _read_sql(
        "SELECT currency FROM securities_cache WHERE security_id=?", (security_id,)
    )
    currency = str(cur_df.iloc[0]["currency"]) if not cur_df.empty and cur_df.iloc[0]["currency"] else "EUR"

    if df.empty:
        return pd.DataFrame()

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    all_dates = pd.date_range(start=df["date"].min(), end=df["date"].max())
    df = df.set_index("date").reindex(all_dates).ffill().reset_index().rename(columns={"index": "date"})
    df["price"] = df["adj_close"].combine_first(df["close"])

    fx_series = get_fx_series(currency, df["date"].min().strftime("%Y-%m-%d"),
                              df["date"].max().strftime("%Y-%m-%d"))
    for col in ["open", "high", "low", "close", "adj_close", "price"]:
        df[col] = df[col] * fx_series.values
    return df


def clear_holdings_timeseries(security_id: int, portfolio_id: Optional[int] = None, conn=None):
    own = conn is None
    if own:
        conn = get_conn()
    cur = conn.cursor()
    if portfolio_id is not None:
        cur.execute(
            _adapt_sql("DELETE FROM holdings_timeseries WHERE portfolio_id=? AND security_id=?"),
            (portfolio_id, security_id),
        )
    else:
        cur.execute(
            _adapt_sql("DELETE FROM holdings_timeseries WHERE security_id=?"), (security_id,)
        )
    if own:
        conn.commit()
        conn.close()


def get_holdings_timeseries(
    portfolio_ids=None, sectors=None, industries=None,
    security_types=None, symbols=None, exchanges=None,
    start_date=None, end_date=None,
) -> pd.DataFrame:
    sql = """
        SELECT ht.date, ht.portfolio_id, ht.security_id,
               ht.quantity, ht.market_value, ht.cost_basis,
               s.yahoo_ticker AS symbol,
               COALESCE(sc."longName", s.yahoo_ticker, 'Unknown') AS name,
               COALESCE(sc.sector,'Unknown') AS sector,
               COALESCE(sc.industry,'Unknown') AS industry,
               COALESCE(sc.exchange,'Unknown') AS exchange,
               COALESCE(sc.security_type,'Unknown') AS security_type
        FROM holdings_timeseries ht
        JOIN securities s ON ht.security_id = s.id
        LEFT JOIN securities_cache sc ON sc.security_id = s.id
        WHERE 1=1
    """
    params: list = []

    def _add_in(col, vals):
        nonlocal sql
        ph = ", ".join([_ph()] * len(vals))
        sql += f" AND {col} IN ({ph})"
        params.extend(vals)

    if portfolio_ids:
        _add_in("ht.portfolio_id", portfolio_ids)
    if sectors:
        _add_in("COALESCE(sc.sector,'Unknown')", sectors)
    if industries:
        _add_in("COALESCE(sc.industry,'Unknown')", industries)
    if security_types:
        _add_in("COALESCE(sc.security_type,'Unknown')", security_types)
    if symbols:
        _add_in("s.yahoo_ticker", symbols)
    if exchanges:
        _add_in("COALESCE(sc.exchange,'Unknown')", exchanges)
    if start_date:
        sql += f" AND ht.date >= {_ph()}"; params.append(start_date)
    if end_date:
        sql += f" AND ht.date <= {_ph()}"; params.append(end_date)
    sql += " ORDER BY ht.date"

    df = _read_sql(sql, tuple(params) if params else None)

    for col in ["date","portfolio_id","security_id","quantity","market_value","cost_basis",
                "symbol","name","sector","industry","exchange","security_type"]:
        if col not in df.columns:
            df[col] = pd.NA

    df["date"] = pd.to_datetime(df["date"])
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0.0)
    df["market_value"] = pd.to_numeric(df["market_value"], errors="coerce").fillna(0.0)
    df["cost_basis"] = pd.to_numeric(df["cost_basis"], errors="coerce").fillna(0.0)
    df = df[(df["quantity"] > 0) | (df["market_value"] > 0)].reset_index(drop=True)
    for m in ["symbol","name","sector","industry","exchange","security_type"]:
        df[m] = df[m].fillna("Unknown")
    return df


def recompute_holdings_timeseries(portfolio_id: int, security_id: int, conn=None) -> None:
    own = conn is None
    if own:
        conn = get_conn()
    try:
        df_tx = list_transactions_for_security(portfolio_id, security_id)
        if df_tx.empty:
            clear_holdings_timeseries(security_id, portfolio_id, conn=conn)
            if own:
                conn.commit()
            return

        df_tx["date"] = pd.to_datetime(df_tx["date"]).dt.tz_localize(None)
        df_tx = df_tx.sort_values("date").reset_index(drop=True)
        start_date = df_tx["date"].min().date()
        end_date = pd.Timestamp.utcnow().normalize().date()
        all_dates = pd.date_range(start=start_date, end=end_date, freq="D")

        df_prices = list_prices_for_security(security_id, start_date.isoformat(), end_date.isoformat())
        if df_prices.empty:
            clear_holdings_timeseries(security_id, portfolio_id, conn=conn)
            if own:
                conn.commit()
            return

        df_prices["date"] = pd.to_datetime(df_prices["date"]).dt.tz_localize(None)
        df_prices = (df_prices.set_index("date").reindex(all_dates).ffill()
                     .reset_index().rename(columns={"index": "date"}))
        df_prices["price"] = df_prices["price"].fillna(0.0)

        lots: deque = deque()
        records = []
        tx_idx = 0

        for _, row in df_prices.iterrows():
            cur_date = row["date"].date()
            cur_price = float(row["price"] or 0.0)
            if cur_price == 0:
                continue
            while tx_idx < len(df_tx) and df_tx.loc[tx_idx, "date"].date() <= cur_date:
                tx = df_tx.loc[tx_idx]
                qty = float(tx.get("quantity") or 0.0)
                tx_price = float(tx.get("price")) if pd.notna(tx.get("price")) else cur_price
                fees = float(tx.get("fees") or 0.0)
                ttype = str(tx.get("type", "")).strip().lower()
                if ttype == "buy":
                    lots.append({"qty": qty, "price": tx_price, "fees": fees})
                elif ttype == "sell":
                    remaining = qty
                    while remaining > 0 and lots:
                        lot = lots[0]
                        take = min(lot["qty"], remaining)
                        lot["qty"] -= take
                        remaining -= take
                        if lot["qty"] <= 1e-12:
                            lots.popleft()
                tx_idx += 1

            qty_hold = sum(l["qty"] for l in lots)
            if qty_hold <= 0:
                continue
            cost_basis = sum(l["qty"] * l["price"] + l.get("fees", 0.0) for l in lots)
            records.append((cur_date.isoformat(), int(portfolio_id), int(security_id),
                            float(qty_hold), float(qty_hold * cur_price), float(cost_basis)))

        clear_holdings_timeseries(security_id, portfolio_id, conn)
        insert_holdings_timeseries(records, conn)
        update_security_risk_timeseries(security_id, portfolio_id, conn=conn)
        if own:
            conn.commit()
    except Exception:
        logging.exception("recompute_holdings_timeseries failed")
    finally:
        if own:
            conn.close()


# ═════════════════════════════════════════════════════════════════════════════
# Risk timeseries
# ═════════════════════════════════════════════════════════════════════════════

def get_security_risk_timeseries(security_id: int, start_date=None, end_date=None) -> pd.DataFrame:
    sql = "SELECT * FROM security_risk_timeseries WHERE security_id=?"
    params: list = [int(security_id)]
    if start_date:
        sql += " AND date >= " + _ph(); params.append(start_date)
    if end_date:
        sql += " AND date <= " + _ph(); params.append(end_date)
    sql += " ORDER BY date"
    df = _read_sql(sql, tuple(params))
    df["security_id"] = df["security_id"].astype(int)
    return df


def update_security_risk_timeseries(security_id: int, portfolio_ids=None, conn=None):
    own = conn is None
    if own:
        conn = get_conn()
    try:
        sec = get_security_by_id(int(security_id))
        if not sec:
            return
        symbol = sec["symbol"]
        prices = get_price_series(symbol)
        if prices.empty or "adj_close" not in prices:
            return

        df_p = prices.copy()
        if isinstance(df_p.index, pd.DatetimeIndex):
            df_p = df_p.reset_index().rename(columns={"index": "date"})
        df_p["date"] = pd.to_datetime(df_p["date"]).dt.tz_localize(None)
        df_p["returns"] = df_p["adj_close"].pct_change()
        df_p["risk_score"] = df_p["returns"].rolling(window=252, min_periods=20).std() * np.sqrt(252)
        df_p = df_p.dropna(subset=["risk_score"])
        if df_p.empty:
            return

        df_hold = get_holdings_timeseries()
        if portfolio_ids:
            pids = [portfolio_ids] if isinstance(portfolio_ids, int) else portfolio_ids
        else:
            pids = df_hold["portfolio_id"].unique().tolist()
        df_hold = df_hold[df_hold["portfolio_id"].isin(pids)]
        df_hold = df_hold[df_hold["security_id"] == security_id]
        if df_hold.empty:
            return
        df_hold["date"] = pd.to_datetime(df_hold["date"]).dt.tz_localize(None)

        df_p["date_only"] = df_p["date"].dt.date
        df_hold["date_only"] = df_hold["date"].dt.date
        df_merge = df_p[df_p["date_only"].isin(df_hold["date_only"])].copy()
        if df_merge.empty:
            return

        new_rows = []
        for _, row in df_merge.iterrows():
            date = row["date"].date()
            mv = df_hold.loc[df_hold["date_only"] == date, "market_value"].sum()
            if mv > 0:
                new_rows.append((date.strftime("%Y-%m-%d"), int(security_id),
                                  row["risk_score"], mv, row["risk_score"]))

        if not new_rows:
            return

        if _IS_POSTGRES:
            sql = _adapt_sql("""
                INSERT INTO security_risk_timeseries
                    (date, security_id, risk_score, market_value, weighted_risk)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (date, security_id) DO UPDATE SET
                    risk_score=EXCLUDED.risk_score,
                    market_value=EXCLUDED.market_value,
                    weighted_risk=EXCLUDED.weighted_risk
            """)
        else:
            sql = """
                INSERT OR REPLACE INTO security_risk_timeseries
                    (date, security_id, risk_score, market_value, weighted_risk)
                VALUES (?, ?, ?, ?, ?)
            """
        conn.cursor().executemany(sql, new_rows)
        if own:
            conn.commit()
    finally:
        if own:
            conn.close()


def delete_security_risk_timeseries(security_id: int, conn=None):
    own = conn is None
    if own:
        conn = get_conn()
    conn.cursor().execute(
        _adapt_sql("DELETE FROM security_risk_timeseries WHERE security_id=?"), (security_id,)
    )
    if own:
        conn.commit()
        conn.close()


def get_portfolio_risk_timeseries(portfolio_ids=None) -> pd.DataFrame:
    df_hold = get_holdings_timeseries(portfolio_ids=portfolio_ids)
    if df_hold.empty:
        return pd.DataFrame(columns=["date", "portfolio_id", "weighted_risk"])

    df_hold["security_id"] = df_hold["security_id"].astype(int)
    df_hold["date"] = pd.to_datetime(df_hold["date"]).dt.tz_localize(None)
    securities = df_hold["security_id"].unique()

    risk_dfs = []
    for sec_id in securities:
        df_sec = get_security_risk_timeseries(int(sec_id))
        if not df_sec.empty:
            df_sec["security_id"] = df_sec["security_id"].astype(int)
            df_sec["date"] = pd.to_datetime(df_sec["date"]).dt.tz_localize(None)
            risk_dfs.append(df_sec)

    if not risk_dfs:
        return pd.DataFrame(columns=["date", "portfolio_id", "weighted_risk"])

    df_risk = pd.concat(risk_dfs, ignore_index=True)
    df_list = []
    for sec_id in securities:
        dh = df_hold[df_hold["security_id"] == sec_id].sort_values("date")
        dr = df_risk[df_risk["security_id"] == sec_id].sort_values("date")
        if dh.empty or dr.empty:
            continue
        merged = pd.merge_asof(dh, dr, on="date", by="security_id",
                               direction="nearest", tolerance=pd.Timedelta("3650D"))
        df_list.append(merged)

    if not df_list:
        return pd.DataFrame(columns=["date", "portfolio_id", "weighted_risk"])

    df = pd.concat(df_list, ignore_index=True).dropna(subset=["weighted_risk"])
    return (df.groupby(["date", "portfolio_id"])["weighted_risk"]
              .sum().reset_index().sort_values("date"))


def get_portfolio_risk_timeseries_detailed(portfolio_ids=None) -> pd.DataFrame:
    df_hold = get_holdings_timeseries(portfolio_ids=portfolio_ids)
    if df_hold.empty:
        return pd.DataFrame(columns=["date","portfolio_id","security_id","symbol",
                                      "sector","industry","security_type","market_value","weighted_risk"])
    df_hold["security_id"] = df_hold["security_id"].astype(int)
    df_hold["date"] = pd.to_datetime(df_hold["date"]).dt.tz_localize(None)
    securities = df_hold["security_id"].unique()
    risk_dfs = []
    for sec_id in securities:
        df_sec = get_security_risk_timeseries(sec_id)
        if not df_sec.empty:
            df_sec["security_id"] = df_sec["security_id"].astype(int)
            df_sec["date"] = pd.to_datetime(df_sec["date"]).dt.tz_localize(None)
            risk_dfs.append(df_sec)
    if not risk_dfs:
        return pd.DataFrame()
    df_risk = pd.concat(risk_dfs, ignore_index=True)
    df_list = []
    for sec_id in securities:
        dh = df_hold[df_hold["security_id"] == sec_id].sort_values("date")
        dr = df_risk[df_risk["security_id"] == sec_id].sort_values("date")
        if dh.empty or dr.empty:
            continue
        merged = pd.merge_asof(dh, dr, on="date", by="security_id",
                               direction="nearest", tolerance=pd.Timedelta("3650D"))
        df_list.append(merged)
    if not df_list:
        return pd.DataFrame()
    return pd.concat(df_list, ignore_index=True).dropna(subset=["weighted_risk"])


# ═════════════════════════════════════════════════════════════════════════════
# FX helpers
# ═════════════════════════════════════════════════════════════════════════════

def get_all_security_currencies() -> List[str]:
    df = _read_sql("SELECT DISTINCT currency FROM securities_cache WHERE currency IS NOT NULL")
    return df["currency"].tolist()


def get_earliest_price_or_dividend_date() -> Optional[str]:
    df = _read_sql("""
        SELECT MIN(date) AS d FROM (
            SELECT MIN(date) AS date FROM prices
            UNION ALL
            SELECT MIN(date) AS date FROM dividends
        ) sub
    """)
    return str(df.iloc[0]["d"]) if not df.empty and df.iloc[0]["d"] is not None else None


def store_fx_rates(df: pd.DataFrame) -> None:
    if df.empty:
        return
    if _IS_POSTGRES:
        sql = _adapt_sql("""
            INSERT INTO fx_rates (date, base_currency, target_currency, rate)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (date, base_currency, target_currency) DO UPDATE SET rate=EXCLUDED.rate
        """)
    else:
        sql = "INSERT OR REPLACE INTO fx_rates (date, base_currency, target_currency, rate) VALUES (?, ?, ?, ?)"
    with get_conn() as conn:
        conn.cursor().executemany(sql, df[["date","base_currency","target_currency","rate"]].values.tolist())
        conn.commit()


def get_latest_fx_date(base_currency: str) -> Optional[str]:
    df = _read_sql(
        "SELECT MAX(date) AS d FROM fx_rates WHERE base_currency=? AND target_currency='EUR'",
        (base_currency,),
    )
    return str(df.iloc[0]["d"]) if not df.empty and df.iloc[0]["d"] is not None else None


def get_latest_fx_rate(base_currency: str, target_currency: str = "EUR") -> float:
    if base_currency.upper() == target_currency.upper():
        return 1.0
    df = _read_sql("""
        SELECT rate FROM fx_rates
        WHERE base_currency=? AND target_currency=?
        ORDER BY date DESC LIMIT 1
    """, (base_currency.upper(), target_currency.upper()))
    return float(df.iloc[0]["rate"]) if not df.empty else 1.0


def get_fx_rate_on_date(base_currency: str, date: pd.Timestamp) -> float:
    if base_currency == "EUR":
        return 1.0
    df = _read_sql("""
        SELECT rate FROM fx_rates
        WHERE base_currency=? AND target_currency='EUR' AND date<=?
        ORDER BY date DESC LIMIT 1
    """, (base_currency, date.strftime("%Y-%m-%d")))
    return float(df.iloc[0]["rate"]) if not df.empty else 1.0


def get_fx_series(base_currency: str, start_date: str, end_date: str,
                  target_currency: str = "EUR") -> pd.Series:
    if base_currency.upper() == target_currency.upper():
        return pd.Series(1.0, index=pd.date_range(start=start_date, end=end_date))
    df = _read_sql("""
        SELECT date, rate FROM fx_rates
        WHERE base_currency=? AND target_currency=?
          AND date BETWEEN ? AND ?
        ORDER BY date
    """, (base_currency.upper(), target_currency.upper(), start_date, end_date))
    if df.empty:
        return pd.Series(1.0, index=pd.date_range(start=start_date, end=end_date))
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    full_index = pd.date_range(start=start_date, end=end_date)
    return df.reindex(full_index).ffill()["rate"]
