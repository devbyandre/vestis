from typing import Optional
import pandas as pd

from .core import get_conn, _adapt_sql, _read_sql, _IS_POSTGRES, _ph
from .fx import get_latest_fx_rate


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

    info = dict(info)
    # Yahoo reports dividendYield in percent (0.95 = 0.95%); everything here
    # works with fractions like profitMargins. ETFs only carry 'yield' (a fraction).
    if info.get("dividendYield") is not None:
        try:
            info["dividendYield"] = float(info["dividendYield"]) / 100.0
        except (TypeError, ValueError):
            info["dividendYield"] = None
    elif info.get("yield") is not None:
        info["dividendYield"] = info.get("yield")

    data_values = []
    for k in keys:
        v = info.get(k)
        if k in numeric_keys:
            try:
                v = float(v) if v is not None else None
            except (TypeError, ValueError):
                v = None
        elif k == "earningsTimestamp":
            try:
                if v is not None:
                    v = pd.Timestamp(int(v), unit="s", tz="UTC").isoformat()
            except (TypeError, ValueError, OSError):
                v = None
        elif v is not None:
            # Convert numpy/large int types to safe Python types
            try:
                import numpy as np
                if isinstance(v, (np.integer,)):
                    v = float(v)  # use float to avoid INTEGER overflow
                elif isinstance(v, (np.floating,)):
                    v = float(v)
            except ImportError:
                pass
            # Also convert plain Python ints that might overflow Postgres INTEGER
            if isinstance(v, int) and (v > 2147483647 or v < -2147483647):
                v = float(v)
        data_values.append(v)

    ts_now = pd.Timestamp.utcnow().isoformat()
    cols = ["security_id", "security_type"] + keys + ["kpis_updated_at"]
    data = [security_id, security_type] + data_values + [ts_now]

    # Unquoted identifiers: Postgres folds them to lowercase matching the
    # (unquoted, thus lowercase) columns declared in db_init.py, and SQLite
    # matches column names case-insensitively regardless of quoting — so
    # this works on both without needing per-dialect casing.
    ph = ", ".join([_ph()] * len(data))
    col_list = ", ".join(cols)

    update_cols = [c for c in cols if c not in ("security_id",)]
    update_set = ",\n".join([
        f"{c} = EXCLUDED.{c}"
        if c not in ("shortName", "longName")
        else f"{c} = COALESCE(securities_cache.{c}, EXCLUDED.{c})"
        for c in update_cols
    ])

    if _IS_POSTGRES:
        sql = f"""
            INSERT INTO securities_cache ({col_list})
            VALUES ({ph})
            ON CONFLICT (security_id) DO UPDATE SET
            {update_set}
        """
    else:
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
                shortName, longName, regularMarketPrice
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (security_id) DO UPDATE SET
                security_type = EXCLUDED.security_type,
                country = EXCLUDED.country,
                exchange = EXCLUDED.exchange,
                sector = EXCLUDED.sector,
                industry = EXCLUDED.industry,
                regularMarketPrice = EXCLUDED.regularMarketPrice,
                shortName = COALESCE(securities_cache.shortName, EXCLUDED.shortName),
                longName  = COALESCE(securities_cache.longName,  EXCLUDED.longName)
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


_KPI_KEYS = [
    "security_type", "country", "exchange", "sector", "industry", "shortName", "longName",
    "regularMarketPrice", "fiftyTwoWeekHigh", "fiftyTwoWeekLow", "volume", "averageVolume",
    "marketCap", "beta", "trailingPE", "forwardPE", "trailingEps", "earningsTimestamp",
    "dividendRate", "dividendYield", "enterpriseValue", "profitMargins", "operatingMargins",
    "returnOnAssets", "returnOnEquity", "totalRevenue", "revenuePerShare", "grossProfits",
    "ebitda", "totalCash", "totalDebt", "currentRatio", "bookValue",
    "operatingCashflow", "freeCashflow", "sharesOutstanding",
]
_MONETARY = [
    "regularMarketPrice", "fiftyTwoWeekHigh", "fiftyTwoWeekLow", "marketCap", "dividendRate",
    "enterpriseValue", "totalRevenue", "revenuePerShare", "grossProfits", "ebitda",
    "totalCash", "totalDebt", "bookValue", "operatingCashflow", "freeCashflow",
]
_CACHE_SQL = """
    SELECT s.id, s.yahoo_ticker AS symbol, sc.*
    FROM securities s
    LEFT JOIN securities_cache sc ON sc.security_id = s.id
"""


def _cache_row_in_eur(data: dict, rates: dict) -> dict:
    """camelCase keys on both backends; monetary fields converted to EUR."""
    # `sc.*` returns Postgres' folded lowercase column names (e.g. "longname"),
    # but SQLite preserves the declared camelCase.
    for k in _KPI_KEYS:
        if k not in data and k.lower() in data:
            data[k] = data.pop(k.lower())
        else:
            data.setdefault(k, None)
    currency = data.get("currency") or "EUR"
    if currency not in rates:
        rates[currency] = get_latest_fx_rate(currency)
    fx = rates[currency]
    if fx != 1.0:
        for field in _MONETARY:
            if data.get(field) is not None:
                data[field] = data[field] * fx
    return data


def get_security_cache(id: int) -> Optional[pd.DataFrame]:
    df = _read_sql(_CACHE_SQL + " WHERE s.id = ?", (id,))
    if df.empty:
        return None
    return pd.DataFrame([_cache_row_in_eur(df.iloc[0].to_dict(), {})])


def get_security_cache_many(symbols) -> dict:
    """{symbol: cache row (as get_security_cache, EUR)} for several symbols in one query."""
    symbols = [str(s) for s in symbols if s]
    if not symbols:
        return {}
    df = _read_sql(_CACHE_SQL + " WHERE s.yahoo_ticker IN (%s)" % ", ".join(["?"] * len(symbols)), tuple(symbols))
    rates: dict = {}
    return {r["symbol"]: _cache_row_in_eur(r, rates) for r in df.to_dict("records")}


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

