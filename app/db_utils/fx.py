import logging
from typing import Optional, List, Tuple
import pandas as pd

from .core import get_conn, _adapt_sql, _read_sql, _IS_POSTGRES


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


# Yahoo quotes some listings in the minor unit (London in pence as "GBp").
_MINOR_UNITS = {"GBp": ("GBP", 100.0), "GBX": ("GBP", 100.0), "ZAc": ("ZAR", 100.0), "ILA": ("ILS", 100.0)}


def _major(currency: str) -> Tuple[str, float]:
    """Map a quote currency to (ISO currency, divisor), e.g. GBp -> (GBP, 100)."""
    if currency in _MINOR_UNITS:
        return _MINOR_UNITS[currency]
    return (currency or "EUR").upper(), 1.0


def get_latest_fx_rate(base_currency: str, target_currency: str = "EUR") -> float:
    base_currency, divisor = _major(base_currency)
    if base_currency == target_currency.upper():
        return 1.0 / divisor
    df = _read_sql("""
        SELECT rate FROM fx_rates
        WHERE base_currency=? AND target_currency=?
        ORDER BY date DESC LIMIT 1
    """, (base_currency, target_currency.upper()))
    if df.empty:
        logging.warning("No FX rate for %s->%s, treating as 1:1", base_currency, target_currency)
        return 1.0 / divisor
    return float(df.iloc[0]["rate"]) / divisor


def get_fx_series(base_currency: str, start_date: str, end_date: str,
                  target_currency: str = "EUR") -> pd.Series:
    """Daily rate (1 unit of base_currency in target_currency) for every calendar day."""
    full_index = pd.date_range(start=start_date, end=end_date)
    base_currency, divisor = _major(base_currency)
    if base_currency == target_currency.upper():
        return pd.Series(1.0 / divisor, index=full_index)
    df = _read_sql("""
        SELECT date, rate FROM fx_rates
        WHERE base_currency=? AND target_currency=?
          AND date BETWEEN ? AND ?
        ORDER BY date
    """, (base_currency, target_currency.upper(),
          (pd.Timestamp(start_date) - pd.Timedelta(days=10)).strftime("%Y-%m-%d"), end_date))
    if df.empty:
        logging.warning("No FX rates for %s->%s, treating as 1:1", base_currency, target_currency)
        return pd.Series(1.0 / divisor, index=full_index)
    df["date"] = pd.to_datetime(df["date"])
    rates = df.drop_duplicates("date").set_index("date")["rate"].sort_index()
    # Weekends/holidays take the previous fix; days before the first fix take the first one.
    rates = rates.reindex(rates.index.union(full_index)).ffill().bfill().reindex(full_index)
    return rates / divisor
