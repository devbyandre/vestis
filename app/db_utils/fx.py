from typing import Optional, List
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


def get_latest_fx_rate(base_currency: str, target_currency: str = "EUR") -> float:
    if base_currency.upper() == target_currency.upper():
        return 1.0
    df = _read_sql("""
        SELECT rate FROM fx_rates
        WHERE base_currency=? AND target_currency=?
        ORDER BY date DESC LIMIT 1
    """, (base_currency.upper(), target_currency.upper()))
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
