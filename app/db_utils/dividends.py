import pandas as pd

from .core import get_conn, _adapt_sql, _read_sql, _IS_POSTGRES, to_iso_date, to_iso_ts


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


def get_dividends_many(symbols) -> pd.DataFrame:
    """Dividends of several securities in one query, with each security's currency."""
    symbols = [str(s) for s in symbols if s]
    if not symbols:
        return pd.DataFrame(columns=["symbol", "date", "dividend", "currency"])
    ph = ", ".join(["?"] * len(symbols))
    return _read_sql(f"""
        SELECT s.yahoo_ticker AS symbol, d.date, d.dividend, sc.currency
        FROM dividends d
        JOIN securities s ON d.security_id = s.id
        LEFT JOIN securities_cache sc ON sc.security_id = s.id
        WHERE s.yahoo_ticker IN ({ph})
        ORDER BY d.date
    """, tuple(symbols))
