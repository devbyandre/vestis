from typing import Optional
import pandas as pd

from .core import get_conn, _adapt_sql, _read_sql, _IS_POSTGRES, to_iso_ts
from .fx import get_fx_series


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


def get_price_bars(symbol: str, lookback_days: int = 400) -> pd.DataFrame:
    """Raw daily bars as stored: trading days only, native currency.

    Unlike get_price_series/get_price_history this does NOT forward-fill
    weekends/holidays or convert to EUR. Technical signals (RSI, MA crosses,
    % moves) must be computed on this — otherwise FX moves leak into the
    indicator and window lengths silently cover fewer trading days.
    Adds a 'price' column (adj_close, falling back to close).
    """
    start = (pd.Timestamp.utcnow().normalize() - pd.Timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    df = _read_sql("""
        SELECT p.date, p.open, p.high, p.low, p.close, p.adj_close, p.volume
        FROM prices p
        JOIN securities s ON s.id = p.security_id
        WHERE s.yahoo_ticker=? AND p.date >= ?
        ORDER BY p.date
    """, (symbol, start))
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    for col in ["open", "high", "low", "close", "adj_close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    # store_prices writes 0.0 for missing values — treat those as missing
    adj = df["adj_close"].where(df["adj_close"] > 0)
    close = df["close"].where(df["close"] > 0)
    df["price"] = adj.combine_first(close)
    return df.dropna(subset=["price"]).reset_index(drop=True)


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
        (int(security_id), str(r.date),
         float(r.open) if pd.notna(r.open) else 0.0,
         float(r.high) if pd.notna(r.high) else 0.0,
         float(r.low) if pd.notna(r.low) else 0.0,
         float(r.close) if pd.notna(r.close) else 0.0,
         float(r.adj_close) if pd.notna(r.adj_close) else 0.0,
         float(r.volume) if pd.notna(r.volume) else 0.0,
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
