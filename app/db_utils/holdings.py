"""
Holdings timeseries + risk timeseries — kept in one module deliberately.

recompute_holdings_timeseries() calls update_security_risk_timeseries(),
passing the just-computed holdings rows directly via `holdings_df` — needed
because update_security_risk_timeseries()'s fallback path (get_holdings_timeseries())
reads through a separate pooled engine connection that can't see rows
inserted-but-not-committed on the same raw `conn` recompute_holdings_timeseries
is using. Splitting these into separate modules would turn this into a
fragile cross-module circular import for no benefit, since they're only ever
used together.
"""
import logging
import os
import threading
import time
from collections import deque
from typing import Optional, List
import numpy as np
import pandas as pd

from .core import get_conn, _raw_conn, _adapt_sql, _read_sql, _IS_POSTGRES, _ph
from .securities import get_security_by_id
from .prices import get_price_series
from .fx import get_fx_series
# Circular dependency: recompute_holdings_timeseries() below needs
# transactions.list_transactions_for_security(), and transactions.py's
# delete_transaction() needs recompute_holdings_timeseries() from this
# module. Import the whole module (not `from .transactions import
# list_transactions_for_security`) so the attribute lookup happens at call
# time, after both modules have finished loading, regardless of which one
# the __init__.py re-export happens to import first.
from . import transactions as _transactions


# The holdings frame is by far the most expensive read (tens of thousands of
# rows) and the Planning tab asks for it from several endpoints at once, so
# identical requests share one result for a short time. Writes made through
# this process drop it immediately; writes from other processes (cron
# workers) show up once the TTL expires. 0 disables the cache.
_TS_TTL = float(os.environ.get("HOLDINGS_CACHE_SECONDS", "60"))
_ts_cache: dict = {}
_ts_lock = threading.Lock()


def invalidate_holdings_cache() -> None:
    with _ts_lock:
        _ts_cache.clear()


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
        conn = _raw_conn()
    try:
        cur = conn.cursor()
        cur.executemany(sql, records)
        if own:
            conn.commit()
    finally:
        if own:
            conn.close()
        invalidate_holdings_cache()


def list_portfolios_holding_security(security_id: int) -> List[int]:
    df = _read_sql(
        "SELECT DISTINCT portfolio_id FROM transactions WHERE security_id=?", (security_id,)
    )
    return df["portfolio_id"].tolist()


def get_holdings_from_transactions(portfolio_ids: Optional[List[int]] = None) -> pd.DataFrame:
    sql = """
        SELECT DISTINCT p.id AS portfolio_id, p.name AS portfolio_name,
               s.id AS security_id, s.yahoo_ticker AS symbol,
               sc.longName AS security_name
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
    df["price"] = df["close"].combine_first(df["adj_close"])

    fx_series = get_fx_series(currency, df["date"].min().strftime("%Y-%m-%d"),
                              df["date"].max().strftime("%Y-%m-%d"))
    for col in ["open", "high", "low", "close", "adj_close", "price"]:
        df[col] = df[col] * fx_series.values
    return df


def clear_holdings_timeseries(security_id: int, portfolio_id: Optional[int] = None, conn=None):
    own = conn is None
    if own:
        conn = _raw_conn()
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
    invalidate_holdings_cache()


def _query_holdings_timeseries(
    portfolio_ids=None, sectors=None, industries=None,
    security_types=None, symbols=None, exchanges=None,
    start_date=None, end_date=None, latest_only=False,
) -> pd.DataFrame:
    """latest_only keeps just each portfolio+security's own most recent row."""
    sql = """
        SELECT ht.date, ht.portfolio_id, ht.security_id,
               ht.quantity, ht.market_value, ht.cost_basis,
               s.yahoo_ticker AS symbol,
               COALESCE(sc.longName, s.yahoo_ticker, 'Unknown') AS name,
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
    if latest_only:
        sql = sql.replace("WHERE 1=1", """
        JOIN (SELECT portfolio_id, security_id, MAX(date) AS max_date
              FROM holdings_timeseries
              WHERE quantity > 0 OR market_value > 0
              GROUP BY portfolio_id, security_id) lt
          ON lt.portfolio_id = ht.portfolio_id AND lt.security_id = ht.security_id
         AND lt.max_date = ht.date
        WHERE 1=1""")

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
    if not latest_only and not end_date and not df.empty:
        df = _carry_open_positions_forward(df)
    return df


def _carry_open_positions_forward(df: pd.DataFrame) -> pd.DataFrame:
    """Extend still-open positions whose series stops early up to the newest date.

    A security's rows are only rebuilt when its prices are stored, so one with
    a failed or skipped fetch would otherwise vanish from the latest days and
    skew every per-date total (allocation shares, portfolio risk, totals).
    """
    end = df["date"].max()
    net = _transactions.get_net_quantities()
    last = df.sort_values("date").groupby(["portfolio_id", "security_id"]).tail(1)
    extra = []
    for row in last.itertuples(index=False):
        if row.date >= end or net.get((int(row.portfolio_id), int(row.security_id)), 0.0) <= 1e-8:
            continue
        days = pd.date_range(row.date + pd.Timedelta(days=1), end, freq="D")
        extra.append(pd.DataFrame([row._asdict()] * len(days)).assign(date=days))
    if not extra:
        return df
    return pd.concat([df] + extra, ignore_index=True).sort_values("date").reset_index(drop=True)



def get_holdings_timeseries(
    portfolio_ids=None, sectors=None, industries=None,
    security_types=None, symbols=None, exchanges=None,
    start_date=None, end_date=None, latest_only=False,
) -> pd.DataFrame:
    """Holdings rows (see _query_holdings_timeseries), shared briefly between
    identical concurrent requests. Always returns a private copy."""
    if _TS_TTL <= 0:
        return _query_holdings_timeseries(portfolio_ids, sectors, industries, security_types,
                                          symbols, exchanges, start_date, end_date, latest_only)
    key = tuple(tuple(v) if isinstance(v, (list, set)) else v for v in
                (portfolio_ids, sectors, industries, security_types, symbols, exchanges,
                 start_date, end_date, latest_only))
    with _ts_lock:
        hit = _ts_cache.get(key)
        if hit and time.monotonic() - hit[0] < _TS_TTL:
            return hit[1].copy()
        df = _query_holdings_timeseries(portfolio_ids, sectors, industries, security_types,
                                        symbols, exchanges, start_date, end_date, latest_only)
        _ts_cache[key] = (time.monotonic(), df)
        return df.copy()


def recompute_holdings_timeseries(portfolio_id: int, security_id: int, conn=None) -> None:
    own = conn is None
    if own:
        conn = _raw_conn()
    try:
        df_tx = _transactions.list_transactions_for_security(portfolio_id, security_id)
        if df_tx.empty:
            clear_holdings_timeseries(security_id, portfolio_id, conn=conn)
            if own:
                conn.commit()
            return

        df_tx["date"] = pd.to_datetime(df_tx["date"]).dt.tz_localize(None)

        # Apply split adjustments before building lots
        # (splits adjust prior buy/sell qty and price, then are removed)
        splits = df_tx[df_tx["type"].str.lower() == "split"].copy()
        if not splits.empty:
            for _, sr in splits.sort_values("date").iterrows():
                try:
                    ratio = float(sr["quantity"])
                except (ValueError, TypeError):
                    continue
                if ratio <= 0:
                    continue
                mask = (
                    df_tx["type"].str.lower().isin(["buy", "sell"]) &
                    (df_tx["date"] < sr["date"])
                )
                df_tx.loc[mask, "quantity"] = df_tx.loc[mask, "quantity"] * ratio
                df_tx.loc[mask, "price"]    = df_tx.loc[mask, "price"]    / ratio
            df_tx = df_tx[df_tx["type"].str.lower() != "split"].copy()

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

        sec = get_security_by_id(int(security_id))
        risk_prices = get_price_series(sec["symbol"]) if sec else pd.DataFrame()

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
                    lots.append({"qty": qty, "qty0": qty, "price": tx_price, "fees": fees})
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
            # A partly sold lot keeps only its remaining share of the purchase fee.
            cost_basis = sum(l["qty"] * l["price"] + l["fees"] * l["qty"] / l["qty0"] for l in lots)
            records.append((cur_date.isoformat(), int(portfolio_id), int(security_id),
                            float(qty_hold), float(qty_hold * cur_price), float(cost_basis)))

        clear_holdings_timeseries(security_id, portfolio_id, conn)
        insert_holdings_timeseries(records, conn)
        # Pass the just-computed rows directly rather than letting
        # update_security_risk_timeseries re-query them — on this same
        # not-yet-committed conn, a fresh read via the engine's connection
        # pool wouldn't see them yet (see its docstring).
        holdings_df = pd.DataFrame(
            records, columns=["date", "portfolio_id", "security_id", "quantity", "market_value", "cost_basis"]
        )
        update_security_risk_timeseries(security_id, portfolio_id, conn=conn, holdings_df=holdings_df,
                                        prices=risk_prices)
        if own:
            conn.commit()
    except Exception:
        logging.exception("recompute_holdings_timeseries failed")
    finally:
        if own:
            conn.close()
        invalidate_holdings_cache()


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


def update_security_risk_timeseries(security_id: int, portfolio_ids=None, conn=None, holdings_df=None,
                                    prices=None):
    """
    holdings_df: optional pre-computed holdings_timeseries rows (columns
    portfolio_id/security_id/date/market_value) for the caller to pass in
    directly. Needed when called from within recompute_holdings_timeseries
    on a connection that hasn't committed yet — get_holdings_timeseries()
    reads via a separate pooled engine connection, which can't see rows
    inserted-but-not-committed on `conn`, so without this the holdings
    join below would silently find nothing and skip every insert.

    prices: optional get_price_series() result, read by the caller before it
    started writing on `conn` (SQLite blocks other connections' reads then).
    """
    own = conn is None
    if own:
        conn = _raw_conn()
    try:
        if prices is None:
            sec = get_security_by_id(int(security_id))
            if not sec:
                return
            prices = get_price_series(sec["symbol"])
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

        df_hold = holdings_df if holdings_df is not None else get_holdings_timeseries()
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
                                  float(row["risk_score"]), float(mv), float(row["risk_score"])))

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


def get_security_risk_timeseries_many(security_ids) -> pd.DataFrame:
    """Risk rows for several securities in one query."""
    ids = [int(i) for i in security_ids]
    if not ids:
        return pd.DataFrame()
    ph = ", ".join([_ph()] * len(ids))
    df = _read_sql(
        f"SELECT * FROM security_risk_timeseries WHERE security_id IN ({ph}) ORDER BY date",
        tuple(ids),
    )
    df["security_id"] = df["security_id"].astype(int)
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    return df


def holdings_with_risk(df_hold: pd.DataFrame) -> pd.DataFrame:
    """Join each holdings row with its security's annualised volatility and weight it.

    weighted_risk = volatility x the row's share of the total market value on
    that date, so summing it per date gives the value-weighted volatility of
    the selection (an upper bound: correlation between holdings is ignored).
    """
    df_hold = df_hold.copy()
    df_hold["security_id"] = df_hold["security_id"].astype(int)
    df_hold["date"] = pd.to_datetime(df_hold["date"]).dt.tz_localize(None)
    df_risk = get_security_risk_timeseries_many(df_hold["security_id"].unique())
    if df_risk.empty:
        return pd.DataFrame()
    df_risk = df_risk[["date", "security_id", "risk_score"]]
    merged = pd.merge_asof(
        df_hold.sort_values("date"), df_risk.sort_values("date"),
        on="date", by="security_id",
        direction="backward", tolerance=pd.Timedelta("10D"),
    )
    merged = merged.dropna(subset=["risk_score"])
    merged = merged[merged["market_value"] > 0]
    total = merged.groupby("date")["market_value"].transform("sum")
    merged["weighted_risk"] = merged["risk_score"] * merged["market_value"] / total
    return merged.reset_index(drop=True)


def get_portfolio_risk_timeseries(portfolio_ids=None) -> pd.DataFrame:
    empty = pd.DataFrame(columns=["date", "weighted_risk"])
    df_hold = get_holdings_timeseries(portfolio_ids=portfolio_ids)
    if df_hold.empty:
        return empty
    df = holdings_with_risk(df_hold)
    if df.empty:
        return empty
    return df.groupby("date", as_index=False)["weighted_risk"].sum().sort_values("date")


def get_portfolio_risk_timeseries_detailed(portfolio_ids=None) -> pd.DataFrame:
    df_hold = get_holdings_timeseries(portfolio_ids=portfolio_ids)
    if df_hold.empty:
        return pd.DataFrame(columns=["date","portfolio_id","security_id","symbol",
                                      "sector","industry","security_type","market_value","weighted_risk"])
    return holdings_with_risk(df_hold)
