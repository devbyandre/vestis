import json
import logging
from typing import Optional, List
import pandas as pd

from .core import get_conn, _adapt_sql, _read_sql, _ph, to_iso_date
# Circular dependency: delete_transaction() below needs
# holdings.recompute_holdings_timeseries(), and holdings.py's
# recompute_holdings_timeseries() needs list_transactions_for_security()
# from this module. Import the whole module (not `from .holdings import
# recompute_holdings_timeseries`) so the attribute lookup happens at call
# time, after both modules have finished loading — mirrors the existing
# middleware.py <-> data_fetcher.py circular import pattern in this codebase.
from . import holdings as _holdings


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
               COALESCE(sc.longName, 'N/A') AS name
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
               COALESCE(sc.longName, NULL) AS security_name,
               t.date AS tx_date, t.type AS tx_type,
               t.quantity, t.price, t.fees AS tx_cost
        FROM transactions t
        LEFT JOIN portfolios p ON p.id = t.portfolio_id
        LEFT JOIN securities s ON s.id = t.security_id
        LEFT JOIN securities_cache sc ON sc.security_id = s.id
        ORDER BY t.date DESC
    """)

def get_first_buy_date(security_id: int):
    """Return earliest buy date for this security across all portfolios, or None."""
    df = _read_sql(
        "SELECT MIN(date) as d FROM transactions WHERE security_id=? AND type='buy'",
        (security_id,)
    )
    if df.empty or pd.isna(df.iloc[0]['d']):
        return None
    return str(df.iloc[0]['d'])[:10]


def get_recorded_splits(security_id: int) -> list:
    """Return list of split dates already recorded as transactions for this security."""
    df = _read_sql(
        "SELECT date FROM transactions WHERE security_id=? AND type='split' ORDER BY date",
        (security_id,)
    )
    if df.empty:
        return []
    return df['date'].tolist()


def store_split_alert(security_id: int, split_date: str, ratio: float) -> None:
    """
    Store an unrecorded split alert so the Telegram worker can notify the user.
    Uses the alerts_log table with a special alert_type marker.
    Creates a placeholder alert if needed.
    """
    # Check if we already have an active split alert for this security/date
    existing = _read_sql(
        "SELECT id FROM alerts WHERE security_id=? AND alert_type='split_pending' AND active=1",
        (security_id,)
    )
    if existing.empty:
        # Create a split_pending alert (not subject to normal cooldown — fires until resolved)
        sql = _adapt_sql("""
            INSERT INTO alerts (security_id, alert_type, params, active, notify_mode, cooldown_seconds, auto_managed)
            VALUES (?, 'split_pending', ?, 1, 'immediate', 3600, 1)
        """)
        with get_conn() as conn:
            conn.cursor().execute(sql, (
                security_id,
                json.dumps({"split_date": split_date, "ratio": ratio})
            ))
            conn.commit()
        logging.info("Created split_pending alert for security_id=%s date=%s ratio=%.4f",
                     security_id, split_date, ratio)


def clear_split_alert(security_id: int) -> None:
    """Deactivate all split_pending alerts for a security (called after user records the split)."""
    sql = _adapt_sql("UPDATE alerts SET active=0 WHERE security_id=? AND alert_type='split_pending'")
    with get_conn() as conn:
        conn.cursor().execute(sql, (security_id,))
        conn.commit()


def list_transactions_for_security_any_portfolio(security_id: int) -> pd.DataFrame:
    """Return all transactions for a security across all portfolios."""
    return _read_sql(
        "SELECT * FROM transactions WHERE security_id=? ORDER BY date",
        (security_id,)
    )


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
        _holdings.recompute_holdings_timeseries(portfolio_id, security_id, conn)
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
