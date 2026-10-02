from typing import Optional
import pandas as pd

from .core import get_conn, _adapt_sql, _read_sql, _IS_POSTGRES, to_iso_ts


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
