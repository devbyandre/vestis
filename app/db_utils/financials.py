import json
import pandas as pd

from .core import get_conn, _adapt_sql, _IS_POSTGRES, to_iso_date, to_iso_ts


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
