from typing import Optional, Dict, Any
import pandas as pd

from .core import get_conn, _adapt_sql, _read_sql


def get_portfolio_by_name(name: str) -> Optional[dict]:
    df = _read_sql("SELECT id, name FROM portfolios WHERE name=?", (name,))
    return df.iloc[0].to_dict() if not df.empty else None


def list_portfolios() -> pd.DataFrame:
    return _read_sql("SELECT id, name FROM portfolios ORDER BY name")


def insert_portfolio(name: str) -> int:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(_adapt_sql("INSERT INTO portfolios (name) VALUES (?)"), (name,))
        conn.commit()
    return get_portfolio_by_name(name)["id"]


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
