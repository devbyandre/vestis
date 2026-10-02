from typing import Optional, List
import pandas as pd

from .core import get_conn, _adapt_sql, _read_sql, _IS_POSTGRES


def get_security_id(symbol: str, conn=None) -> Optional[int]:
    df = _read_sql("SELECT id FROM securities WHERE yahoo_ticker=?", (symbol,))
    return int(df.iloc[0]["id"]) if not df.empty else None


def list_securities() -> pd.DataFrame:
    return _read_sql("""
        SELECT s.id,
               s.yahoo_ticker AS symbol,
               COALESCE(sc.longName, sc.shortName) AS name,
               s.isin
        FROM securities s
        LEFT JOIN securities_cache sc ON sc.security_id = s.id
        ORDER BY name
    """)


def get_all_symbols() -> List[str]:
    df = _read_sql("""
        SELECT yahoo_ticker AS symbol FROM securities
        UNION
        SELECT DISTINCT s.yahoo_ticker AS symbol
        FROM transactions t
        JOIN securities s ON s.id = t.security_id
        WHERE s.yahoo_ticker IS NOT NULL
    """)
    return sorted(df["symbol"].dropna().unique())


def insert_security(symbol: str) -> int:
    if _IS_POSTGRES:
        sql = _adapt_sql("""
            INSERT INTO securities (yahoo_ticker) VALUES (?)
            ON CONFLICT (yahoo_ticker) DO NOTHING
        """)
    else:
        sql = "INSERT OR IGNORE INTO securities (yahoo_ticker) VALUES (?)"
    with get_conn() as conn:
        conn.cursor().execute(sql, (symbol,))
        conn.commit()
    return get_security_id(symbol)


def get_security_by_symbol(symbol: str) -> Optional[dict]:
    df = _read_sql("SELECT * FROM securities WHERE yahoo_ticker=?", (symbol,))
    return df.iloc[0].to_dict() if not df.empty else None


def get_security_by_id(sec_id: int) -> Optional[dict]:
    df = _read_sql(
        "SELECT id, yahoo_ticker AS symbol, isin FROM securities WHERE id=?", (sec_id,)
    )
    return df.iloc[0].to_dict() if not df.empty else None


def list_securities_metadata() -> pd.DataFrame:
    return _read_sql("""
        SELECT s.id,
               s.yahoo_ticker AS symbol,
               sc.longName AS name,
               sc.security_type,
               sc.sector,
               sc.industry,
               sc.country,
               sc.exchange
        FROM securities s
        LEFT JOIN securities_cache sc ON sc.security_id = s.id
    """)


def update_security(sec_id: int, name: Optional[str] = None, isin: Optional[str] = None):
    with get_conn() as conn:
        cur = conn.cursor()
        if isin is not None:
            cur.execute(_adapt_sql("UPDATE securities SET isin=? WHERE id=?"), (isin, sec_id))
        if name is not None:
            cur.execute(
                _adapt_sql("""UPDATE securities_cache SET longName=? WHERE security_id=?"""),
                (name, sec_id),
            )
        conn.commit()
