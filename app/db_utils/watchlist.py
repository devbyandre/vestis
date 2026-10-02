from typing import Dict
import pandas as pd

from .core import get_conn, _read_sql
from .securities import get_security_id
from .transactions import delete_security_if_no_transactions


def get_watchlist() -> pd.DataFrame:
    monetary_fields = [
        "regularMarketPrice", "fiftyTwoWeekHigh", "fiftyTwoWeekLow",
        "marketCap", "enterpriseValue", "totalRevenue", "revenuePerShare",
        "grossProfits", "ebitda", "totalCash", "totalDebt",
        "operatingCashflow", "freeCashflow", "bookValue", "dividendRate",
    ]
    df = _read_sql("""
        SELECT s.id AS security_id, s.yahoo_ticker AS symbol,
               sc.longName AS security_name, sc.shortName AS "shortName",
               sc.security_type, sc.country, sc.exchange,
               sc.sector, sc.industry, sc.currency,
               sc.regularMarketPrice AS "regularMarketPrice",
               sc.fiftyTwoWeekHigh AS "fiftyTwoWeekHigh",
               sc.fiftyTwoWeekLow AS "fiftyTwoWeekLow",
               sc.volume, sc.averageVolume AS "averageVolume",
               sc.marketCap AS "marketCap", sc.beta,
               sc.trailingPE AS "trailingPE", sc.forwardPE AS "forwardPE",
               sc.trailingEps AS eps,
               sc.earningsTimestamp AS earnings_date,
               sc.dividendRate AS "dividendRate", sc.dividendYield AS "dividendYield",
               sc.enterpriseValue AS "enterpriseValue",
               sc.profitMargins AS "profitMargins", sc.operatingMargins AS "operatingMargins",
               sc.returnOnAssets AS "returnOnAssets", sc.returnOnEquity AS "returnOnEquity",
               sc.totalRevenue AS "totalRevenue", sc.revenuePerShare AS "revenuePerShare",
               sc.grossProfits AS "grossProfits", sc.ebitda,
               sc.totalCash AS "totalCash", sc.totalDebt AS "totalDebt",
               sc.currentRatio AS "currentRatio", sc.bookValue AS "bookValue",
               sc.operatingCashflow AS "operatingCashflow",
               sc.freeCashflow AS "freeCashflow", sc.sharesOutstanding AS "sharesOutstanding"
        FROM securities s
        LEFT JOIN securities_cache sc ON sc.security_id = s.id
        WHERE s.id NOT IN (SELECT DISTINCT t.security_id FROM transactions t)
        ORDER BY s.yahoo_ticker
    """)

    currencies = df["currency"].dropna().unique() if "currency" in df.columns else []
    fx_map: Dict[str, float] = {}
    for cur in currencies:
        if cur.upper() == "EUR":
            fx_map[cur] = 1.0
        else:
            r = _read_sql(
                "SELECT rate FROM fx_rates WHERE base_currency=? AND target_currency='EUR' ORDER BY date DESC LIMIT 1",
                (cur.upper(),),
            )
            fx_map[cur] = float(r.iloc[0]["rate"]) if not r.empty else 1.0

    for field in monetary_fields:
        if field in df.columns:
            df[field] = df.apply(
                lambda row: row[field] * fx_map.get(row["currency"], 1.0)
                if pd.notna(row[field]) else None,
                axis=1,
            )
    return df


def delete_watchlist_item(symbol: str) -> None:
    sec_id = get_security_id(symbol)
    if not sec_id:
        return
    with get_conn() as conn:
        delete_security_if_no_transactions(sec_id, conn)
        conn.commit()
