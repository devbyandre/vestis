import pandas as pd

import db_utils as db


def get_watchlist()-> pd.DataFrame:
    """
    Add derived metrics, scores, and temperature labels to the raw watchlist.
    """
    df = db.get_watchlist()

    return calc_security_KPIs(df)

def calc_security_KPIs(df: pd.DataFrame)-> pd.DataFrame:
    """
    Add derived metrics, scores, and temperature labels to the raw watchlist.
    """
    df = df.copy()

    # Derived metrics
    df['pb_ratio'] = df.apply(lambda r: r['regularMarketPrice']/r['bookValue'] if r['bookValue'] else None, axis=1)
    df['from_52w_low'] = df.apply(
        lambda r: (r['regularMarketPrice'] - r['fiftyTwoWeekLow']) /
                  (r['fiftyTwoWeekHigh'] - r['fiftyTwoWeekLow'])
        if r['fiftyTwoWeekHigh'] and r['fiftyTwoWeekLow'] and r['fiftyTwoWeekHigh'] != r['fiftyTwoWeekLow'] else None,
        axis=1
    )

    # Scoring functions
    def score_beta(b): return 1.0 if b is not None and b < 1 else (0.5 if b is not None and b <= 1.2 else 0)
    def score_pe(pe): return 1.0 if pe is not None and pe < 15 else (0.5 if pe is not None and pe <= 30 else 0)
    def score_pb(pb): return 1.0 if pb is not None and pb < 1.5 else (0.5 if pb is not None and pb <= 3 else 0)
    def score_div(y): return 1.0 if y is not None and y > 0.03 else (0.5 if y is not None and y > 0.01 else 0)
    def score_52w(low_pct): return 1.0 if low_pct is not None and low_pct < 0.2 else (0.5 if low_pct is not None and low_pct <= 0.8 else 0)
    def score_profit(pm): return 1.0 if pm is not None and pm > 0.2 else (0.5 if pm is not None and pm > 0.1 else 0)

    df['beta_score'] = df['beta'].apply(score_beta)
    df['pe_score'] = df['trailingPE'].apply(score_pe)
    df['pb_score'] = df['pb_ratio'].apply(score_pb)
    df['div_yield_score'] = df['dividendYield'].apply(score_div)
    df['from_52w_low_score'] = df['from_52w_low'].apply(score_52w)
    df['profit_margin_score'] = df['profitMargins'].apply(score_profit)

    # Overall temperature
    df['temperature_score'] = df[[
        'beta_score', 'pe_score', 'pb_score', 'div_yield_score', 'from_52w_low_score', 'profit_margin_score'
    ]].mean(axis=1)

    def temperature_label(score):
        if score is None:
            return "N/A"
        elif score <= 0.33:
            return "Cold"
        elif score <= 0.66:
            return "Warm"
        else:
            return "Hot"

    df['Temperature'] = df['temperature_score'].apply(temperature_label)
    return df


def delete_security_from_watchlist(symbol: str)-> None:
    db.delete_watchlist_item(symbol)


def get_watchlist_symbols() -> list:
    """Return all symbols currently on the watchlist."""
    df = get_watchlist()
    return df['symbol'].dropna().tolist() if not df.empty else []
