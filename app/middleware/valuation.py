import json
import pandas as pd

import db_utils as db
from config_utils import get_config
from .securities import get_security_basic


def extract_fcf_from_cashflow_payloads(symbol: str, lookback_years: int = 8) -> pd.Series:
    """
    Extract Free Cash Flow (FCF) from cashflow financial statements.
    Returns a pandas Series indexed by year (most recent first).
    """
    df = db.get_cashflow_payloads(symbol)
    if df.empty:
        return pd.Series(dtype=float)

    rows = []

    for _, r in df.iterrows():
        try:
            asof = r['as_of_date']
            payload = json.loads(r['payload']) if isinstance(r['payload'], str) else r['payload']
            fcf = None

            if isinstance(payload, dict):
                # Direct freeCashflow
                for k, v in payload.items():
                    if 'free cash' in k.lower() or 'freecash' in k.lower():
                        try:
                            fcf = float(v)
                            break
                        except Exception:
                            continue

                # If not found, compute FCF = Operating Cashflow - CapEx
                if fcf is None:
                    oc = None
                    capex = None
                    for k, v in payload.items():
                        kl = k.lower()
                        if 'operat' in kl and 'cash' in kl:
                            try: oc = float(v)
                            except Exception: oc = None
                        if 'capitalexpend' in kl or 'capex' in kl:
                            try: capex = float(v)
                            except Exception: capex = None
                    if oc is not None:
                        fcf = oc - abs(capex) if capex is not None else oc

            if fcf is not None:
                year = pd.to_datetime(asof).year
                rows.append((year, float(fcf)))

        except Exception:
            continue

    if not rows:
        return pd.Series(dtype=float)

    # Keep only the most recent FCF per year
    seen = {}
    for y, v in rows:
        if y not in seen:
            seen[y] = v

    ser = pd.Series({y: seen[y] for y in sorted(seen.keys(), reverse=True)})
    if len(ser) > lookback_years:
        ser = ser.iloc[:lookback_years]

    return ser


def compute_dcf_raw(symbol):
    projection_years = int(get_config('dcf_projection_years'))
    discount_rate = float(get_config('dcf_discount_rate'))
    terminal_growth = float(get_config('dcf_terminal_growth'))
    conservative = bool(get_config('dcf_conservative'))

    # Skip financials / REITs
    try:
        meta = db.list_securities_metadata()
        sector = meta['sector'] if meta else None
        if sector and any(x in sector.lower() for x in ['bank','financial','insurance','real estate','reit']):
            return {'note': 'DCF not meaningful for financial / REIT business models', 'intrinsic_per_share': None}
    except Exception:
        pass

    fcf_series = extract_fcf_from_cashflow_payloads(symbol, lookback_years=8)
    if fcf_series.empty:
        return {'note': 'Insufficient FCF data for DCF', 'intrinsic_per_share': None}

    # Median growth
    growth_rates = fcf_series.pct_change().dropna()
    g = float(growth_rates.median()) if not growth_rates.empty else 0.05
    if conservative:
        g *= 0.6

    # Project FCF
    last_fcf = float(fcf_series.iloc[-1])
    projected = []
    f = last_fcf
    for _ in range(projection_years):
        f *= (1 + g)
        projected.append(f)

    # Terminal value
    terminal = projected[-1] * (1 + terminal_growth) / (discount_rate - terminal_growth) if discount_rate > terminal_growth else 0.0

    # Present value
    pv = sum(cf / ((1 + discount_rate) ** (i + 1)) for i, cf in enumerate(projected))
    pv_terminal = terminal / ((1 + discount_rate) ** projection_years)
    total_pv = pv + pv_terminal

    # Shares & market price
    so = get_security_basic(symbol).get('sharesOutstanding')
    market_price = db.get_latest_price(symbol)
    intrinsic_per_share = total_pv / so if so and so > 0 else None

    # Margin of safety
    margin_of_safety = None
    rating = 'N/A'
    if intrinsic_per_share and market_price:
        margin_of_safety = (intrinsic_per_share - market_price) / intrinsic_per_share
        mos_pct = margin_of_safety * 100
        if mos_pct >= 20:
            rating = 'Buy'
        elif mos_pct >= 0:
            rating = 'Hold'
        else:
            rating = 'Sell'

    return {
        'intrinsic_per_share': intrinsic_per_share,
        'total_pv': total_pv,
        'terminal_value': terminal,
        'shares_outstanding': so,
        'market_price': market_price,
        'margin_of_safety': margin_of_safety,
        'rating': rating,
        'note': None
    }


def compute_dcf_cached(symbol: str, assumptions: dict = {}, max_age_hours: float = 24, force_refresh: bool = False):
    """
    Compute DCF for a security, using cached values if available and valid.
    """
    # Try fetching cached DCF
    cached = db.get_cached_dcf(symbol)
    if cached and not force_refresh:
        try:
            computed_at = pd.to_datetime(cached['valuation_computed_at'])
            age_hours = (pd.Timestamp.utcnow() - computed_at).total_seconds() / 3600.0
            cached_assumptions = json.loads(cached['assumptions']) if cached.get('assumptions') else {}
            if age_hours < max_age_hours and cached_assumptions == assumptions:
                cached['cached'] = True
                cached['computed_at'] = str(cached['valuation_computed_at'])
                return cached
        except Exception:
            pass

    # Compute raw DCF
    res = compute_dcf_raw(symbol)

    # Get security_id
    security_id = db.get_security_id(symbol)
    if not security_id:
        raise ValueError(f"Security {symbol} not found in DB")

    # Store in DB if DCF is valid
    if res.get('intrinsic_per_share') is not None:
        db.store_dcf(security_id, res)

    res['cached'] = False
    res['computed_at'] = pd.Timestamp.utcnow().isoformat()
    return res
