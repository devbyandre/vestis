import json
import logging
from typing import Optional, Dict, List
import pandas as pd

import db_utils as db
from .indicators import rsi
from .valuation import compute_dcf_cached


def get_alerts() -> pd.DataFrame:
    """Return ACTIVE alerts only — used by telegram worker for evaluation."""
    df = db.get_all_alerts(active_only=True)
    if df.empty:
        return pd.DataFrame(columns=[
            "id", "security_id", "alert_type", "params",
            "active", "notify_mode", "cooldown_seconds",
            "last_evaluated", "last_triggered", "note", "auto_managed"
        ])
    return df


def get_all_alerts_for_ui() -> pd.DataFrame:
    """Return ALL alerts including inactive — used by the UI management page."""
    df = db.get_all_alerts(active_only=False)
    if df.empty:
        return pd.DataFrame(columns=[
            "id", "security_id", "alert_type", "params",
            "active", "notify_mode", "cooldown_seconds",
            "last_evaluated", "last_triggered", "note", "auto_managed"
        ])
    return df

def get_automatic_alerts() -> pd.DataFrame:
    """Return only alerts that are automatically managed."""
    df = get_alerts()
    if 'auto_managed' not in df.columns:
        return pd.DataFrame(columns=df.columns)  # no automatic alerts if column missing
    return df[df['auto_managed'].isin([1, True])]

def create_alert(security_id: int, alert_type: str, params: dict,
                 notify_mode: str = "immediate", cooldown_seconds: int = 3600,
                 active: bool = True, note: str | None = None,
                 automatic: bool = False) -> int:
    """Create a new alert, optionally marking it as automatic."""
    params_json = json.dumps(params)
    return db.create_alert(
        security_id=security_id,
        alert_type=alert_type,
        params_json=params_json,
        notify_mode=notify_mode,
        cooldown_seconds=cooldown_seconds,
        active=active,
        note=note,
        auto_managed=automatic
    )


def edit_alert(alert_id: int, alert_type: str = None, params: dict = None,
               notify_mode: str = None, cooldown_seconds: int = None,
               active: bool = None, note: str = None, automatic=False) -> None:
    """
    Middleware function to update an alert.
    """
    db.update_alert(
        alert_id=alert_id,
        alert_type=alert_type,
        params=params,
        notify_mode=notify_mode,
        cooldown_seconds=cooldown_seconds,
        active=active,
        note=note,
        automatic=automatic
    )

def toggle_alert(alert_id: int, active: bool) -> None:
    """Activate or deactivate an alert."""
    db.toggle_alert_active(alert_id, active)


def delete_alert(alert_id: int) -> None:
    """Delete an alert."""
    db.delete_alert(alert_id)


def log_trigger(alert_id: int, payload: Dict) -> None:
    """Log alert trigger."""
    db.log_alert_trigger(alert_id, payload)


def last_trigger(alert_id: int) -> Optional[pd.Timestamp]:
    """Return last trigger time."""
    return db.last_trigger_time(alert_id)


def get_alert_log_entries(since_ts: str, notify_mode: str) -> List[Dict]:
    """Alerts logged since since_ts for a given notify_mode (used by digests)."""
    return db.get_alerts_for_digest(since_ts, notify_mode)



def fetch_symbol_data(symbol: str) -> dict:
    """
    Gather latest market data and indicators for a symbol.
    Returns a dict, or {} if no data available.

    Ensures all datetime values are tz-naive to avoid tz-aware/tz-naive comparison errors.
    """
    try:
        prices = db.get_price_history(symbol, lookback_days=400)
        if prices is None or prices.empty:
            logging.warning("fetch_symbol_data: no price history for %s", symbol)
            return {}

        # --- Ensure date column is timezone-naive (robust) ---
        prices = prices.copy()
        prices['date'] = pd.to_datetime(prices['date'], errors='coerce')

        # convert each Timestamp to a tz-naive python datetime to be 100% safe
        def _to_naive(ts):
            if pd.isna(ts):
                return pd.NaT
            # pd.Timestamp -> to_pydatetime, remove tzinfo
            try:
                py = pd.Timestamp(ts).to_pydatetime()
                return py.replace(tzinfo=None)
            except Exception:
                return pd.NaT

        prices['date'] = prices['date'].apply(_to_naive)
        prices = prices.sort_values('date').reset_index(drop=True)

        # --- last price & last volume ---
        last_row = prices.iloc[-1]
        last_price = None
        # prefer adj_close -> close
        for col in ('adj_close', 'close'):
            if col in last_row and pd.notna(last_row[col]):
                try:
                    last_price = float(last_row[col])
                    break
                except Exception:
                    pass
        if last_price is None:
            logging.warning("fetch_symbol_data: last price missing for %s", symbol)
            return {}

        last_vol = 0.0
        if 'volume' in last_row and pd.notna(last_row['volume']):
            try:
                last_vol = float(last_row['volume'])
            except Exception:
                last_vol = 0.0

        # --- 52-week high / low (use tz-naive cutoff) ---
        cutoff = pd.Timestamp(pd.Timestamp.utcnow().to_pydatetime().replace(tzinfo=None)) - pd.Timedelta(days=365)
        last_52w = prices[prices['date'] >= cutoff]
        high_52w = None
        low_52w = None
        if not last_52w.empty and 'adj_close' in last_52w.columns:
            adj_52 = pd.to_numeric(last_52w['adj_close'], errors='coerce').dropna()
            if not adj_52.empty:
                high_52w = float(adj_52.max())
                low_52w = float(adj_52.min())

        # --- Prepare adj_close series for indicators ---
        if 'adj_close' in prices.columns:
            adj = pd.to_numeric(prices['adj_close'], errors='coerce')
        elif 'close' in prices.columns:
            adj = pd.to_numeric(prices['close'], errors='coerce')
        else:
            adj = pd.Series(dtype=float)

        adj = adj.ffill().bfill()  # best-effort fill


        # --- SMA / EMA ---
        sma = {}
        ema = {}
        for win in (5, 10, 50, 100, 200):
            if len(adj) >= win:
                try:
                    sma_val = adj.rolling(window=win).mean().iloc[-1]
                    ema_val = adj.ewm(span=win, adjust=False).mean().iloc[-1]
                    sma[win] = float(sma_val) if pd.notna(sma_val) else None
                    ema[win] = float(ema_val) if pd.notna(ema_val) else None
                except Exception:
                    sma[win] = None
                    ema[win] = None
            else:
                sma[win] = None
                ema[win] = None

        # --- RSI (uses rsi() defined elsewhere in middleware) ---
        rsi_val = None
        try:
            if len(adj) >= 15:  # need at least window+1 for a meaningful RSI
                rsi_series = rsi(adj, window=14)
                if not rsi_series.empty and pd.notna(rsi_series.iloc[-1]):
                    rsi_val = float(rsi_series.iloc[-1])
        except Exception:
            logging.exception("fetch_symbol_data: RSI failed for %s", symbol)
            rsi_val = None

        # --- Volume averages ---
        avg_volume = {}
        if 'volume' in prices.columns:
            vol = pd.to_numeric(prices['volume'], errors='coerce').fillna(0.0)
            for look in (10, 20, 50):
                if len(vol) >= look:
                    try:
                        v = vol.rolling(window=look).mean().iloc[-1]
                        avg_volume[look] = float(v) if pd.notna(v) else None
                    except Exception:
                        avg_volume[look] = None
                else:
                    avg_volume[look] = None

        # --- Margin of safety (DCF-derived) if available ---
        mos_val = None
        try:
            dcf = compute_dcf_cached(symbol)
            mos_val = dcf.get("margin_of_safety")
        except Exception:
            logging.exception("fetch_symbol_data: DCF/mos lookup failed for %s", symbol)

        return {
            "last_price": last_price,
            "rsi": rsi_val,
            "sma": sma,
            "ema": ema,
            "52w_high": high_52w,
            "52w_low": low_52w,
            "volume": last_vol,
            "avg_volume": avg_volume,
            "mos": mos_val
        }

    except Exception:
        logging.exception("fetch_symbol_data failed for %s", symbol)
        return {}




def evaluate_alert(alert: dict) -> bool:
    symbol = alert["symbol"]
    a_type = alert["alert_type"]
    params = json.loads(alert.get("params") or "{}")

    try:
        data = fetch_symbol_data(symbol)
        if not data:
            logging.info("evaluate_alert: %s (%s) — skipped (no data)", symbol, a_type)
            return False

        result = False

        # Price alert — crossing detection only
        # Fires once when price crosses the threshold, not continuously while below/above.
        # Stores the last "side" in the alert log to detect transitions.
        if a_type == "price":
            lp = data.get("last_price")
            if lp is not None:
                threshold = float(params.get("threshold"))
                mode = params.get("mode", "absolute")
                direction = params.get("direction", "above")
                target = threshold if mode == "absolute" else lp * (1 + threshold)
                curr_side = "above" if lp > target else "below"
                # Get last recorded side from alert log
                last_log = db.get_last_alert_log(alert.get("id"))
                prev_side = (last_log.get("side") if last_log else None)
                # Fire only on transition to the alert direction
                if curr_side == direction and prev_side != direction:
                    result = True
                # Always update side so next run knows where we are
                alert["_curr_side"] = curr_side

        elif a_type == "rsi":
            rsi_val = data.get("rsi")
            if rsi_val is not None:
                thr = float(params.get("threshold", 70))
                direction = params.get("direction", "above")
                result = (direction == "above" and rsi_val > thr) or \
                         (direction == "below" and rsi_val < thr)

        elif a_type == "ma_crossover":
            # Only fire if the cross happened within the last `lookback` bars
            # (not just that fast MA is currently above slow MA — that would fire forever)
            short = int(params.get("short", 50))
            long_ = int(params.get("long", 200))
            crossover_type = params.get("crossover_type", params.get("direction", "golden"))
            lookback = int(params.get("lookback_bars", 3))
            try:
                prices = db.get_price_history(symbol, lookback_days=400)
                if prices is not None and not prices.empty and len(prices) > long_ + lookback:
                    if 'adj_close' in prices.columns:
                        adj = pd.to_numeric(prices['adj_close'], errors='coerce').ffill().bfill()
                    else:
                        adj = pd.to_numeric(prices['close'], errors='coerce').ffill().bfill()
                    fast = adj.rolling(short).mean()
                    slow = adj.rolling(long_).mean()
                    # Check if cross occurred in the last `lookback` bars
                    for i in range(-lookback, 0):
                        prev_fast = fast.iloc[i - 1]
                        prev_slow = slow.iloc[i - 1]
                        curr_fast = fast.iloc[i]
                        curr_slow = slow.iloc[i]
                        if pd.isna(prev_fast) or pd.isna(prev_slow):
                            continue
                        if crossover_type == "golden" and prev_fast <= prev_slow and curr_fast > curr_slow:
                            result = True
                            break
                        if crossover_type == "death" and prev_fast >= prev_slow and curr_fast < curr_slow:
                            result = True
                            break
            except Exception:
                logging.exception("evaluate_alert: ma_crossover failed for %s", symbol)

        elif a_type == "52w":
            # Only fire if the 52w high/low was broken in the last `lookback` bars
            typ = params.get("type", "high")
            lookback = int(params.get("lookback_bars", 3))
            try:
                prices = db.get_price_history(symbol, lookback_days=400)
                if prices is not None and not prices.empty and len(prices) > lookback + 1:
                    if 'adj_close' in prices.columns:
                        adj = pd.to_numeric(prices['adj_close'], errors='coerce').ffill().bfill()
                    else:
                        adj = pd.to_numeric(prices['close'], errors='coerce').ffill().bfill()
                    cutoff = pd.Timestamp.utcnow().replace(tzinfo=None) - pd.Timedelta(days=365)
                    prices_copy = prices.copy()
                    prices_copy['date'] = pd.to_datetime(prices_copy['date'], errors='coerce').apply(
                        lambda t: t.replace(tzinfo=None) if pd.notna(t) else pd.NaT)
                    for i in range(-lookback, 0):
                        curr_price = adj.iloc[i]
                        # compute 52w high/low up to bar i-1 (not including current bar)
                        hist = adj.iloc[:len(adj)+i]
                        hist_dates = prices_copy['date'].iloc[:len(adj)+i]
                        hist_52w = hist[hist_dates >= cutoff]
                        if hist_52w.empty:
                            continue
                        if typ == "high" and curr_price >= hist_52w.max():
                            result = True
                            break
                        elif typ == "low" and curr_price <= hist_52w.min():
                            result = True
                            break
            except Exception:
                logging.exception("evaluate_alert: 52w failed for %s", symbol)

        elif a_type == "volume_spike":
            mult = float(params.get("multiplier", 2.0))
            look = int(params.get("lookback", 20))
            vol = data.get("volume")
            avg = data.get("avg_volume", {}).get(look)
            if vol is not None and avg is not None and avg > 0:
                result = vol >= mult * avg

        elif a_type == "pct_change":
            # Fire when price has changed by more than pct% within the last `days` calendar days
            # e.g. {"pct": 5, "days": 1, "direction": "down"} = dropped >5% in last 24h
            pct = float(params.get("pct", 5)) / 100.0
            days = int(params.get("days", 1))
            direction = params.get("direction", "down")
            try:
                prices = db.get_price_history(symbol, lookback_days=days + 5)
                if prices is not None and not prices.empty:
                    if 'adj_close' in prices.columns:
                        adj = pd.to_numeric(prices['adj_close'], errors='coerce').ffill()
                    else:
                        adj = pd.to_numeric(prices['close'], errors='coerce').ffill()
                    prices_copy = prices.copy()
                    prices_copy['date'] = pd.to_datetime(prices_copy['date'], errors='coerce').apply(
                        lambda t: t.replace(tzinfo=None) if pd.notna(t) else pd.NaT)
                    cutoff = pd.Timestamp.utcnow().replace(tzinfo=None) - pd.Timedelta(days=days)
                    past = adj[prices_copy['date'] <= cutoff]
                    if not past.empty:
                        ref_price = float(past.iloc[-1])
                        curr_price = float(adj.iloc[-1])
                        change = (curr_price - ref_price) / ref_price
                        if direction == "down":
                            result = change <= -pct
                        else:
                            result = change >= pct
            except Exception:
                logging.exception("evaluate_alert: pct_change failed for %s", symbol)

        elif a_type == "earnings_soon":
            # Fire when earnings date is within `days` calendar days
            days = int(params.get("days", 3))
            try:
                cache = db.get_security_cache(alert.get("security_id"))
                if cache is not None:
                    ts = cache.get("earningsTimestamp")
                    if ts:
                        earnings_dt = pd.Timestamp(ts).replace(tzinfo=None)
                        now = pd.Timestamp.utcnow().replace(tzinfo=None)
                        diff = (earnings_dt - now).days
                        result = 0 <= diff <= days
            except Exception:
                logging.exception("evaluate_alert: earnings_soon failed for %s", symbol)

        elif a_type == "mos":
            mos_val = data.get("mos")
            thr = float(params.get("threshold_pct", 0.25))
            if mos_val is not None:
                result = mos_val >= thr

        # TODO: dividend/earnings alerts can be added with event calendar logic

        logging.info("evaluate_alert: %s (%s) => %s [params=%s]",
                     symbol, a_type, result, params)
        return result

    except Exception:
        logging.exception("evaluate_alert: error for %s (%s)", symbol, a_type)
        return False
