import json
import logging
from typing import Optional, Dict, List
import pandas as pd

import db_utils as db
from .indicators import rsi
from .security_cache import get_security_cache_row, earnings_date
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


def dedupe_auto_alerts() -> int:
    return db.dedupe_auto_alerts()


def log_trigger(alert_id: int, payload: Dict) -> None:
    """Log alert trigger."""
    db.log_alert_trigger(alert_id, payload)


def last_trigger(alert_id: int) -> Optional[pd.Timestamp]:
    """Return last trigger time."""
    return db.last_trigger_time(alert_id)


def get_alert_log_entries(since_ts: str, notify_mode: str) -> List[Dict]:
    """Alerts logged since since_ts for a given notify_mode (used by digests)."""
    return db.get_alerts_for_digest(since_ts, notify_mode)



def get_alert_history(limit: int = 200, alert_id: Optional[int] = None) -> List[Dict]:
    """Recent triggers for the UI; payload JSON unpacked into delivery/detail."""
    df = db.get_alert_history(limit=limit, alert_id=alert_id)
    rows = []
    for r in df.to_dict("records"):
        try:
            payload = json.loads(r.pop("payload") or "{}")
        except Exception:
            payload = {}
        note = str(payload.get("note") or "")
        r["delivery"] = ("digest" if note.startswith("digest")
                         else "held" if note == "held" else "immediate")
        r["detail"] = payload.get("detail")
        rows.append(r)
    return rows


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




# ── Alert evaluation ────────────────────────────────────────────────────────
#
# Every alert is edge-triggered: it fires when a condition *starts* (a cross,
# a new move on a new trading day), not on every run while the condition
# holds. Per-alert state lives in alerts.state (JSON) and is returned to the
# caller as alert["_state"] for persisting; alert["_detail"] carries the
# actual values that triggered it, for the notification text.
#
# Technical signals (RSI, MA cross, % move, 52w, volume) are computed on raw
# trading-day bars in the security's native currency (db.get_price_bars).
# Price-threshold alerts stay on the EUR series, since thresholds like
# trailing stops and buy-price recovery are set in EUR.

_BARS_CACHE: Dict[str, tuple] = {}
_BARS_TTL_SEC = 120


def _bars(symbol: str) -> pd.DataFrame:
    """Native-currency trading-day bars, cached briefly so one worker run
    reads each symbol once no matter how many alerts it has."""
    now = pd.Timestamp.utcnow()
    hit = _BARS_CACHE.get(symbol)
    if hit and (now - hit[0]).total_seconds() < _BARS_TTL_SEC:
        return hit[1]
    df = db.get_price_bars(symbol, lookback_days=550)
    if df is None:
        df = pd.DataFrame()
    _BARS_CACHE[symbol] = (now, df)
    return df


def _load_state(alert: dict) -> dict:
    raw = alert.get("state")
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            v = json.loads(raw)
            return v if isinstance(v, dict) else {}
        except Exception:
            return {}
    return {}


def _currency(alert: dict) -> str:
    try:
        cache = get_security_cache_row(alert.get("security_id"))
        return str(cache.get("currency") or "")
    except Exception:
        return ""


def _hysteresis_regimes(spread: pd.Series, band: float) -> List[Optional[str]]:
    """'above'/'below' regime per bar; only changes once spread clears ±band,
    so MAs hugging each other don't flip the regime back and forth."""
    out, reg = [], None
    for v in spread:
        if pd.notna(v):
            if v >= band:
                reg = "above"
            elif v <= -band:
                reg = "below"
        out.append(reg)
    return out


def evaluate_alert(alert: dict) -> bool:
    symbol = alert["symbol"]
    a_type = alert["alert_type"]
    params = json.loads(alert.get("params") or "{}")
    state = _load_state(alert)
    new_state = dict(state)
    detail = ""
    result = False

    try:
        if a_type in ("price", "mos"):
            data = fetch_symbol_data(symbol)
            if not data:
                logging.info("evaluate_alert: %s (%s) — skipped (no data)", symbol, a_type)
                return False
        else:
            bars = _bars(symbol)
            if bars.empty:
                logging.info("evaluate_alert: %s (%s) — skipped (no bars)", symbol, a_type)
                return False
            price = bars["price"].reset_index(drop=True)
            bar_dates = bars["date"].dt.strftime("%Y-%m-%d").reset_index(drop=True)
            last_bar = bar_dates.iloc[-1]
            ccy = _currency(alert)

        # Price threshold — fires on the transition to the alert side.
        if a_type == "price":
            lp = data.get("last_price")
            if lp is not None:
                threshold = float(params.get("threshold"))
                mode = params.get("mode", "absolute")
                direction = params.get("direction", "above")
                target = threshold if mode == "absolute" else lp * (1 + threshold)
                curr_side = "above" if lp > target else "below"
                prev_side = state.get("side")
                if prev_side is None:
                    # Alerts from before alerts.state existed kept their side in the log
                    last_log = db.get_last_alert_log(alert.get("id"))
                    prev_side = last_log.get("side") if last_log else None
                result = curr_side == direction and prev_side != direction
                new_state["side"] = curr_side
                alert["_curr_side"] = curr_side
                detail = f"price €{lp:.2f} vs threshold €{target:.2f}"

        # RSI — fires on entering the zone, re-arms after leaving it by `rearm_gap`.
        elif a_type == "rsi":
            if len(price) >= 30:
                rsi_val = float(rsi(price, window=14).iloc[-1])
                thr = float(params.get("threshold", 70))
                gap = float(params.get("rearm_gap", 5))
                direction = params.get("direction", "above")
                if direction == "above":
                    in_zone, rearm = rsi_val > thr, rsi_val < thr - gap
                else:
                    in_zone, rearm = rsi_val < thr, rsi_val > thr + gap
                armed = state.get("armed", True)
                if armed and in_zone:
                    result, armed = True, False
                elif not armed and rearm:
                    armed = True
                new_state["armed"] = armed
                detail = f"RSI(14) {rsi_val:.1f} (threshold {thr:g})"

        # MA crossover — regime change with a min spread, each cross reported once.
        elif a_type == "ma_crossover":
            short = int(params.get("short", 50))
            long_ = int(params.get("long", 200))
            crossover_type = params.get("crossover_type", params.get("direction", "golden"))
            lookback = int(params.get("lookback_bars", 3))
            band = float(params.get("min_spread_pct", 0.5)) / 100.0
            if len(price) > long_ + lookback:
                fast = price.rolling(short).mean()
                slow = price.rolling(long_).mean()
                spread = fast / slow - 1
                regimes = _hysteresis_regimes(spread, band)
                want, other = ("above", "below") if crossover_type == "golden" else ("below", "above")
                event = None
                for i in range(len(regimes) - lookback, len(regimes)):
                    if regimes[i] == want and regimes[i - 1] == other:
                        event = bar_dates.iloc[i]
                if event and state.get("last_event") != event:
                    result = True
                    new_state["last_event"] = event
                detail = (f"SMA{short} {fast.iloc[-1]:.2f} vs SMA{long_} {slow.iloc[-1]:.2f} {ccy}"
                          f" (spread {spread.iloc[-1] * 100:+.2f}%)").replace("  ", " ")

        # 52-week high/low — once per new extreme bar.
        elif a_type == "52w":
            typ = params.get("type", "high")
            window = price.iloc[-253:-1]  # prior ~52 weeks of trading days
            if len(window) >= 50:
                curr = float(price.iloc[-1])
                hit = curr >= window.max() if typ == "high" else curr <= window.min()
                if hit and state.get("last_event") != last_bar:
                    result = True
                    new_state["last_event"] = last_bar
                ref = window.max() if typ == "high" else window.min()
                detail = f"{curr:.2f} {ccy} vs prior 52w {typ} {ref:.2f}".replace("  ", " ")

        # Volume spike — latest bar vs average of the preceding bars, once per bar.
        elif a_type == "volume_spike":
            mult = float(params.get("multiplier", 2.0))
            look = int(params.get("lookback", 20))
            vol = bars["volume"].reset_index(drop=True)
            if len(vol) > look:
                avg = float(vol.iloc[-1 - look:-1].mean())
                curr = float(vol.iloc[-1])
                if avg > 0 and curr >= mult * avg and state.get("last_event") != last_bar:
                    result = True
                    new_state["last_event"] = last_bar
                if avg > 0:
                    detail = f"volume {curr / avg:.1f}× the {look}-day average"

        # % move over `days` trading days — once per trading day.
        elif a_type == "pct_change":
            pct = float(params.get("pct", 5)) / 100.0
            days = int(params.get("days", 1))
            direction = params.get("direction", "down")
            if len(price) > days:
                curr = float(price.iloc[-1])
                ref = float(price.iloc[-1 - days])
                change = (curr - ref) / ref
                hit = change <= -pct if direction == "down" else change >= pct
                if hit and state.get("last_event") != last_bar:
                    result = True
                    new_state["last_event"] = last_bar
                detail = (f"{change * 100:+.1f}% ({ref:.2f} → {curr:.2f} {ccy}, "
                          f"bar {last_bar})").replace(" ,", ",")

        # Earnings within `days` — once per earnings date.
        elif a_type == "earnings_soon":
            days = int(params.get("days", 3))
            earnings_dt = earnings_date(get_security_cache_row(alert.get("security_id")))
            if earnings_dt is not None:
                now = pd.Timestamp.utcnow().replace(tzinfo=None)
                diff = (earnings_dt - now).days
                event = earnings_dt.strftime("%Y-%m-%d")
                if 0 <= diff <= days and state.get("last_event") != event:
                    result = True
                    new_state["last_event"] = event
                detail = f"earnings on {event}"

        # Margin of safety — fires on reaching the threshold, re-arms 5pp below it.
        elif a_type == "mos":
            mos_val = data.get("mos")
            thr = float(params.get("threshold_pct", 0.25))
            if mos_val is not None:
                armed = state.get("armed", True)
                if armed and mos_val >= thr:
                    result, armed = True, False
                elif not armed and mos_val < thr - 0.05:
                    armed = True
                new_state["armed"] = armed
                detail = f"margin of safety {mos_val * 100:.0f}%"

        alert["_state"] = new_state
        alert["_detail"] = detail
        logging.info("evaluate_alert: %s (%s) => %s [%s] [params=%s]",
                     symbol, a_type, result, detail, params)
        return result

    except Exception:
        logging.exception("evaluate_alert: error for %s (%s)", symbol, a_type)
        return False


def save_alert_state(alert: dict) -> None:
    """Persist the state evaluate_alert computed (no-op if it didn't run)."""
    if alert.get("id") is not None and alert.get("_state") is not None:
        db.set_alert_state(int(alert["id"]), alert["_state"])
