"""Validation of user-supplied alert parameters (pure, no DB access)."""

_DIRECTIONS = {
    "price": ("above", "below"),
    "rsi": ("above", "below"),
    "pct_change": ("up", "down"),
}


def _num(params, key, default, lo=None, hi=None, integer=False, lo_open=False):
    raw = params.get(key, default)
    if raw is None or raw == "":
        raise ValueError(f"'{key}' is required")
    try:
        val = float(raw)
    except (TypeError, ValueError):
        raise ValueError(f"'{key}' must be a number")
    if integer:
        if val != int(val):
            raise ValueError(f"'{key}' must be a whole number")
        val = int(val)
    if lo is not None and (val <= lo if lo_open else val < lo):
        raise ValueError(f"'{key}' must be {'>' if lo_open else '>='} {lo:g}")
    if hi is not None and val > hi:
        raise ValueError(f"'{key}' must be <= {hi:g}")
    return val


def _choice(params, key, default, options):
    val = params.get(key, default)
    if val not in options:
        raise ValueError(f"'{key}' must be one of: {', '.join(options)}")
    return val


ALERT_TYPES = ("price", "rsi", "ma_crossover", "52w", "volume_spike",
               "pct_change", "earnings_soon", "mos")


def validate_alert_params(alert_type: str, params: dict) -> dict:
    """Return params with known keys type-checked and coerced.

    Unknown keys are kept (advanced settings such as rearm_gap).
    Raises ValueError with a user-readable message.
    """
    if alert_type not in ALERT_TYPES:
        raise ValueError(f"Unknown alert type '{alert_type}'")
    p = dict(params or {})

    if alert_type == "price":
        p["threshold"] = _num(p, "threshold", None, lo=0, lo_open=True) if p.get("mode", "absolute") == "absolute" \
            else _num(p, "threshold", None)
        p["direction"] = _choice(p, "direction", "above", _DIRECTIONS["price"])
    elif alert_type == "rsi":
        p["threshold"] = _num(p, "threshold", 70, lo=1, hi=99)
        p["direction"] = _choice(p, "direction", "above", _DIRECTIONS["rsi"])
    elif alert_type == "ma_crossover":
        p["short"] = _num(p, "short", 50, lo=2, integer=True)
        p["long"] = _num(p, "long", 200, lo=3, integer=True)
        if p["short"] >= p["long"]:
            raise ValueError("'short' must be smaller than 'long'")
        p["crossover_type"] = _choice(p, "crossover_type", "golden", ("golden", "death"))
    elif alert_type == "52w":
        p["type"] = _choice(p, "type", "high", ("high", "low"))
    elif alert_type == "volume_spike":
        p["multiplier"] = _num(p, "multiplier", 2.0, lo=1, lo_open=True)
        p["lookback"] = _num(p, "lookback", 20, lo=5, hi=250, integer=True)
    elif alert_type == "pct_change":
        p["pct"] = _num(p, "pct", 5, lo=0, hi=100, lo_open=True)
        p["direction"] = _choice(p, "direction", "down", _DIRECTIONS["pct_change"])
        p["days"] = _num(p, "days", 1, lo=1, hi=60, integer=True)
    elif alert_type == "earnings_soon":
        p["days"] = _num(p, "days", 3, lo=1, hi=60, integer=True)
    elif alert_type == "mos":
        p["threshold_pct"] = _num(p, "threshold_pct", 0.25, lo=0, hi=1, lo_open=True)
    return p
