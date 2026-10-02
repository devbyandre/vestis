"""Shared JSON-serialisation helpers used by every router."""
import pandas as pd
import numpy as np


def _df(df: pd.DataFrame) -> list:
    """Convert DataFrame to JSON-serialisable list, handling NaN/Inf/Timestamps."""
    if df is None or df.empty:
        return []
    df = df.copy()
    for col in df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns:
        df[col] = df[col].astype(str)
    df = df.where(pd.notnull(df), None)
    records = df.to_dict(orient="records")
    def clean(v):
        if isinstance(v, float) and (v != v or v == float("inf") or v == float("-inf")):
            return None
        if isinstance(v, (np.integer,)):
            return int(v)
        if isinstance(v, (np.floating,)):
            return float(v) if (v == v and v != float("inf") and v != float("-inf")) else None
        if isinstance(v, pd.Timestamp):
            return str(v)
        return v
    return [{k: clean(val) for k, val in row.items()} for row in records]


def _safe_float(v):
    try:
        if v is None:
            return None
        f = float(v)
        if f != f or f in (float("inf"), float("-inf")):
            return None
        return f
    except Exception:
        return None
