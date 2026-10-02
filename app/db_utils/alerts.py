import json
from typing import Optional, List, Dict
import pandas as pd

from .core import get_conn, _adapt_sql, _read_sql, _IS_POSTGRES, _ph


def get_all_alerts(active_only: bool = True) -> pd.DataFrame:
    """Return alerts. active_only=True (default) returns only active alerts.
    Pass active_only=False to get all including inactive (for UI management)."""
    where = "WHERE a.active=1" if active_only else ""
    return _read_sql(f"""
        SELECT a.id, a.security_id,
               COALESCE(sc.longName, sc.shortName) AS security_name,
               s.yahoo_ticker AS symbol,
               a.alert_type, a.params, a.active, a.notify_mode,
               a.cooldown_seconds, a.last_evaluated, a.last_triggered,
               a.note, a.auto_managed, a.state
        FROM alerts a
        LEFT JOIN securities_cache sc ON sc.security_id = a.security_id
        LEFT JOIN securities s ON s.id = a.security_id
        {where}
        ORDER BY a.id DESC
    """)


def create_alert(security_id, alert_type, params_json, notify_mode,
                 cooldown_seconds, active=True, note=None, auto_managed=False) -> int:
    sql = _adapt_sql("""
        INSERT INTO alerts(security_id, alert_type, params, active,
                           notify_mode, cooldown_seconds, note, auto_managed)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, (security_id, alert_type, params_json, int(active),
                          notify_mode, cooldown_seconds, note, int(auto_managed)))
        conn.commit()
        if _IS_POSTGRES:
            cur.execute("SELECT lastval()")
        else:
            cur.execute("SELECT last_insert_rowid()")
        return cur.fetchone()[0]


def update_alert(alert_id, alert_type=None, params=None, notify_mode=None,
                 cooldown_seconds=None, active=None, note=None, automatic=False):
    updates, values = [], []
    if alert_type is not None:
        updates.append("alert_type = " + _ph()); values.append(alert_type)
    if params is not None:
        updates.append("params = " + _ph()); values.append(json.dumps(params))
    if notify_mode is not None:
        updates.append("notify_mode = " + _ph()); values.append(notify_mode)
    if cooldown_seconds is not None:
        updates.append("cooldown_seconds = " + _ph()); values.append(cooldown_seconds)
    if active is not None:
        updates.append("active = " + _ph()); values.append(int(active))
    if note is not None:
        updates.append("note = " + _ph()); values.append(note)
    updates.append("auto_managed = " + _ph()); values.append(int(automatic))
    if not updates:
        return
    values.append(int(alert_id))
    with get_conn() as conn:
        conn.cursor().execute(f"UPDATE alerts SET {', '.join(updates)} WHERE id = {_ph()}", values)
        conn.commit()


def toggle_alert_active(alert_id: int, active: bool) -> None:
    with get_conn() as conn:
        conn.cursor().execute(
            _adapt_sql("UPDATE alerts SET active=? WHERE id=?"), (int(active), alert_id)
        )
        conn.commit()


def delete_alert(alert_id: int) -> None:
    with get_conn() as conn:
        conn.cursor().execute(_adapt_sql("DELETE FROM alerts WHERE id=?"), (alert_id,))
        conn.commit()


def log_alert_trigger(alert_id: int, payload: dict) -> None:
    now = pd.Timestamp.utcnow().isoformat()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            _adapt_sql("INSERT INTO alerts_log(alert_id, triggered_at, payload) VALUES (?, ?, ?)"),
            (alert_id, now, json.dumps(payload)),
        )
        cur.execute(
            _adapt_sql("UPDATE alerts SET last_triggered=?, last_evaluated=? WHERE id=?"),
            (now, now, alert_id),
        )
        conn.commit()


def set_alert_state(alert_id: int, state: dict) -> None:
    """Persist edge-trigger state for an alert (see middleware.alerts)."""
    now = pd.Timestamp.utcnow().isoformat()
    with get_conn() as conn:
        conn.cursor().execute(
            _adapt_sql("UPDATE alerts SET state=?, last_evaluated=? WHERE id=?"),
            (json.dumps(state), now, int(alert_id)),
        )
        conn.commit()


def get_last_alert_log(alert_id: int) -> Optional[dict]:
    """Return the payload dict from the most recent log entry for this alert."""
    df = _read_sql(
        "SELECT payload FROM alerts_log WHERE alert_id=? ORDER BY triggered_at DESC LIMIT 1",
        (alert_id,),
    )
    if df.empty:
        return None
    try:
        return json.loads(df.iloc[0]["payload"] or "{}")
    except Exception:
        return {}

def last_trigger_time(alert_id: int) -> Optional[pd.Timestamp]:
    df = _read_sql(
        "SELECT triggered_at FROM alerts_log WHERE alert_id=? ORDER BY triggered_at DESC LIMIT 1",
        (alert_id,),
    )
    if df.empty:
        return None
    ts = pd.to_datetime(df.iloc[0]["triggered_at"])
    # Normalise to tz-naive UTC so callers can compare with pd.Timestamp.utcnow()
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts


def get_active_alerts() -> List[Dict]:
    df = _read_sql("SELECT * FROM alerts WHERE active=1")
    return df.to_dict("records")


def get_alert_by_id(alert_id: int) -> Optional[Dict]:
    df = _read_sql("SELECT * FROM alerts WHERE id=?", (alert_id,))
    return df.iloc[0].to_dict() if not df.empty else None


def get_alert_history(limit: int = 200, alert_id: Optional[int] = None) -> pd.DataFrame:
    """Most recent alert triggers (newest first) with the security and what fired."""
    where, args = "", []
    if alert_id is not None:
        where, args = "WHERE al.alert_id=?", [int(alert_id)]
    args.append(int(limit))
    return _read_sql(f"""
        SELECT al.id, al.alert_id, al.triggered_at, al.payload,
               a.alert_type, a.params, a.note, a.notify_mode, a.security_id,
               s.yahoo_ticker AS symbol,
               COALESCE(sc.longName, sc.shortName) AS security_name
        FROM alerts_log al
        JOIN alerts a ON a.id = al.alert_id
        LEFT JOIN securities s ON s.id = a.security_id
        LEFT JOIN securities_cache sc ON sc.security_id = a.security_id
        {where}
        ORDER BY al.triggered_at DESC
        LIMIT ?
    """, tuple(args))


def get_alerts_for_digest(since_ts: str, notify_mode: str) -> List[Dict]:
    df = _read_sql("""
        SELECT al.alert_id, al.triggered_at, al.payload,
               a.security_id, a.alert_type, a.params, a.note
        FROM alerts_log al
        JOIN alerts a ON a.id = al.alert_id
        WHERE a.notify_mode=? AND al.triggered_at>?
        ORDER BY al.triggered_at
    """, (notify_mode, since_ts))
    return df.to_dict("records")
