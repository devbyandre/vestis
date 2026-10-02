from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

import pandas as pd

import middleware as mw

from ._helpers import _df

router = APIRouter(tags=["alerts"])


@router.get("/alerts")
def get_alerts(active_only: bool = False):
    df = mw.get_all_alerts_for_ui()
    if active_only:
        df = df[df["active"] == 1] if not df.empty else df
    return _df(df)

@router.get("/alerts/history")
def get_alert_history(limit: int = 200, alert_id: Optional[int] = None):
    """Recently fired alerts, newest first."""
    rows = mw.get_alert_history(limit=min(max(limit, 1), 1000), alert_id=alert_id)
    return _df(pd.DataFrame(rows))

def _validated(alert_type: str, params: dict) -> dict:
    try:
        return mw.validate_alert_params(alert_type, params)
    except ValueError as e:
        raise HTTPException(422, str(e))

class AlertCreate(BaseModel):
    security_id: int
    alert_type: str
    params: dict
    note: Optional[str] = ""
    notify_mode: str = "immediate"
    cooldown_seconds: int = 14400
    active: bool = True

class AlertEdit(BaseModel):
    alert_type: Optional[str] = None
    params: Optional[dict] = None
    note: Optional[str] = None
    active: Optional[bool] = None
    cooldown_seconds: Optional[int] = None
    notify_mode: Optional[str] = None

@router.post("/alerts")
def create_alert(body: AlertCreate):
    params = _validated(body.alert_type, body.params)
    alert_id = mw.create_alert(
        security_id=body.security_id,
        alert_type=body.alert_type,
        params=params,
        note=body.note,
        notify_mode=body.notify_mode,
        cooldown_seconds=body.cooldown_seconds,
    )
    return {"id": alert_id}

@router.put("/alerts/{alert_id}")
def edit_alert(alert_id: int, body: AlertEdit):
    params = body.params
    if params is not None:
        alert_type = body.alert_type
        if alert_type is None:
            df = mw.get_all_alerts_for_ui()
            row = df[df["id"] == alert_id] if not df.empty else df
            if row.empty:
                raise HTTPException(404, "Alert not found")
            alert_type = row.iloc[0]["alert_type"]
        params = _validated(alert_type, params)
    mw.edit_alert(
        alert_id=alert_id,
        alert_type=body.alert_type,
        params=params,
        note=body.note,
        active=body.active,
        cooldown_seconds=body.cooldown_seconds,
        notify_mode=body.notify_mode,
    )
    if params is not None or body.alert_type is not None:
        mw.db.set_alert_state(alert_id, {})  # new condition → fresh edge state
    return {"ok": True}

@router.delete("/alerts/{alert_id}")
def delete_alert(alert_id: int):
    mw.delete_alert(alert_id)
    return {"ok": True}
