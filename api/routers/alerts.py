from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional

import middleware as mw

from ._helpers import _df

router = APIRouter(tags=["alerts"])


@router.get("/alerts")
def get_alerts(active_only: bool = False):
    df = mw.get_all_alerts_for_ui()
    if active_only:
        df = df[df["active"] == 1] if not df.empty else df
    return _df(df)

class AlertCreate(BaseModel):
    security_id: int
    alert_type: str
    params: dict
    note: Optional[str] = ""
    notify_mode: str = "immediate"
    cooldown_seconds: int = 14400
    active: bool = True

class AlertEdit(BaseModel):
    params: Optional[dict] = None
    note: Optional[str] = None
    active: Optional[bool] = None
    cooldown_seconds: Optional[int] = None
    notify_mode: Optional[str] = None

@router.post("/alerts")
def create_alert(body: AlertCreate):
    alert_id = mw.create_alert(
        security_id=body.security_id,
        alert_type=body.alert_type,
        params=body.params,
        note=body.note,
        notify_mode=body.notify_mode,
        cooldown_seconds=body.cooldown_seconds,
    )
    return {"id": alert_id}

@router.put("/alerts/{alert_id}")
def edit_alert(alert_id: int, body: AlertEdit):
    mw.edit_alert(
        alert_id=alert_id,
        params=body.params,
        note=body.note,
        active=body.active,
        cooldown_seconds=body.cooldown_seconds,
        notify_mode=body.notify_mode,
    )
    return {"ok": True}

@router.delete("/alerts/{alert_id}")
def delete_alert(alert_id: int):
    mw.delete_alert(alert_id)
    return {"ok": True}
