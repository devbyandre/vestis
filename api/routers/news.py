from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel

import middleware as mw

router = APIRouter(tags=["news"])


@router.get("/news")
def get_news(
    scope: str = Query("all", pattern="^(all|holdings|watchlist|market)$"),
    symbol: Optional[str] = None,
    days: int = Query(7, ge=1, le=30),
    limit: Optional[int] = Query(None, ge=1, le=500),
):
    return mw.get_news_feed(scope=scope, symbol=symbol, days=days, limit=limit)


class NewsRefresh(BaseModel):
    symbol: Optional[str] = None


@router.post("/news/refresh")
def refresh_news(background: BackgroundTasks, body: NewsRefresh = NewsRefresh()):
    """One symbol: fetch now (throttle bypassed) and return the stats.
    No symbol: refresh everything that is due in the background (it can take a
    minute or more, longer than a request should)."""
    if body.symbol:
        try:
            return mw.refresh_news(symbol=body.symbol, force=True)
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"News fetch failed: {exc}")
    background.add_task(mw.refresh_news)
    return {"started": True}
