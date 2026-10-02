from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional

import middleware as mw

from ._helpers import _df

router = APIRouter(tags=["portfolios"])


@router.get("/portfolios")
def get_portfolios():
    return _df(mw.list_portfolios())


class PortfolioCreate(BaseModel):
    name: str

class PortfolioRename(BaseModel):
    old_name: str
    new_name: str

class PortfolioDelete(BaseModel):
    name: str
    reassign_to: Optional[str] = None

@router.post("/portfolios")
def create_portfolio(body: PortfolioCreate):
    pid = mw.create_portfolio(body.name)
    return {"id": pid, "name": body.name}

@router.put("/portfolios/rename")
def rename_portfolio(body: PortfolioRename):
    mw.rename_portfolio(body.old_name, body.new_name)
    return {"ok": True}

@router.delete("/portfolios")
def delete_portfolio(body: PortfolioDelete):
    mw.delete_and_reassign_portfolio(body.name, body.reassign_to)
    return {"ok": True}
