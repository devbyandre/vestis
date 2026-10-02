from fastapi import APIRouter, Query
from pydantic import BaseModel
from typing import Optional
import pandas as pd

import middleware as mw
import db_utils as db

from ._helpers import _df

router = APIRouter(tags=["transactions"])


@router.get("/transactions")
def get_transactions(portfolio_ids: Optional[str] = Query(None)):
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    df = db.list_transactions_detailed() if ids is None else db.list_transactions(ids)
    if df is None or df.empty:
        return []
    df = df.copy()
    # Normalise column names across the two query shapes
    if "tx_date" not in df.columns and "date" in df.columns:
        df["tx_date"] = df["date"]
    if "tx_type" not in df.columns and "type" in df.columns:
        df["tx_type"] = df["type"]
    if "tx_cost" not in df.columns and "fees" in df.columns:
        df["tx_cost"] = df["fees"]
    if "portfolio" not in df.columns and "portfolio_name" in df.columns:
        df["portfolio"] = df["portfolio_name"]
    if "security_name" not in df.columns and "name" in df.columns:
        df["security_name"] = df["name"]
    # Ensure numeric
    for c in ["quantity", "price", "tx_cost"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    # Compute total_cost = quantity*price + fees (splits have price=0 → total 0)
    df["total_cost"] = df["quantity"] * df["price"] + df.get("tx_cost", 0.0)
    # security_label for display
    if "security_label" not in df.columns:
        df["security_label"] = df.apply(
            lambda r: (r.get("security_name") or r.get("symbol") or "?"), axis=1
        )
    return _df(df)


@router.get("/transactions/summary")
def get_transactions_summary(portfolio_ids: Optional[str] = Query(None)):
    """Pre-computed summary metrics for the transactions tab."""
    ids = [int(x) for x in portfolio_ids.split(",")] if portfolio_ids else None
    df = db.list_transactions_detailed() if ids is None else db.list_transactions(ids)
    if df is None or df.empty:
        return {"count": 0, "total_quantity": 0, "total_buys": 0, "num_buys": 0,
                "total_sells": 0, "num_sells": 0, "total_fees": 0}
    df = df.copy()
    if "tx_type" not in df.columns and "type" in df.columns:
        df["tx_type"] = df["type"]
    if "tx_cost" not in df.columns and "fees" in df.columns:
        df["tx_cost"] = df["fees"]
    for c in ["quantity", "price", "tx_cost"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df["total_cost"] = df["quantity"] * df["price"] + df.get("tx_cost", 0.0)
    buys = df[df["tx_type"] == "buy"]
    sells = df[df["tx_type"] == "sell"]
    return {
        "count": int(len(df)),
        "total_quantity": float(df["quantity"].sum()),
        "total_buys": float(buys["total_cost"].sum()),
        "num_buys": int(len(buys)),
        "total_sells": float(sells["total_cost"].sum()),
        "num_sells": int(len(sells)),
        "total_fees": float(df["tx_cost"].sum()),
    }

class TransactionCreate(BaseModel):
    portfolio_id: int
    symbol: str
    tx_date: str
    tx_type: str
    quantity: float
    price: float
    fees: float = 0.0

class TransactionEdit(BaseModel):
    tx_id: int
    portfolio_id: int
    symbol: str
    tx_date: str
    tx_type: str
    quantity: float
    price: float
    fees: float = 0.0

class SplitCreate(BaseModel):
    symbol: str
    split_date: str
    ratio: float
    portfolio_id: Optional[int] = None

@router.post("/transactions")
def add_transaction(body: TransactionCreate):
    mw.add_transaction(
        portfolio_id=body.portfolio_id,
        symbol=body.symbol,
        tx_date=body.tx_date,
        tx_type=body.tx_type,
        quantity=body.quantity,
        price=body.price,
        fees=body.fees,
    )
    return {"ok": True}

@router.put("/transactions/{tx_id}")
def edit_transaction(tx_id: int, body: TransactionEdit):
    # mw.edit_transaction only updates the transaction's own fields — it has
    # no support for reassigning a transaction's portfolio or security, so
    # body.portfolio_id/body.symbol (echoed back from the existing row by
    # the frontend) aren't passed through.
    mw.edit_transaction(
        tx_id=tx_id,
        tx_date=body.tx_date,
        tx_type=body.tx_type,
        quantity=body.quantity,
        price=body.price,
        fees=body.fees,
    )
    return {"ok": True}

@router.delete("/transactions/{tx_id}")
def delete_transaction(tx_id: int):
    mw.remove_transaction(tx_id)
    return {"ok": True}

@router.post("/transactions/split")
def add_split(body: SplitCreate):
    applied = mw.add_split_transaction(
        symbol=body.symbol,
        split_date=body.split_date,
        ratio=body.ratio,
        portfolio_id=body.portfolio_id,
    )
    return {"ok": True, "portfolios_applied": applied}
