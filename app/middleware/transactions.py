import logging
from typing import Optional, List
import pandas as pd

import db_utils as db
from .securities import add_security


def add_transaction(portfolio_id: int, symbol: str, tx_date: str,
                    tx_type: str, quantity: float, price: float, fees: float = 0.0):
    sec_id = add_security(symbol)
    db.insert_transaction(portfolio_id, sec_id, tx_date, tx_type, quantity, price, fees)
    db.recompute_holdings_timeseries(portfolio_id, sec_id)

def edit_transaction(tx_id: int, tx_date: str, tx_type: str,
                     quantity: float, price: float, fees: float,
                     security_id: int | None = None,
                     security_name: str | None = None,
                     security_isin: str | None = None):
    """
    Edit a transaction and optionally update the related security.
    """
    # Update transaction
    db.update_transaction(tx_id, tx_date, tx_type, quantity, price, fees)

    # Update security info if provided
    if security_id and (security_name is not None or security_isin is not None):
        db.update_security(security_id, name=security_name, isin=security_isin)

    # Recompute holdings timeseries if needed
    row = db.get_transaction_by_id(tx_id)
    if row:
        db.recompute_holdings_timeseries(row["portfolio_id"], row["security_id"])

def remove_transaction(tx_id: int) -> None:
    row = db.get_transaction_by_id(tx_id)
    db.delete_transaction(tx_id)
    if row:
        db.recompute_holdings_timeseries(row["portfolio_id"], row["security_id"])

def list_transactions(portfolio_ids: Optional[List[int]] = None) -> pd.DataFrame:
    return db.list_transactions(portfolio_ids)


def list_transactions_detailed() -> pd.DataFrame:
    return db.list_transactions_detailed()

def apply_splits_to_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Retroactively adjust buy/sell quantities and prices for any stock splits
    recorded as tx_type='split'.

    A split row encodes:
        quantity = new_shares / old_shares (e.g. 3.0 for a 3-for-1 split)
        price    = 0
        tx_date  = effective date of the split

    For each split on symbol S with ratio R on date D:
      - All buy/sell rows for S with tx_date < D get:
          quantity *= R
          price    /= R
      - The split row itself is excluded from the returned DataFrame

    Call this before any FIFO, dividends, or holdings calculation.
    """
    if df.empty:
        return df

    df = df.copy()
    date_col = 'tx_date' if 'tx_date' in df.columns else 'date'
    if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

    type_col = 'type' if 'type' in df.columns else 'tx_type'
    splits = df[df[type_col].str.lower() == 'split'].copy()

    if splits.empty:
        return df[df[type_col].str.lower() != 'split'].copy()

    for _, split_row in splits.iterrows():
        sym        = split_row.get('symbol')
        split_date = pd.to_datetime(split_row[date_col])
        try:
            ratio = float(split_row['quantity'])
        except (ValueError, TypeError):
            logging.warning("apply_splits: invalid ratio for %s on %s", sym, split_date)
            continue
        if ratio <= 0:
            continue

        mask = (
            (df['symbol'] == sym) &
            (df[type_col].str.lower().isin(['buy', 'sell'])) &
            (pd.to_datetime(df[date_col]) < split_date)
        )
        df.loc[mask, 'quantity'] = df.loc[mask, 'quantity'] * ratio
        df.loc[mask, 'price']    = df.loc[mask, 'price']    / ratio
        logging.info(
            "apply_splits: adjusted %d rows for %s (ratio %.4f, effective %s)",
            mask.sum(), sym, ratio, split_date.date()
        )

    return df[df[type_col].str.lower() != 'split'].copy()


def add_split_transaction(portfolio_id: int, symbol: str, split_date: str, ratio: float) -> None:
    """
    Record a stock split. ratio = new_shares / old_shares (e.g. 3.0 for 3-for-1).
    Use 0.5 for a 1-for-2 reverse split.
    All historical buy/sell quantities and prices are adjusted automatically
    when holdings, FIFO gains, and dividends are calculated.
    """
    if ratio <= 0:
        raise ValueError(f"Split ratio must be positive, got {ratio}")
    sec_id = add_security(symbol)
    db.insert_transaction(
        portfolio_id, sec_id, split_date,
        tx_type='split',
        quantity=ratio,
        price=0.0,
        fees=0.0,
    )
    db.recompute_holdings_timeseries(portfolio_id, sec_id)
    db.clear_split_alert(sec_id)
    logging.info("Recorded split for %s: ratio=%.4f on %s", symbol, ratio, split_date)
