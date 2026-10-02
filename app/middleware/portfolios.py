from typing import List

import db_utils as db
from .transactions import list_transactions


def create_portfolio(name: str) -> int:
    return db.insert_portfolio(name)


def rename_portfolio(old_name: str, new_name: str):
    db.rename_portfolio(old_name, new_name)


def list_portfolios():
    return db.list_portfolios()


def delete_and_reassign_portfolio(del_name: str, target_name: str):
    """Delete portfolio by name; reassign its transactions to target."""

    if del_name == target_name:
        raise ValueError("Deleted portfolio and target portfolio cannot be the same.")

    # resolve IDs for names
    del_row = db.get_portfolio_by_name(del_name)
    if not del_row:
        raise ValueError(f"Portfolio '{del_name}' not found.")
    del_id = del_row["id"]

    target_row = db.get_portfolio_by_name(target_name)
    if not target_row:
        raise ValueError(f"Target portfolio '{target_name}' not found.")
    target_id = target_row["id"]

    # reassign transactions and delete portfolio
    db.reassign_transactions(del_id, target_id)
    db.delete_portfolio(del_id)


def get_all_symbols() -> List[str]:
    return db.get_all_symbols()


def get_portfolio_symbols(portfolio_name: str) -> list:
    portfolios = list_portfolios()
    row = portfolios[portfolios['name'] == portfolio_name]
    if row.empty:
        return []

    portfolio_id = int(row.iloc[0]['id'])
    tx = list_transactions([portfolio_id])
    if tx.empty:
        return []

    # Compute signed quantities: buy = +quantity, sell = -quantity
    tx['signed_qty'] = tx.apply(lambda r: r['quantity'] if r['type'] == 'buy' else -r['quantity'], axis=1)

    # Sum by symbol
    net_quantities = tx.groupby('symbol')['signed_qty'].sum()

    # Keep only symbols with net quantity > 0
    active_symbols = net_quantities[net_quantities > 0].index.tolist()

    return active_symbols
