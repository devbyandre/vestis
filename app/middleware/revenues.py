from bisect import bisect_right
from typing import Optional
import pandas as pd

import db_utils as db
from .transactions import list_transactions, apply_splits_to_transactions


def calc_dividends_for_portfolio(portfolio_ids: Optional[list] = None, year: Optional[int] = None):
    tx = list_transactions(portfolio_ids)
    if tx.empty:
        return pd.DataFrame(columns=['symbol','portfolio_id','date','dividend_per_share','shares','total','year'])

    tx['tx_date'] = pd.to_datetime(tx['date']).dt.date

    # Apply split adjustments before FIFO — splits are not taxable events
    tx = apply_splits_to_transactions(tx)
    # Re-normalize tx_date to date objects after split processing (apply_splits converts to datetime64)
    tx["tx_date"] = pd.to_datetime(tx["tx_date"] if "tx_date" in tx.columns else tx["date"], errors="coerce").dt.date

    rows = []

    tickers = tx['symbol'].dropna().unique()

    tx = tx.copy()
    is_buy = tx['type'].astype(str).str.lower() == 'buy'
    tx['qty_signed'] = tx['quantity'].where(is_buy, -tx['quantity'])

    all_divs = db.get_dividends_many(tickers)
    if all_divs.empty:
        return pd.DataFrame(columns=['symbol','portfolio_id','date','dividend_per_share','shares','total','year'])
    # Dividends are stored in the listing currency; report them in EUR at the
    # rate of the payment date (one FX series per currency).
    all_divs['date'] = pd.to_datetime(all_divs['date']).dt.tz_localize(None).dt.normalize()
    all_divs['currency'] = all_divs['currency'].fillna('EUR')
    start, end = all_divs['date'].min().strftime('%Y-%m-%d'), all_divs['date'].max().strftime('%Y-%m-%d')
    rates = {cur: db.get_fx_series(cur, start, end) for cur in all_divs['currency'].unique()}
    all_divs['dividend'] = [float(d) * float(rates[c].get(dt, 1.0))
                            for d, c, dt in zip(all_divs['dividend'], all_divs['currency'], all_divs['date'])]
    divs_by_symbol = dict(tuple(all_divs.groupby('symbol')))

    for sym in tickers:
        df_divs = divs_by_symbol.get(sym)
        if df_divs is None or df_divs.empty:
            continue

        # Per portfolio: transaction dates and running net quantity, so each
        # dividend is a binary search instead of re-filtering the frame.
        positions = {}
        for pid, g in tx[tx['symbol'] == sym].sort_values('tx_date').groupby('portfolio_id'):
            positions[pid] = (g['tx_date'].tolist(), g['qty_signed'].cumsum().tolist())
        if not positions:
            continue

        for _, d in df_divs.iterrows():
            d_date = pd.to_datetime(d['date']).date()
            amt = float(d['dividend'])
            yr = d_date.year
            if year and yr != int(year):
                continue

            for pid in sorted(positions):
                dates, cum = positions[pid]
                n = bisect_right(dates, d_date)
                qty = cum[n - 1] if n else 0.0
                if qty <= 0:
                    continue
                rows.append({
                    'symbol': sym,
                    'portfolio_id': pid,
                    'date': d_date.isoformat(),
                    'dividend_per_share': amt,
                    'shares': qty,
                    'total': amt*qty,
                    'year': yr
                })

    return pd.DataFrame(rows)


def calc_capital_gains_fifo(portfolio_ids: Optional[list] = None, year: Optional[int] = None):
    df_tx = list_transactions(portfolio_ids)
    if df_tx.empty:
        return pd.DataFrame(columns=['portfolio_id','symbol','sell_date','quantity','proceeds','cost_basis','profit','year'])

    df_tx['tx_date'] = pd.to_datetime(df_tx['date'])
    # Apply split adjustments before FIFO — splits are not taxable events
    df_tx = apply_splits_to_transactions(df_tx)
    rows = []

    for pid, g in df_tx.groupby('portfolio_id'):
        buys = {}
        for _, r in g.sort_values(['tx_date','id']).iterrows():
            sym = r.get('symbol')
            if not sym:
                continue  # skip if no symbol

            typ = r['type'].lower()


            qty = float(r['quantity'])
            price = float(r['price']) if r['price'] else 0.0
            fees = float(r.get('fees') or 0.0)

            if typ == 'buy':
                buys.setdefault(sym, []).append({'qty': qty, 'price': price, 'fees': fees, 'date': r['tx_date']})
            elif typ == 'sell':
                remaining = qty
                proceeds = qty * price - fees
                cost_basis = 0.0
                while remaining > 0 and buys.get(sym):
                    b = buys[sym][0]
                    take = min(b['qty'], remaining)
                    fee_share = b['fees'] * take / b['qty'] if b['qty'] else 0.0
                    cost_basis += take * b['price'] + fee_share
                    b['fees'] -= fee_share
                    b['qty'] -= take
                    remaining -= take
                    if b['qty'] <= 1e-12:
                        buys[sym].pop(0)
                profit = proceeds - cost_basis
                yr = int(pd.to_datetime(r['tx_date']).year)
                if year and yr != int(year):
                    continue
                rows.append({
                    'portfolio_id': pid,
                    'symbol': sym,
                    'sell_date': r['tx_date'].isoformat(),
                    'quantity': qty,
                    'proceeds': proceeds,
                    'cost_basis': cost_basis,
                    'profit': profit,
                    'year': yr
                })
    return pd.DataFrame(rows)
