from typing import Optional, List
import pandas as pd

import db_utils as db


def get_holdings():
    """
    Currently-held positions only (net quantity > 0), aggregated across
    portfolios — used by telegram_worker.maintain_alerts() to decide which
    securities get automatic alerts. db.get_holdings_from_transactions()
    returns every security ever transacted regardless of current position,
    which is wrong for this purpose (see get_latest_holdings_snapshot()).
    """
    return get_latest_holdings_snapshot(aggregate=True)


def get_latest_holdings_snapshot(
    portfolio_ids: Optional[List[int]] = None,
    sectors: Optional[List[str]] = None,
    industries: Optional[List[str]] = None,
    security_types: Optional[List[str]] = None,
    symbols: Optional[List[str]] = None,
    exchanges: Optional[List[str]] = None,
    aggregate: bool = True
) -> pd.DataFrame:
    """
    Snapshot (latest global date across all holdings).
    Returns only open holdings (quantity>0).
    If aggregate=True, aggregates across portfolios (1 row per security).
    Guaranteed columns: portfolio_id (if aggregate=False), security_id, symbol, name,
    sector, industry, security_type, exchange, quantity, market_value, cost_basis,
    abs_perf, rel_perf, security_label
    """
    df = holdings_timeseries(
        portfolio_ids=portfolio_ids,
        sectors=sectors,
        industries=industries,
        security_types=security_types,
        symbols=symbols,
        exchanges=exchanges,
        aggregate=False   # 🔑 get raw rows
    )
    if df.empty:
        return pd.DataFrame()

    df['date'] = pd.to_datetime(df['date'])

    # For each security+portfolio, take only its own most recent row.
    # Using global MAX would drop securities whose prices haven't been
    # fetched today yet — they'd fall off when any other security updates.
    latest = (
        df.sort_values('date')
          .groupby(['security_id', 'portfolio_id'], as_index=False)
          .last()
    ).copy()

    # Filter out fully sold positions: recompute skips qty<=0 rows, so the
    # last row in holdings_timeseries is pre-sell. Check actual net quantity
    # from transactions directly.
    if not latest.empty and 'security_id' in latest.columns and 'portfolio_id' in latest.columns:
        keep = []
        for _, row in latest.iterrows():
            try:
                sid = int(row['security_id'])
                pid = int(row['portfolio_id'])
                tx_df = db.list_transactions_for_security(pid, sid)
                if tx_df is None or tx_df.empty:
                    keep.append(False)
                    continue
                net = 0.0
                for _, tx in tx_df.iterrows():
                    t = str(tx.get('type', '')).lower()
                    q = float(tx.get('quantity') or 0)
                    if t == 'buy':
                        net += q
                    elif t == 'sell':
                        net -= q
                keep.append(net > 1e-8)
            except Exception:
                keep.append(True)  # on error, keep the row
        latest = latest[keep].copy()

    # compute performance columns
    latest['cost_basis'] = latest.get('cost_basis', 0.0)
    latest['abs_perf'] = latest['market_value'] - latest['cost_basis']
    latest['rel_perf'] = latest.apply(
        lambda r: (r['abs_perf'] / r['cost_basis']) if r['cost_basis'] else 0.0,
        axis=1
    )
    latest['security_label'] = latest.apply(
        lambda r: f"{r.get('name','Unknown')} ({r.get('symbol','Unknown')})",
        axis=1
    )

    # fill metadata if missing
    for col in ['sector','industry','security_type','exchange','symbol','name']:
        if col in latest.columns:
            latest[col] = latest[col].fillna('Unknown')

    if aggregate:
        agg_cols = {
            'quantity': 'sum',
            'market_value': 'sum',
            'cost_basis': 'sum',
            'abs_perf': 'sum'
        }
        grouped = (
            latest.groupby(
                ['security_id','symbol','name','sector','industry',
                 'security_type','exchange','security_label'],
                as_index=False
            ).agg(agg_cols)
        )
        grouped['rel_perf'] = grouped.apply(
            lambda r: (r['abs_perf']/r['cost_basis']) if r['cost_basis'] else 0.0,
            axis=1
        )
        return grouped
    else:
        return latest.reset_index(drop=True)


def holdings_timeseries(
    portfolio_ids: Optional[List[int]] = None,
    sectors: Optional[List[str]] = None,
    industries: Optional[List[str]] = None,
    security_types: Optional[List[str]] = None,
    symbols: Optional[List[str]] = None,
    exchanges: Optional[List[str]] = None,
    aggregate: bool = True
) -> pd.DataFrame:
    """
    Returns holdings timeseries.
    If aggregate=True: daily aggregated timeseries (suitable for charts).
    If aggregate=False: raw detailed timeseries rows (per portfolio+security+date).
    """
    df = db.get_holdings_timeseries(
        portfolio_ids=portfolio_ids,
        sectors=sectors,
        industries=industries,
        security_types=security_types,
        symbols=symbols,
        exchanges=exchanges
    )
    if df.empty:
        return pd.DataFrame()

    df['date'] = pd.to_datetime(df['date'])
    # normalize numeric columns
    df['market_value'] = pd.to_numeric(df['market_value'], errors='coerce').fillna(0.0)
    df['cost_basis'] = pd.to_numeric(df.get('cost_basis', 0.0), errors='coerce').fillna(0.0)

    # Each security's own timeseries only extends as far as its own last
    # recompute (triggered per-security by price updates/transactions) — a
    # security whose price hasn't been refreshed yet today simply has no
    # row for today, while others do. Summing per date as-is would make the
    # total look like it crashed on the most recent day(s), since only the
    # already-refreshed securities would be counted. Forward-fill each
    # security's series up to the global max date so a lagging security
    # keeps contributing its last-known value instead of vanishing.
    max_date = df['date'].max()
    lagging = df.groupby(['portfolio_id', 'security_id'])['date'].transform('max') < max_date
    if lagging.any():
        filled = [g for _, g in df[~lagging].groupby(['portfolio_id', 'security_id'], sort=False)]
        for _, g in df[lagging].groupby(['portfolio_id', 'security_id'], sort=False):
            g = g.sort_values('date').set_index('date')
            full_index = pd.date_range(start=g.index.min(), end=max_date, freq='D')
            g = g.reindex(full_index).ffill()
            g.index.name = 'date'
            filled.append(g.reset_index())
        df = pd.concat(filled, ignore_index=True)
        # reindex/ffill turns portfolio_id/security_id into floats (NaN on
        # the newly-introduced rows before ffill fills them back in) —
        # restore int dtype since downstream code (DB lookups, groupbys)
        # expects real ints, not numpy floats.
        df['portfolio_id'] = df['portfolio_id'].astype(int)
        df['security_id'] = df['security_id'].astype(int)

    if not aggregate:
        return df

    # aggregate per date (all portfolios/securities combined)
    df_grouped = (
        df.groupby('date')
          .agg({'market_value': 'sum', 'cost_basis': 'sum'})
          .reset_index()
          .sort_values('date')
    )
    df_grouped['net_value'] = df_grouped['market_value'] - df_grouped['cost_basis']

    df_grouped['rel_perf'] = 0.0
    mask = df_grouped['cost_basis'] != 0
    df_grouped.loc[mask, 'rel_perf'] = (
        (df_grouped.loc[mask, 'market_value'] - df_grouped.loc[mask, 'cost_basis'])
        / df_grouped.loc[mask, 'cost_basis']
    )

    return df_grouped


def recompute_all_holdings_timeseries():
    """
    Recompute holdings timeseries for all portfolios and all securities,
    including equities, ETFs, and crypto.
    """
    holdings = db.get_holdings_from_transactions()
    for _, row in holdings.iterrows():
        pf_id = row['portfolio_id']
        sec_id = row['security_id']
        db.recompute_holdings_timeseries(pf_id, sec_id)


def store_prices(security_id: int, df: pd.DataFrame) -> None:
    db.store_prices(security_id, df)
    portfolios = db.list_portfolios_holding_security(security_id)
    for pf_id in portfolios:
        db.recompute_holdings_timeseries(pf_id, security_id)


def fetch_portfolio_risk_timeseries(portfolio_ids: Optional[List[int]] = None, aggregate=True) -> pd.DataFrame:
    """
    Wrapper to fetch aggregated portfolio-level risk timeseries.
    """
    if aggregate:
        return db.get_portfolio_risk_timeseries(portfolio_ids)
    else:
        return db.get_portfolio_risk_timeseries_detailed(portfolio_ids)


def update_portfolio_risk_timeseries_for_portfolios(portfolio_ids: Optional[List[int]] = None):
    """
    Ensure that all security risk timeseries for the selected portfolios are up to date.
    """
    df_hold = holdings_timeseries(portfolio_ids=portfolio_ids, aggregate=False)
    if df_hold.empty:
        return pd.DataFrame(columns=["date", "portfolio_id", "weighted_risk"])

    # Only compute risk for securities that are actually held
    security_ids = df_hold["security_id"].unique()
    for sec_id in security_ids:
        db.update_security_risk_timeseries(sec_id, portfolio_ids=portfolio_ids)

    # Return aggregated portfolio-level risk timeseries
    return db.get_portfolio_risk_timeseries(portfolio_ids)
