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
    latest = db.get_holdings_timeseries(
        portfolio_ids=portfolio_ids,
        sectors=sectors,
        industries=industries,
        security_types=security_types,
        symbols=symbols,
        exchanges=exchanges,
        latest_only=True,
    )
    if latest.empty:
        return pd.DataFrame()

    # Each security contributes its own most recent row (not the global max
    # date) so securities whose prices haven't been fetched today yet don't
    # drop off. Like the forward-filled timeseries, they carry the global
    # latest date.
    latest['date'] = latest['date'].max()

    # Drop fully sold positions: the last timeseries row is pre-sell, so use
    # the actual net quantity from transactions.
    net = db.get_net_quantities()
    keep = [net.get((int(p), int(s)), 0.0) > 1e-8
            for p, s in zip(latest['portfolio_id'], latest['security_id'])]
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

    # Open positions are already carried forward to the newest date by
    # db.get_holdings_timeseries; sold ones correctly stop at their sale.
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


def performance_series(portfolio_ids: Optional[List[int]] = None) -> pd.DataFrame:
    """Daily market value, cost basis, net cash flow and time-weighted return index.

    Buying or selling changes the market value without being a gain or loss,
    so returns strip out each day's net flow (EUR, fees included):
        r_t = (MV_t - MV_{t-1} - F_t) / (MV_{t-1} + F_t)
    twr is the cumulative product of (1 + r_t), starting at 1. Dividends are
    not included (price return).
    """
    ts = db.get_holdings_timeseries(portfolio_ids=portfolio_ids)
    if ts.empty:
        return pd.DataFrame(columns=["date", "market_value", "cost_basis", "flow", "twr"])
    ts["date"] = pd.to_datetime(ts["date"])
    daily = ts.groupby("date")[["market_value", "cost_basis"]].sum().sort_index()
    daily = daily.reindex(pd.date_range(daily.index.min(), daily.index.max(), freq="D")).ffill()

    tx = db.list_transactions(portfolio_ids)
    flow = pd.Series(0.0, index=daily.index)
    if not tx.empty:
        tx = tx[tx["security_id"].isin(ts["security_id"].unique())].copy()
        tx["type"] = tx["type"].astype(str).str.lower()
        tx = tx[tx["type"].isin(["buy", "sell"])]
        gross = pd.to_numeric(tx["quantity"], errors="coerce").fillna(0) * pd.to_numeric(tx["price"], errors="coerce").fillna(0)
        fees = pd.to_numeric(tx["fees"], errors="coerce").fillna(0)
        tx["flow"] = (gross + fees).where(tx["type"] == "buy", -(gross - fees))
        # A position is only valued from its first priced day (holidays, late
        # price history), so a purchase's cash counts from that day too.
        when = pd.to_datetime(tx["date"]).dt.tz_localize(None).dt.normalize()
        first = ts.groupby(["portfolio_id", "security_id"])["date"].min()
        starts = pd.Series([first.get((p, sid), pd.NaT) for p, sid in zip(tx["portfolio_id"], tx["security_id"])],
                           index=tx.index)
        is_buy = tx["type"] == "buy"
        when = when.where(~is_buy | starts.isna() | (when >= starts), starts)
        pos = daily.index.searchsorted(when)
        pos = pos.clip(0, len(daily.index) - 1)
        flow = flow.add(tx.groupby(daily.index[pos])["flow"].sum(), fill_value=0.0)

    mv = daily["market_value"]
    prev = mv.shift(1).fillna(0.0)
    base = prev + flow
    ret = ((mv - prev - flow) / base).where(base > 0, 0.0)
    out = daily.assign(flow=flow, twr=(1 + ret).cumprod())
    out.index.name = "date"
    return out.reset_index()


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
