import json
from typing import Optional, Dict, Any, List
import pandas as pd

from config_utils import safe_json_load, get_config
from .holdings import get_latest_holdings_snapshot, fetch_portfolio_risk_timeseries
from .watchlist import get_watchlist


def suggest_rebalancing(
    portfolio_ids: Optional[List[int]] = None,
    retirement_year: int = 2047,
    threshold: float = 0.05,
    max_watchlist_suggestions: int = 3
) -> Dict[str, Any]:
    """
    Suggest rebalancing steps combining:
    1. Allocation targets (asset type, sector, industry).
    2. Risk-aware adjustments.
    3. Aggregated, prioritized recommendations with expected impact.
    """

    # --- Fetch current holdings snapshot ---
    df = get_latest_holdings_snapshot(portfolio_ids=portfolio_ids, aggregate=False)
    if df.empty:
        return {"message": "No holdings available", "suggestions": []}

    # --- Fetch watchlist ---
    watchlist = get_watchlist()
    current_syms = set(df['symbol'].unique())
    watchlist_syms = set(watchlist['symbol'].unique())
    valid_syms = current_syms | watchlist_syms

    # --- Compute weights and risk ---
    df['market_value'] = df.get('market_value', 1.0)
    total_value = df['market_value'].sum() or 1.0
    df['weight'] = df['market_value'] / total_value

    # Merge in weighted_risk from historical risk timeseries if available
    df_risk = fetch_portfolio_risk_timeseries(portfolio_ids=portfolio_ids, aggregate=False)
    if not df_risk.empty:
        latest_risk = df_risk.groupby('symbol')['weighted_risk'].last().to_dict()
        df['weighted_risk'] = df['symbol'].map(latest_risk).fillna(df['market_value'])
    else:
        df['weighted_risk'] = df['market_value']

    df['risk_pct'] = df['weighted_risk'] / df['weighted_risk'].sum()

    # --- Only current holdings for reductions ---
    df_current = df[df['symbol'].isin(current_syms)]

    # --- Load targets ---
    asset_targets_raw = safe_json_load(get_config("asset_allocation_targets"), {})
    asset_targets = {
        "pre_retirement": asset_targets_raw.get("pre_retirement") or asset_targets_raw.get("current") or {},
    }
    sectors_config = safe_json_load(get_config("target_sector_allocation"), {})
    industries_config = safe_json_load(get_config("target_industry_allocation"), {})

    # --- Aggregate current weights ---
    asset_weights = df.groupby('security_type')['weight'].sum()
    sector_weights = df.groupby('sector')['weight'].sum()
    industry_weights = df.groupby(['sector','industry'])['weight'].sum()
    security_weights = df.groupby('symbol')['weight'].sum()
    security_risk = df.groupby('symbol')['risk_pct'].sum()

    actions_dict = {}

    def add_action(sym, pct_delta, market_value_delta, reason, impact_allocation=0.0, impact_risk=0.0):
        if sym not in actions_dict:
            actions_dict[sym] = {"pct": 0.0, "market_value": 0.0, "reasons": [], "impact_allocation": 0.0, "impact_risk": 0.0}
        actions_dict[sym]["pct"] += pct_delta
        actions_dict[sym]["market_value"] += market_value_delta
        actions_dict[sym]["reasons"].append(reason)
        actions_dict[sym]["impact_allocation"] += impact_allocation
        actions_dict[sym]["impact_risk"] += impact_risk

    def prioritize_low_risk(candidates: pd.DataFrame):
        return candidates.sort_values(by='beta').head(max_watchlist_suggestions)

    # --- Compute deltas ---
    def compute_delta(target_dict, actual_series):
        return {k: target_dict.get(k,0)-actual for k, actual in actual_series.items() if abs(target_dict.get(k,0)-actual) > threshold}

    asset_deltas = compute_delta(asset_targets.get("pre_retirement", {}), asset_weights)
    sector_deltas = compute_delta(sectors_config, sector_weights)
    industry_deltas = {}
    for (sec, ind), actual in industry_weights.items():
        target = industries_config.get(sec, {}).get(ind, 0)
        delta = target - actual
        if abs(delta) > threshold:
            industry_deltas[(sec, ind)] = delta

    # --- Allocation adjustments ---
    for t, delta in asset_deltas.items():
        syms = df[df['security_type']==t]['symbol'].unique()
        for sym in syms:
            if sym not in valid_syms:
                continue
            adj_pct = delta * (security_weights[sym]/asset_weights[t])
            reason = f"Adjust {t} allocation ({'increase' if adj_pct>0 else 'reduce'})"
            if abs(adj_pct) > 0.01:
                add_action(sym, adj_pct, adj_pct*total_value, reason, impact_allocation=abs(adj_pct))

    for s, delta in sector_deltas.items():
        syms = df[df['sector']==s]['symbol'].unique()
        if delta > 0 and len(syms)==0 and watchlist is not None:
            # underweight, buy from watchlist
            candidates = prioritize_low_risk(watchlist[watchlist['sector']==s])
            for idx, row in candidates.iterrows():
                adj_pct = delta / len(candidates)
                reason = f"Buy from watchlist to increase sector '{s}'"
                add_action(row['symbol'], adj_pct, adj_pct*total_value, reason, impact_allocation=abs(adj_pct))
        else:
            # adjust existing holdings
            for sym in syms:
                if sym not in current_syms:
                    continue
                adj_pct = delta * (security_weights[sym]/sector_weights[s])
                reason = f"Adjust sector '{s}' ({'increase' if adj_pct>0 else 'reduce'})"
                if abs(adj_pct) > 0.01:
                    add_action(sym, adj_pct, adj_pct*total_value, reason, impact_allocation=abs(adj_pct))

    for (sec, ind), delta in industry_deltas.items():
        syms = df[(df['sector']==sec) & (df['industry']==ind)]['symbol'].unique()
        if delta > 0 and len(syms)==0 and watchlist is not None:
            candidates = prioritize_low_risk(watchlist[(watchlist['sector']==sec) & (watchlist['industry']==ind)])
            for idx, row in candidates.iterrows():
                adj_pct = delta / len(candidates)
                reason = f"Buy from watchlist to increase industry '{sec} → {ind}'"
                add_action(row['symbol'], adj_pct, adj_pct*total_value, reason, impact_allocation=abs(adj_pct))
        else:
            for sym in syms:
                if sym not in current_syms:
                    continue
                adj_pct = delta * (security_weights[sym]/industry_weights[(sec,ind)])
                reason = f"Adjust industry '{sec} → {ind}' ({'increase' if adj_pct>0 else 'reduce'})"
                if abs(adj_pct) > 0.01:
                    add_action(sym, adj_pct, adj_pct*total_value, reason, impact_allocation=abs(adj_pct))

    # --- Security-level target adjustments ---
    security_config = safe_json_load(get_config("target_security_allocation"), {})
    security_deltas = compute_delta(security_config, security_weights)
    # compute_delta only sees currently-held securities (security_weights is
    # grouped from current holdings) — a security with a target but zero
    # current weight needs its own delta so it still gets suggested.
    for sym, target in security_config.items():
        if sym not in security_weights.index and sym in valid_syms and abs(target) > threshold:
            security_deltas[sym] = target

    for sym, delta in security_deltas.items():
        if sym not in valid_syms or abs(delta) <= 0.01:
            continue
        reason = f"Adjust '{sym}' toward its target weight ({'increase' if delta > 0 else 'reduce'})"
        add_action(sym, delta, delta * total_value, reason, impact_allocation=abs(delta))

    # --- Risk adjustments ---
    avg_risk = df['risk_pct'].mean()
    risk_threshold = avg_risk * 1.5
    for sym in df_current['symbol'].unique():
        if security_risk[sym] > risk_threshold:
            adj_pct = -min(security_weights[sym], 0.1)
            reason = f"Reduce '{sym}' due to high risk ({security_risk[sym]:.1%})"
            add_action(sym, adj_pct, adj_pct*total_value, reason, impact_risk=security_risk[sym])

    # --- Aggregate actions ---
    actions = []
    for sym, info in actions_dict.items():
        priority_score = info["impact_allocation"] + info["impact_risk"]
        actions.append({
            "symbol": sym,
            "pct_change": info["pct"],
            "market_value_change": info["market_value"],
            "reasons": info["reasons"],
            "impact_allocation": info["impact_allocation"],
            "impact_risk": info["impact_risk"],
            "priority_score": priority_score
        })

    actions.sort(key=lambda x: x["priority_score"], reverse=True)

    return {
        "asset_weights": asset_weights.to_dict(),
        "sector_weights": sector_weights.to_dict(),
        "industry_weights": industry_weights.to_dict(),
        "security_weights": security_weights.to_dict(),
        "security_risk": security_risk.to_dict(),
        "suggestions": actions,
        "details": df.to_dict(orient="records")
    }


def get_complete_taxonomy(holdings_df):
    """
    Returns taxonomy dict with Yahoo baseline + any extra sectors/industries found in holdings.
    """
    baseline = get_config("taxonomy")
    if isinstance(baseline, str): baseline = json.loads(baseline)
    taxonomy = baseline.copy()

    for _, row in holdings_df.iterrows():
        sec = str(row.get("sector", "")).strip().lower()
        ind = str(row.get("industry", "")).strip().lower()
        if not sec:
            continue
        if sec not in taxonomy:
            taxonomy[sec] = []
        if ind and ind not in taxonomy[sec]:
            taxonomy[sec].append(ind)

    return taxonomy
