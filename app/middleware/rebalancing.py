import json
from typing import Optional, Dict, Any, List
import pandas as pd

import db_utils as db
from config_utils import safe_json_load, get_config
from .holdings import get_latest_holdings_snapshot
from .watchlist import get_watchlist


ASSET_CLASS = {
    "EQUITY": "Equity", "ETF": "ETF", "MUTUALFUND": "Bond", "BOND": "Bond",
    "CRYPTOCURRENCY": "Crypto", "CURRENCY": "Cash", "COMMODITY": "Commodity",
}
_UNKNOWN = {"", "Unknown", "N/A", None}


def _fractions(raw) -> Dict[str, float]:
    """Target dict -> fractions summing to 1 ('Bonds' alias, whole percents tolerated)."""
    out: Dict[str, float] = {}
    for k, v in (raw or {}).items():
        try:
            v = float(v)
        except (TypeError, ValueError):
            continue
        if v > 0:
            out["Bond" if k == "Bonds" else k] = out.get("Bond" if k == "Bonds" else k, 0.0) + v
    total = sum(out.values())
    return {k: v / total for k, v in out.items()} if total > 0 else {}


def _spread(target: float, weights: pd.Series) -> Dict[str, float]:
    """Split a target across holdings in proportion to their current weights."""
    total = weights.sum()
    if total <= 0:
        return {k: target / len(weights) for k in weights.index} if len(weights) else {}
    return {k: target * w / total for k, w in weights.items()}


def suggest_rebalancing(
    portfolio_ids: Optional[List[int]] = None,
    retirement_year: int = 2047,
    threshold: float = 0.01,
    max_watchlist_suggestions: int = 3
) -> Dict[str, Any]:
    """Suggest trades that move the portfolio toward its targets.

    One target weight is derived per holding, top-down: asset class (pre- or
    post-retirement targets), then for single stocks the sector and industry
    targets (shares of the stock sleeve / of the sector), then each group's
    current internal mix. Explicit security targets override that weight.
    Suggestions are target minus current weight, so levels never add up.
    """
    snap = get_latest_holdings_snapshot(portfolio_ids=portfolio_ids, aggregate=True)
    if snap.empty:
        return {"message": "No holdings available", "suggestions": []}

    df = snap.copy()
    df["market_value"] = pd.to_numeric(df["market_value"], errors="coerce").fillna(0.0)
    df = df[df["market_value"] > 0].set_index("symbol")
    total_value = df["market_value"].sum() or 1.0
    df["weight"] = df["market_value"] / total_value
    df["asset_class"] = df["security_type"].map(lambda t: ASSET_CLASS.get(str(t).upper(), "Other"))
    for col in ("sector", "industry"):
        df[col] = df[col].where(~df[col].isin(_UNKNOWN), "Unknown")

    asset_cfg = safe_json_load(get_config("asset_allocation_targets"), {})
    phase = "post_retirement" if pd.Timestamp.now().year >= int(retirement_year) else "pre_retirement"
    class_targets = _fractions(asset_cfg.get(phase) or asset_cfg.get("current"))
    sector_targets = _fractions(safe_json_load(get_config("target_sector_allocation"), {}))
    industry_cfg = safe_json_load(get_config("target_industry_allocation"), {}) or {}
    security_cfg = {k: float(v) for k, v in (safe_json_load(get_config("target_security_allocation"), {}) or {}).items()
                    if v is not None and str(v) != ""}

    class_weights = df.groupby("asset_class")["weight"].sum()
    if not class_targets:
        class_targets = class_weights.to_dict()

    target: Dict[str, float] = {}
    reasons: Dict[str, List[str]] = {}
    missing: List[dict] = []

    def note(sym, text):
        reasons.setdefault(sym, []).append(text)

    for cls in sorted(set(class_weights.index) | set(class_targets)):
        cls_target = class_targets.get(cls, 0.0)
        cls_actual = float(class_weights.get(cls, 0.0))
        members = df[df["asset_class"] == cls]
        if members.empty:
            if cls_target > 0:
                missing.append({"label": f"{cls} (no holding)", "pct": cls_target,
                                "reason": f"{cls} target is {cls_target:.0%} but nothing is held"})
            continue
        if abs(cls_target - cls_actual) >= threshold:
            for sym in members.index:
                note(sym, f"{cls} is {cls_actual:.0%} of the portfolio vs target {cls_target:.0%}")

        if cls != "Equity" or not sector_targets:
            target.update(_spread(cls_target, members["weight"]))
            continue

        known = members[members["sector"] != "Unknown"]
        unknown = members[members["sector"] == "Unknown"]
        known_share = known["weight"].sum() / members["weight"].sum()
        target.update(_spread(cls_target * (1 - known_share), unknown["weight"]))
        stock_budget = cls_target * known_share
        stock_total = known["weight"].sum() or 1.0
        for sector in sorted(set(known["sector"]) | set(sector_targets)):
            s_target = stock_budget * sector_targets.get(sector, 0.0)
            s_members = known[known["sector"] == sector]
            s_actual = s_members["weight"].sum() / stock_total
            if s_members.empty:
                if s_target > 0:
                    missing.append({"label": f"{sector} (no holding)", "pct": s_target, "sector": sector,
                                    "reason": f"Sector '{sector}' target is {sector_targets[sector]:.0%} of stocks but nothing is held"})
                continue
            if abs(sector_targets.get(sector, 0.0) - s_actual) >= threshold:
                for sym in s_members.index:
                    note(sym, f"Sector '{sector}' is {s_actual:.0%} of stocks vs target {sector_targets.get(sector, 0.0):.0%}")
            ind_targets = _fractions(industry_cfg.get(sector))
            if not ind_targets:
                target.update(_spread(s_target, s_members["weight"]))
                continue
            for ind in sorted(set(s_members["industry"]) | set(ind_targets)):
                i_members = s_members[s_members["industry"] == ind]
                i_target = s_target * ind_targets.get(ind, 0.0)
                i_actual = i_members["weight"].sum() / (s_members["weight"].sum() or 1.0)
                if i_members.empty:
                    if i_target > 0:
                        missing.append({"label": f"{ind} (no holding)", "pct": i_target, "sector": sector,
                                        "industry": ind,
                                        "reason": f"Industry '{sector} → {ind}' target is {ind_targets[ind]:.0%} of the sector but nothing is held"})
                    continue
                if abs(ind_targets.get(ind, 0.0) - i_actual) >= threshold:
                    for sym in i_members.index:
                        note(sym, f"Industry '{ind}' is {i_actual:.0%} of {sector} vs target {ind_targets.get(ind, 0.0):.0%}")
                target.update(_spread(i_target, i_members["weight"]))

    # Explicit security targets win; everything else is rescaled to fill the rest.
    overrides = {sym: w for sym, w in security_cfg.items() if sym in df.index or w > 0}
    if overrides:
        free = [s for s in target if s not in overrides]
        rest = sum(target[s] for s in free)
        budget = max(0.0, 1.0 - sum(overrides.values()))
        for s in free:
            target[s] = target[s] * budget / rest if rest > 0 else 0.0
        for sym, w in overrides.items():
            target[sym] = w
            note(sym, f"Your target weight for {sym} is {w:.0%}")

    # Latest annualised volatility per holding, to flag outsized risk contributors.
    risk = {}
    try:
        risk_df = db.get_security_risk_timeseries_many(df["security_id"].astype(int).unique())
        if not risk_df.empty:
            latest = risk_df.sort_values("date").groupby("security_id")["risk_score"].last()
            risk = {sym: float(latest.get(int(sid))) for sym, sid in df["security_id"].items()
                    if int(sid) in latest.index}
    except Exception:
        risk = {}
    contrib = {sym: risk[sym] * df.at[sym, "weight"] for sym in risk}
    total_contrib = sum(contrib.values()) or 1.0
    risk_share = {sym: c / total_contrib for sym, c in contrib.items()}

    actions = []
    for sym in sorted(set(target) | set(df.index)):
        actual = float(df["weight"].get(sym, 0.0))
        tgt = float(target.get(sym, 0.0))
        delta = tgt - actual
        r_share = risk_share.get(sym, 0.0)
        risky = actual > 0 and r_share > 1.5 * actual and r_share >= threshold
        if risky:
            note(sym, f"Volatility {risk[sym]:.0%}: {r_share:.0%} of portfolio risk from {actual:.0%} of its value")
        if abs(delta) < threshold and not risky:
            continue
        actions.append({
            "symbol": sym,
            "current_weight": actual,
            "target_weight": tgt,
            "pct_change": delta if abs(delta) >= threshold else 0.0,
            "market_value_change": delta * total_value if abs(delta) >= threshold else 0.0,
            "reasons": reasons.get(sym) or ["Follows its group's target"],
            "impact_allocation": abs(delta),
            "impact_risk": r_share if risky else 0.0,
            "priority_score": abs(delta) + (0.5 * r_share if risky else 0.0),
        })

    watch = None
    for gap in missing:
        cands = []
        if gap.get("sector"):
            if watch is None:
                watch = get_watchlist()
            if not watch.empty and "sector" in watch:
                pool = watch[watch["sector"] == gap["sector"]]
                if gap.get("industry"):
                    pool = pool[pool["industry"] == gap["industry"]]
                if "beta" in pool:
                    pool = pool.sort_values("beta", na_position="last")
                cands = pool["symbol"].head(max_watchlist_suggestions).tolist()
        if cands:
            for sym in cands:
                share = gap["pct"] / len(cands)
                actions.append({"symbol": sym, "current_weight": 0.0, "target_weight": share,
                                "pct_change": share, "market_value_change": share * total_value,
                                "reasons": [gap["reason"] + " — candidate from your watchlist"],
                                "impact_allocation": share, "impact_risk": 0.0, "priority_score": share})
        else:
            actions.append({"symbol": gap["label"], "current_weight": 0.0, "target_weight": gap["pct"],
                            "pct_change": gap["pct"], "market_value_change": gap["pct"] * total_value,
                            "reasons": [gap["reason"]], "impact_allocation": gap["pct"],
                            "impact_risk": 0.0, "priority_score": gap["pct"]})

    actions.sort(key=lambda a: a["priority_score"], reverse=True)

    stocks = df[(df["asset_class"] == "Equity") & (df["sector"] != "Unknown")]
    stock_total = stocks["weight"].sum() or 1.0
    return {
        "asset_weights": class_weights.to_dict(),
        "sector_weights": (stocks.groupby("sector")["weight"].sum() / stock_total).to_dict(),
        "industry_weights": (stocks.groupby(["sector", "industry"])["weight"].sum() / stock_total).to_dict(),
        "security_weights": df["weight"].to_dict(),
        "security_risk": risk_share,
        "target_phase": phase,
        "suggestions": actions,
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
