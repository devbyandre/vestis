"""Tax-aware selling for German private investors (Abgeltungsteuer).

Rules modelled — simplified, the page says so:
- FIFO per broker account (= Vestis portfolio): you choose the account and
  security, but within an account the oldest shares are always sold first.
- Two loss pots: losses on single stocks only offset gains on stocks; other
  losses (funds, …) offset everything. Losses carry forward per pot.
- Yearly allowance (Sparer-Pauschbetrag, setting `tax_allowance`) on the
  income left after loss offsetting.
- Equity funds/ETFs: a share of gains, losses and distributions is tax-free
  (Teilfreistellung, setting `fund_partial_exemption`, default 30%).
- Crypto is a private sale (§23 EStG): tax-free after holding > 1 year,
  otherwise taxed at the personal income-tax rate — kept out of the
  suggestions and only listed.
Not modelled: Vorabpauschale, church tax beyond the configured rate,
foreign withholding tax, broker fees on hypothetical sales.
"""
from datetime import date
from typing import Optional

import pandas as pd

import db_utils as db
from config_utils import get_config
from .holdings import get_latest_holdings_snapshot
from .revenues import calc_dividends_for_portfolio, run_fifo
from .transactions import list_transactions

STOCK, FUND, OTHER, CRYPTO = "stock", "fund", "other", "crypto"
_EPS = 1e-6


def _category(security_type: Optional[str]) -> str:
    t = (security_type or "").upper()
    if t in ("ETF", "MUTUALFUND"):
        return FUND
    if t == "CRYPTOCURRENCY":
        return CRYPTO
    # Unknown types count as stocks: the stricter loss pot, so losses are
    # never assumed to offset more than they legally can.
    return STOCK if t in ("EQUITY", "") else OTHER


def _cfg_float(key, default):
    try:
        v = get_config(key)
        return float(v) if v not in (None, "") else default
    except Exception:
        return default


class TaxModel:
    """Tax for the current year given the realised income so far, with
    hypothetical extra stock-pot / general-pot amounts on top."""

    def __init__(self, rate, allowance, stock, other, carry_stock=0.0, carry_other=0.0):
        self.rate, self.allowance = rate, allowance
        self.stock, self.other = stock, other
        self.carry_stock, self.carry_other = carry_stock, carry_other

    def base(self, add_stock=0.0, add_other=0.0):
        s = self.stock + add_stock + self.carry_stock
        o = self.other + add_other + self.carry_other
        # Negative stock balance stays in the stock pot; general losses may
        # offset stock gains.
        return max(0.0, max(s, 0.0) + o)

    def tax(self, add_stock=0.0, add_other=0.0):
        return max(0.0, self.base(add_stock, add_other) - self.allowance) * self.rate


def _carry_forward(by_year: dict, upto_year: int):
    """Loss carry-forward per pot into `upto_year` from all earlier years."""
    cs = co = 0.0
    for y in sorted(by_year):
        if y >= upto_year:
            break
        s = by_year[y]["stock"] + cs
        o = by_year[y]["other"] + co
        cs = min(s, 0.0)
        total = max(s, 0.0) + o
        co = min(total, 0.0)
    return cs, co


def _contribution(category, gain, exemption):
    """(stock pot, general pot) amounts a realised gain/loss adds."""
    if category == STOCK:
        return gain, 0.0
    if category == FUND:
        return 0.0, gain * (1 - exemption)
    if category == OTHER:
        return 0.0, gain
    return 0.0, 0.0


def _realised_by_year(realised, dividends, categories, exemption):
    by_year = {}
    for r in realised:
        cat = categories.get(r["symbol"], STOCK)
        if cat == CRYPTO:
            continue
        s, o = _contribution(cat, r["profit"], exemption)
        y = by_year.setdefault(int(r["year"]), {"stock": 0.0, "other": 0.0, "dividends": 0.0})
        y["stock"] += s
        y["other"] += o
    if dividends is not None and not dividends.empty:
        for _, d in dividends.iterrows():
            cat = categories.get(d["symbol"], STOCK)
            if cat == CRYPTO:
                continue
            amount = float(d["total"]) * ((1 - exemption) if cat == FUND else 1.0)
            y = by_year.setdefault(int(d["year"]), {"stock": 0.0, "other": 0.0, "dividends": 0.0})
            y["other"] += amount          # dividends are never in the stock pot
            y["dividends"] += amount
    return by_year


class Position:
    """One security in one account: open lots oldest first, sold strictly in order."""

    def __init__(self, pid, symbol, lots, price, category, exemption):
        self.pid, self.symbol, self.price, self.category = pid, symbol, price, category
        self.exemption = exemption
        # cost per share includes the buy fee share
        self.lots = [{"qty": l["qty"], "cost": l["price"] + (l["fees"] / l["qty"] if l["qty"] else 0.0),
                      "date": pd.Timestamp(l["date"]).date()} for l in lots]
        self.idx, self.used = 0, 0.0      # current lot, qty already taken from it

    @property
    def qty(self):
        return sum(l["qty"] for l in self.lots)

    def next_segment(self):
        while self.idx < len(self.lots):
            lot = self.lots[self.idx]
            left = lot["qty"] - self.used
            if left > _EPS:
                return lot, left
            self.idx, self.used = self.idx + 1, 0.0
        return None, 0.0

    def take(self, qty):
        self.used += qty

    def contribution(self, lot, qty):
        return _contribution(self.category, qty * (self.price - lot["cost"]), self.exemption)


def _sell_rows(taken, positions_meta):
    """Merge taken segments into one row per position."""
    rows = {}
    for (pid, sym), lot, qty, price in taken:
        r = rows.setdefault((pid, sym), {
            **positions_meta[(pid, sym)], "quantity": 0.0, "proceeds": 0.0, "cost_basis": 0.0,
            "gain": 0.0, "oldest_lot": lot["date"].isoformat(),
        })
        r["quantity"] += qty
        r["proceeds"] += qty * price
        r["cost_basis"] += qty * lot["cost"]
        r["gain"] += qty * (price - lot["cost"])
    return sorted(rows.values(), key=lambda r: -r["proceeds"])


def _fill(positions, pick_key, limit_fn, max_steps=2000):
    """Greedy: repeatedly take the best next FIFO segment across positions.

    state = {"stock": added stock pot, "other": added general pot, "proceeds": €}
    pick_key(pos, lot, qty_left, state) -> sortable key, or None if not eligible.
    limit_fn(pos, lot, qty_left, state) -> qty to take (0 = skip this position).
    Returns (taken segments, state).
    """
    state = {"stock": 0.0, "other": 0.0, "proceeds": 0.0}
    taken, blocked = [], set()
    for _ in range(max_steps):
        best = None
        for p in positions:
            if id(p) in blocked:
                continue
            lot, left = p.next_segment()
            if lot is None:
                continue
            key = pick_key(p, lot, left, state)
            if key is not None and (best is None or key < best[0]):
                best = (key, p, lot, left)
        if best is None:
            break
        _, p, lot, left = best
        q = limit_fn(p, lot, left, state)
        if q <= _EPS:
            blocked.add(id(p))
            continue
        ds, do = p.contribution(lot, q)
        state["stock"] += ds
        state["other"] += do
        state["proceeds"] += q * p.price
        p.take(q)
        taken.append(((p.pid, p.symbol), lot, q, p.price))
        if q < left - _EPS:
            break                               # limit reached inside a lot
    return taken, state


def _max_qty(cond, left):
    """Largest q in [0, left] with cond(q) true (cond monotone, true at 0)."""
    if cond(left):
        return left
    lo, hi = 0.0, left
    for _ in range(40):
        mid = (lo + hi) / 2
        lo, hi = (mid, hi) if cond(mid) else (lo, mid)
    return lo


def plan(portfolio_ids=None, target_amount: Optional[float] = None, today: Optional[date] = None):
    today = today or date.today()
    year = today.year
    rate = _cfg_float("tax_rate", 0.26375)
    allowance = _cfg_float("tax_allowance", 1000.0)
    exemption = _cfg_float("fund_partial_exemption", 0.30)

    tx = list_transactions(portfolio_ids)
    realised, open_lots = run_fifo(tx)
    snapshot = get_latest_holdings_snapshot(portfolio_ids=portfolio_ids, aggregate=False)
    symbols = {sym for (_, sym) in open_lots} | {r["symbol"] for r in realised}
    cache = db.get_security_cache_many(symbols)
    categories = {s: _category((cache.get(s) or {}).get("security_type")) for s in symbols}
    names = {s: (cache.get(s) or {}).get("longName") or (cache.get(s) or {}).get("shortName") or s for s in symbols}
    portfolio_names = {int(r["id"]): r["name"] for _, r in db.list_portfolios().iterrows()}

    dividends = calc_dividends_for_portfolio(portfolio_ids=portfolio_ids)
    by_year = _realised_by_year(realised, dividends, categories, exemption)
    carry_s, carry_o = _carry_forward(by_year, year)
    cur = by_year.get(year, {"stock": 0.0, "other": 0.0, "dividends": 0.0})
    model = TaxModel(rate, allowance, cur["stock"], cur["other"], carry_s, carry_o)
    tax_now = model.tax()
    base_now = model.base()

    # Current EUR price per position from the holdings snapshot
    prices = {}
    if snapshot is not None and not snapshot.empty:
        for _, r in snapshot.iterrows():
            q = float(r.get("quantity") or 0)
            if q > _EPS and r.get("market_value") is not None:
                prices[(int(r["portfolio_id"]), r["symbol"])] = float(r["market_value"]) / q

    def positions(include=lambda cat: cat != CRYPTO):
        out = []
        for (pid, sym), lots in open_lots.items():
            price = prices.get((int(pid), sym))
            cat = categories.get(sym, STOCK)
            if price and include(cat):
                out.append(Position(int(pid), sym, lots, price, cat, exemption))
        return out

    meta = {(int(pid), sym): {"portfolio_id": int(pid), "portfolio": portfolio_names.get(int(pid), str(pid)),
                              "symbol": sym, "name": names.get(sym, sym), "category": categories.get(sym, STOCK),
                              "price": prices.get((int(pid), sym))}
            for (pid, sym) in open_lots}

    def tax_after(state, p, lot, q):
        ds, do = p.contribution(lot, q)
        return model.tax(state["stock"] + ds, state["other"] + do)

    def taxable_ratio(p, lot):
        s, o = p.contribution(lot, 1.0)
        return (s + o) / p.price

    # ── 1. Use the rest of this year's allowance (sell & rebuy) ──────────────
    allowance_plan = None
    if model.tax() < _EPS and allowance - base_now > 1:
        def key(p, lot, left, state):
            # gains only; most taxable gain per € traded first → fewest sales
            return -taxable_ratio(p, lot) if p.price > lot["cost"] else None

        def limit(p, lot, left, state):
            return _max_qty(lambda q: tax_after(state, p, lot, q) < 0.005, left)
        taken, st = _fill(positions(), key, limit)
        sells = _sell_rows(taken, meta)
        if sells:
            realised_taxable = model.base(st["stock"], st["other"]) - base_now
            allowance_plan = {
                "sells": sells,
                "gain": sum(r["gain"] for r in sells),
                "taxable_gain": realised_taxable,
                "future_tax_saved": realised_taxable * rate,
            }

    # ── 2. Harvest losses against gains already taxed this year ──────────────
    loss_plan = None
    if tax_now > _EPS:
        def key(p, lot, left, state):
            if p.price >= lot["cost"]:
                return None
            saving = model.tax(state["stock"], state["other"]) - tax_after(state, p, lot, left)
            if saving <= _EPS:
                return None                     # e.g. stock loss but no stock gains to offset
            return -saving / (left * p.price)   # most tax saved per € sold

        def limit(p, lot, left, state):
            if model.tax(state["stock"], state["other"]) < 0.01:
                return 0.0
            # only as much as it takes to bring this year's tax to zero
            return _max_qty(lambda q: tax_after(state, p, lot, q) > 0.005, left)
        taken, st = _fill(positions(), key, limit)
        sells = _sell_rows(taken, meta)
        if sells:
            loss_plan = {
                "sells": sells,
                "loss": sum(r["gain"] for r in sells),
                "tax_saved": tax_now - model.tax(st["stock"], st["other"]),
            }

    # ── 3. Raise a cash amount with the least tax ────────────────────────────
    cash_plan = None
    if target_amount and target_amount > 0:
        total_value = sum(p.qty * p.price for p in positions())

        def key(p, lot, left, state):
            # least extra tax per € raised; among tax-free options the one
            # using the least allowance
            extra = tax_after(state, p, lot, left) - model.tax(state["stock"], state["other"])
            return (extra / (left * p.price), taxable_ratio(p, lot))

        def limit(p, lot, left, state):
            return min(left, max(0.0, (target_amount - state["proceeds"]) / p.price))
        taken, st = _fill(positions(), key, limit)
        sells = _sell_rows(taken, meta)
        proceeds = sum(r["proceeds"] for r in sells)
        tax = model.tax(st["stock"], st["other"]) - tax_now

        # Baseline: sell the same share of every position (FIFO within each)
        naive_tax = None
        if total_value > 0:
            frac = min(1.0, target_amount / total_value)
            ns = no = 0.0
            for p in positions():
                left_total = p.qty * frac
                for lot in p.lots:
                    q = min(lot["qty"], left_total)
                    if q <= _EPS:
                        break
                    cs, co = p.contribution(lot, q)
                    ns, no = ns + cs, no + co
                    left_total -= q
            naive_tax = model.tax(ns, no) - tax_now
        cash_plan = {
            "target": target_amount,
            "proceeds": proceeds,
            "shortfall": max(0.0, target_amount - proceeds),
            "gain": sum(r["gain"] for r in sells),
            "tax": tax,
            "effective_rate": tax / proceeds if proceeds else 0.0,
            "naive_tax": naive_tax,
            "sells": sells,
        }

    # ── Crypto: when does each lot become tax-free? ──────────────────────────
    crypto = []
    for p in positions(include=lambda cat: cat == CRYPTO):
        for lot in p.lots:
            d = lot["date"]
            try:
                anniversary = d.replace(year=d.year + 1)
            except ValueError:                  # bought on 29 Feb
                anniversary = date(d.year + 1, 3, 1)
            # "more than one year": tax-free from the day after the anniversary
            free_from = anniversary + pd.Timedelta(days=1).to_pytimedelta()
            crypto.append({
                "portfolio": portfolio_names.get(p.pid, str(p.pid)), "symbol": p.symbol, "name": names.get(p.symbol, p.symbol),
                "quantity": lot["qty"], "bought": lot["date"].isoformat(), "value": lot["qty"] * p.price,
                "gain": lot["qty"] * (p.price - lot["cost"]),
                "tax_free_from": free_from.isoformat(), "tax_free_now": free_from <= today,
            })
    crypto.sort(key=lambda c: c["tax_free_from"])

    return {
        "year": year,
        "tax_rate": rate,
        "allowance": allowance,
        "fund_partial_exemption": exemption,
        "status": {
            "stock_pot": cur["stock"],
            "general_pot": cur["other"],
            "dividends_taxable": cur["dividends"],
            "carry_stock": carry_s,
            "carry_general": carry_o,
            "taxable_base": base_now,
            "allowance_used": min(base_now, allowance),
            "allowance_left": max(0.0, allowance - base_now),
            "tax_so_far": tax_now,
        },
        "allowance_plan": allowance_plan,
        "loss_plan": loss_plan,
        "cash_plan": cash_plan,
        "crypto": crypto,
    }
