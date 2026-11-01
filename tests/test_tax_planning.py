"""Tax-aware selling (Planning tab): German loss pots, allowance, fund
partial exemption, crypto holding period, and the three suggestion types."""
import os
import sys
from datetime import date

import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "app")))


@pytest.fixture
def tp(api_client):
    # api_client binds middleware to a fresh test DB; we feed data directly
    import middleware.tax_planning as tp
    return tp


class TestTaxModel:
    def test_stock_losses_only_offset_stock_gains(self, tp):
        m = tp.TaxModel(rate=0.25, allowance=0, stock=-500, other=1000)
        assert m.base() == 1000          # stock loss can't touch the fund gain
        assert m.tax() == 250

    def test_general_losses_offset_stock_gains(self, tp):
        m = tp.TaxModel(rate=0.25, allowance=0, stock=1000, other=-400)
        assert m.base() == 600

    def test_allowance_after_loss_offsetting(self, tp):
        m = tp.TaxModel(rate=0.25, allowance=1000, stock=800, other=700)
        assert m.tax() == pytest.approx(125)
        assert m.tax(add_stock=-500) == 0

    def test_carry_forward_per_pot(self, tp):
        by_year = {2024: {"stock": -300, "other": 100}, 2025: {"stock": 100, "other": -50}}
        cs, co = tp._carry_forward(by_year, 2026)
        assert cs == pytest.approx(-200)  # stock loss carried, 2024 general income not reduced by it
        assert co == pytest.approx(-50)   # 2025 general loss carried separately

    def test_categories(self, tp):
        assert tp._category("ETF") == tp.FUND
        assert tp._category("CRYPTOCURRENCY") == tp.CRYPTO
        assert tp._category(None) == tp.STOCK   # unknown → stricter stock pot


def _setup(tp, monkeypatch, transactions, prices, types, dividends=None, settings=None):
    """transactions: (portfolio, symbol, date, type, qty, price); prices: {symbol: EUR}."""
    tx = pd.DataFrame([
        {"id": i, "portfolio_id": p, "symbol": s, "date": d, "type": t, "quantity": q, "price": pr, "fees": 0.0}
        for i, (p, s, d, t, q, pr) in enumerate(transactions)
    ])
    held = {}
    for p, s, _, t, q, _ in transactions:
        held[(p, s)] = held.get((p, s), 0) + (q if t == "buy" else -q)
    snapshot = pd.DataFrame([
        {"portfolio_id": p, "symbol": s, "quantity": q, "market_value": q * prices[s]}
        for (p, s), q in held.items() if q > 0
    ])
    cfg = {"tax_rate": 0.25, "tax_allowance": 1000, "fund_partial_exemption": 0.3, **(settings or {})}
    monkeypatch.setattr(tp, "list_transactions", lambda ids=None: tx)
    monkeypatch.setattr(tp, "get_latest_holdings_snapshot", lambda portfolio_ids=None, aggregate=False: snapshot)
    monkeypatch.setattr(tp, "calc_dividends_for_portfolio",
                        lambda portfolio_ids=None: pd.DataFrame(dividends or [], columns=["symbol", "total", "year"]))
    monkeypatch.setattr(tp, "get_config", lambda k: cfg.get(k))
    monkeypatch.setattr(tp.db, "get_security_cache_many",
                        lambda syms: {s: {"security_type": types.get(s), "longName": s} for s in syms})
    monkeypatch.setattr(tp.db, "list_portfolios", lambda: pd.DataFrame([{"id": 1, "name": "Broker"}, {"id": 2, "name": "Crypto"}]))


TODAY = date(2026, 11, 1)


class TestPlan:
    def test_allowance_plan_realises_exactly_the_remaining_allowance(self, tp, monkeypatch):
        # 400 € dividends used → 600 € left; ETF gains count at 70%
        _setup(tp, monkeypatch,
               [(1, "WORLD", "2020-01-02", "buy", 100, 50.0)],
               prices={"WORLD": 100.0}, types={"WORLD": "ETF"},
               dividends=[{"symbol": "AAA", "total": 400.0, "year": 2026}])
        r = tp.plan(today=TODAY)
        assert r["status"]["allowance_left"] == pytest.approx(600)
        ap = r["allowance_plan"]
        assert ap["taxable_gain"] == pytest.approx(600, abs=0.5)
        assert ap["gain"] == pytest.approx(600 / 0.7, abs=1)      # partial exemption
        assert ap["sells"][0]["quantity"] == pytest.approx(600 / 0.7 / 50, abs=0.02)

    def test_fifo_oldest_lot_first(self, tp, monkeypatch):
        _setup(tp, monkeypatch,
               [(1, "AAA", "2019-01-02", "buy", 10, 10.0), (1, "AAA", "2025-01-02", "buy", 10, 90.0)],
               prices={"AAA": 100.0}, types={"AAA": "EQUITY"})
        sells = tp.plan(today=TODAY)["allowance_plan"]["sells"]
        assert sells[0]["oldest_lot"] == "2019-01-02"
        # whole 2019 lot (10 × 90 € gain), then 10 of the 2025 lot (10 × 10 €) = allowance
        assert sells[0]["quantity"] == pytest.approx(20, abs=0.01)
        assert sells[0]["gain"] == pytest.approx(1000, abs=0.5)

    def test_loss_plan_ignores_stock_losses_against_fund_gains(self, tp, monkeypatch):
        # 3,000 € realised ETF gain this year → taxable; a stock loss can't help,
        # an ETF loss can.
        _setup(tp, monkeypatch,
               [(1, "WORLD", "2020-01-02", "buy", 100, 50.0), (1, "WORLD", "2026-03-01", "sell", 100, 92.86),
                (1, "LOSER", "2024-01-02", "buy", 10, 100.0),
                (1, "EM", "2024-01-02", "buy", 100, 30.0)],
               prices={"WORLD": 90.0, "LOSER": 40.0, "EM": 20.0}, types={"WORLD": "ETF", "LOSER": "EQUITY", "EM": "ETF"})
        r = tp.plan(today=TODAY)
        assert r["status"]["tax_so_far"] > 0
        lp = r["loss_plan"]
        assert r["status"]["tax_so_far"] == pytest.approx(500, abs=0.5)   # (3,000 − 1,000) × 25%
        assert {s["symbol"] for s in lp["sells"]} == {"EM"}
        assert lp["tax_saved"] == pytest.approx(175, abs=0.05)             # 1,000 € loss × 70% × 25%

    def test_loss_plan_sells_only_what_brings_tax_to_zero(self, tp, monkeypatch):
        _setup(tp, monkeypatch,
               [(1, "WORLD", "2020-01-02", "buy", 100, 50.0), (1, "WORLD", "2026-03-01", "sell", 100, 92.86),
                (1, "EM", "2024-01-02", "buy", 1000, 30.0)],
               prices={"EM": 20.0}, types={"WORLD": "ETF", "EM": "ETF"})
        lp = tp.plan(today=TODAY)["loss_plan"]
        assert lp["tax_saved"] == pytest.approx(500, abs=0.05)
        # 2,000 € taxable to offset ÷ 70% ÷ 10 € loss per share ≈ 286 shares
        assert lp["sells"][0]["quantity"] == pytest.approx(2000 / 0.7 / 10, abs=0.5)

    def test_cash_plan_beats_selling_evenly(self, tp, monkeypatch):
        _setup(tp, monkeypatch,
               [(1, "WINNER", "2018-01-02", "buy", 100, 10.0), (1, "FLAT", "2024-01-02", "buy", 100, 100.0)],
               prices={"WINNER": 100.0, "FLAT": 100.0}, types={"WINNER": "EQUITY", "FLAT": "EQUITY"})
        cp = tp.plan(today=TODAY, target_amount=8000)["cash_plan"]
        assert cp["proceeds"] == pytest.approx(8000)
        assert cp["tax"] == pytest.approx(0)                 # FLAT has no gain
        assert cp["naive_tax"] > 0
        assert {s["symbol"] for s in cp["sells"]} == {"FLAT"}

    def test_crypto_listed_with_holding_period_not_suggested(self, tp, monkeypatch):
        _setup(tp, monkeypatch,
               [(2, "BTC-EUR", "2018-09-04", "buy", 1, 5000.0), (2, "BTC-EUR", "2026-06-01", "buy", 1, 90000.0),
                (2, "BTC-EUR", "2024-02-29", "buy", 0.5, 50000.0)],
               prices={"BTC-EUR": 95000.0}, types={"BTC-EUR": "CRYPTOCURRENCY"})
        r = tp.plan(today=TODAY, target_amount=1000)
        assert r["allowance_plan"] is None and r["cash_plan"]["sells"] == []
        free = {c["bought"]: (c["tax_free_from"], c["tax_free_now"]) for c in r["crypto"]}
        assert free["2018-09-04"] == ("2019-09-05", True)
        assert free["2026-06-01"] == ("2027-06-02", False)
        assert free["2024-02-29"] == ("2025-03-02", True)    # leap day


def test_endpoint_with_empty_portfolio(api_client):
    r = api_client.get("/planning/tax-sales", params={"target_amount": 1000})
    assert r.status_code == 200
    body = r.json()
    assert body["status"]["allowance_left"] > 0
    assert body["cash_plan"]["sells"] == []
