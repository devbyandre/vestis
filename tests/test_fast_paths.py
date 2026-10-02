"""Performance-oriented read paths: latest snapshot, holdings cache, risk join."""
import pandas as pd

from tests.conftest import seed, seed_security


def _tx(db_path, sec, kind, qty, date="2024-01-02", pid=1):
    seed(db_path, "INSERT INTO transactions (portfolio_id, security_id, date, type, quantity, price) "
                  "VALUES (?, ?, ?, ?, ?, 10)", (pid, sec, date, kind, qty))


def _ht(db_path, sec, date, qty, mv, pid=1):
    seed(db_path, "INSERT INTO holdings_timeseries (date, portfolio_id, security_id, quantity, market_value, cost_basis) "
                  "VALUES (?, ?, ?, ?, ?, 100)", (date, pid, sec, qty, mv))


class TestLatestSnapshot:
    def test_each_security_uses_its_own_last_row_and_sold_positions_drop(self, mw, db_path):
        held, lagging, sold = (seed_security(db_path, t) for t in ("HELD", "LAG", "SOLD"))
        for sec in (held, lagging, sold):
            _tx(db_path, sec, "buy", 5)
        _tx(db_path, sold, "sell", 5, "2024-02-01")
        _ht(db_path, held, "2024-03-01", 5, 50)
        _ht(db_path, held, "2024-03-05", 5, 60)
        _ht(db_path, lagging, "2024-03-02", 5, 70)       # price not refreshed since
        _ht(db_path, sold, "2024-01-30", 5, 80)           # last row is pre-sell

        snap = mw.get_latest_holdings_snapshot().set_index("symbol")

        assert set(snap.index) == {"HELD", "LAG"}
        assert snap.loc["HELD", "market_value"] == 60
        assert snap.loc["LAG", "market_value"] == 70

    def test_filters_apply(self, mw, db_path):
        a, b = seed_security(db_path, "AAA"), seed_security(db_path, "BBB")
        for sec in (a, b):
            _tx(db_path, sec, "buy", 1)
            _ht(db_path, sec, "2024-03-01", 1, 10)
        assert list(mw.get_latest_holdings_snapshot(symbols=["BBB"])["symbol"]) == ["BBB"]


class TestHoldingsCache:
    def test_repeat_reads_share_one_query_and_writes_invalidate(self, db, db_path, monkeypatch):
        sec = seed_security(db_path, "AAA")
        _ht(db_path, sec, "2024-03-01", 1, 10)
        calls = []
        real = db.holdings._query_holdings_timeseries
        monkeypatch.setattr(db.holdings, "_query_holdings_timeseries",
                            lambda *a, **k: calls.append(1) or real(*a, **k))

        first = db.get_holdings_timeseries()
        first["market_value"] = 999            # callers mutate their copy
        again = db.get_holdings_timeseries()
        assert len(calls) == 1 and again["market_value"].iloc[0] == 10

        db.insert_holdings_timeseries([("2024-03-02", 1, sec, 1.0, 20.0, 100.0)])
        assert len(db.get_holdings_timeseries()) == 2 and len(calls) == 2

    def test_cache_can_be_disabled(self, db, db_path, monkeypatch):
        monkeypatch.setattr(db.holdings, "_TS_TTL", 0)
        sec = seed_security(db_path, "AAA")
        _ht(db_path, sec, "2024-03-01", 1, 10)
        assert len(db.get_holdings_timeseries()) == 1
        _ht(db_path, sec, "2024-03-02", 1, 20)
        assert len(db.get_holdings_timeseries()) == 2


class TestRiskJoin:
    def test_each_holding_row_gets_its_securitys_nearest_risk(self, db, db_path):
        a, b = seed_security(db_path, "AAA"), seed_security(db_path, "BBB")
        for sec, risk in ((a, 0.2), (b, 0.4)):
            _ht(db_path, sec, "2024-03-01", 1, 10)
            _ht(db_path, sec, "2024-03-02", 1, 10)
            seed(db_path, "INSERT INTO security_risk_timeseries VALUES (?, ?, ?, 10, ?)",
                 ("2024-03-01", sec, risk, risk))
        agg = db.get_portfolio_risk_timeseries()
        assert list(agg["date"].dt.strftime("%Y-%m-%d")) == ["2024-03-01", "2024-03-02"]
        assert agg["weighted_risk"].round(6).tolist() == [0.6, 0.6]

        det = db.get_portfolio_risk_timeseries_detailed()
        assert set(det["symbol"]) == {"AAA", "BBB"} and len(det) == 4

    def test_no_risk_data_gives_empty_frames(self, db, db_path):
        sec = seed_security(db_path, "AAA")
        _ht(db_path, sec, "2024-03-01", 1, 10)
        assert db.get_portfolio_risk_timeseries().empty
        assert db.get_portfolio_risk_timeseries_detailed().empty


class TestDividends:
    def test_shares_are_the_position_held_on_each_payment_date(self, mw, db_path):
        sec = seed_security(db_path, "DIV")
        seed(db_path, "INSERT OR IGNORE INTO portfolios (id, name) VALUES (2, 'Second')")
        _tx(db_path, sec, "buy", 10, "2023-01-10")
        _tx(db_path, sec, "sell", 4, "2023-06-01")
        _tx(db_path, sec, "buy", 3, "2023-02-01", pid=2)
        for d, amt in (("2023-01-05", 1.0), ("2023-03-01", 0.5), ("2024-03-01", 0.25)):
            seed(db_path, "INSERT INTO dividends (security_id, date, dividend) VALUES (?, ?, ?)", (sec, d, amt))

        out = mw.calc_dividends_for_portfolio()
        got = {(r.date, r.portfolio_id): r.shares for r in out.itertuples()}

        assert got == {("2023-03-01", 1): 10, ("2023-03-01", 2): 3,
                       ("2024-03-01", 1): 6, ("2024-03-01", 2): 3}   # nothing held on 2023-01-05
        only_2024 = mw.calc_dividends_for_portfolio(year=2024)
        assert set(only_2024["year"]) == {2024} and only_2024["total"].sum() == 0.25 * 9
