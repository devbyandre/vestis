"""
tests/test_api.py

FastAPI endpoint tests — verify the api/main.py layer against a fresh SQLite
DB per test. This is the layer the React frontend actually talks to
(frontend/src/lib/api.js), and previously had zero test coverage: a change
to middleware.py/db_utils.py could silently break the shape a tab depends
on without any test failing. Covers every endpoint group api.js calls,
prioritising the mutating (POST/PUT/DELETE) endpoints.
"""

import json

import pytest

from tests.conftest import seed, seed_security


def _seed_transaction(db_path, ticker="AAPL", portfolio_id=1, qty=10, price=150.0,
                       tx_type="buy", fees=0.0, date="2023-06-01"):
    sec_id = seed_security(db_path, ticker)
    seed(
        db_path,
        "INSERT INTO transactions (portfolio_id, security_id, date, type, quantity, price, fees) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (portfolio_id, sec_id, date, tx_type, qty, price, fees),
    )
    return sec_id


# ═════════════════════════════════════════════════════════════════════════════
# Portfolios
# ═════════════════════════════════════════════════════════════════════════════

class TestPortfoliosAPI:
    def test_list_portfolios_includes_default(self, api_client):
        r = api_client.get("/portfolios")
        assert r.status_code == 200
        names = [p["name"] for p in r.json()]
        assert "Default" in names

    def test_create_portfolio(self, api_client):
        r = api_client.post("/portfolios", json={"name": "Satellite"})
        assert r.status_code == 200
        assert r.json()["name"] == "Satellite"
        names = [p["name"] for p in api_client.get("/portfolios").json()]
        assert "Satellite" in names

    def test_rename_portfolio(self, api_client):
        api_client.post("/portfolios", json={"name": "Growth"})
        r = api_client.put("/portfolios/rename", json={"old_name": "Growth", "new_name": "Growth2"})
        assert r.status_code == 200
        names = [p["name"] for p in api_client.get("/portfolios").json()]
        assert "Growth2" in names
        assert "Growth" not in names

    def test_delete_portfolio_reassigns(self, api_client):
        api_client.post("/portfolios", json={"name": "ToDelete"})
        r = api_client.request(
            "DELETE", "/portfolios", json={"name": "ToDelete", "reassign_to": "Default"}
        )
        assert r.status_code == 200
        names = [p["name"] for p in api_client.get("/portfolios").json()]
        assert "ToDelete" not in names

    def test_rename_unknown_portfolio_is_a_noop(self, api_client):
        # db.rename_portfolio doesn't check existence — renaming an unknown
        # portfolio updates 0 rows rather than erroring. Documenting current
        # behavior here rather than asserting a 4xx that doesn't happen.
        r = api_client.put(
            "/portfolios/rename", json={"old_name": "DoesNotExist", "new_name": "X"}
        )
        assert r.status_code == 200
        names = [p["name"] for p in api_client.get("/portfolios").json()]
        assert "X" not in names


# ═════════════════════════════════════════════════════════════════════════════
# Securities
# ═════════════════════════════════════════════════════════════════════════════

class TestSecuritiesAPI:
    def test_list_securities_empty(self, api_client):
        r = api_client.get("/securities")
        assert r.status_code == 200
        assert r.json() == []

    def test_get_security_symbols_includes_seeded(self, api_client, db_path):
        seed_security(db_path, "MSFT")
        r = api_client.get("/securities/symbols")
        assert r.status_code == 200
        assert "MSFT" in r.json()["symbols"]

    def test_get_unknown_security_404(self, api_client):
        r = api_client.get("/securities/DOES_NOT_EXIST")
        assert r.status_code == 404


# ═════════════════════════════════════════════════════════════════════════════
# Transactions
# ═════════════════════════════════════════════════════════════════════════════

class TestTransactionsAPI:
    def test_list_transactions_empty(self, api_client):
        r = api_client.get("/transactions")
        assert r.status_code == 200
        assert r.json() == []

    def test_add_transaction(self, api_client, db_path):
        seed_security(db_path, "AAPL")
        r = api_client.post("/transactions", json={
            "portfolio_id": 1, "symbol": "AAPL", "tx_date": "2023-06-01",
            "tx_type": "buy", "quantity": 10, "price": 150.0, "fees": 1.0,
        })
        assert r.status_code == 200
        rows = api_client.get("/transactions").json()
        assert len(rows) == 1
        assert rows[0]["symbol"] == "AAPL"
        # This field is populated via a securities_cache join that used to
        # raise UndefinedColumn on real Postgres — its mere presence here is
        # the regression guard, regardless of value (no cache row yet → None).
        assert "security_name" in rows[0]

    def test_transactions_summary_reflects_totals(self, api_client, db_path):
        _seed_transaction(db_path, "AAPL", qty=10, price=100.0, fees=1.0)
        r = api_client.get("/transactions/summary")
        assert r.status_code == 200
        body = r.json()
        assert body["count"] == 1
        assert body["total_buys"] == pytest.approx(1001.0)

    def test_edit_transaction(self, api_client, db_path):
        _seed_transaction(db_path, "AAPL", qty=10, price=100.0)
        tx_id = api_client.get("/transactions").json()[0]["id"]
        r = api_client.put(f"/transactions/{tx_id}", json={
            "tx_id": tx_id, "portfolio_id": 1, "symbol": "AAPL", "tx_date": "2023-07-01",
            "tx_type": "buy", "quantity": 20, "price": 160.0, "fees": 2.0,
        })
        assert r.status_code == 200
        updated = api_client.get("/transactions").json()[0]
        assert float(updated["quantity"]) == 20.0

    def test_delete_transaction(self, api_client, db_path):
        _seed_transaction(db_path, "AAPL")
        tx_id = api_client.get("/transactions").json()[0]["id"]
        r = api_client.delete(f"/transactions/{tx_id}")
        assert r.status_code == 200
        assert api_client.get("/transactions").json() == []


# ═════════════════════════════════════════════════════════════════════════════
# Holdings
# ═════════════════════════════════════════════════════════════════════════════

class TestHoldingsAPI:
    def test_snapshot_empty_portfolio(self, api_client):
        r = api_client.get("/holdings/snapshot")
        assert r.status_code == 200
        assert r.json() == []

    def test_snapshot_reflects_transaction(self, api_client, db_path):
        # Go through the real POST endpoint (not the raw _seed_transaction
        # helper) — holdings_timeseries is only populated via
        # mw.add_transaction's recompute step, which a raw INSERT skips.
        # recompute_holdings_timeseries also needs a price row to exist,
        # or it clears (rather than populates) the timeseries.
        sec_id = seed_security(db_path, "AAPL")
        seed(db_path,
             "INSERT INTO prices (security_id, date, open, high, low, close, adj_close, volume) "
             "VALUES (?, ?, 100.0, 100.0, 100.0, 100.0, 100.0, 1000)",
             (sec_id, "2023-06-01"))
        api_client.post("/transactions", json={
            "portfolio_id": 1, "symbol": "AAPL", "tx_date": "2023-06-01",
            "tx_type": "buy", "quantity": 10, "price": 100.0, "fees": 0.0,
        })
        r = api_client.get("/holdings/snapshot")
        assert r.status_code == 200
        rows = r.json()
        assert len(rows) == 1
        assert rows[0]["symbol"] == "AAPL"
        assert rows[0]["quantity"] == 10.0


# ═════════════════════════════════════════════════════════════════════════════
# Watchlist
# ═════════════════════════════════════════════════════════════════════════════

class TestWatchlistAPI:
    def test_watchlist_empty(self, api_client):
        r = api_client.get("/watchlist")
        assert r.status_code == 200
        assert r.json() == []

    def test_add_and_list_watchlist(self, api_client):
        r = api_client.post("/watchlist", json={"symbol": "NVDA", "name": "NVIDIA Corp"})
        assert r.status_code == 200
        symbols = api_client.get("/watchlist/symbols").json()["symbols"]
        assert "NVDA" in symbols

    def test_remove_from_watchlist(self, api_client):
        api_client.post("/watchlist", json={"symbol": "NVDA"})
        r = api_client.request("DELETE", "/watchlist", json={"symbol": "NVDA"})
        assert r.status_code == 200
        symbols = api_client.get("/watchlist/symbols").json()["symbols"]
        assert "NVDA" not in symbols


# ═════════════════════════════════════════════════════════════════════════════
# Alerts
# ═════════════════════════════════════════════════════════════════════════════

class TestAlertsAPI:
    def test_alerts_empty(self, api_client):
        r = api_client.get("/alerts")
        assert r.status_code == 200
        assert r.json() == []

    def test_create_list_edit_delete_alert(self, api_client, db_path):
        sec_id = seed_security(db_path, "TSLA")
        r = api_client.post("/alerts", json={
            "security_id": sec_id, "alert_type": "price",
            "params": {"threshold": 200.0, "direction": "above"},
        })
        assert r.status_code == 200
        alert_id = r.json()["id"]

        alerts = api_client.get("/alerts").json()
        assert len(alerts) == 1

        r = api_client.put(f"/alerts/{alert_id}", json={"active": False})
        assert r.status_code == 200
        alerts = api_client.get("/alerts", params={"active_only": False}).json()
        assert alerts[0]["active"] == 0

        r = api_client.delete(f"/alerts/{alert_id}")
        assert r.status_code == 200
        assert api_client.get("/alerts").json() == []


# ═════════════════════════════════════════════════════════════════════════════
# Analytics
# ═════════════════════════════════════════════════════════════════════════════

class TestAnalyticsAPI:
    def test_capital_gains_empty(self, api_client):
        r = api_client.get("/analytics/capital-gains")
        assert r.status_code == 200
        assert r.json() == []

    def test_dividends_empty(self, api_client):
        r = api_client.get("/analytics/dividends")
        assert r.status_code == 200
        assert r.json() == []

    def test_revenues_summary_shape(self, api_client):
        r = api_client.get("/analytics/revenues-summary")
        assert r.status_code == 200
        body = r.json()
        assert "total_gains" in body and "total_dividends" in body

    def test_indicators_404_for_unknown_symbol(self, api_client):
        r = api_client.get("/analytics/indicators/DOES_NOT_EXIST")
        assert r.status_code == 404

    def test_indicators_includes_cagr_calmar_treynor(self, api_client, db_path):
        sec_id = seed_security(db_path, "AAPL")
        seed(db_path,
             "INSERT INTO securities_cache (security_id, beta) VALUES (?, 1.25)", (sec_id,))
        for i in range(30):
            seed(db_path,
                 "INSERT INTO prices (security_id, date, open, high, low, close, adj_close, volume) "
                 "VALUES (?, date('now', '-' || (30 - ?) || ' days'), 100, 105, 95, ?, ?, 1000)",
                 (sec_id, i, 100 + i, 100 + i))
        r = api_client.get("/analytics/indicators/AAPL")
        assert r.status_code == 200
        metrics = r.json()["metrics"]
        assert "cagr" in metrics
        assert "calmar" in metrics
        assert "treynor" in metrics

    def test_rebalancing_with_holdings_returns_200(self, api_client, db_path):
        # Regression test: industry_weights is keyed by (sector, industry)
        # tuples internally, which crashed FastAPI's JSON encoder
        # (jsonable_encoder turns the tuple into a list, then can't use
        # that list as an output dict key) — only reproduces with a real
        # sector+industry present, which is why the empty-holdings test
        # above never caught it.
        sec_id = seed_security(db_path, "AAPL")
        seed(db_path,
             "INSERT INTO securities_cache (security_id, sector, industry, security_type) "
             "VALUES (?, 'Technology', 'Consumer Electronics', 'EQUITY')", (sec_id,))
        seed(db_path,
             "INSERT INTO prices (security_id, date, open, high, low, close, adj_close, volume) "
             "VALUES (?, '2023-06-01', 150, 150, 150, 150, 150, 1000)", (sec_id,))
        api_client.post("/transactions", json={
            "portfolio_id": 1, "symbol": "AAPL", "tx_date": "2023-06-01",
            "tx_type": "buy", "quantity": 10, "price": 150.0, "fees": 0.0,
        })
        r = api_client.get("/analytics/rebalancing")
        assert r.status_code == 200
        body = r.json()
        assert isinstance(body.get("industry_weights"), dict)
        for key in body["industry_weights"]:
            assert isinstance(key, str)

    def test_rebalancing_includes_security_level_target_suggestion(self, api_client, db_path):
        # target_security_allocation was UI-only in Streamlit (computed, then
        # silently never saved or read by suggest_rebalancing). This confirms
        # the fixed version actually feeds security-level targets into
        # rebalancing suggestions.
        held_id = seed_security(db_path, "AAPL")
        seed(db_path,
             "INSERT INTO securities_cache (security_id, sector, industry, security_type) "
             "VALUES (?, 'Technology', 'Consumer Electronics', 'EQUITY')", (held_id,))
        seed(db_path,
             "INSERT INTO prices (security_id, date, open, high, low, close, adj_close, volume) "
             "VALUES (?, '2023-06-01', 150, 150, 150, 150, 150, 1000)", (held_id,))
        api_client.post("/transactions", json={
            "portfolio_id": 1, "symbol": "AAPL", "tx_date": "2023-06-01",
            "tx_type": "buy", "quantity": 10, "price": 150.0, "fees": 0.0,
        })
        # Target AAPL at 0% (holding it entirely, so this should trigger a
        # large "reduce" suggestion) — bypasses the noisy asset/sector/
        # industry deltas that would otherwise also fire for this holding.
        api_client.put("/settings", json={"settings": {"target_security_allocation": {"AAPL": 0.0}}})

        r = api_client.get("/analytics/rebalancing")
        assert r.status_code == 200
        suggestions = r.json()["suggestions"]
        aapl = next(s for s in suggestions if s["symbol"] == "AAPL")
        assert any("target weight" in reason for reason in aapl["reasons"])


# ═════════════════════════════════════════════════════════════════════════════
# Planning
# ═════════════════════════════════════════════════════════════════════════════

class TestPlanningAPI:
    def test_kpis_empty(self, api_client):
        r = api_client.get("/planning/kpis")
        assert r.status_code == 200
        assert r.json() == []

    def test_kpis_includes_pb_ratio_and_temperature(self, api_client, db_path):
        # Regression test: /planning/kpis previously omitted pb_ratio and
        # Temperature (present in the near-identical GET /watchlist shape),
        # since it didn't run the snapshot through calc_security_KPIs.
        sec_id = seed_security(db_path, "AAPL")
        seed(db_path,
             "INSERT INTO prices (security_id, date, open, high, low, close, adj_close, volume) "
             "VALUES (?, '2023-06-01', 150, 150, 150, 150, 150, 1000)", (sec_id,))
        api_client.post("/transactions", json={
            "portfolio_id": 1, "symbol": "AAPL", "tx_date": "2023-06-01",
            "tx_type": "buy", "quantity": 10, "price": 150.0, "fees": 0.0,
        })
        rows = api_client.get("/planning/kpis").json()
        assert len(rows) == 1
        assert "pb_ratio" in rows[0]
        assert "Temperature" in rows[0]

    def test_taxonomy_is_dict(self, api_client):
        r = api_client.get("/planning/taxonomy")
        assert r.status_code == 200
        assert isinstance(r.json(), dict)


# ═════════════════════════════════════════════════════════════════════════════
# Settings
# ═════════════════════════════════════════════════════════════════════════════

class TestSettingsAPI:
    def test_get_settings_masks_secrets(self, api_client):
        r = api_client.get("/settings")
        assert r.status_code == 200
        body = r.json()
        assert "telegram_bot_token" not in body
        assert "telegram_bot_token_set" in body

    def test_update_settings_roundtrip(self, api_client):
        r = api_client.put("/settings", json={"settings": {"tax_rate": 0.30}})
        assert r.status_code == 200
        body = api_client.get("/settings").json()
        assert body["tax_rate"] == 0.30


# ═════════════════════════════════════════════════════════════════════════════
# Health
# ═════════════════════════════════════════════════════════════════════════════

class TestHealthAPI:
    def test_health_ok(self, api_client):
        r = api_client.get("/health")
        assert r.status_code == 200
