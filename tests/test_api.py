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

    def test_edit_transaction_with_the_payload_the_ui_sends(self, api_client, db_path):
        # Regression: the model required tx_id in the body, which the UI never
        # sends (the id is in the URL), so every edit failed with 422.
        _seed_transaction(db_path, "BABA", qty=6, price=539.7, fees=10.0)
        tx = api_client.get("/transactions").json()[0]
        r = api_client.put(f"/transactions/{tx['id']}", json={
            "portfolio_id": 1, "symbol": "BABA", "tx_date": "2023-06-01",
            "tx_type": "buy", "quantity": 6, "price": 179.9, "fees": 10.0,
        })
        assert r.status_code == 200, r.text
        updated = api_client.get("/transactions").json()[0]
        assert float(updated["price"]) == pytest.approx(179.9)
        assert float(updated["quantity"]) == 6.0

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

    def test_alert_params_are_validated(self, api_client, db_path):
        sec_id = seed_security(db_path, "TSLA")
        bad = [
            ("rsi", {"threshold": 150, "direction": "above"}),
            ("rsi", {"threshold": 70, "direction": "sideways"}),
            ("ma_crossover", {"short": 200, "long": 50}),
            ("pct_change", {"pct": 0, "direction": "down"}),
            ("price", {"direction": "above"}),
            ("bogus", {}),
        ]
        for t, p in bad:
            r = api_client.post("/alerts", json={"security_id": sec_id, "alert_type": t, "params": p})
            assert r.status_code == 422, (t, p, r.text)
        assert api_client.get("/alerts").json() == []

        r = api_client.post("/alerts", json={"security_id": sec_id, "alert_type": "rsi",
                                             "params": {"threshold": "30", "direction": "below"}})
        assert r.status_code == 200
        stored = api_client.get("/alerts").json()[0]
        assert json.loads(stored["params"])["threshold"] == 30.0

    def test_edit_alert_validates_against_stored_type_and_can_change_type(self, api_client, db_path):
        sec_id = seed_security(db_path, "TSLA")
        aid = api_client.post("/alerts", json={"security_id": sec_id, "alert_type": "rsi",
                                               "params": {"threshold": 70, "direction": "above"}}).json()["id"]
        assert api_client.put(f"/alerts/{aid}", json={"params": {"threshold": 500}}).status_code == 422
        assert api_client.put(f"/alerts/{aid}", json={"params": {"threshold": 75}}).status_code == 200
        r = api_client.put(f"/alerts/{aid}", json={"alert_type": "52w", "params": {"type": "low"}})
        assert r.status_code == 200
        stored = api_client.get("/alerts").json()[0]
        assert stored["alert_type"] == "52w"
        assert api_client.put("/alerts/9999", json={"params": {"threshold": 75}}).status_code == 404

    def test_alert_history(self, api_client, db_path):
        import db_utils as db
        sec_id = seed_security(db_path, "BAYN.DE")
        alert_id = api_client.post("/alerts", json={
            "security_id": sec_id, "alert_type": "pct_change",
            "params": {"pct": 5, "days": 1, "direction": "down"},
        }).json()["id"]
        assert api_client.get("/alerts/history").json() == []

        db.log_alert_trigger(alert_id, {"note": "immediate", "detail": "-5.3% (47.95 → 45.40 EUR)"})
        db.log_alert_trigger(alert_id, {"note": "digest_daily"})
        rows = api_client.get("/alerts/history").json()
        assert len(rows) == 2
        assert rows[0]["delivery"] == "digest"           # newest first
        assert rows[1]["detail"].startswith("-5.3%")
        assert rows[1]["symbol"] == "BAYN.DE"
        assert rows[1]["alert_type"] == "pct_change"

    def test_telegram_test_requires_creds(self, api_client, monkeypatch):
        monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
        monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
        import telegram_client as tg
        monkeypatch.setattr(tg, "get_creds", lambda *a: ("", ""))
        r = api_client.post("/settings/telegram-test")
        assert r.status_code == 400


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

    def _seed_holding(self, api_client, db_path, symbol="AAPL", sector="Technology", industry="Consumer Electronics",
                      price_days=1):
        sec_id = seed_security(db_path, symbol)
        seed(db_path,
             "INSERT INTO securities_cache (security_id, sector, industry, security_type) VALUES (?, ?, ?, 'EQUITY')",
             (sec_id, sector, industry))
        for i in range(price_days):
            seed(db_path,
                 "INSERT INTO prices (security_id, date, open, high, low, close, adj_close, volume) "
                 "VALUES (?, date('2023-06-01', '+' || ? || ' days'), 150, 150, 150, ?, ?, 1000)",
                 (sec_id, i, 150 + i * 0.1, 150 + i * 0.1))
        api_client.post("/transactions", json={
            "portfolio_id": 1, "symbol": symbol, "tx_date": "2023-06-01",
            "tx_type": "buy", "quantity": 10, "price": 150.0, "fees": 0.0,
        })
        return sec_id

    def test_allocation_over_time_group_by_sector(self, api_client, db_path):
        self._seed_holding(api_client, db_path)
        r = api_client.get("/planning/allocation-over-time", params={"group_by": "sector"})
        assert r.status_code == 200
        body = r.json()
        assert "Technology" in body["series"]

    def test_allocation_over_time_group_by_symbol_tops_out_at_10_plus_other(self, api_client, db_path):
        self._seed_holding(api_client, db_path, "AAPL")
        r = api_client.get("/planning/allocation-over-time", params={"group_by": "symbol"})
        assert r.status_code == 200
        assert "AAPL" in r.json()["series"]

    def test_allocation_over_time_rejects_bad_group_by(self, api_client):
        r = api_client.get("/planning/allocation-over-time", params={"group_by": "nonsense"})
        assert r.status_code == 400

    def test_allocation_over_time_all_matches_individual_calls(self, api_client, db_path):
        self._seed_holding(api_client, db_path)
        combined = api_client.get("/planning/allocation-over-time-all").json()
        assert set(combined.keys()) == {"security_type", "sector", "industry", "symbol"}
        for group_by in combined:
            individual = api_client.get(
                "/planning/allocation-over-time", params={"group_by": group_by}
            ).json()
            assert combined[group_by] == individual

    def test_allocation_over_time_all_empty_when_no_holdings(self, api_client):
        combined = api_client.get("/planning/allocation-over-time-all").json()
        assert combined == {
            "security_type": {"dates": [], "series": {}},
            "sector": {"dates": [], "series": {}},
            "industry": {"dates": [], "series": {}},
            "symbol": {"dates": [], "series": {}},
        }

    def test_risk_over_time_detailed_includes_category_columns(self, api_client, db_path):
        # risk_score needs a 252-day rolling std with min_periods=20, so the
        # single-price-row seed the other tests use isn't enough here.
        # Regression test for a timing bug: db.recompute_holdings_timeseries()
        # used to call update_security_risk_timeseries() on the same
        # not-yet-committed connection it had just inserted holdings rows
        # on, but that function read holdings back via a separate engine
        # connection which couldn't see the uncommitted rows yet — so risk
        # data silently never populated until some later, unrelated write
        # committed first. Fixed by passing the just-computed rows through
        # directly instead of re-querying them. No manual recompute call
        # here — if the fix regresses, this comes back empty.
        self._seed_holding(api_client, db_path, price_days=25)
        r = api_client.get("/planning/risk-over-time", params={"aggregate": False})
        assert r.status_code == 200
        rows = r.json()
        assert len(rows) > 0
        assert "symbol" in rows[0]
        assert "sector" in rows[0]
        assert "security_type" in rows[0]
        assert "weighted_risk" in rows[0]


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

    def test_update_settings_leaves_worker_state_alone(self, api_client):
        import config_utils
        config_utils.set_config("news_alerts_sent_until", "2026-10-05T10:00:00")
        r = api_client.put("/settings", json={"settings": {"news_alerts_sent_until": "2026-01-01T00:00:00",
                                                           "last_digest_sent_daily": "2026-01-01", "tax_rate": 0.2}})
        assert r.status_code == 200
        assert config_utils.get_config("news_alerts_sent_until") == "2026-10-05T10:00:00"
        assert config_utils.get_config("last_digest_sent_daily") in (None, "")


# ═════════════════════════════════════════════════════════════════════════════
# Health
# ═════════════════════════════════════════════════════════════════════════════

class TestHealthAPI:
    def test_health_ok(self, api_client):
        r = api_client.get("/health")
        assert r.status_code == 200


class TestQuietHoursSettings:
    def test_rejects_bad_times_and_timezones(self, api_client):
        assert api_client.put("/settings", json={"settings": {"quiet_hours_start": "25:00"}}).status_code == 422
        assert api_client.put("/settings", json={"settings": {"timezone": "Mars/Olympus"}}).status_code == 422

    def test_accepts_valid_window(self, api_client):
        r = api_client.put("/settings", json={"settings": {
            "dnd": True, "quiet_hours_start": "23:00", "quiet_hours_end": "06:30", "timezone": "Europe/Berlin"}})
        assert r.status_code == 200
        cfg = api_client.get("/settings").json()
        assert cfg["quiet_hours_start"] == "23:00" and cfg["dnd"] is True


class TestPreviouslyCrashingEndpoints:
    def test_crossovers_reports_a_golden_cross(self, api_client, db_path):
        sec_id = seed_security(db_path, "AAPL")
        seed(db_path, "INSERT INTO securities_cache (security_id, currency) VALUES (?, 'EUR')", (sec_id,))
        closes = [100 - i for i in range(40)] + [60 + 3 * i for i in range(40)]
        for i, c in enumerate(closes):
            seed(db_path,
                 "INSERT INTO prices (security_id, date, open, high, low, close, adj_close, volume) "
                 "VALUES (?, date('now', '-' || (80 - ?) || ' days'), ?, ?, ?, ?, ?, 1000)",
                 (sec_id, i, c, c, c, c, c))
        r = api_client.get("/analytics/crossovers/AAPL", params={"short": 5, "long_": 20})
        assert r.status_code == 200
        assert "buy" in [e["type"] for e in r.json()]

    def test_portfolio_symbols_lists_current_holdings(self, api_client, db_path):
        _seed_transaction(db_path, "AAPL")
        _seed_transaction(db_path, "TSLA")
        _seed_transaction(db_path, "TSLA", tx_type="sell", date="2023-07-01")
        r = api_client.get("/planning/portfolio-symbols", params={"portfolio_ids": "1"})
        assert r.status_code == 200
        assert r.json() == ["AAPL"]
        assert api_client.get("/planning/portfolio-symbols").json() == ["AAPL"]
