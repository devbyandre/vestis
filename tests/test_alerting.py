"""alerting package: digest wiring and the telegram_worker entry point."""
import sys

from tests.conftest import seed, seed_security


class TestDigest:
    def test_digest_lists_alerts_logged_for_that_mode_and_records_send_time(self, mw, alerting, db_path, monkeypatch):
        sec_id = seed_security(db_path, "TSM")
        aid = mw.create_alert(sec_id, "pct_change", {"pct": 5, "direction": "down"}, notify_mode="digest_daily")
        mw.log_trigger(aid, {"note": "digest_daily", "detail": "-6.0% (100.00 → 94.00 USD)"})
        cfg, sent = {}, []
        monkeypatch.setattr(alerting.digest, "get_config", lambda k: cfg.get(k))
        monkeypatch.setattr(alerting.digest, "set_config", lambda k, v: cfg.__setitem__(k, v))

        alerting.digest.send_digest(lambda text, link=None: sent.append(text) or True, "daily")

        assert len(sent) == 1
        assert "Alerts since last digest" in sent[0] and "-6.0%" in sent[0]
        assert "last_digest_sent_daily" in cfg

    def test_failed_send_does_not_advance_the_digest_window(self, mw, alerting, monkeypatch):
        cfg = {}
        monkeypatch.setattr(alerting.digest, "get_config", lambda k: cfg.get(k))
        monkeypatch.setattr(alerting.digest, "set_config", lambda k, v: cfg.__setitem__(k, v))
        alerting.digest.send_digest(lambda text, link=None: False, "daily")
        assert cfg == {}


class TestWorkerEntryPoint:
    def test_notifier_is_none_without_credentials(self, mw, alerting, monkeypatch):
        monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
        monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
        sys.modules.pop("telegram_worker", None)
        import telegram_worker
        monkeypatch.setattr(telegram_worker.tg, "get_creds", lambda *a, **k: ("", ""))
        assert telegram_worker.make_notifier() is None

    def test_notifier_sends_with_button(self, mw, alerting, monkeypatch):
        sys.modules.pop("telegram_worker", None)
        import telegram_worker
        calls = []
        monkeypatch.setattr(telegram_worker.tg, "get_creds", lambda *a, **k: ("tok", "chat"))
        monkeypatch.setattr(telegram_worker.tg, "send_message",
                            lambda tok, chat, text, **k: calls.append((tok, chat, text, k.get("link"))) or True)
        notify = telegram_worker.make_notifier()
        assert notify("hi", ("Open", "https://x")) is True
        assert calls == [("tok", "chat", "hi", ("Open", "https://x"))]


class TestSecurityCacheRow:
    def _cache(self, db_path, sec_id, earnings, currency="USD"):
        seed(db_path, "INSERT INTO securities_cache (security_id, earningsTimestamp, currency) VALUES (?, ?, ?)",
             (sec_id, earnings, currency))

    def test_row_is_a_dict_and_unknown_id_is_empty(self, mw, db_path):
        sec_id = seed_security(db_path, "AAPL")
        assert mw.get_security_cache_row(sec_id)["currency"] is None
        assert mw.get_security_cache_row(99999) == {}
        self._cache(db_path, sec_id, None, "EUR")
        assert mw.get_security_cache_row(sec_id)["currency"] == "EUR"

    def test_earnings_date_accepts_iso_epoch_and_missing(self, mw):
        import pandas as pd
        assert mw.earnings_date({}) is None
        assert mw.earnings_date({"earningsTimestamp": float("nan")}) is None
        assert mw.earnings_date({"earningsTimestamp": "2026-10-05T20:00:00+00:00"}) == pd.Timestamp("2026-10-05 20:00")
        assert mw.earnings_date({"earningsTimestamp": 1768915800}) == pd.Timestamp("2026-01-20 13:30")

    def test_earnings_soon_fires_once_per_date(self, mw, db_path):
        import pandas as pd
        sec_id = seed_security(db_path, "AAPL")
        when = (pd.Timestamp.utcnow() + pd.Timedelta(days=2, hours=1)).isoformat()
        self._cache(db_path, sec_id, when)
        for i in range(30):
            d = (pd.Timestamp.utcnow().normalize() - pd.Timedelta(days=30 - i)).strftime("%Y-%m-%d")
            seed(db_path, "INSERT INTO prices (security_id, date, open, high, low, close, adj_close, volume) "
                 "VALUES (?, ?, 100, 101, 99, 100, 100, 1000)", (sec_id, d))
        aid = mw.create_alert(sec_id, "earnings_soon", {"days": 3})
        alert = {**mw.get_alerts().set_index("id").loc[aid].to_dict(), "id": aid}
        assert mw.evaluate_alert(alert) is True
        assert "earnings on" in alert["_detail"]
        mw.save_alert_state(alert)
        again = {**mw.get_alerts().set_index("id").loc[aid].to_dict(), "id": aid}
        assert mw.evaluate_alert(again) is False

    def test_currency_comes_from_the_cache_row(self, mw, db_path):
        sec_id = seed_security(db_path, "SAP")
        self._cache(db_path, sec_id, None, "EUR")
        assert mw.alerts._currency({"security_id": sec_id}) == "EUR"
