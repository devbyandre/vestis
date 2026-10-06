"""alerting package: digest wiring and the telegram_worker entry point."""
import sys

import pandas as pd

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
        assert "TSM" in sent[0] and "• *?*" not in sent[0]     # regression: security showed as "?"
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


class TestNewsUpdates:
    def _setup(self, mw, alerting, db_path, monkeypatch, cfg):
        held = seed_security(db_path, "ALV.DE")
        seed(db_path, "INSERT INTO securities_cache (security_id, longName) VALUES (?, 'Allianz SE')", (held,))
        seed(db_path, "INSERT INTO transactions (portfolio_id, security_id, date, type, quantity, price) "
                      "VALUES (1, ?, '2024-01-02', 'buy', 5, 100)", (held,))
        nu = alerting.news_updates
        monkeypatch.setattr(nu, "get_config", lambda k: cfg.get(k))
        monkeypatch.setattr(nu, "set_config", lambda k, v: cfg.__setitem__(k, v))
        monkeypatch.setattr(nu, "quiet_hours_active", lambda: False)
        return held, nu

    def _store(self, mw, held, rows, fetched_at):
        mw.db.upsert_news(held, [{"uid": u, "title": t, "publisher": "Wire", "url": f"https://x/{u}",
                                  "published_at": "2026-10-05T08:00:00", "sentiment": s} for u, t, s in rows],
                          fetched_at)

    def test_off_by_default(self, mw, alerting, db_path, monkeypatch):
        cfg, sent = {}, []
        held, nu = self._setup(mw, alerting, db_path, monkeypatch, cfg)
        assert nu.send_news_alerts(lambda t, l=None: sent.append(t) or True) == 0 and not sent

    def test_sends_only_new_strong_headlines_once(self, mw, alerting, db_path, monkeypatch):
        cfg, sent = {"news_alerts": True}, []
        held, nu = self._setup(mw, alerting, db_path, monkeypatch, cfg)
        notify = lambda t, l=None: sent.append(t) or True
        self._store(mw, held, [("old", "Allianz record profit", 0.9)], "2026-10-05T07:00:00")
        nu.send_news_alerts(notify, now=pd.Timestamp("2026-10-05T07:30:00"))    # first run: no backlog
        assert not sent and cfg["news_alerts_sent_until"] == "2026-10-05T07:30:00"

        self._store(mw, held, [("a", "Allianz beats estimates", 0.8), ("b", "Allianz board meets", 0.0)],
                    "2026-10-05T08:15:00")
        assert nu.send_news_alerts(notify) == 1
        assert "Allianz beats estimates" in sent[0] and "board meets" not in sent[0]
        assert "*Allianz*" in sent[0]                       # short company name, not the ticker
        assert nu.send_news_alerts(notify) == 0 and len(sent) == 1   # not repeated

    def test_quiet_hours_and_failed_sends_keep_items_for_later(self, mw, alerting, db_path, monkeypatch):
        cfg = {"news_alerts": True, "news_alerts_sent_until": "2026-10-05T00:00:00"}
        held, nu = self._setup(mw, alerting, db_path, monkeypatch, cfg)
        self._store(mw, held, [("a", "Allianz shares plunge", -0.8)], "2026-10-05T08:15:00")
        monkeypatch.setattr(nu, "quiet_hours_active", lambda: True)
        assert nu.send_news_alerts(lambda t, l=None: True) == 0
        monkeypatch.setattr(nu, "quiet_hours_active", lambda: False)
        assert nu.send_news_alerts(lambda t, l=None: False) == 0                 # send failed
        assert cfg["news_alerts_sent_until"] == "2026-10-05T00:00:00"
        sent = []
        assert nu.send_news_alerts(lambda t, l=None: sent.append(t) or True) == 1
        assert "📉" in sent[0]

    def test_digest_lists_strongest_headline_per_holding(self, mw, alerting, db_path, monkeypatch):
        held, nu = self._setup(mw, alerting, db_path, monkeypatch, {})
        recent = (pd.Timestamp.utcnow().replace(tzinfo=None) - pd.Timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S")
        mw.db.upsert_news(held, [
            {"uid": "a", "title": "Allianz beats estimates", "publisher": "Wire", "url": "https://x/a",
             "published_at": recent, "sentiment": 0.8},
            {"uid": "b", "title": "Allianz gains", "publisher": "Wire", "url": "https://x/b",
             "published_at": recent, "sentiment": 0.5},
        ], recent)
        lines = nu.news_digest_lines(pd.Timestamp.utcnow().replace(tzinfo=None) - pd.Timedelta(days=1))
        assert lines[0].startswith("📰") and len(lines) == 3      # header, one item, blank
        assert "[Allianz beats estimates](https://x/a)" in lines[1]
