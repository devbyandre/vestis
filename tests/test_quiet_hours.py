"""Quiet hours: window maths and the hold-then-flush behaviour of the worker."""
from datetime import datetime, time

import pytest

from tests.conftest import seed_security


@pytest.fixture
def qh():
    from alerting import quiet_hours
    return quiet_hours


class TestWindow:
    def test_overnight_window_in_local_time(self, qh):
        # Europe/Berlin is UTC+2 in October: 22:00 local = 20:00 UTC
        s, e = time(22, 0), time(7, 0)
        assert qh.in_quiet_hours(datetime(2026, 10, 2, 20, 30), s, e, "Europe/Berlin") is True
        assert qh.in_quiet_hours(datetime(2026, 10, 2, 3, 0), s, e, "Europe/Berlin") is True    # 05:00 local
        assert qh.in_quiet_hours(datetime(2026, 10, 2, 5, 0), s, e, "Europe/Berlin") is False   # 07:00 local, end exclusive
        assert qh.in_quiet_hours(datetime(2026, 10, 2, 12, 0), s, e, "Europe/Berlin") is False

    def test_same_day_window(self, qh):
        s, e = time(12, 0), time(13, 0)
        assert qh.in_quiet_hours(datetime(2026, 10, 2, 12, 30), s, e, "UTC") is True
        assert qh.in_quiet_hours(datetime(2026, 10, 2, 13, 0), s, e, "UTC") is False

    def test_follows_daylight_saving(self, qh):
        s, e = time(22, 0), time(7, 0)
        # January: UTC+1, so 21:30 UTC is already 22:30 local
        assert qh.in_quiet_hours(datetime(2026, 1, 15, 21, 30), s, e, "Europe/Berlin") is True
        assert qh.in_quiet_hours(datetime(2026, 7, 15, 20, 30), s, e, "Europe/Berlin") is True
        assert qh.in_quiet_hours(datetime(2026, 7, 15, 19, 30), s, e, "Europe/Berlin") is False

    def test_equal_start_end_means_no_window(self, qh):
        assert qh.in_quiet_hours(datetime(2026, 10, 2, 3, 0), time(7, 0), time(7, 0), "UTC") is False

    def test_validators(self, qh):
        assert qh.is_valid_hhmm("07:30") and not qh.is_valid_hhmm("25:00") and not qh.is_valid_hhmm("abc")
        assert qh.is_valid_tz("Europe/Berlin") and not qh.is_valid_tz("Mars/Olympus")

    def test_disabled_flag_means_never_quiet(self, qh, monkeypatch):
        monkeypatch.setattr(qh, "get_config", lambda k: {"dnd": False}.get(k))
        assert qh.quiet_hours_active(datetime(2026, 10, 2, 22, 0)) is False

    def test_enabled_uses_configured_window(self, qh, monkeypatch):
        cfg = {"dnd": True, "quiet_hours_start": "22:00", "quiet_hours_end": "07:00", "timezone": "UTC"}
        monkeypatch.setattr(qh, "get_config", cfg.get)
        assert qh.quiet_hours_active(datetime(2026, 10, 2, 23, 0)) is True
        assert qh.quiet_hours_active(datetime(2026, 10, 2, 9, 0)) is False


class TestEngine:
    @pytest.fixture
    def setup(self, mw, alerting, db_path, monkeypatch):
        sec_id = seed_security(db_path, "TSM")
        mw.create_alert(sec_id, "pct_change", {"pct": 5, "direction": "down"})
        sent, cfg = [], {}
        eng = alerting.engine
        monkeypatch.setattr(eng, "get_config", lambda k: cfg.get(k))
        monkeypatch.setattr(eng, "set_config", lambda k, v: cfg.__setitem__(k, v))

        def fake_eval(alert):
            alert["_state"] = {"last_event": "2026-10-02"}
            alert["_detail"] = "-6.0% (100.00 → 94.00 USD)"
            return True
        monkeypatch.setattr(mw, "evaluate_alert", fake_eval)
        notify = lambda text, link=None: sent.append(text) or True
        return eng, mw, notify, sent, cfg

    def test_alert_inside_quiet_hours_is_held_not_sent(self, setup, monkeypatch):
        eng, mw, notify, sent, cfg = setup
        monkeypatch.setattr(eng, "quiet_hours_active", lambda: True)
        eng.run_immediate(notify)
        assert sent == []
        rows = mw.get_alert_history()
        assert [r["delivery"] for r in rows] == ["held"]
        assert rows[0]["detail"].startswith("-6.0%")

    def test_held_alerts_are_delivered_once_after_quiet_hours(self, setup, monkeypatch):
        eng, mw, notify, sent, cfg = setup
        monkeypatch.setattr(eng, "quiet_hours_active", lambda: True)
        eng.run_immediate(notify)

        monkeypatch.setattr(eng, "quiet_hours_active", lambda: False)
        monkeypatch.setattr(mw, "evaluate_alert", lambda a: False)   # nothing new fires
        eng.run_immediate(notify)
        assert len(sent) == 1
        assert "Held during quiet hours" in sent[0] and "TSM" in sent[0] and "-6.0%" in sent[0]
        assert cfg["last_held_flush"]

        eng.run_immediate(notify)
        assert len(sent) == 1   # not repeated

    def test_failed_delivery_keeps_alert_held_for_retry(self, setup, monkeypatch):
        eng, mw, notify, sent, cfg = setup
        monkeypatch.setattr(eng, "quiet_hours_active", lambda: True)
        eng.run_immediate(notify)
        monkeypatch.setattr(eng, "quiet_hours_active", lambda: False)
        monkeypatch.setattr(mw, "evaluate_alert", lambda a: False)
        eng.run_immediate(lambda text, link=None: False)
        assert "last_held_flush" not in cfg
        eng.run_immediate(notify)
        assert len(sent) == 1

    def test_alerts_outside_quiet_hours_are_sent_immediately(self, setup, monkeypatch):
        eng, mw, notify, sent, cfg = setup
        monkeypatch.setattr(eng, "quiet_hours_active", lambda: False)
        eng.run_immediate(notify)
        assert len(sent) == 1 and "Alert" in sent[0] and "Held" not in sent[0]
        assert [r["delivery"] for r in mw.get_alert_history()] == ["immediate"]

    def test_unsent_alert_is_not_logged_and_fires_again(self, setup, monkeypatch):
        eng, mw, notify, sent, cfg = setup
        monkeypatch.setattr(eng, "quiet_hours_active", lambda: False)
        eng.run_immediate(lambda text, link=None: False)
        assert mw.get_alert_history() == []
        eng.run_immediate(notify)
        assert len(sent) == 1
