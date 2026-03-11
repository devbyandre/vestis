"""
tests/test_smoke.py

Smoke tests — verify that the app can be imported and key modules initialise
without crashing.  Does NOT start a live HTTP server (no Streamlit runtime needed).

What it checks:
  1. All Python modules import cleanly (no circular imports, missing __init__, etc.)
  2. db_init.py creates the full schema without errors on a fresh SQLite DB
  3. The Streamlit app module can be parsed and loaded (catches broken top-level code)
  4. The critical runtime path (db → middleware) initialises correctly
"""

import sys
import os
import types
import sqlite3
import importlib
import pytest
import pandas as pd

# ── path setup ────────────────────────────────────────────────────────────────
APP_DIR = os.path.join(os.path.dirname(__file__), "..", "app")
sys.path.insert(0, os.path.abspath(APP_DIR))


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _fresh_db(tmp_path):
    """Return path to a fresh SQLite database for the smoke test."""
    path = str(tmp_path / "smoke.db")
    os.environ["DATABASE_URL"] = f"sqlite:///{path}"
    return path


def _stub_streamlit():
    """
    Return a minimal streamlit stub so app_streamlit.py can be imported
    without a running Streamlit server.  We only stub what the top-level
    module scope uses; actual widget behaviour is not tested here.
    """
    st = types.ModuleType("streamlit")

    # Common display helpers
    for attr in [
        "title", "header", "subheader", "write", "markdown",
        "caption", "divider", "spinner", "info", "warning",
        "error", "success", "toast", "balloons", "snow",
    ]:
        setattr(st, attr, lambda *a, **kw: None)

    # Layout helpers that return context managers or column tuples
    class _Noop:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def __call__(self, *a, **kw): return self
        # common column/tab access
        def __iter__(self): return iter([self, self, self, self])

    for attr in ["sidebar", "expander", "container", "empty",
                 "columns", "tabs", "popover", "form"]:
        setattr(st, attr, _Noop())

    # Input widgets — return sensible defaults
    st.text_input = lambda *a, **kw: kw.get("value", "")
    st.number_input = lambda *a, **kw: float(kw.get("value", 0))
    st.selectbox = lambda *a, **kw: (kw.get("options") or [None])[0]
    st.multiselect = lambda *a, **kw: []
    st.checkbox = lambda *a, **kw: bool(kw.get("value", False))
    st.radio = lambda *a, **kw: (kw.get("options") or [None])[0]
    st.slider = lambda *a, **kw: kw.get("value", 0)
    st.date_input = lambda *a, **kw: None
    st.button = lambda *a, **kw: False
    st.form_submit_button = lambda *a, **kw: False
    st.file_uploader = lambda *a, **kw: None
    st.data_editor = lambda data, **kw: data
    st.dataframe = lambda *a, **kw: None
    st.table = lambda *a, **kw: None
    st.metric = lambda *a, **kw: None
    st.plotly_chart = lambda *a, **kw: None
    st.altair_chart = lambda *a, **kw: None
    st.line_chart = lambda *a, **kw: None
    st.bar_chart = lambda *a, **kw: None
    st.image = lambda *a, **kw: None
    st.code = lambda *a, **kw: None
    st.json = lambda *a, **kw: None

    # Session state
    class _SessionState(dict):
        def __getattr__(self, key):
            return self.get(key)
        def __setattr__(self, key, val):
            self[key] = val
    st.session_state = _SessionState()

    # Page config (must be first call in many apps)
    st.set_page_config = lambda *a, **kw: None

    # Caching
    st.cache_data = lambda f=None, **kw: (f if f else lambda fn: fn)
    st.cache_resource = lambda f=None, **kw: (f if f else lambda fn: fn)

    # Rerun / stop
    class _RerunException(Exception): pass
    class _StopException(Exception): pass
    st.rerun = lambda: None
    st.stop  = lambda: None
    st.experimental_rerun = lambda: None

    # Switch page (multi-page apps)
    st.switch_page = lambda *a, **kw: None
    st.navigation = lambda pages, **kw: _Noop()
    st.Page = lambda *a, **kw: _Noop()

    return st


def _patch_modules(monkeypatch, db_path):
    """Register all necessary stubs in sys.modules before importing the app."""

    # ── streamlit ──
    monkeypatch.setitem(sys.modules, "streamlit", _stub_streamlit())

    # ── config_utils: point to our temp DB ──
    fake_cfg = types.ModuleType("config_utils")
    fake_cfg.get_config = lambda key: db_path if key == "db_path" else None
    fake_cfg.set_config = lambda key, val: None
    fake_cfg.get_all_config = lambda: {}
    fake_cfg.safe_json_load = lambda v, default=None: default or {}
    monkeypatch.setitem(sys.modules, "config_utils", fake_cfg)

    # ── data_fetcher ──
    fake_fetcher = types.ModuleType("data_fetcher")
    fake_fetcher.fetch_and_store_lazy = lambda *a, **kw: None
    fake_fetcher.run_full_fetch = lambda *a, **kw: None
    monkeypatch.setitem(sys.modules, "data_fetcher", fake_fetcher)

    # ── yfinance ──
    fake_yf = types.ModuleType("yfinance")
    fake_yf.Ticker = lambda *a, **kw: types.SimpleNamespace(
        info={}, financials=pd.DataFrame(),
        balance_sheet=pd.DataFrame(), cashflow=pd.DataFrame(),
        dividends=pd.Series(dtype=float),
    )
    fake_yf.download = lambda *a, **kw: pd.DataFrame()
    monkeypatch.setitem(sys.modules, "yfinance", fake_yf)

    # ── scipy ──
    try:
        import scipy.signal  # noqa
    except ImportError:
        fake_scipy = types.ModuleType("scipy")
        fake_signal = types.ModuleType("scipy.signal")
        import numpy as np
        fake_signal.argrelextrema = lambda arr, cmp, order=5: (np.array([], dtype=int),)
        fake_scipy.signal = fake_signal
        monkeypatch.setitem(sys.modules, "scipy", fake_scipy)
        monkeypatch.setitem(sys.modules, "scipy.signal", fake_signal)

    # ── plotly ──
    for mod_name in ["plotly", "plotly.graph_objects", "plotly.express",
                     "plotly.subplots", "plotly.graph_objs"]:
        if mod_name not in sys.modules:
            fake = types.ModuleType(mod_name)
            fake.Figure = lambda *a, **kw: types.SimpleNamespace(
                update_layout=lambda **kw: None,
                add_trace=lambda *a, **kw: None,
            )
            fake.scatter = lambda *a, **kw: fake.Figure()
            fake.bar = lambda *a, **kw: fake.Figure()
            fake.line = lambda *a, **kw: fake.Figure()
            fake.pie = lambda *a, **kw: fake.Figure()
            fake.Scatter = lambda *a, **kw: None
            fake.Bar = lambda *a, **kw: None
            fake.make_subplots = lambda *a, **kw: fake.Figure()
            monkeypatch.setitem(sys.modules, mod_name, fake)

    # ── requests ──
    if "requests" not in sys.modules:
        fake_req = types.ModuleType("requests")
        fake_req.get = lambda *a, **kw: types.SimpleNamespace(json=lambda: {}, text="", status_code=200)
        fake_req.post = fake_req.get
        monkeypatch.setitem(sys.modules, "requests", fake_req)


# ═════════════════════════════════════════════════════════════════════════════
# 1. Module import smoke tests
# ═════════════════════════════════════════════════════════════════════════════

class TestModuleImports:
    """Each module should be importable without side-effects."""

    def _clean_import(self, module_name, monkeypatch, db_path):
        for mod in list(sys.modules.keys()):
            if mod in (module_name, "db_utils", "config_utils",
                       "data_fetcher", "middleware"):
                monkeypatch.delitem(sys.modules, mod, raising=False)
        _patch_modules(monkeypatch, db_path)
        return importlib.import_module(module_name)

    def test_db_utils_imports(self, monkeypatch, tmp_path):
        db_path = _fresh_db(tmp_path)
        _patch_modules(monkeypatch, db_path)
        monkeypatch.delitem(sys.modules, "db_utils", raising=False)
        import db_utils  # noqa
        assert hasattr(db_utils, "get_engine")

    def test_config_utils_imports(self, monkeypatch, tmp_path):
        # Use the real config_utils (it only reads a JSON file)
        monkeypatch.delitem(sys.modules, "config_utils", raising=False)
        import config_utils  # noqa
        assert callable(config_utils.get_config)

    def test_middleware_imports(self, monkeypatch, tmp_path):
        db_path = _fresh_db(tmp_path)
        self._clean_import("middleware", monkeypatch, db_path)

    def test_telegram_worker_imports(self, monkeypatch, tmp_path):
        db_path = _fresh_db(tmp_path)
        _patch_modules(monkeypatch, db_path)
        monkeypatch.delitem(sys.modules, "telegram_worker", raising=False)
        import telegram_worker  # noqa


# ═════════════════════════════════════════════════════════════════════════════
# 2. DB Init smoke test
# ═════════════════════════════════════════════════════════════════════════════

class TestDBInit:
    def test_schema_created_successfully(self, monkeypatch, tmp_path):
        db_path = _fresh_db(tmp_path)
        _patch_modules(monkeypatch, db_path)

        # Clear cached modules so db_init picks up the fresh DB
        for mod in ["db_utils", "setup.db_init"]:
            monkeypatch.delitem(sys.modules, mod, raising=False)

        from setup import db_init
        importlib.reload(db_init)
        db_init.init_db()

        # Verify expected tables exist
        conn = sqlite3.connect(db_path)
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        conn.close()

        expected = {
            "portfolios", "securities", "transactions", "prices",
            "holdings_timeseries", "alerts", "alerts_log",
            "securities_cache", "fx_rates", "dividends", "financials",
            "valuations", "security_risk_timeseries",
        }
        missing = expected - tables
        assert not missing, f"Missing tables after db_init: {missing}"

    def test_default_portfolio_created(self, monkeypatch, tmp_path):
        db_path = _fresh_db(tmp_path)
        _patch_modules(monkeypatch, db_path)
        for mod in ["db_utils", "setup.db_init"]:
            monkeypatch.delitem(sys.modules, mod, raising=False)
        from setup import db_init
        importlib.reload(db_init)
        db_init.init_db()

        conn = sqlite3.connect(db_path)
        row = conn.execute("SELECT name FROM portfolios WHERE id=1").fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "Default"


# ═════════════════════════════════════════════════════════════════════════════
# 3. Streamlit app import smoke test
# ═════════════════════════════════════════════════════════════════════════════

class TestStreamlitSmoke:
    """
    Import app_streamlit.py and verify it doesn't crash at module scope.

    The real Streamlit functions are all stubbed to no-ops so no server
    is needed.  This catches:
      - Syntax errors in the app file
      - Missing imports / typos in module-level code
      - Exceptions raised during module initialisation
    """

    def test_app_module_imports_without_error(self, monkeypatch, tmp_path):
        db_path = _fresh_db(tmp_path)
        _patch_modules(monkeypatch, db_path)

        # Remove cached app module to force a fresh import
        monkeypatch.delitem(sys.modules, "app_streamlit", raising=False)
        # Also clear db_utils / middleware so they pick up the temp DB
        for mod in ["db_utils", "middleware"]:
            monkeypatch.delitem(sys.modules, mod, raising=False)

        # This should NOT raise
        try:
            import app_streamlit  # noqa
        except SystemExit:
            pass   # some apps call st.stop() or sys.exit() — acceptable
        except Exception as exc:
            pytest.fail(f"app_streamlit raised an unexpected exception: {exc}")

    def test_middleware_functions_callable(self, monkeypatch, tmp_path):
        """Key middleware functions exist and are callable (not just importable)."""
        db_path = _fresh_db(tmp_path)
        _patch_modules(monkeypatch, db_path)
        for mod in ["db_utils", "middleware"]:
            monkeypatch.delitem(sys.modules, mod, raising=False)

        import middleware as mw
        importlib.reload(mw)

        for fn_name in [
            "list_portfolios", "get_all_symbols", "get_alerts",
            "calc_security_KPIs", "evaluate_alert",
        ]:
            assert callable(getattr(mw, fn_name, None)), \
                f"middleware.{fn_name} is not callable"

    def test_db_utils_functions_callable(self, monkeypatch, tmp_path):
        """Key db_utils functions exist and are callable."""
        db_path = _fresh_db(tmp_path)
        _patch_modules(monkeypatch, db_path)
        monkeypatch.delitem(sys.modules, "db_utils", raising=False)

        import db_utils as db
        importlib.reload(db)

        for fn_name in [
            "get_engine", "list_portfolios", "insert_security",
            "store_prices", "get_active_alerts", "store_fx_rates",
        ]:
            assert callable(getattr(db, fn_name, None)), \
                f"db_utils.{fn_name} is not callable"
