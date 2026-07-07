"""
tests/conftest.py

Shared fixtures for integration and API tests: a fresh SQLite database per
test (schema mirrors app/setup/db_init.py) and a db_utils module bound to it.
"""

import importlib
import importlib.util
import json
import os
import sqlite3
import sys
import types

import pytest

APP_DIR = os.path.join(os.path.dirname(__file__), "..", "app")
API_DIR = os.path.join(os.path.dirname(__file__), "..", "api")
for _p in (APP_DIR, API_DIR):
    if _p not in sys.path:
        sys.path.insert(0, os.path.abspath(_p))


def apply_schema(conn):
    """Create all tables from the canonical DDL (mirrors setup/db_init.py)."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS portfolios (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        );
        INSERT OR IGNORE INTO portfolios (id, name) VALUES (1, 'Default');

        CREATE TABLE IF NOT EXISTS securities (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            yahoo_ticker TEXT UNIQUE,
            isin         TEXT,
            created_at   DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS fx_rates (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date            DATE NOT NULL,
            base_currency   TEXT NOT NULL,
            target_currency TEXT NOT NULL,
            rate            REAL NOT NULL,
            UNIQUE(date, base_currency, target_currency)
        );

        CREATE TABLE IF NOT EXISTS securities_cache (
            security_id          INTEGER PRIMARY KEY,
            security_type        TEXT,
            country              TEXT,
            exchange             TEXT,
            sector               TEXT,
            industry              TEXT,
            shortName            TEXT,
            longName             TEXT,
            regularMarketPrice   REAL,
            fiftyTwoWeekHigh     REAL,
            fiftyTwoWeekLow      REAL,
            volume               BIGINT,
            averageVolume        BIGINT,
            marketCap            BIGINT,
            beta                 REAL,
            trailingPE           REAL,
            forwardPE            REAL,
            trailingEps          REAL,
            earningsTimestamp    DATETIME,
            dividendRate         REAL,
            dividendYield        REAL,
            enterpriseValue      BIGINT,
            profitMargins        REAL,
            operatingMargins     REAL,
            returnOnAssets       REAL,
            returnOnEquity       REAL,
            totalRevenue         BIGINT,
            revenuePerShare      REAL,
            grossProfits         BIGINT,
            ebitda               BIGINT,
            totalCash            BIGINT,
            totalDebt            BIGINT,
            currentRatio         REAL,
            bookValue            REAL,
            operatingCashflow    BIGINT,
            freeCashflow         BIGINT,
            sharesOutstanding    BIGINT,
            currency             TEXT,
            kpis_updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(security_id) REFERENCES securities(id)
        );

        CREATE TABLE IF NOT EXISTS transactions (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id INTEGER REFERENCES portfolios(id),
            security_id  INTEGER REFERENCES securities(id),
            date         DATE,
            type         TEXT,
            quantity     REAL,
            price        REAL,
            fees         REAL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS prices (
            security_id       INTEGER,
            date              TEXT,
            open              REAL, high REAL, low REAL, close REAL,
            adj_close         REAL, volume REAL,
            prices_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (security_id, date),
            FOREIGN KEY(security_id) REFERENCES securities(id)
        );

        CREATE TABLE IF NOT EXISTS holdings_timeseries (
            date         TEXT    NOT NULL,
            portfolio_id INTEGER NOT NULL,
            security_id  INTEGER NOT NULL,
            quantity     REAL    NOT NULL,
            market_value REAL    NOT NULL,
            cost_basis   REAL    NOT NULL,
            PRIMARY KEY (date, portfolio_id, security_id)
        );

        CREATE TABLE IF NOT EXISTS security_risk_timeseries (
            date          TEXT    NOT NULL,
            security_id   INTEGER NOT NULL,
            risk_score    REAL    NOT NULL,
            market_value  REAL    NOT NULL,
            weighted_risk REAL    NOT NULL,
            PRIMARY KEY (date, security_id)
        );

        CREATE TABLE IF NOT EXISTS dividends (
            security_id          INTEGER,
            date                 TEXT NOT NULL,
            dividend             REAL,
            dividends_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (security_id, date),
            FOREIGN KEY(security_id) REFERENCES securities(id)
        );

        CREATE TABLE IF NOT EXISTS financials (
            security_id     INTEGER,
            statement       TEXT    NOT NULL,
            period          TEXT,
            as_of_date      TEXT    NOT NULL,
            payload         TEXT,
            kpis_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (security_id, statement, as_of_date),
            FOREIGN KEY(security_id) REFERENCES securities(id)
        );

        CREATE TABLE IF NOT EXISTS valuations (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            security_id           INTEGER REFERENCES securities(id),
            intrinsic_per_share   REAL,
            total_pv              REAL,
            terminal_value        REAL,
            market_price          REAL,
            margin_of_safety      REAL,
            rating                TEXT,
            assumptions           TEXT,
            valuation_computed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(security_id)
        );

        CREATE TABLE IF NOT EXISTS alerts (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            security_id      INTEGER NOT NULL,
            alert_type       TEXT    NOT NULL,
            params           TEXT,
            active           INTEGER DEFAULT 1,
            snooze_until     TEXT,
            cooldown_seconds INTEGER DEFAULT 3600,
            notify_mode      TEXT DEFAULT 'immediate',
            last_evaluated   TEXT,
            last_triggered   TEXT,
            note             TEXT,
            auto_managed     INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS alerts_log (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_id     INTEGER NOT NULL REFERENCES alerts(id),
            triggered_at TEXT    NOT NULL,
            payload      TEXT
        );
    """)
    conn.commit()


@pytest.fixture
def db_path(tmp_path):
    """Create a fresh SQLite DB file per test using the real db_init schema."""
    path = str(tmp_path / "test_portfolio.db")
    conn = sqlite3.connect(path)
    apply_schema(conn)
    conn.close()
    return path


def _patch_config_and_env(db_path, monkeypatch):
    """
    Stub config_utils with an in-memory store — real config_utils writes
    settings to app/config.json on disk, which tests must never touch.
    """
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    store = {"db_path": db_path, "taxonomy": {}, "tax_rate": 0.25}
    fake_cfg = types.ModuleType("config_utils")
    fake_cfg.get_config = lambda key: store.get(key)
    fake_cfg.set_config = lambda key, value: store.__setitem__(
        key, __import__("json").dumps(value) if isinstance(value, (dict, list)) else value
    )
    fake_cfg.get_all_config = lambda: dict(store)
    fake_cfg.safe_json_load = lambda v, default=None: (
        (json.loads(v) if isinstance(v, str) else v) if v else (default if default is not None else {})
    )
    sys.modules["config_utils"] = fake_cfg
    return store


@pytest.fixture
def db(db_path, monkeypatch):
    """
    Import db_utils fresh for each test, pointed at the temp SQLite file.
    Monkeypatches DATABASE_URL so no Postgres is needed.
    """
    _patch_config_and_env(db_path, monkeypatch)

    if "db_utils" in sys.modules:
        del sys.modules["db_utils"]

    import db_utils as _db
    importlib.reload(_db)
    return _db


@pytest.fixture
def api_client(db_path, monkeypatch):
    """
    A FastAPI TestClient wired to the same fresh SQLite DB as the `db` fixture.
    Reloads middleware/data_fetcher/db_utils/api.main fresh so they all bind
    to the patched DATABASE_URL — middleware must be (re)imported before
    data_fetcher due to their circular import (data_fetcher imports
    middleware; middleware imports fetch_and_store_lazy from data_fetcher).
    """
    _patch_config_and_env(db_path, monkeypatch)

    for mod in ("api_main", "middleware", "data_fetcher", "db_utils"):
        sys.modules.pop(mod, None)

    import middleware  # noqa: F401 — establishes the safe import order

    spec = importlib.util.spec_from_file_location("api_main", os.path.join(API_DIR, "main.py"))
    api_main = importlib.util.module_from_spec(spec)
    sys.modules["api_main"] = api_main
    spec.loader.exec_module(api_main)

    from fastapi.testclient import TestClient
    return TestClient(api_main.app)


@pytest.fixture
def mw(db_path, monkeypatch):
    """
    The middleware module, reloaded fresh and bound to the temp SQLite DB —
    for tests that need mw.* functions directly (e.g. telegram_worker logic
    that isn't reachable through the API or db_utils alone).
    """
    _patch_config_and_env(db_path, monkeypatch)

    for mod in ("middleware", "data_fetcher", "db_utils"):
        sys.modules.pop(mod, None)

    import middleware as _mw
    return _mw


@pytest.fixture
def telegram_worker(mw, monkeypatch):
    """telegram_worker reloaded fresh, bound to the same DB as the `mw` fixture."""
    sys.modules.pop("telegram_worker", None)
    sys.modules.pop("telegram_client", None)
    import telegram_worker as _tw
    return _tw


def seed(db_path, sql, params=()):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(sql, params)
    conn.commit()
    conn.close()


def seed_security(db_path, ticker="AAPL", isin=None):
    seed(db_path,
         "INSERT OR IGNORE INTO securities (yahoo_ticker, isin) VALUES (?, ?)",
         (ticker, isin))
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT id FROM securities WHERE yahoo_ticker=?", (ticker,)).fetchone()
    conn.close()
    return row[0]
