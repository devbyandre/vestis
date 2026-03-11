#!/usr/bin/env python3
"""
db_init.py  –  creates all tables in PostgreSQL (or SQLite for local dev).
Run once at startup via the db-init service in docker-compose.yml.
"""

import os
import sys
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def wait_for_db(engine, retries: int = 15, delay: float = 2.0):
    """Retry until the DB is reachable (important for Docker startup ordering)."""
    from sqlalchemy import text
    for attempt in range(1, retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logging.info("✅ Database is reachable.")
            return
        except Exception as exc:
            logging.warning(f"⏳ DB not ready ({attempt}/{retries}): {exc}")
            time.sleep(delay)
    logging.error("❌ Could not connect to the database after multiple retries. Exiting.")
    sys.exit(1)


DDL = """
-- Portfolios
CREATE TABLE IF NOT EXISTS portfolios (
    id   SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);
INSERT INTO portfolios (id, name) VALUES (1, 'Default') ON CONFLICT DO NOTHING;

-- Securities
CREATE TABLE IF NOT EXISTS securities (
    id            SERIAL PRIMARY KEY,
    yahoo_ticker  TEXT UNIQUE,
    isin          TEXT,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- FX rates
CREATE TABLE IF NOT EXISTS fx_rates (
    id              SERIAL PRIMARY KEY,
    date            DATE        NOT NULL,
    base_currency   TEXT        NOT NULL,
    target_currency TEXT        NOT NULL,
    rate            REAL        NOT NULL,
    UNIQUE (date, base_currency, target_currency)
);

-- Securities cache (KPI store)
CREATE TABLE IF NOT EXISTS securities_cache (
    security_id          INTEGER PRIMARY KEY REFERENCES securities(id),
    security_type        TEXT,
    country              TEXT,
    exchange             TEXT,
    sector               TEXT,
    industry             TEXT,
    "shortName"          TEXT,
    "longName"           TEXT,
    "regularMarketPrice" REAL,
    "fiftyTwoWeekHigh"   REAL,
    "fiftyTwoWeekLow"    REAL,
    volume               BIGINT,
    "averageVolume"      BIGINT,
    "marketCap"          BIGINT,
    beta                 REAL,
    "trailingPE"         REAL,
    "forwardPE"          REAL,
    "trailingEps"        REAL,
    "earningsTimestamp"  TIMESTAMPTZ,
    "dividendRate"       REAL,
    "dividendYield"      REAL,
    "enterpriseValue"    BIGINT,
    "profitMargins"      REAL,
    "operatingMargins"   REAL,
    "returnOnAssets"     REAL,
    "returnOnEquity"     REAL,
    "totalRevenue"       BIGINT,
    "revenuePerShare"    REAL,
    "grossProfits"       BIGINT,
    ebitda               BIGINT,
    "totalCash"          BIGINT,
    "totalDebt"          BIGINT,
    "currentRatio"       REAL,
    "bookValue"          REAL,
    "operatingCashflow"  BIGINT,
    "freeCashflow"       BIGINT,
    "sharesOutstanding"  BIGINT,
    currency             TEXT,
    kpis_updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Transactions
CREATE TABLE IF NOT EXISTS transactions (
    id           SERIAL PRIMARY KEY,
    portfolio_id INTEGER REFERENCES portfolios(id),
    security_id  INTEGER REFERENCES securities(id),
    date         DATE,
    type         TEXT,
    quantity     REAL,
    price        REAL,
    fees         REAL DEFAULT 0
);

-- Recurring transactions
CREATE TABLE IF NOT EXISTS recurring_transactions (
    id           SERIAL PRIMARY KEY,
    portfolio_id INTEGER REFERENCES portfolios(id),
    security_id  INTEGER REFERENCES securities(id),
    budget       REAL,
    fees         REAL,
    start_date   DATE,
    end_date     DATE,
    frequency    TEXT
);

-- Prices
CREATE TABLE IF NOT EXISTS prices (
    security_id        INTEGER REFERENCES securities(id),
    date               DATE    NOT NULL,
    open               REAL,
    high               REAL,
    low                REAL,
    close              REAL,
    adj_close          REAL,
    volume             REAL,
    prices_updated_at  TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (security_id, date)
);

-- Holdings timeseries
CREATE TABLE IF NOT EXISTS holdings_timeseries (
    date         DATE    NOT NULL,
    portfolio_id INTEGER NOT NULL,
    security_id  INTEGER NOT NULL,
    quantity     REAL    NOT NULL,
    market_value REAL    NOT NULL,
    cost_basis   REAL    NOT NULL,
    PRIMARY KEY (date, portfolio_id, security_id)
);

-- Security risk timeseries
CREATE TABLE IF NOT EXISTS security_risk_timeseries (
    date          DATE    NOT NULL,
    security_id   INTEGER NOT NULL,
    risk_score    REAL    NOT NULL,
    market_value  REAL    NOT NULL,
    weighted_risk REAL    NOT NULL,
    PRIMARY KEY (date, security_id)
);

-- Dividends
CREATE TABLE IF NOT EXISTS dividends (
    security_id          INTEGER REFERENCES securities(id),
    date                 DATE    NOT NULL,
    dividend             REAL,
    dividends_updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (security_id, date)
);

-- Financials
CREATE TABLE IF NOT EXISTS financials (
    security_id      INTEGER REFERENCES securities(id),
    statement        TEXT    NOT NULL,
    period           TEXT,
    as_of_date       DATE    NOT NULL,
    payload          TEXT,
    kpis_updated_at  TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (security_id, statement, as_of_date)
);

-- Valuations
CREATE TABLE IF NOT EXISTS valuations (
    id                    SERIAL PRIMARY KEY,
    security_id           INTEGER REFERENCES securities(id),
    intrinsic_per_share   REAL,
    total_pv              REAL,
    terminal_value        REAL,
    market_price          REAL,
    margin_of_safety      REAL,
    rating                TEXT,
    assumptions           TEXT,
    valuation_computed_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (security_id)   -- one active valuation per security
);

-- Alerts
CREATE TABLE IF NOT EXISTS alerts (
    id               SERIAL PRIMARY KEY,
    security_id      INTEGER NOT NULL,
    alert_type       TEXT    NOT NULL,
    params           TEXT,
    active           SMALLINT DEFAULT 1,
    snooze_until     TIMESTAMPTZ,
    cooldown_seconds INTEGER  DEFAULT 3600,
    notify_mode      TEXT     DEFAULT 'immediate',
    last_evaluated   TIMESTAMPTZ,
    last_triggered   TIMESTAMPTZ,
    note             TEXT,
    auto_managed     SMALLINT DEFAULT 0
);

-- Alerts log
CREATE TABLE IF NOT EXISTS alerts_log (
    id           SERIAL PRIMARY KEY,
    alert_id     INTEGER NOT NULL REFERENCES alerts(id),
    triggered_at TIMESTAMPTZ NOT NULL,
    payload      TEXT
);
"""


def init_db():
    # Import here so the module can be imported without triggering DB connection
    from db_utils import get_engine
    engine = get_engine()
    wait_for_db(engine)

    from sqlalchemy import text
    with engine.begin() as conn:
        for statement in DDL.split(";"):
            stmt = statement.strip()
            if stmt:
                try:
                    conn.execute(text(stmt))
                except Exception as exc:
                    logging.warning(f"DDL warning (skipping): {exc}")

    logging.info("✅ Database schema initialised.")


if __name__ == "__main__":
    init_db()
