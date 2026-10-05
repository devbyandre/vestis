#!/usr/bin/env python3
"""
db_init.py  -  creates all tables for both SQLite (local dev) and PostgreSQL (production).

SQLite:   used automatically when DATABASE_URL is not set
Postgres: used when DATABASE_URL=postgresql://...

Run once at startup:  PYTHONPATH=app python app/setup/db_init.py
"""

import logging
import sys
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


def wait_for_db(engine, retries=15, delay=2.0):
    from sqlalchemy import text
    for attempt in range(1, retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logging.info("Database is reachable.")
            return
        except Exception as exc:
            logging.warning(f"DB not ready ({attempt}/{retries}): {exc}")
            time.sleep(delay)
    logging.error("Could not connect to database after multiple retries.")
    sys.exit(1)


# Dialect-neutral DDL — uses only types both SQLite and PostgreSQL accept
# AUTOINCREMENT and INSERT OR IGNORE are converted for Postgres below
DDL_STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS portfolios (
        id   INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL
    )""",

    """INSERT OR IGNORE INTO portfolios (id, name) VALUES (1, 'Default')""",

    """CREATE TABLE IF NOT EXISTS securities (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        yahoo_ticker TEXT UNIQUE,
        isin         TEXT,
        created_at   DATETIME DEFAULT CURRENT_TIMESTAMP
    )""",

    """CREATE TABLE IF NOT EXISTS fx_rates (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        date            TEXT NOT NULL,
        base_currency   TEXT NOT NULL,
        target_currency TEXT NOT NULL,
        rate            REAL NOT NULL,
        UNIQUE (date, base_currency, target_currency)
    )""",

    """CREATE TABLE IF NOT EXISTS securities_cache (
        security_id        INTEGER PRIMARY KEY,
        security_type      TEXT,
        country            TEXT,
        exchange           TEXT,
        sector             TEXT,
        industry           TEXT,
        shortName          TEXT,
        longName           TEXT,
        regularMarketPrice REAL,
        fiftyTwoWeekHigh   REAL,
        fiftyTwoWeekLow    REAL,
        volume             BIGINT,
        averageVolume      BIGINT,
        marketCap          BIGINT,
        beta               REAL,
        trailingPE         REAL,
        forwardPE          REAL,
        trailingEps        REAL,
        earningsTimestamp  DATETIME,
        dividendRate       REAL,
        dividendYield      REAL,
        enterpriseValue    BIGINT,
        profitMargins      REAL,
        operatingMargins   REAL,
        returnOnAssets     REAL,
        returnOnEquity     REAL,
        totalRevenue       BIGINT,
        revenuePerShare    REAL,
        grossProfits       BIGINT,
        ebitda             BIGINT,
        totalCash          BIGINT,
        totalDebt          BIGINT,
        currentRatio       REAL,
        bookValue          REAL,
        operatingCashflow  BIGINT,
        freeCashflow       BIGINT,
        sharesOutstanding  BIGINT,
        currency           TEXT,
        kpis_updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (security_id) REFERENCES securities(id)
    )""",

    """CREATE TABLE IF NOT EXISTS transactions (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        portfolio_id INTEGER,
        security_id  INTEGER,
        date         TEXT,
        type         TEXT,
        quantity     REAL,
        price        REAL,
        fees         REAL DEFAULT 0,
        FOREIGN KEY (portfolio_id) REFERENCES portfolios(id),
        FOREIGN KEY (security_id)  REFERENCES securities(id)
    )""",

    """CREATE TABLE IF NOT EXISTS recurring_transactions (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        portfolio_id INTEGER,
        security_id  INTEGER,
        budget       REAL,
        fees         REAL,
        start_date   TEXT,
        end_date     TEXT,
        frequency    TEXT,
        FOREIGN KEY (portfolio_id) REFERENCES portfolios(id),
        FOREIGN KEY (security_id)  REFERENCES securities(id)
    )""",

    """CREATE TABLE IF NOT EXISTS prices (
        security_id       INTEGER,
        date              TEXT NOT NULL,
        open              REAL,
        high              REAL,
        low               REAL,
        close             REAL,
        adj_close         REAL,
        volume            REAL,
        prices_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (security_id, date),
        FOREIGN KEY (security_id) REFERENCES securities(id)
    )""",

    """CREATE TABLE IF NOT EXISTS holdings_timeseries (
        date         TEXT    NOT NULL,
        portfolio_id INTEGER NOT NULL,
        security_id  INTEGER NOT NULL,
        quantity     REAL    NOT NULL,
        market_value REAL    NOT NULL,
        cost_basis   REAL,
        PRIMARY KEY (date, portfolio_id, security_id),
        FOREIGN KEY (portfolio_id) REFERENCES portfolios(id),
        FOREIGN KEY (security_id)  REFERENCES securities(id)
    )""",

    """CREATE TABLE IF NOT EXISTS security_risk_timeseries (
        date          TEXT    NOT NULL,
        security_id   INTEGER NOT NULL,
        risk_score    REAL    NOT NULL,
        market_value  REAL    NOT NULL,
        weighted_risk REAL    NOT NULL,
        PRIMARY KEY (date, security_id)
    )""",

    """CREATE TABLE IF NOT EXISTS dividends (
        security_id          INTEGER,
        date                 TEXT NOT NULL,
        dividend             REAL,
        dividends_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (security_id, date),
        FOREIGN KEY (security_id) REFERENCES securities(id)
    )""",

    """CREATE TABLE IF NOT EXISTS financials (
        security_id     INTEGER,
        statement       TEXT NOT NULL,
        period          TEXT,
        as_of_date      TEXT NOT NULL,
        payload         TEXT,
        kpis_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (security_id, statement, as_of_date),
        FOREIGN KEY (security_id) REFERENCES securities(id)
    )""",

    """CREATE TABLE IF NOT EXISTS valuations (
        id                    INTEGER PRIMARY KEY AUTOINCREMENT,
        security_id           INTEGER,
        intrinsic_per_share   REAL,
        total_pv              REAL,
        terminal_value        REAL,
        market_price          REAL,
        margin_of_safety      REAL,
        rating                TEXT,
        assumptions           TEXT,
        valuation_computed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (security_id),
        FOREIGN KEY (security_id) REFERENCES securities(id)
    )""",

    """CREATE TABLE IF NOT EXISTS alerts (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        security_id      INTEGER NOT NULL,
        alert_type       TEXT    NOT NULL,
        params           TEXT,
        active           INTEGER DEFAULT 1,
        snooze_until     TEXT,
        cooldown_seconds INTEGER DEFAULT 3600,
        notify_mode      TEXT    DEFAULT 'immediate',
        last_evaluated   TEXT,
        last_triggered   TEXT,
        note             TEXT,
        auto_managed     INTEGER DEFAULT 0,
        state            TEXT
    )""",

    """CREATE TABLE IF NOT EXISTS alerts_log (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        alert_id     INTEGER NOT NULL,
        triggered_at TEXT    NOT NULL,
        payload      TEXT,
        FOREIGN KEY (alert_id) REFERENCES alerts(id)
    )""",

    """CREATE TABLE IF NOT EXISTS news (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        security_id  INTEGER NOT NULL,
        uid          TEXT    NOT NULL,
        title        TEXT    NOT NULL,
        publisher    TEXT,
        url          TEXT,
        published_at TEXT    NOT NULL,
        sentiment    REAL,
        fetched_at   TEXT    NOT NULL,
        UNIQUE (security_id, uid),
        FOREIGN KEY (security_id) REFERENCES securities(id)
    )""",

    "CREATE INDEX IF NOT EXISTS idx_news_published ON news (published_at)",

    """CREATE TABLE IF NOT EXISTS news_fetch_log (
        security_id INTEGER PRIMARY KEY,
        fetched_at  TEXT    NOT NULL,
        items       INTEGER DEFAULT 0,
        query       TEXT,
        FOREIGN KEY (security_id) REFERENCES securities(id)
    )""",
]


def _pg(sql):
    """Convert SQLite-specific syntax to PostgreSQL."""
    sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
    sql = sql.replace("INSERT OR IGNORE INTO", "INSERT INTO")
    sql = sql.replace("VALUES (1, 'Default')", "VALUES (1, 'Default') ON CONFLICT DO NOTHING")
    sql = sql.replace("DATETIME DEFAULT CURRENT_TIMESTAMP", "TIMESTAMPTZ DEFAULT NOW()")
    sql = sql.replace("DATETIME", "TIMESTAMPTZ")
    return sql


def _lowercase_quoted_columns(engine):
    """Rename case-sensitive columns (e.g. "longName") to their lowercase form.

    The DDL declares columns unquoted, so Postgres creates them lowercase and the
    queries reference them unquoted. Databases whose columns were once renamed to
    quoted camelCase would otherwise fail every query touching them.
    """
    from sqlalchemy import text
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT table_name, column_name FROM information_schema.columns
            WHERE table_schema = current_schema() AND column_name <> lower(column_name)
        """)).fetchall()
        existing = {(t, c) for t, c in conn.execute(text(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema = current_schema()")).fetchall()}
        renamed = 0
        for table, col in rows:
            if (table, col.lower()) in existing:
                logging.warning(f'Not renaming {table}."{col}": a column "{col.lower()}" already exists')
                continue
            conn.execute(text(f'ALTER TABLE "{table}" RENAME COLUMN "{col}" TO "{col.lower()}"'))
            renamed += 1
    if renamed:
        logging.info(f"Renamed {renamed} case-sensitive columns to lowercase")


def init_db():
    from db_utils import get_engine
    engine = get_engine()
    wait_for_db(engine)

    is_postgres = "postgresql" in str(engine.url) or "postgres" in str(engine.url)
    dialect = "PostgreSQL" if is_postgres else "SQLite"

    from sqlalchemy import text
    ok = 0
    skipped = 0
    with engine.begin() as conn:
        for stmt in DDL_STATEMENTS:
            sql = _pg(stmt) if is_postgres else stmt
            try:
                conn.execute(text(sql))
                ok += 1
            except Exception as exc:
                logging.warning(f"DDL skipped: {exc}")
                skipped += 1

    if is_postgres:
        # The 'Default' portfolio is seeded with an explicit id=1, which
        # doesn't advance Postgres' SERIAL sequence — without this, the next
        # plain INSERT (no explicit id) collides with id=1.
        with engine.begin() as conn:
            conn.execute(text(
                "SELECT setval(pg_get_serial_sequence('portfolios', 'id'), "
                "COALESCE((SELECT MAX(id) FROM portfolios), 1))"
            ))

        _lowercase_quoted_columns(engine)

        # Migration: operatingCashflow/freeCashflow were originally declared
        # INTEGER (max ~2.1B), too small for large-cap cashflow figures.
        # CREATE TABLE IF NOT EXISTS above won't widen an already-existing
        # column, so do it explicitly — safe/idempotent to rerun.
        for col in ("operatingCashflow", "freeCashflow"):
            try:
                with engine.begin() as conn:
                    # Unquoted: Postgres folds this to the actual (lowercase) column name.
                    conn.execute(text(
                        f"ALTER TABLE securities_cache ALTER COLUMN {col} TYPE BIGINT"
                    ))
            except Exception as exc:
                logging.warning(f"Column widen skipped for {col}: {exc}")

    # Migration: alerts.state holds per-alert edge-trigger state (last side,
    # armed flag, last fired bar) so alerts fire on transitions, not levels.
    with engine.begin() as conn:
        try:
            if is_postgres:
                conn.execute(text("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS state TEXT"))
            else:
                cols = [r[1] for r in conn.execute(text("PRAGMA table_info(alerts)"))]
                if "state" not in cols:
                    conn.execute(text("ALTER TABLE alerts ADD COLUMN state TEXT"))
        except Exception as exc:
            logging.warning(f"alerts.state migration skipped: {exc}")

    # Migration (SQLite only): older fetches stored numpy int64 volumes as raw
    # 8-byte blobs, which crash JSON encoding in the price endpoints.
    if not is_postgres:
        try:
            with engine.begin() as conn:
                rows = conn.execute(text(
                    "SELECT security_id, date, volume FROM prices WHERE typeof(volume)='blob'"
                )).fetchall()
                for sid, d, blob in rows:
                    conn.execute(
                        text("UPDATE prices SET volume=:v WHERE security_id=:s AND date=:d"),
                        {"v": float(int.from_bytes(bytes(blob), "little", signed=True)), "s": sid, "d": d},
                    )
                if rows:
                    logging.info(f"Repaired {len(rows)} blob-typed price volumes")
        except Exception as exc:
            logging.warning(f"prices.volume repair skipped: {exc}")

    logging.info(f"Schema initialised on {dialect}: {ok} statements OK, {skipped} skipped.")


if __name__ == "__main__":
    init_db()
