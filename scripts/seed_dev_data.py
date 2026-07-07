#!/usr/bin/env python3
"""
scripts/seed_dev_data.py — populate the LOCAL DEV database with synthetic
portfolio data on real tickers, then pull real market data for those tickers
from Yahoo Finance via the existing data_fetcher.

DEV-ONLY: refuses to run unless DATABASE_URL clearly points at the dev
database (see _assert_dev_database below). Safe to re-run — skips seeding
if the Default portfolio already has transactions.

Usage (from the repo root, against docker-compose.dev.yml):
    docker compose -f docker-compose.dev.yml exec api python /app/scripts/seed_dev_data.py
"""
import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("seed_dev_data")

APP_DIR = os.path.join(os.path.dirname(__file__), "..", "app")
if APP_DIR not in sys.path:
    sys.path.insert(0, os.path.abspath(APP_DIR))

# Synthetic buys: (symbol, tx_date, quantity, price, fees) — prices/quantities are
# made up, not historically accurate. The point is to have real, tradeable tickers
# in the DB so data_fetcher can pull genuine prices/KPIs/FX for them afterwards.
SEED_TRANSACTIONS = [
    ("AAPL",     "2023-01-15", 10,  135.0, 1.0),
    ("AAPL",     "2023-09-05",  5,  180.0, 1.0),
    ("MSFT",     "2023-03-10",  8,  255.0, 1.0),
    ("VWCE.DE",  "2023-02-01", 20,   95.0, 2.0),
    ("VWCE.DE",  "2024-01-08", 15,  105.0, 2.0),
    ("IWDA.AS",  "2023-06-20", 12,   78.0, 2.0),
    ("SAP.DE",   "2023-11-02",  6,  120.0, 1.5),
]

DEFAULT_PORTFOLIO = "Default"


def _assert_dev_database():
    """Hard guard: refuse to run against anything that isn't obviously the dev DB."""
    if os.environ.get("SEED_FORCE") == "1":
        log.warning("SEED_FORCE=1 set — skipping dev-database safety check.")
        return

    url = os.environ.get("DATABASE_URL", "")
    lowered = url.lower()
    if "vestis_dev" not in lowered and "dev" not in lowered:
        log.error(
            "DATABASE_URL does not look like the dev database (expected 'vestis_dev' "
            "in the connection string). Refusing to seed to avoid touching a real DB.\n"
            "Set SEED_FORCE=1 to override if you're sure."
        )
        sys.exit(1)


def main():
    _assert_dev_database()

    import middleware as mw
    import db_utils as db
    from data_fetcher import run_fetch

    portfolio = db.get_portfolio_by_name(DEFAULT_PORTFOLIO)
    if not portfolio:
        log.error(f"Portfolio '{DEFAULT_PORTFOLIO}' not found — has db_init.py run yet?")
        sys.exit(1)
    portfolio_id = portfolio["id"]

    # NOTE: deliberately not using db.list_transactions() here — it joins
    # securities_cache via quoted "longName" and currently raises
    # UndefinedColumn on real Postgres (pre-existing bug, see conversation).
    # A plain count avoids that codepath entirely.
    from sqlalchemy import text as _sa_text
    with db.get_engine().connect() as _conn:
        tx_count = _conn.execute(
            _sa_text("SELECT COUNT(*) FROM transactions WHERE portfolio_id = :pid"),
            {"pid": portfolio_id},
        ).scalar()
    if tx_count:
        log.info(
            f"'{DEFAULT_PORTFOLIO}' already has {tx_count} transaction(s) — "
            "skipping seed (script is idempotent). Delete them via the UI/API first "
            "if you want to reseed from scratch."
        )
        return

    log.info(f"Seeding {len(SEED_TRANSACTIONS)} synthetic transactions into '{DEFAULT_PORTFOLIO}'...")
    symbols = set()
    for symbol, tx_date, qty, price, fees in SEED_TRANSACTIONS:
        mw.add_transaction(portfolio_id, symbol, tx_date, "buy", qty, price, fees)
        symbols.add(symbol)
        log.info(f"  + {symbol}: {qty} @ {price} on {tx_date}")

    log.info(f"Fetching real market data from Yahoo Finance for: {sorted(symbols)}")
    run_fetch(sorted(symbols), force=True)

    log.info("Done. Holdings should now show real prices/KPIs against the synthetic transactions.")


if __name__ == "__main__":
    main()
