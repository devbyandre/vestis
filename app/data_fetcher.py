#!/usr/bin/env python3
# data_fetcher.py

import logging
import argparse
import time
from typing import List, Optional, Dict
import pandas as pd
import yfinance as yf

import db_utils as db
import middleware as mw

import sys


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")


# ---------------------
# Throttling
# ---------------------

class Throttler:
    """Simple rate limiter to avoid Yahoo Finance throttling."""

    def __init__(self, max_per_min: int = 45):
        self.interval = max(60.0 / max(1, max_per_min), 0.1)
        self._last = 0.0

    def wait(self):
        now = time.time()
        delta = now - self._last
        if delta < self.interval:
            time.sleep(self.interval - delta)
        self._last = time.time()


# ---------------------
# Utility
# ---------------------

def should_update(last_update: Optional[str], update_hours: int) -> bool:
    """Return True if data should be updated (based on hours since last_update)."""
    if not last_update:
        return True
    last_dt = pd.to_datetime(last_update)

    # Make naive to avoid tz-aware vs tz-naive issues
    if last_dt.tzinfo is not None:
        last_dt = last_dt.tz_convert(None)

    now = pd.Timestamp.now()
    return (now - last_dt).total_seconds() > update_hours * 3600


# ---------------------
# Throttler
# ---------------------
class Throttler:
    """Simple rate limiter to avoid too many requests to Yahoo."""
    def __init__(self, max_per_min: int = 45):
        self.interval = max(60.0 / max(1, max_per_min), 0.1)
        self._last = 0.0

    def wait(self):
        now = time.time()
        delta = now - self._last
        if delta < self.interval:
            time.sleep(self.interval - delta)
        self._last = time.time()



# ---------------------
# Fetch prices (batch)
# ---------------------
def fetch_prices_batch(
    tickers: List[str],
    throttler: Throttler,
    start_date: str = None,
    max_retries: int = 3,
    price_update_hours: int = 24  # update only once per day
) -> Dict[str, bool]:
    """
    Fetch daily adjusted prices for multiple tickers in a single request.
    Retries on network errors. Stops immediately on Yahoo rate limit.
    Returns dict: symbol -> True/False depending on success.
    """
    results = {sym: False for sym in tickers}
    if not tickers:
        return results

    # Determine which tickers actually need update
    tickers_to_fetch = []
    for sym in tickers:
        security_id = db.get_security_id(sym)
        last_price = db.get_last_prices_update(security_id)
        if should_update(last_price, price_update_hours):
            tickers_to_fetch.append(sym)
        else:
            logging.info("Skipping %s, prices updated within last %d hours.", sym, price_update_hours)
            results[sym] = True  # already up-to-date

    if not tickers_to_fetch:
        return results

    # Attempt download with retries
    attempt = 0
    while attempt < max_retries:
        try:
            logging.info("Fetching batch prices for %d tickers (attempt %d)...", len(tickers_to_fetch), attempt + 1)
            df = yf.download(
                tickers=tickers_to_fetch,
                start=start_date,
                interval="1d",
                group_by='ticker',
                auto_adjust=True,
                threads=True
            )
            break  # success
        except Exception as e:
            msg = str(e).lower()
            if "rate limit" in msg or "too many requests" in msg:
                logging.error("❌ Yahoo Finance rate limit reached. Stopping script.")
                sys.exit(1)
            else:
                attempt += 1
                logging.warning("Batch download failed (attempt %d/%d): %s", attempt, max_retries, e)
                time.sleep(2 ** attempt)
    else:
        logging.error("❌ Failed to fetch batch prices after %d attempts: %s", max_retries, tickers_to_fetch)
        return results

    # Store prices per symbol
    for symbol in tickers_to_fetch:
        try:
            security_id = db.get_security_id(symbol)
            hist = None

            # Handle MultiIndex (multiple tickers) vs single ticker
            if isinstance(df.columns, pd.MultiIndex) and symbol in df.columns.levels[0]:
                hist = df[symbol].reset_index().rename(columns={
                    'Date': 'date',
                    'Open': 'open',
                    'High': 'high',
                    'Low': 'low',
                    'Close': 'close',
                    'Adj Close': 'adj_close',
                    'Volume': 'volume'
                })
            elif not isinstance(df.columns, pd.MultiIndex):
                hist = df.reset_index().rename(columns={
                    'Date': 'date',
                    'Open': 'open',
                    'High': 'high',
                    'Low': 'low',
                    'Close': 'close',
                    'Adj Close': 'adj_close',
                    'Volume': 'volume'
                })

            if hist is not None:
                # Ensure all expected columns exist
                for col in ['date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']:
                    if col not in hist.columns:
                        hist[col] = None

                # Convert date to ISO string (YYYY-MM-DD)
                hist['date'] = pd.to_datetime(hist['date']).dt.strftime('%Y-%m-%d')

                # Fill missing adj_close with close
                hist['adj_close'] = hist['adj_close'].fillna(hist['close'])

                # Keep only rows with valid date and at least one price
                hist = hist.dropna(subset=['date'])
                hist = hist.loc[hist['adj_close'].notna() | hist['close'].notna()]

                if hist.empty:
                    logging.warning("⚠️ No valid price data for %s, skipping store.", symbol)
                    results[symbol] = False
                    continue

                # Store prices in DB
                mw.store_prices(security_id, hist)
                results[symbol] = True
                logging.info("✅ Stored prices for %s (%d rows)", symbol, len(hist))

        except Exception:
            logging.exception("Failed to store prices for %s", symbol)
            results[symbol] = False

        # Respect throttling
        throttler.wait()

    return results

# ---------------------
# Fetch fundamentals & dividends
# ---------------------
def fetch_fundamentals_and_dividends(
    symbol: str,
    throttler: Throttler,
    max_retries: int = 3,
    fundamentals_update_hours: int = 24 * 7  # weekly
) -> bool:
    """
    Fetch info, financials, and dividends for a single ticker.
    Only updates if last fundamentals update is older than fundamentals_update_hours.
    """
    security_id = db.get_security_id(symbol)
    last_update = db.get_last_info_update(security_id)
    if not should_update(last_update, fundamentals_update_hours):
        logging.info("Skipping %s, fundamentals updated recently.", symbol)
        return False

    for attempt in range(max_retries):
        try:
            logging.info("Fetching fundamentals & dividends for %s (attempt %d)...", symbol, attempt + 1)
            t = yf.Ticker(symbol)

            # Info
            info = t.info or {}
            if info:
                db.store_security_cache(security_id, info)

            # Financials
            statements = {
                'income_statement': getattr(t, 'financials', None),
                'balance_sheet': getattr(t, 'balance_sheet', None),
                'cashflow': getattr(t, 'cashflow', None),
            }
            db.store_financials(security_id, statements)

            # Dividends
            if not t.dividends.empty:
                db.store_dividends(security_id, t.dividends)

            # Split detection — check if Yahoo Finance reports splits not yet recorded
            try:
                yf_splits = t.splits
                if yf_splits is not None and not yf_splits.empty:
                    # Get splits already recorded in our transactions
                    recorded = db.get_recorded_splits(security_id)
                    recorded_dates = set(recorded) if recorded else set()
                    for split_date, ratio in yf_splits.items():
                        split_date_str = pd.Timestamp(split_date).strftime("%Y-%m-%d")
                        if split_date_str not in recorded_dates:
                            logging.warning(
                                "⚠️ UNRECORDED SPLIT detected for %s: ratio=%.4f on %s — "
                                "please add a split transaction in Vestis to adjust your holdings.",
                                symbol, float(ratio), split_date_str
                            )
                            # Store in split_alerts table for Telegram notification
                            db.store_split_alert(security_id, split_date_str, float(ratio))
            except Exception:
                logging.exception("Split detection failed for %s", symbol)

            logging.info("✅ Stored fundamentals & dividends for %s", symbol)
            throttler.wait()
            return True

        except Exception as e:
            msg = str(e).lower()
            if "rate limit" in msg or "too many requests" in msg:
                logging.error("❌ Yahoo Finance rate limit reached for %s. Stopping script.", symbol)
                sys.exit(1)
            else:
                logging.warning("Failed to fetch fundamentals/dividends for %s (attempt %d/%d): %s", symbol, attempt + 1, max_retries, e)
                time.sleep(2 ** attempt)

    logging.error("❌ Failed to fetch fundamentals/dividends for %s after %d retries.", symbol, max_retries)
    throttler.wait()
    return False



# ---------------------
# Update a list of symbols: prices + fundamentals
# ---------------------
def update_symbols(symbols: List[str], throttler: Throttler) -> None:
    if not symbols:
        return

    # Compute earliest start date for batch fetch
    start_dates = {}
    for sym in symbols:
        latest_df = db.get_price_series(sym, None, "2100-01-01")
        start_dates[sym] = latest_df["date"].max() if not latest_df.empty else None

    start_date = min([d for d in start_dates.values() if d is not None], default=None)
    logging.info("Starting batch price fetch for %d symbols", len(symbols))
    prices_result = fetch_prices_batch(symbols, throttler, start_date=start_date)

    for sym in symbols:
        fundamentals_success = fetch_fundamentals_and_dividends(sym, throttler)
        price_success = prices_result.get(sym, False)

        if price_success and fundamentals_success:
            logging.info("✅ Fully updated %s", sym)
        elif price_success:
            logging.warning("⚠️ Only prices updated for %s", sym)
        elif fundamentals_success:
            logging.warning("⚠️ Only fundamentals updated for %s", sym)
        else:
            logging.error("❌ Failed to update %s", sym)



def run_fetch(tickers: List[str], batch_size: int = 20):
    """Fetch and store data for a list of tickers in batches."""
    if not tickers:
        logging.info("No tickers provided, updating all securities in DB")
        tickers = db.get_all_symbols()

    throttler = Throttler()

    for i in range(0, len(tickers), batch_size):
        batch = [t.strip() for t in tickers[i:i + batch_size]]
        update_symbols(batch, throttler)

    # -----------------------------
    # Fetch missing FX conversion rates
    # -----------------------------
    logging.info("🔄 Checking for missing FX rates...")
    fx_success = fetch_missing_fx_rates(throttler=throttler)

    if fx_success:
        logging.info("✅ FX rates updated successfully.")
    else:
        logging.warning("⚠️ FX rate update incomplete or failed.")


def fetch_missing_fx_rates(
    throttler: Throttler,
    base_currencies: Optional[List[str]] = None,
    batch_size: int = 5
) -> bool:
    """
    Fetch and store *missing* FX rates for base currencies → EUR.

    - Determines earliest price/dividend date and latest known FX date per currency.
    - Downloads only missing ranges.
    - Batches Yahoo Finance requests to minimize API traffic.
    - Returns True if all currencies updated successfully.
    """
    try:
        # Determine base currencies
        if not base_currencies:
            base_currencies = db.get_all_security_currencies()

        base_currencies = [c for c in base_currencies if c and c != "EUR"]
        if not base_currencies:
            logging.info("No non-EUR currencies found; no FX fetch needed.")
            return True

        # Earliest date we ever need
        global_start_date = db.get_earliest_price_or_dividend_date()
        if not global_start_date:
            logging.warning("No historical data found to determine FX range.")
            return False

        all_success = True
        today = pd.Timestamp.today().normalize()
        # If today is Saturday (5) or Sunday (6), go back to Friday
        if today.weekday() >= 5:
            today = today - pd.Timedelta(days=today.weekday() - 4)
        now_str = today.strftime("%Y-%m-%d")
        # now_str = pd.Timestamp.today().strftime("%Y-%m-%d")

        # Collect tasks (only missing date ranges)
        tasks = []
        for base in base_currencies:
            latest_fx_date = db.get_latest_fx_date(base)
            if latest_fx_date:
                start_date = (pd.to_datetime(latest_fx_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                start_date = global_start_date

            # If up to date, skip
            if pd.to_datetime(start_date) > pd.to_datetime(now_str):
                logging.info("FX for %s→EUR already up to date.", base)
                continue

            tasks.append((base, start_date, now_str))

        if not tasks:
            logging.info("All FX rates already up to date.")
            return True

        logging.info("Need to fetch FX for %d currencies.", len(tasks))

        # Batch tasks to reduce requests
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            # tickers = [f"{b}EUR=X" for (b, _, _) in batch]
            tickers = []
            for (b, _, _) in batch:
                base = b
                if base == "GBp":  # Yahoo uses GBP, not GBp
                    base = "GBP"
                tickers.append(f"{base}EUR=X")
            start = min(t[1] for t in batch)
            end = max(t[2] for t in batch)

            logging.info("📦 Fetching FX batch: %s (%s → %s)", ", ".join(tickers), start, end)

            try:
                df = yf.download(
                    tickers=tickers,
                    start=start,
                    end=end,
                    interval="1d",
                    group_by="ticker",
                    threads=True
                )

                # Parse MultiIndex output
                if isinstance(df.columns, pd.MultiIndex):
                    for ticker in tickers:
                        if ticker not in df.columns.levels[0]:
                            logging.warning("No FX data for %s", ticker)
                            all_success = False
                            continue

                        base = ticker.replace("EUR=X", "")
                        if base == "GBp":  # normalize before saving
                            base = "GBP"
                        sub = df[ticker].reset_index().rename(columns={"Date": "date", "Close": "rate"})
                        sub["date"] = pd.to_datetime(sub["date"]).dt.strftime("%Y-%m-%d")
                        sub["base_currency"] = base
                        sub["target_currency"] = "EUR"
                        sub = sub[["date", "base_currency", "target_currency", "rate"]]
                        sub = sub.dropna(subset=["rate"])

                        if sub.empty:
                            # logging.warning("Empty FX data for %s→EUR", base)
                            logging.info("No new FX data available for %s→EUR (likely up to date or weekend).", base)
                            # all_success = False
                            continue

                        mw.store_fx_rates(sub)
                        logging.info("✅ Stored FX for %s→EUR (%d rows)", base, len(sub))

                else:
                    # Single ticker
                    ticker = tickers[0]
                    base = ticker.replace("EUR=X", "")
                    if base == "GBp":  # normalize before saving
                        base = "GBP"
                    sub = df.reset_index().rename(columns={"Date": "date", "Close": "rate"})
                    sub["date"] = pd.to_datetime(sub["date"]).dt.strftime("%Y-%m-%d")
                    sub["base_currency"] = base
                    sub["target_currency"] = "EUR"
                    sub = sub[["date", "base_currency", "target_currency", "rate"]].dropna(subset=["rate"])

                    mw.store_fx_rates(sub)
                    logging.info("✅ Stored FX for %s→EUR (%d rows)", base, len(sub))

            except Exception as e:
                logging.warning("❌ FX batch failed (%s): %s", ", ".join(tickers), e)
                all_success = False

            throttler.wait()

        return all_success

    except Exception:
        logging.exception("FX rate fetching failed.")
        return False



def fetch_lazy_security(symbol: str) -> Optional[dict]:
    """
    Fetch minimal Yahoo data: type, country, exchange, sector, industry, names, and market price.
    Returns a dict or None if fetch fails.
    """
    try:
        t = yf.Ticker(symbol)
        info = t.info or {}
        return {
            "symbol": symbol,
            "security_type": info.get("quoteType"),
            "country": info.get("country"),
            "exchange": info.get("exchange"),
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "shortName": info.get("shortName"),
            "longName": info.get("longName"),
            "regularMarketPrice": info.get("regularMarketPrice"),
        }
    except Exception as e:
        logging.exception("Lazy fetch failed for %s: %s", symbol, e)
        return None


def fetch_and_store_lazy(symbol: str) -> Optional[int]:
    """
    Fetch minimal info for a symbol and store it.
    Returns security_id.
    """
    security_id = db.get_security_id(symbol)
    if security_id is None:
        security_id = db.insert_security(symbol)

    data = fetch_lazy_security(symbol)
    if data:
        db.store_lazy_security(security_id, data)

    return security_id


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Fetch Yahoo Finance data and update SQLite DB")
    ap.add_argument(
        "--tickers",
        default="",
        help="Comma-separated tickers (optional). If empty, updates all securities in DB"
    )
    args = ap.parse_args()

    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()] if args.tickers else []
    run_fetch(tickers)