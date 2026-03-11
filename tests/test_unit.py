"""
tests/test_unit.py

Unit tests for pure business logic — no DB, no network, no Streamlit.
These run in milliseconds and are safe for pre-commit hooks.

Covers:
  - Technical indicators (SMA, EMA, RSI, Bollinger, crossovers)
  - Risk metrics (volatility, max drawdown, Sharpe, Sortino, CAGR)
  - KPI scoring and temperature labels (middleware.calc_security_KPIs)
  - Alert evaluation logic (middleware.evaluate_alert) via data mocking
  - FIFO capital gains calculation
  - Dividend attribution logic
"""

import sys
import os
import json
import pytest
import pandas as pd
import numpy as np

# ── make the app importable without installing it ──────────────────────────
APP_DIR = os.path.join(os.path.dirname(__file__), "..", "app")
sys.path.insert(0, os.path.abspath(APP_DIR))


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _price_series(values, start="2023-01-01"):
    """Return a pd.Series with a DatetimeIndex from a plain list of floats."""
    idx = pd.date_range(start=start, periods=len(values), freq="D")
    return pd.Series(values, index=idx, dtype=float)


def _price_df(values, col="close", start="2023-01-01"):
    """Return a single-column DataFrame suitable for volatility / Sharpe etc."""
    idx = pd.date_range(start=start, periods=len(values), freq="D")
    return pd.DataFrame({col: values}, index=idx)


# ─────────────────────────────────────────────────────────────────────────────
# Import the modules under test
# ─────────────────────────────────────────────────────────────────────────────

# middleware imports db_utils at module level, which tries to connect — we stub
# the db_utils module so the import doesn't fail without a real DB.
import types

_fake_db = types.ModuleType("db_utils")
# Provide minimal stubs that middleware may call at import time
for _attr in ["get_conn", "get_engine", "_read_sql", "get_config"]:
    setattr(_fake_db, _attr, lambda *a, **kw: None)
# get_dividends must return a DataFrame (middleware iterates over it)
_fake_db.get_dividends = lambda sym: pd.DataFrame()
sys.modules.setdefault("db_utils", _fake_db)

# config_utils also reads from disk — stub it too
_fake_cfg = types.ModuleType("config_utils")
_fake_cfg.get_config = lambda key: None
_fake_cfg.safe_json_load = lambda v, default=None: default or {}
sys.modules.setdefault("config_utils", _fake_cfg)

# data_fetcher is pulled in by middleware
_fake_df = types.ModuleType("data_fetcher")
_fake_df.fetch_and_store_lazy = lambda *a, **kw: None
sys.modules.setdefault("data_fetcher", _fake_df)

# yfinance — stub so unit tests have zero network/install requirements
_fake_yf = types.ModuleType("yfinance")
_fake_yf.Ticker = lambda *a, **kw: None
_fake_yf.download = lambda *a, **kw: pd.DataFrame()
sys.modules.setdefault("yfinance", _fake_yf)

# requests — may be imported at top level in some helpers
_fake_req = types.ModuleType("requests")
_fake_req.get  = lambda *a, **kw: None
_fake_req.post = lambda *a, **kw: None
sys.modules.setdefault("requests", _fake_req)

# scipy is used by middleware.local_min_max — provide a real-ish stub if absent
try:
    from scipy.signal import argrelextrema
except ImportError:
    _fake_scipy = types.ModuleType("scipy")
    _fake_signal = types.ModuleType("scipy.signal")
    _fake_signal.argrelextrema = lambda arr, cmp, order=5: (np.array([], dtype=int),)
    _fake_scipy.signal = _fake_signal
    sys.modules.setdefault("scipy", _fake_scipy)
    sys.modules.setdefault("scipy.signal", _fake_signal)

import middleware as mw  # noqa: E402  (import after stubs)


# ═════════════════════════════════════════════════════════════════════════════
# 1. Technical Indicators
# ═════════════════════════════════════════════════════════════════════════════

class TestSMA:
    def test_simple_average(self):
        s = _price_series([1, 2, 3, 4, 5])
        result = mw.sma(s, 3)
        assert result.iloc[-1] == pytest.approx(4.0)

    def test_window_larger_than_series_gives_nan(self):
        s = _price_series([1, 2])
        result = mw.sma(s, 5)
        assert result.isna().all()

    def test_window_1_equals_input(self):
        values = [10.0, 20.0, 30.0]
        s = _price_series(values)
        assert list(mw.sma(s, 1).values) == pytest.approx(values)


class TestEMA:
    def test_ema_is_series(self):
        s = _price_series(list(range(1, 21)))
        result = mw.ema(s, span=5)
        assert isinstance(result, pd.Series)
        assert len(result) == 20

    def test_ema_reacts_faster_than_sma_to_jump(self):
        # After a big price jump at the end, EMA should be > SMA of same window
        values = [100.0] * 19 + [200.0]
        s = _price_series(values)
        assert mw.ema(s, span=10).iloc[-1] > mw.sma(s, 10).iloc[-1]


class TestBollinger:
    def test_returns_three_series(self):
        s = _price_series(list(range(1, 31)))
        mid, upper, lower = mw.bollinger(s, window=20)
        assert isinstance(mid, pd.Series)
        assert isinstance(upper, pd.Series)
        assert isinstance(lower, pd.Series)

    def test_upper_above_mid_above_lower(self):
        s = _price_series([float(i) for i in range(1, 31)])
        mid, upper, lower = mw.bollinger(s, window=20)
        valid = mid.dropna()
        assert (upper.dropna() > valid).all()
        assert (valid > lower.dropna()).all()

    def test_flat_series_gives_zero_band_width(self):
        s = _price_series([50.0] * 30)
        mid, upper, lower = mw.bollinger(s, window=20)
        # std of a constant series is 0 → bands collapse to mid
        assert (upper.dropna() == mid.dropna()).all()


class TestRSI:
    def test_rsi_bounds(self):
        import random
        random.seed(42)
        vals = [100.0]
        for _ in range(99):
            vals.append(vals[-1] * (1 + random.uniform(-0.03, 0.03)))
        s = _price_series(vals)
        result = mw.rsi(s, window=14).dropna()
        assert (result >= 0).all()
        assert (result <= 100).all()

    def test_always_rising_gives_high_rsi(self):
        s = _price_series([float(i) for i in range(1, 50)])
        result = mw.rsi(s, window=14).dropna()
        assert result.iloc[-1] > 70

    def test_always_falling_gives_low_rsi(self):
        s = _price_series([float(50 - i) for i in range(50)])
        result = mw.rsi(s, window=14).dropna()
        assert result.iloc[-1] < 30


class TestCalcReturns:
    def test_first_value_is_nan(self):
        s = _price_series([100.0, 110.0, 99.0])
        r = mw.calc_returns(s)
        assert pd.isna(r.iloc[0])

    def test_correct_return_value(self):
        s = _price_series([100.0, 110.0])
        r = mw.calc_returns(s)
        assert r.iloc[1] == pytest.approx(0.10)

    def test_negative_return(self):
        s = _price_series([200.0, 180.0])
        r = mw.calc_returns(s)
        assert r.iloc[1] == pytest.approx(-0.10)


class TestCrossover:
    def test_golden_cross_detected(self):
        # short MA crosses above long MA at index 5
        short = pd.Series([1, 1, 1, 1, 1, 3, 3, 3])
        long_  = pd.Series([2, 2, 2, 2, 2, 2, 2, 2])
        buys, sells = mw.find_crossovers(short, long_)
        assert 5 in buys

    def test_death_cross_detected(self):
        short = pd.Series([3, 3, 3, 3, 3, 1, 1, 1])
        long_  = pd.Series([2, 2, 2, 2, 2, 2, 2, 2])
        buys, sells = mw.find_crossovers(short, long_)
        assert 5 in sells

    def test_no_cross_gives_empty_lists(self):
        short = pd.Series([5.0] * 10)
        long_  = pd.Series([3.0] * 10)
        buys, sells = mw.find_crossovers(short, long_)
        assert buys == [] and sells == []

    def test_mismatched_lengths_raises(self):
        with pytest.raises(ValueError):
            mw.find_crossovers(pd.Series([1, 2, 3]), pd.Series([1, 2]))


# ═════════════════════════════════════════════════════════════════════════════
# 2. Risk Metrics
# ═════════════════════════════════════════════════════════════════════════════

class TestVolatility:
    def test_constant_prices_give_zero_vol(self):
        df = _price_df([100.0] * 50)
        assert mw.volatility(df) == pytest.approx(0.0, abs=1e-10)

    def test_volatile_series_gives_positive_vol(self):
        np.random.seed(0)
        prices = np.cumprod(1 + np.random.normal(0, 0.02, 200)) * 100
        df = _price_df(prices.tolist())
        assert mw.volatility(df) > 0

    def test_higher_variance_means_higher_vol(self):
        np.random.seed(1)
        low_noise = np.cumprod(1 + np.random.normal(0, 0.005, 200)) * 100
        high_noise = np.cumprod(1 + np.random.normal(0, 0.05, 200)) * 100
        assert mw.volatility(_price_df(high_noise)) > mw.volatility(_price_df(low_noise))


class TestMaxDrawdown:
    def test_empty_series_returns_zero(self):
        assert mw.max_drawdown(pd.Series([], dtype=float)) == 0.0

    def test_always_rising_gives_zero_drawdown(self):
        s = _price_series([float(i) for i in range(1, 101)])
        assert mw.max_drawdown(s) == pytest.approx(0.0, abs=1e-10)

    def test_known_drawdown(self):
        # price goes from 100 → 50: drawdown should be 50 %
        s = _price_series([100.0, 80.0, 50.0])
        assert mw.max_drawdown(s) == pytest.approx(0.5)

    def test_recovers_but_max_dd_preserved(self):
        s = _price_series([100.0, 50.0, 150.0])
        # max drawdown was -50% (100→50) even though it later recovered
        assert mw.max_drawdown(s) == pytest.approx(0.5)


class TestSharpeRatio:
    def test_positive_for_steadily_rising_series(self):
        df = _price_df([100.0 * (1.001 ** i) for i in range(300)])
        assert mw.sharpe_ratio(df) > 0

    def test_negative_for_falling_series(self):
        df = _price_df([100.0 * (0.999 ** i) for i in range(300)])
        assert mw.sharpe_ratio(df) < 0


class TestSortinoRatio:
    def test_sortino_higher_than_sharpe_for_right_skewed_returns(self):
        """
        For a series with only upside volatility, Sortino should be >= Sharpe.
        """
        # only positive daily moves → no downside → Sortino → inf or very high
        df = _price_df([100.0 + i for i in range(300)])
        sharpe = mw.sharpe_ratio(df)
        # Sortino should not be worse
        sortino = mw.sortino_ratio(df)
        assert sortino >= sharpe or np.isnan(sortino)  # nan if no downside days


class TestCAGR:
    def test_empty_returns_nan(self):
        assert np.isnan(mw.cagr(_price_df([])))

    def test_known_cagr(self):
        # price doubles in ~365 days → CAGR ≈ 100%
        prices = [100.0 * (2 ** (i / 365)) for i in range(366)]
        df = _price_df(prices)
        result = mw.cagr(df)
        assert result == pytest.approx(1.0, rel=0.01)


# ═════════════════════════════════════════════════════════════════════════════
# 3. KPI Scoring & Temperature Labels
# ═════════════════════════════════════════════════════════════════════════════

def _make_watchlist_row(**overrides):
    """Return a single-row DataFrame with all KPI columns defaulted to None."""
    defaults = dict(
        security_id=1, symbol="TEST", security_name="Test Corp",
        shortName="Test", security_type="Equity",
        country="DE", exchange="XETRA", sector="Technology",
        industry="Software", currency="EUR",
        regularMarketPrice=None, fiftyTwoWeekHigh=None, fiftyTwoWeekLow=None,
        volume=None, averageVolume=None, marketCap=None,
        beta=None, trailingPE=None, forwardPE=None,
        eps=None, earnings_date=None,
        dividendRate=None, dividendYield=None,
        enterpriseValue=None, profitMargins=None, operatingMargins=None,
        returnOnAssets=None, returnOnEquity=None, totalRevenue=None,
        revenuePerShare=None, grossProfits=None, ebitda=None,
        totalCash=None, totalDebt=None, currentRatio=None,
        bookValue=None, operatingCashflow=None, freeCashflow=None,
        sharesOutstanding=None,
    )
    defaults.update(overrides)
    return pd.DataFrame([defaults])


class TestCalcSecurityKPIs:
    def test_pb_ratio_computed(self):
        df = _make_watchlist_row(regularMarketPrice=30.0, bookValue=10.0)
        result = mw.calc_security_KPIs(df)
        assert result.iloc[0]["pb_ratio"] == pytest.approx(3.0)

    def test_pb_ratio_none_when_book_value_zero(self):
        df = _make_watchlist_row(regularMarketPrice=30.0, bookValue=0.0)
        result = mw.calc_security_KPIs(df)
        assert result.iloc[0]["pb_ratio"] is None

    def test_from_52w_low_midpoint(self):
        # at the midpoint between 52w low and high → 0.5
        df = _make_watchlist_row(
            regularMarketPrice=150.0, fiftyTwoWeekLow=100.0, fiftyTwoWeekHigh=200.0
        )
        result = mw.calc_security_KPIs(df)
        assert result.iloc[0]["from_52w_low"] == pytest.approx(0.5)

    def test_from_52w_low_at_low(self):
        df = _make_watchlist_row(
            regularMarketPrice=100.0, fiftyTwoWeekLow=100.0, fiftyTwoWeekHigh=200.0
        )
        result = mw.calc_security_KPIs(df)
        assert result.iloc[0]["from_52w_low"] == pytest.approx(0.0)

    def test_temperature_hot_for_strong_fundamentals(self):
        df = _make_watchlist_row(
            regularMarketPrice=100.0, bookValue=80.0,
            fiftyTwoWeekLow=90.0, fiftyTwoWeekHigh=200.0,
            beta=0.7, trailingPE=10.0,
            dividendYield=0.05, profitMargins=0.25,
        )
        result = mw.calc_security_KPIs(df)
        assert result.iloc[0]["Temperature"] == "Hot"

    def test_temperature_cold_for_weak_fundamentals(self):
        df = _make_watchlist_row(
            regularMarketPrice=190.0, bookValue=10.0,
            fiftyTwoWeekLow=100.0, fiftyTwoWeekHigh=200.0,
            beta=2.5, trailingPE=80.0,
            dividendYield=0.001, profitMargins=0.01,
        )
        result = mw.calc_security_KPIs(df)
        assert result.iloc[0]["Temperature"] == "Cold"

    def test_temperature_cold_when_all_kpis_missing(self):
        # All scoring functions return 0 for None inputs → mean=0.0 → "Cold"
        # (pandas mean() of all-zero series is 0.0, which maps to the lowest tier)
        df = _make_watchlist_row()
        result = mw.calc_security_KPIs(df)
        assert result.iloc[0]["Temperature"] == "Cold"

    def test_original_dataframe_not_mutated(self):
        df = _make_watchlist_row(regularMarketPrice=50.0, bookValue=20.0)
        original_cols = set(df.columns)
        mw.calc_security_KPIs(df)
        assert set(df.columns) == original_cols  # original unchanged


# ═════════════════════════════════════════════════════════════════════════════
# 4. Alert Evaluation Logic
# ═════════════════════════════════════════════════════════════════════════════

def _alert(alert_type, params, symbol="AAPL"):
    return {"symbol": symbol, "alert_type": alert_type, "params": json.dumps(params)}


class TestEvaluateAlert:
    """
    evaluate_alert calls fetch_symbol_data internally, which hits the DB.
    We monkeypatch fetch_symbol_data to return controlled data.
    """

    def _eval(self, monkeypatch, alert, market_data: dict) -> bool:
        monkeypatch.setattr(mw, "fetch_symbol_data", lambda sym: market_data)
        return mw.evaluate_alert(alert)

    # --- price alerts ---
    def test_price_above_triggers(self, monkeypatch):
        alert = _alert("price", {"threshold": 100.0, "mode": "absolute", "direction": "above"})
        assert self._eval(monkeypatch, alert, {"last_price": 110.0}) is True

    def test_price_above_does_not_trigger_when_below(self, monkeypatch):
        alert = _alert("price", {"threshold": 100.0, "mode": "absolute", "direction": "above"})
        assert self._eval(monkeypatch, alert, {"last_price": 90.0}) is False

    def test_price_below_triggers(self, monkeypatch):
        alert = _alert("price", {"threshold": 50.0, "mode": "absolute", "direction": "below"})
        assert self._eval(monkeypatch, alert, {"last_price": 40.0}) is True

    def test_price_below_no_trigger_when_above(self, monkeypatch):
        alert = _alert("price", {"threshold": 50.0, "mode": "absolute", "direction": "below"})
        assert self._eval(monkeypatch, alert, {"last_price": 60.0}) is False

    # --- RSI alerts ---
    def test_rsi_overbought_triggers(self, monkeypatch):
        alert = _alert("rsi", {"threshold": 70, "direction": "above"})
        assert self._eval(monkeypatch, alert, {"rsi": 75.0}) is True

    def test_rsi_oversold_triggers(self, monkeypatch):
        alert = _alert("rsi", {"threshold": 30, "direction": "below"})
        assert self._eval(monkeypatch, alert, {"rsi": 25.0}) is True

    def test_rsi_no_trigger_when_neutral(self, monkeypatch):
        alert = _alert("rsi", {"threshold": 70, "direction": "above"})
        assert self._eval(monkeypatch, alert, {"rsi": 55.0}) is False

    # --- MA crossover alerts ---
    def test_golden_cross_triggers(self, monkeypatch):
        alert = _alert("ma_crossover", {"short": 50, "long": 200, "direction": "golden"})
        data = {"sma": {50: 205.0, 200: 200.0}}
        assert self._eval(monkeypatch, alert, data) is True

    def test_death_cross_triggers(self, monkeypatch):
        alert = _alert("ma_crossover", {"short": 50, "long": 200, "direction": "death"})
        data = {"sma": {50: 195.0, 200: 200.0}}
        assert self._eval(monkeypatch, alert, data) is True

    def test_golden_cross_no_trigger_when_below(self, monkeypatch):
        alert = _alert("ma_crossover", {"short": 50, "long": 200, "direction": "golden"})
        data = {"sma": {50: 190.0, 200: 200.0}}
        assert self._eval(monkeypatch, alert, data) is False

    # --- 52-week high/low ---
    def test_52w_high_triggers(self, monkeypatch):
        alert = _alert("52w", {"type": "high"})
        assert self._eval(monkeypatch, alert, {"last_price": 200.0, "52w_high": 200.0}) is True

    def test_52w_low_triggers(self, monkeypatch):
        alert = _alert("52w", {"type": "low"})
        assert self._eval(monkeypatch, alert, {"last_price": 100.0, "52w_low": 100.0}) is True

    def test_52w_high_no_trigger_below_high(self, monkeypatch):
        alert = _alert("52w", {"type": "high"})
        assert self._eval(monkeypatch, alert, {"last_price": 190.0, "52w_high": 200.0}) is False

    # --- Volume spike ---
    def test_volume_spike_triggers(self, monkeypatch):
        alert = _alert("volume_spike", {"multiplier": 2.0, "lookback": 20})
        data = {"volume": 2_000_000, "avg_volume": {20: 800_000}}
        assert self._eval(monkeypatch, alert, data) is True

    def test_volume_spike_no_trigger_below_mult(self, monkeypatch):
        alert = _alert("volume_spike", {"multiplier": 3.0, "lookback": 20})
        data = {"volume": 1_500_000, "avg_volume": {20: 1_000_000}}
        assert self._eval(monkeypatch, alert, data) is False

    # --- Edge cases ---
    def test_empty_data_returns_false(self, monkeypatch):
        alert = _alert("price", {"threshold": 100.0, "mode": "absolute", "direction": "above"})
        assert self._eval(monkeypatch, alert, {}) is False

    def test_unknown_alert_type_returns_false(self, monkeypatch):
        alert = _alert("nonexistent_type", {})
        assert self._eval(monkeypatch, alert, {"last_price": 100.0}) is False


# ═════════════════════════════════════════════════════════════════════════════
# 5. FIFO Capital Gains
# ═════════════════════════════════════════════════════════════════════════════

class TestCalcCapitalGainsFIFO:
    """
    calc_capital_gains_fifo pulls transactions from the DB via list_transactions.
    We monkeypatch that function to supply synthetic data.
    """

    def _make_tx(self, monkeypatch, rows):
        df = pd.DataFrame(rows)
        monkeypatch.setattr(mw, "list_transactions", lambda pids=None: df)

    def test_simple_buy_sell_profit(self, monkeypatch):
        self._make_tx(monkeypatch, [
            {"id": 1, "portfolio_id": 1, "symbol": "AAPL", "type": "buy",
             "quantity": 10, "price": 100.0, "fees": 0.0, "date": "2023-01-01"},
            {"id": 2, "portfolio_id": 1, "symbol": "AAPL", "type": "sell",
             "quantity": 10, "price": 150.0, "fees": 0.0, "date": "2023-06-01"},
        ])
        result = mw.calc_capital_gains_fifo()
        assert len(result) == 1
        assert result.iloc[0]["profit"] == pytest.approx(500.0)

    def test_fifo_order_respected(self, monkeypatch):
        # buy 5@100 then 5@120, sell 5 — should use cheapest lot first
        self._make_tx(monkeypatch, [
            {"id": 1, "portfolio_id": 1, "symbol": "TSLA", "type": "buy",
             "quantity": 5, "price": 100.0, "fees": 0.0, "date": "2023-01-01"},
            {"id": 2, "portfolio_id": 1, "symbol": "TSLA", "type": "buy",
             "quantity": 5, "price": 120.0, "fees": 0.0, "date": "2023-02-01"},
            {"id": 3, "portfolio_id": 1, "symbol": "TSLA", "type": "sell",
             "quantity": 5, "price": 130.0, "fees": 0.0, "date": "2023-07-01"},
        ])
        result = mw.calc_capital_gains_fifo()
        assert len(result) == 1
        # proceeds = 5*130=650, cost_basis = 5*100=500 (first lot)
        assert result.iloc[0]["cost_basis"] == pytest.approx(500.0)
        assert result.iloc[0]["profit"] == pytest.approx(150.0)

    def test_fees_reduce_profit(self, monkeypatch):
        self._make_tx(monkeypatch, [
            {"id": 1, "portfolio_id": 1, "symbol": "MSFT", "type": "buy",
             "quantity": 10, "price": 100.0, "fees": 10.0, "date": "2023-01-01"},
            {"id": 2, "portfolio_id": 1, "symbol": "MSFT", "type": "sell",
             "quantity": 10, "price": 120.0, "fees": 5.0, "date": "2023-06-01"},
        ])
        result = mw.calc_capital_gains_fifo()
        # proceeds = 10*120 - 5 = 1195
        # cost_basis = 10*100 + 10 = 1010
        # profit = 185
        assert result.iloc[0]["profit"] == pytest.approx(185.0)

    def test_year_filter(self, monkeypatch):
        self._make_tx(monkeypatch, [
            {"id": 1, "portfolio_id": 1, "symbol": "AMZN", "type": "buy",
             "quantity": 1, "price": 100.0, "fees": 0.0, "date": "2022-01-01"},
            {"id": 2, "portfolio_id": 1, "symbol": "AMZN", "type": "sell",
             "quantity": 1, "price": 200.0, "fees": 0.0, "date": "2022-06-01"},
            {"id": 3, "portfolio_id": 1, "symbol": "AMZN", "type": "buy",
             "quantity": 1, "price": 150.0, "fees": 0.0, "date": "2022-07-01"},
            {"id": 4, "portfolio_id": 1, "symbol": "AMZN", "type": "sell",
             "quantity": 1, "price": 300.0, "fees": 0.0, "date": "2023-03-01"},
        ])
        result_2022 = mw.calc_capital_gains_fifo(year=2022)
        result_2023 = mw.calc_capital_gains_fifo(year=2023)
        assert len(result_2022) == 1
        assert len(result_2023) == 1
        assert result_2022.iloc[0]["profit"] == pytest.approx(100.0)
        assert result_2023.iloc[0]["profit"] == pytest.approx(150.0)

    def test_empty_transactions(self, monkeypatch):
        monkeypatch.setattr(mw, "list_transactions", lambda pids=None: pd.DataFrame())
        result = mw.calc_capital_gains_fifo()
        assert result.empty


# ═════════════════════════════════════════════════════════════════════════════
# 6. Dividend Attribution
# ═════════════════════════════════════════════════════════════════════════════

class TestCalcDividends:
    """Tests for calc_dividends_for_portfolio — DB calls are monkeypatched."""

    def _setup(self, monkeypatch, tx_rows, div_rows_by_symbol):
        tx_df = pd.DataFrame(tx_rows)
        monkeypatch.setattr(mw, "list_transactions", lambda pids=None: tx_df)

        import db_utils as _db  # the stub
        def fake_get_dividends(sym):
            rows = div_rows_by_symbol.get(sym, [])
            return pd.DataFrame(rows)
        monkeypatch.setattr(_db, "get_dividends", fake_get_dividends)

    def test_dividend_attributed_correctly(self, monkeypatch):
        self._setup(monkeypatch,
            tx_rows=[
                {"id": 1, "portfolio_id": 1, "symbol": "VW", "type": "buy",
                 "quantity": 100, "price": 10.0, "fees": 0.0, "date": "2023-01-01"},
            ],
            div_rows_by_symbol={
                "VW": [{"date": "2023-06-15", "dividend": 0.50}]
            }
        )
        result = mw.calc_dividends_for_portfolio()
        assert len(result) == 1
        assert result.iloc[0]["total"] == pytest.approx(50.0)   # 100 shares * 0.50

    def test_no_dividend_before_purchase(self, monkeypatch):
        self._setup(monkeypatch,
            tx_rows=[
                {"id": 1, "portfolio_id": 1, "symbol": "BMW", "type": "buy",
                 "quantity": 10, "price": 50.0, "fees": 0.0, "date": "2023-06-01"},
            ],
            div_rows_by_symbol={
                "BMW": [{"date": "2023-01-01", "dividend": 1.0}]  # before purchase
            }
        )
        result = mw.calc_dividends_for_portfolio()
        assert result.empty

    def test_sold_shares_reduce_dividend(self, monkeypatch):
        self._setup(monkeypatch,
            tx_rows=[
                {"id": 1, "portfolio_id": 1, "symbol": "SAP", "type": "buy",
                 "quantity": 100, "price": 100.0, "fees": 0.0, "date": "2023-01-01"},
                {"id": 2, "portfolio_id": 1, "symbol": "SAP", "type": "sell",
                 "quantity": 60, "price": 110.0, "fees": 0.0, "date": "2023-03-01"},
            ],
            div_rows_by_symbol={
                "SAP": [{"date": "2023-06-01", "dividend": 2.0}]
            }
        )
        result = mw.calc_dividends_for_portfolio()
        assert result.iloc[0]["shares"] == pytest.approx(40.0)
        assert result.iloc[0]["total"] == pytest.approx(80.0)
