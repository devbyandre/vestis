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
_fake_db.get_last_alert_log = lambda alert_id: None  # no previous log = first trigger
_fake_db.get_security_cache = lambda security_id: {}

# get_price_history returns enough data for ma_crossover and 52w tests
def _fake_price_history(symbol, lookback_days=400):
    n = 300
    dates = pd.date_range(end=pd.Timestamp.utcnow(), periods=n, freq='D')
    # Flat prices then a jump at the end — creates golden cross in last few bars
    # First 250 bars: price=100, last 50 bars: price=200
    # This makes 50-SMA cross above 200-SMA near the end
    prices = [100.0] * 250 + [200.0] * 50
    return pd.DataFrame({
        "date": dates,
        "adj_close": pd.Series(prices, dtype=float),
        "close": pd.Series(prices, dtype=float),
        "volume": [1000000] * n
    })

_fake_db.get_price_history = _fake_price_history
sys.modules.setdefault("db_utils", _fake_db)

# config_utils also reads from disk — stub it too
_fake_cfg = types.ModuleType("config_utils")
_fake_cfg.get_config = lambda key: None
_fake_cfg.set_config = lambda key, value: None
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
    Technical alerts read native-currency trading-day bars via
    middleware.alerts._bars; price/mos alerts read fetch_symbol_data (EUR).
    Both are patched on the owning submodule (mw.alerts), since evaluate_alert
    looks them up as module globals.
    """

    @staticmethod
    def _bars_df(prices, volumes=None, end="2026-10-01"):
        dates = pd.bdate_range(end=end, periods=len(prices))
        return pd.DataFrame({
            "date": dates,
            "price": pd.Series(prices, dtype=float),
            "volume": pd.Series(volumes if volumes is not None else [1_000_000] * len(prices), dtype=float),
        })

    def _eval(self, monkeypatch, alert, market_data=None, bars=None):
        monkeypatch.setattr(mw.alerts, "fetch_symbol_data", lambda sym: market_data or {})
        monkeypatch.setattr(mw.alerts, "_bars", lambda sym: bars if bars is not None else pd.DataFrame())
        return mw.evaluate_alert(alert)

    def _run(self, monkeypatch, alert, series_list):
        """Evaluate once per bars snapshot, carrying state like the worker does."""
        fired = []
        for bars in series_list:
            hit = self._eval(monkeypatch, alert, bars=bars)
            fired.append(hit)
            alert["state"] = json.dumps(alert["_state"])
        return fired

    # --- price alerts (EUR, crossing) ---
    def _cross(self, monkeypatch, alert, prices):
        fired = []
        for lp in prices:
            fired.append(self._eval(monkeypatch, alert, {"last_price": lp}))
            alert["state"] = json.dumps(alert["_state"])
        return fired

    def test_price_above_triggers(self, monkeypatch):
        alert = _alert("price", {"threshold": 100.0, "mode": "absolute", "direction": "above"})
        assert self._cross(monkeypatch, alert, [90.0, 110.0]) == [False, True]

    def test_price_already_past_threshold_does_not_fire_on_first_look(self, monkeypatch):
        # Regression: alerts without stored state fired immediately when the price
        # was already beyond the threshold (no crossing happened).
        alert = _alert("price", {"threshold": 326.75, "mode": "absolute", "direction": "below"})
        assert self._cross(monkeypatch, alert, [33.27, 33.0]) == [False, False]

    def test_price_above_does_not_trigger_when_below(self, monkeypatch):
        alert = _alert("price", {"threshold": 100.0, "mode": "absolute", "direction": "above"})
        assert self._eval(monkeypatch, alert, {"last_price": 90.0}) is False

    def test_price_below_triggers(self, monkeypatch):
        alert = _alert("price", {"threshold": 50.0, "mode": "absolute", "direction": "below"})
        assert self._cross(monkeypatch, alert, [55.0, 40.0]) == [False, True]

    def test_price_below_no_trigger_when_above(self, monkeypatch):
        alert = _alert("price", {"threshold": 50.0, "mode": "absolute", "direction": "below"})
        assert self._eval(monkeypatch, alert, {"last_price": 60.0}) is False

    def test_price_refires_after_crossing_back(self, monkeypatch):
        # Regression: side used to be recorded only when firing, so a price
        # alert could never fire a second time.
        alert = _alert("price", {"threshold": 50.0, "mode": "absolute", "direction": "below"})
        fired = []
        for lp in (60.0, 40.0, 41.0, 60.0, 45.0):
            fired.append(self._eval(monkeypatch, alert, {"last_price": lp}))
            alert["state"] = json.dumps(alert["_state"])
        assert fired == [False, True, False, False, True]

    # --- RSI (edge-triggered with re-arm gap) ---
    def test_rsi_fires_once_then_rearms(self, monkeypatch):
        alert = _alert("rsi", {"threshold": 70, "direction": "above"})
        up = list(np.linspace(100, 160, 60))                 # strong rally → RSI high
        down = up + list(np.linspace(160, 130, 20))          # pullback → RSI low
        again = down + list(np.linspace(130, 190, 40))       # new rally
        fired = self._run(monkeypatch, alert, [
            self._bars_df(up), self._bars_df(up + [160.5]),  # still overbought: no repeat
            self._bars_df(down), self._bars_df(again),
        ])
        assert fired == [True, False, False, True]

    def test_rsi_oversold_triggers(self, monkeypatch):
        alert = _alert("rsi", {"threshold": 30, "direction": "below"})
        assert self._eval(monkeypatch, alert, bars=self._bars_df(list(np.linspace(160, 100, 60)))) is True

    def test_rsi_no_trigger_when_neutral(self, monkeypatch):
        alert = _alert("rsi", {"threshold": 70, "direction": "above"})
        flat = [100 + (1 if i % 2 else -1) for i in range(60)]
        assert self._eval(monkeypatch, alert, bars=self._bars_df(flat)) is False

    # --- MA crossover ---
    def test_golden_cross_triggers_once(self, monkeypatch):
        alert = _alert("ma_crossover", {"short": 5, "long": 20, "crossover_type": "golden"})
        prices = list(np.linspace(120, 100, 40)) + list(np.linspace(100, 115, 6))
        fired = self._run(monkeypatch, alert, [self._bars_df(prices), self._bars_df(prices + [116], end="2026-10-02")])
        assert fired == [True, False]

    def test_ma_hugging_does_not_flip(self, monkeypatch):
        # MAs within the min spread band must not produce crosses
        alert = _alert("ma_crossover", {"short": 5, "long": 20, "crossover_type": "golden"})
        prices = [100 + 0.05 * (1 if i % 3 else -1) for i in range(60)]
        assert self._eval(monkeypatch, alert, bars=self._bars_df(prices)) is False

    # --- 52-week high/low ---
    def test_52w_high_triggers(self, monkeypatch):
        alert = _alert("52w", {"type": "high"})
        assert self._eval(monkeypatch, alert, bars=self._bars_df([100.0] * 260 + [120.0])) is True

    def test_52w_low_triggers(self, monkeypatch):
        alert = _alert("52w", {"type": "low"})
        assert self._eval(monkeypatch, alert, bars=self._bars_df([100.0] * 260 + [80.0])) is True

    def test_52w_high_no_trigger_below_high(self, monkeypatch):
        alert = _alert("52w", {"type": "high"})
        assert self._eval(monkeypatch, alert, bars=self._bars_df([100.0] * 100 + [200.0] + [100.0] * 100 + [190.0])) is False

    # --- Volume spike ---
    def test_volume_spike_triggers(self, monkeypatch):
        alert = _alert("volume_spike", {"multiplier": 2.0, "lookback": 20})
        bars = self._bars_df([100.0] * 30, volumes=[800_000] * 29 + [2_000_000])
        assert self._eval(monkeypatch, alert, bars=bars) is True

    def test_volume_spike_no_trigger_below_mult(self, monkeypatch):
        alert = _alert("volume_spike", {"multiplier": 3.0, "lookback": 20})
        bars = self._bars_df([100.0] * 30, volumes=[1_000_000] * 29 + [1_500_000])
        assert self._eval(monkeypatch, alert, bars=bars) is False

    # --- % move ---
    def test_pct_drop_fires_once_per_trading_day(self, monkeypatch):
        alert = _alert("pct_change", {"pct": 5, "days": 1, "direction": "down"})
        day1 = self._bars_df([100.0] * 10 + [94.0], end="2026-10-01")
        day2 = self._bars_df([100.0] * 9 + [94.0, 88.0], end="2026-10-02")
        fired = self._run(monkeypatch, alert, [day1, day1, day1, day2])
        assert fired == [True, False, False, True]
        assert "-6.0%" in alert["_detail"] or "-6.4%" in alert["_detail"]

    # --- Edge cases ---
    def test_empty_data_returns_false(self, monkeypatch):
        alert = _alert("price", {"threshold": 100.0, "mode": "absolute", "direction": "above"})
        assert self._eval(monkeypatch, alert, {}) is False

    def test_unknown_alert_type_returns_false(self, monkeypatch):
        alert = _alert("nonexistent_type", {})
        assert self._eval(monkeypatch, alert, bars=self._bars_df([100.0] * 10)) is False


class TestAlertRegression20261001:
    """
    Replays the real market data behind the 2026-10-01 alerts (see
    tests/fixtures). Before the fix, TSM fired "RSI overbought" every 4h
    (RSI computed on EUR-converted, weekend-filled prices: 74.8 vs 66.1 in
    USD) and Visa fired a golden cross while its 20/50 SMAs were 0.04%
    apart. Bayer's -5.3% drop was real and must still fire — once.
    """

    FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "alert_regression_2026-10-01.csv")

    def _bars(self, symbol, upto):
        df = pd.read_csv(self.FIXTURE, parse_dates=["date"])
        df = df[(df["symbol"] == symbol) & (df["date"] <= upto)]
        return df.rename(columns={"close": "price"}).reset_index(drop=True)

    def _replay(self, monkeypatch, symbol, alert, days):
        monkeypatch.setattr(mw.alerts, "fetch_symbol_data", lambda sym: {})
        fired = []
        for day in days:
            bars = self._bars(symbol, day)
            monkeypatch.setattr(mw.alerts, "_bars", lambda sym, b=bars: b)
            for _run in range(3):  # several worker runs per day
                fired.append((day, mw.evaluate_alert(alert)))
                alert["state"] = json.dumps(alert["_state"])
        return [d for d, hit in fired if hit]

    def test_tsm_rsi_not_overbought_in_usd(self, monkeypatch):
        alert = _alert("rsi", {"threshold": 70, "direction": "above"}, symbol="TSM")
        assert self._replay(monkeypatch, "TSM", alert,
                            ["2026-09-28", "2026-09-29", "2026-09-30", "2026-10-01"]) == []
        assert "66.1" in alert["_detail"]

    def test_visa_no_golden_cross(self, monkeypatch):
        alert = _alert("ma_crossover", {"short": 20, "long": 50, "crossover_type": "golden"}, symbol="V")
        assert self._replay(monkeypatch, "V", alert,
                            ["2026-09-29", "2026-09-30", "2026-10-01"]) == []

    def test_bayer_drop_fires_exactly_once(self, monkeypatch):
        alert = _alert("pct_change", {"pct": 5, "days": 1, "direction": "down"}, symbol="BAYN.DE")
        assert self._replay(monkeypatch, "BAYN.DE", alert,
                            ["2026-09-30", "2026-10-01"]) == ["2026-10-01"]
        assert "-5.3%" in alert["_detail"]


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
        # calc_capital_gains_fifo calls list_transactions() as a name bound
        # inside middleware/revenues.py (`from .transactions import
        # list_transactions`) — patch it there, not on the mw package itself.
        monkeypatch.setattr(mw.revenues, "list_transactions", lambda pids=None: df)

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

    def test_buy_fee_is_split_across_partial_sells(self, monkeypatch):
        self._make_tx(monkeypatch, [
            {"id": 1, "portfolio_id": 1, "symbol": "MSFT", "type": "buy",
             "quantity": 10, "price": 100.0, "fees": 10.0, "date": "2023-01-01"},
            {"id": 2, "portfolio_id": 1, "symbol": "MSFT", "type": "sell",
             "quantity": 5, "price": 100.0, "fees": 0.0, "date": "2023-06-01"},
            {"id": 3, "portfolio_id": 1, "symbol": "MSFT", "type": "sell",
             "quantity": 5, "price": 100.0, "fees": 0.0, "date": "2023-07-01"},
        ])
        result = mw.calc_capital_gains_fifo()
        assert result["cost_basis"].tolist() == pytest.approx([505.0, 505.0])

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
        monkeypatch.setattr(mw.revenues, "list_transactions", lambda pids=None: pd.DataFrame())
        result = mw.calc_capital_gains_fifo()
        assert result.empty


# ═════════════════════════════════════════════════════════════════════════════
# 6. Dividend Attribution
# ═════════════════════════════════════════════════════════════════════════════

class TestCalcDividends:
    """Tests for calc_dividends_for_portfolio — DB calls are monkeypatched."""

    def _setup(self, monkeypatch, tx_rows, div_rows_by_symbol, currency="EUR", rate=1.0):
        tx_df = pd.DataFrame(tx_rows)
        monkeypatch.setattr(mw.db, "get_dividends_many", lambda syms: pd.DataFrame(
            [{**r, "symbol": sym, "currency": currency} for sym in syms for r in div_rows_by_symbol.get(sym, [])],
            columns=["symbol", "date", "dividend", "currency"]), raising=False)
        monkeypatch.setattr(mw.db, "get_fx_series",
                            lambda cur, start, end: pd.Series(rate, index=pd.date_range(start, end)),
                            raising=False)
        monkeypatch.setattr(mw.revenues, "list_transactions", lambda pids=None: tx_df)

        # Patch get_dividends on the db module that middleware has already imported
        def fake_get_dividends(sym):
            rows = div_rows_by_symbol.get(sym, [])
            return pd.DataFrame(rows)
        monkeypatch.setattr(mw.db, "get_dividends", fake_get_dividends)

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

    def test_foreign_dividend_is_converted_to_eur(self, monkeypatch):
        self._setup(monkeypatch,
            tx_rows=[{"id": 1, "portfolio_id": 1, "symbol": "MSFT", "type": "buy",
                      "quantity": 10, "price": 300.0, "fees": 0.0, "date": "2023-01-01"}],
            div_rows_by_symbol={"MSFT": [{"date": "2023-06-15", "dividend": 0.80}]},
            currency="USD", rate=0.9)
        result = mw.calc_dividends_for_portfolio()
        assert result.iloc[0]["total"] == pytest.approx(10 * 0.80 * 0.9)

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


# ─────────────────────────────────────────────────────────────────────────────
# 7. Stock split tests — paste this class into tests/test_unit.py
# ─────────────────────────────────────────────────────────────────────────────

class TestApplySplitsToTransactions:
    """Unit tests for middleware.apply_splits_to_transactions()."""

    def _make_tx(self, rows):
        """Helper: build a transactions DataFrame from a list of dicts."""
        return pd.DataFrame(rows)

    def test_no_splits_returns_unchanged(self):
        """If there are no split rows the DataFrame passes through unchanged."""
        df = self._make_tx([
            {"symbol": "AAPL", "type": "buy",  "date": "2020-01-01", "quantity": 10.0, "price": 300.0},
            {"symbol": "AAPL", "type": "sell", "date": "2021-06-01", "quantity":  5.0, "price": 350.0},
        ])
        result = mw.apply_splits_to_transactions(df)
        assert len(result) == 2
        assert result.iloc[0]["quantity"] == 10.0
        assert result.iloc[0]["price"] == 300.0

    def test_split_row_removed_from_output(self):
        """Split rows themselves are excluded from the returned DataFrame."""
        df = self._make_tx([
            {"symbol": "AAPL", "type": "buy",   "date": "2020-01-01", "quantity": 10.0, "price": 300.0},
            {"symbol": "AAPL", "type": "split", "date": "2020-08-31", "quantity":  4.0, "price":   0.0},
        ])
        result = mw.apply_splits_to_transactions(df)
        assert len(result) == 1
        assert result.iloc[0]["type"] == "buy"

    def test_4_for_1_split_adjusts_quantity_and_price(self):
        """A 4-for-1 split multiplies quantity by 4 and divides price by 4."""
        df = self._make_tx([
            {"symbol": "AAPL", "type": "buy",   "date": "2020-01-01", "quantity": 10.0, "price": 300.0},
            {"symbol": "AAPL", "type": "split", "date": "2020-08-31", "quantity":  4.0, "price":   0.0},
        ])
        result = mw.apply_splits_to_transactions(df)
        row = result.iloc[0]
        assert row["quantity"] == pytest.approx(40.0)
        assert row["price"]    == pytest.approx(75.0)

    def test_split_only_affects_rows_before_split_date(self):
        """Buys AFTER the split date are not adjusted."""
        df = self._make_tx([
            {"symbol": "AAPL", "type": "buy",   "date": "2020-01-01", "quantity": 10.0, "price": 300.0},
            {"symbol": "AAPL", "type": "split", "date": "2020-08-31", "quantity":  4.0, "price":   0.0},
            {"symbol": "AAPL", "type": "buy",   "date": "2020-09-01", "quantity":  5.0, "price":  80.0},
        ])
        result = mw.apply_splits_to_transactions(df).sort_values("date").reset_index(drop=True)
        # Pre-split buy: adjusted
        assert result.iloc[0]["quantity"] == pytest.approx(40.0)
        assert result.iloc[0]["price"]    == pytest.approx(75.0)
        # Post-split buy: unchanged
        assert result.iloc[1]["quantity"] == pytest.approx(5.0)
        assert result.iloc[1]["price"]    == pytest.approx(80.0)

    def test_split_only_affects_matching_symbol(self):
        """A split for AAPL does not adjust MSFT rows."""
        df = self._make_tx([
            {"symbol": "AAPL", "type": "buy",   "date": "2020-01-01", "quantity": 10.0, "price": 300.0},
            {"symbol": "MSFT", "type": "buy",   "date": "2020-01-01", "quantity":  8.0, "price": 200.0},
            {"symbol": "AAPL", "type": "split", "date": "2020-08-31", "quantity":  4.0, "price":   0.0},
        ])
        result = mw.apply_splits_to_transactions(df)
        aapl = result[result["symbol"] == "AAPL"].iloc[0]
        msft = result[result["symbol"] == "MSFT"].iloc[0]
        assert aapl["quantity"] == pytest.approx(40.0)
        assert aapl["price"]    == pytest.approx(75.0)
        assert msft["quantity"] == pytest.approx(8.0)   # unchanged
        assert msft["price"]    == pytest.approx(200.0) # unchanged

    def test_reverse_split_adjusts_correctly(self):
        """A 1-for-2 reverse split (ratio=0.5) halves quantity and doubles price."""
        df = self._make_tx([
            {"symbol": "GME", "type": "buy",   "date": "2022-01-01", "quantity": 100.0, "price": 20.0},
            {"symbol": "GME", "type": "split", "date": "2022-06-01", "quantity":   0.5, "price":  0.0},
        ])
        result = mw.apply_splits_to_transactions(df)
        row = result.iloc[0]
        assert row["quantity"] == pytest.approx(50.0)
        assert row["price"]    == pytest.approx(40.0)

    def test_multiple_splits_applied_chronologically(self):
        """Two splits are applied in date order, compounding correctly."""
        df = self._make_tx([
            {"symbol": "NVDA", "type": "buy",   "date": "2019-01-01", "quantity":  1.0, "price": 1000.0},
            {"symbol": "NVDA", "type": "split", "date": "2021-07-20", "quantity":  4.0, "price":    0.0},
            {"symbol": "NVDA", "type": "split", "date": "2024-06-10", "quantity": 10.0, "price":    0.0},
        ])
        result = mw.apply_splits_to_transactions(df)
        # After 4-for-1: qty=4, price=250
        # After 10-for-1: qty=40, price=25
        row = result.iloc[0]
        assert row["quantity"] == pytest.approx(40.0)
        assert row["price"]    == pytest.approx(25.0)

    def test_sell_rows_also_adjusted(self):
        """Sell transactions before the split are also adjusted."""
        df = self._make_tx([
            {"symbol": "AAPL", "type": "buy",   "date": "2020-01-01", "quantity": 10.0, "price": 300.0},
            {"symbol": "AAPL", "type": "sell",  "date": "2020-06-01", "quantity":  2.0, "price": 320.0},
            {"symbol": "AAPL", "type": "split", "date": "2020-08-31", "quantity":  4.0, "price":   0.0},
        ])
        result = mw.apply_splits_to_transactions(df).sort_values("date").reset_index(drop=True)
        buy  = result[result["type"] == "buy"].iloc[0]
        sell = result[result["type"] == "sell"].iloc[0]
        assert buy["quantity"]  == pytest.approx(40.0)
        assert buy["price"]     == pytest.approx(75.0)
        assert sell["quantity"] == pytest.approx(8.0)
        assert sell["price"]    == pytest.approx(80.0)

    def test_empty_dataframe_returns_empty(self):
        """Empty input returns empty output without error."""
        df = pd.DataFrame(columns=["symbol", "type", "date", "quantity", "price"])
        result = mw.apply_splits_to_transactions(df)
        assert result.empty

    def test_cost_basis_preserved_after_split(self):
        """Total cost basis (qty × price) is unchanged by a split."""
        qty, price = 10.0, 300.0
        ratio = 4.0
        df = self._make_tx([
            {"symbol": "AAPL", "type": "buy",   "date": "2020-01-01",
             "quantity": qty, "price": price},
            {"symbol": "AAPL", "type": "split", "date": "2020-08-31",
             "quantity": ratio, "price": 0.0},
        ])
        result = mw.apply_splits_to_transactions(df)
        row = result.iloc[0]
        original_cost = qty * price
        adjusted_cost = row["quantity"] * row["price"]
        assert adjusted_cost == pytest.approx(original_cost)


# ═════════════════════════════════════════════════════════════════════════════
# 8. Daily price refresh schedule
# ═════════════════════════════════════════════════════════════════════════════

class TestPricesDue:
    """Prices refresh once a day at price_refresh_hour_utc (default 22:00)."""

    @pytest.fixture(autouse=True)
    def _df(self, monkeypatch):
        # sys.modules["data_fetcher"] is a stub here — load the real file
        # under another name (its own imports resolve to the stubs above).
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "data_fetcher_real", os.path.join(APP_DIR, "data_fetcher.py"))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        monkeypatch.setattr(mod, "get_config", lambda k: {"price_refresh_hour_utc": 22, "price_refresh_minutes": 60}.get(k))
        self.due = mod.prices_due
        self.refresh_due = mod.price_refresh_due
        self.fx_due = mod.fx_due

    def _ts(self, s):
        return pd.Timestamp(s, tz="UTC")

    def test_never_fetched(self):
        assert self.due(None, now=self._ts("2026-10-02 10:00")) is True

    def test_fetched_last_night_not_due_during_day(self):
        assert self.due("2026-10-01T22:00:05+00:00", now=self._ts("2026-10-02 15:00")) is False

    def test_due_at_refresh_time(self):
        assert self.due("2026-10-01T22:00:05+00:00", now=self._ts("2026-10-02 22:00")) is True

    def test_not_due_twice_in_same_evening(self):
        assert self.due("2026-10-02T22:00:05+00:00", now=self._ts("2026-10-02 23:00")) is False

    def test_missed_cycle_catches_up(self):
        assert self.due("2026-09-30T22:00:05+00:00", now=self._ts("2026-10-02 09:00")) is True

    def test_naive_timestamp_treated_as_utc(self):
        assert self.due("2026-10-01 22:30:00", now=self._ts("2026-10-02 08:00")) is False

    def test_stocks_refresh_hourly_while_markets_open(self):
        # Fri 2026-10-02 10:00 UTC
        assert self.refresh_due("2026-10-02T08:50:00+00:00", now=self._ts("2026-10-02 10:00")) is True
        assert self.refresh_due("2026-10-02T09:30:00+00:00", now=self._ts("2026-10-02 10:00")) is False

    def test_stocks_not_refreshed_intraday_on_weekend_or_overnight(self):
        assert self.refresh_due("2026-10-02T22:05:00+00:00", now=self._ts("2026-10-03 12:00")) is False
        assert self.refresh_due("2026-10-02T21:00:00+00:00", now=self._ts("2026-10-02 22:30")) is True  # post-close catch-up
        assert self.refresh_due("2026-10-05T05:00:00+00:00", now=self._ts("2026-10-05 06:00")) is False

    def test_fx_only_after_close(self):
        assert self.fx_due(now=self._ts("2026-10-02 10:00")) is False
        assert self.fx_due(now=self._ts("2026-10-02 23:00")) is True


# ═════════════════════════════════════════════════════════════════════════════
# Rebalancing
# ═════════════════════════════════════════════════════════════════════════════

class TestRebalancing:
    def _run(self, monkeypatch, rows, cfg, risk=None):
        reb = sys.modules["middleware.rebalancing"]
        snap = pd.DataFrame(rows)
        snap["security_id"] = range(1, len(snap) + 1)
        monkeypatch.setattr(reb, "get_latest_holdings_snapshot", lambda **kw: snap.copy())
        monkeypatch.setattr(reb, "get_config", lambda key: cfg.get(key))
        monkeypatch.setattr(reb, "safe_json_load", lambda v, default=None: v if v is not None else default)
        monkeypatch.setattr(reb, "get_watchlist", lambda: pd.DataFrame(columns=["symbol", "sector", "industry", "beta"]))
        risk_df = pd.DataFrame(risk or [], columns=["date", "security_id", "risk_score"])
        monkeypatch.setattr(reb.db, "get_security_risk_timeseries_many", lambda ids: risk_df, raising=False)
        return reb.suggest_rebalancing(retirement_year=2100)

    def _row(self, sym, mv, typ="EQUITY", sector="Technology", industry="Software"):
        return {"symbol": sym, "market_value": mv, "security_type": typ, "sector": sector, "industry": industry}

    def test_asset_class_shift_is_spread_and_balanced(self, monkeypatch):
        out = self._run(monkeypatch,
            [self._row("A", 300), self._row("B", 200), self._row("ETF1", 500, "ETF", "Unknown", "Unknown")],
            {"asset_allocation_targets": {"pre_retirement": {"Equity": 0.7, "ETF": 0.3}}})
        moves = {a["symbol"]: a["pct_change"] for a in out["suggestions"]}
        assert moves["A"] == pytest.approx(0.12)      # 0.3 -> 0.42
        assert moves["B"] == pytest.approx(0.08)      # 0.2 -> 0.28
        assert moves["ETF1"] == pytest.approx(-0.2)   # 0.5 -> 0.3
        assert sum(moves.values()) == pytest.approx(0.0)

    def test_sector_targets_only_apply_to_stocks(self, monkeypatch):
        out = self._run(monkeypatch,
            [self._row("TECH", 400), self._row("BANK", 100, sector="Financial Services", industry="Banks"),
             self._row("ETF1", 500, "ETF", "Unknown", "Unknown")],
            {"asset_allocation_targets": {"pre_retirement": {"Equity": 0.5, "ETF": 0.5}},
             "target_sector_allocation": {"Technology": 0.5, "Financial Services": 0.5}})
        moves = {a["symbol"]: a["pct_change"] for a in out["suggestions"]}
        assert "ETF1" not in moves                     # already on target, untouched by sector targets
        assert moves["TECH"] == pytest.approx(-0.15)  # 0.40 -> 0.25
        assert moves["BANK"] == pytest.approx(0.15)   # 0.10 -> 0.25
        assert all(abs(m) <= 1 for m in moves.values())

    def test_security_target_overrides_and_flags_risk(self, monkeypatch):
        out = self._run(monkeypatch,
            [self._row("A", 500), self._row("B", 500)],
            {"target_security_allocation": {"A": 0.2}},
            risk=[("2026-01-01", 1, 0.9), ("2026-01-01", 2, 0.1)])
        by_sym = {a["symbol"]: a for a in out["suggestions"]}
        assert by_sym["A"]["pct_change"] == pytest.approx(-0.3)
        assert by_sym["B"]["pct_change"] == pytest.approx(0.3)
        assert by_sym["A"]["impact_risk"] == pytest.approx(0.9)
