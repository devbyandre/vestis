"""Technical indicators and risk/return statistics — pure pandas/numpy math,
no db_utils or cross-module dependencies."""
import numpy as np
import pandas as pd
from scipy.signal import argrelextrema


def sma(series, window):
    return series.rolling(window).mean()

def ema(series, span):
    return series.ewm(span=span, adjust=False).mean()

def bollinger(series, window=20, n_std=2.0):
    m = sma(series, window); s = series.rolling(window).std()
    return m, m + n_std*s, m - n_std*s

def rsi(series, window=14):
    delta = series.diff()
    up = delta.clip(lower=0); down = -delta.clip(upper=0)
    ma_up = up.ewm(com=window-1, adjust=False).mean(); ma_down = down.ewm(com=window-1, adjust=False).mean()
    rs = ma_up / ma_down
    return 100 - (100 / (1 + rs))

def local_min_max(series, order=5):
    try:
        arr = series.values
        idx_max = argrelextrema(arr, np.greater, order=order)[0]
        idx_min = argrelextrema(arr, np.less, order=order)[0]
    except Exception:
        idx_min = np.array([], dtype=int); idx_max = np.array([], dtype=int)
    return idx_min, idx_max


def calc_returns(prices: pd.Series) -> pd.Series:
    """
    Calculate daily returns from a price series.
    Returns NaN for the first value.
    """
    prices = prices.astype(float)
    returns = prices.pct_change()
    return returns


def find_crossovers(short_ma: pd.Series, long_ma: pd.Series):
    """
    Identify crossover points between two moving averages.
    Returns buy and sell indices (buy when short MA crosses above long MA).
    """
    if len(short_ma) != len(long_ma):
        raise ValueError("MA series must be the same length")

    buy_signals = []
    sell_signals = []
    prev_diff = short_ma.iloc[0] - long_ma.iloc[0]

    for i in range(1, len(short_ma)):
        diff = short_ma.iloc[i] - long_ma.iloc[i]
        if prev_diff <= 0 and diff > 0:
            buy_signals.append(i)
        elif prev_diff >= 0 and diff < 0:
            sell_signals.append(i)
        prev_diff = diff

    return buy_signals, sell_signals

def volatility(df, price_col='close'):
    """
    Calculate annualized volatility from daily returns.
    df: DataFrame with a column price_col
    """
    df = df.copy()
    df['returns'] = df[price_col].pct_change()
    vol = df['returns'].std() * (252 ** 0.5)  # annualized
    return vol

def max_drawdown(series: pd.Series) -> float:
    """
    Calculate the maximum drawdown of a price series.

    Parameters:
        series (pd.Series): Price series

    Returns:
        float: Max drawdown as a fraction (e.g., 0.25 = 25%)
    """
    if series.empty:
        return 0.0
    roll_max = series.cummax()
    drawdown = (series - roll_max) / roll_max
    max_dd = drawdown.min()
    return abs(max_dd)

def sharpe_ratio(df, price_col='close', risk_free_rate=0.0):
    """
    Sharpe Ratio = Excess return / Total volatility
    Higher is better. >1.0 is good, >2.0 is very good.
    """
    df = df.copy()
    df['returns'] = df[price_col].pct_change().dropna()
    excess_ret = df['returns'] - (risk_free_rate / 252)
    avg_excess_ret = excess_ret.mean() * 252
    vol = excess_ret.std() * np.sqrt(252)
    return avg_excess_ret / vol if vol != 0 else np.nan


def sortino_ratio(df, price_col='close', risk_free_rate=0.0):
    """
    Sortino Ratio = Excess return / Downside volatility
    Focuses only on downside risk. >1.0 is good, >2.0 is very good.
    """
    df = df.copy()
    df['returns'] = df[price_col].pct_change().dropna()
    excess_ret = df['returns'] - (risk_free_rate / 252)
    avg_excess_ret = excess_ret.mean() * 252
    downside = excess_ret[excess_ret < 0]
    downside_vol = downside.std() * np.sqrt(252)
    return avg_excess_ret / downside_vol if downside_vol != 0 else np.nan


def cagr(df, price_col='close'):
    """
    CAGR = Annualized return
    Positive CAGR is required. 5–10% is typical for equities.
    """
    if df.empty:
        return np.nan
    start_val = df[price_col].iloc[0]
    end_val = df[price_col].iloc[-1]
    n_days = (df.index[-1] - df.index[0]).days
    if n_days <= 0:
        return np.nan
    years = n_days / 365.25
    return (end_val / start_val) ** (1 / years) - 1


def calmar_ratio(df, price_col='close'):
    """
    Calmar Ratio = CAGR / Max Drawdown
    Higher is better. >0.5 is decent, >1.0 is strong.
    """
    cagr_val = cagr(df, price_col)
    max_dd_val = max_drawdown(df[price_col])
    return cagr_val / max_dd_val if max_dd_val != 0 else np.nan


def treynor_ratio(df, price_col='close', beta=1.0, risk_free_rate=0.0):
    """
    Treynor Ratio = Excess return / Beta
    Measures return per unit of systematic risk.
    Higher is better.
    """
    df = df.copy()
    df['returns'] = df[price_col].pct_change().dropna()
    avg_excess_ret = (df['returns'].mean() - (risk_free_rate / 252)) * 252
    return avg_excess_ret / beta if beta != 0 else np.nan


def information_ratio(df, benchmark, price_col='close'):
    """
    Information Ratio = Active return / Tracking error
    >0.5 good, >1.0 very good.
    """
    df = df.copy()
    df['returns'] = df[price_col].pct_change().dropna()
    benchmark = benchmark.copy()
    benchmark['returns'] = benchmark[price_col].pct_change().dropna()

    active_ret = df['returns'].align(benchmark['returns'], join='inner')[0] - \
                 df['returns'].align(benchmark['returns'], join='inner')[1]

    avg_active_ret = active_ret.mean() * 252
    tracking_err = active_ret.std() * np.sqrt(252)
    return avg_active_ret / tracking_err if tracking_err != 0 else np.nan
