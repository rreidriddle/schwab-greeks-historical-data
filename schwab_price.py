"""
schwab_price.py — Schwab Historical Price Data
================================================
Fetches intraday OHLCV bars from the Schwab /pricehistory endpoint.

Used by the backtest tab to show the price chart for a selected date.
Designed so the data source can be swapped for a paid vendor later
without touching the dashboard.

Supported frequencies: 1min, 5min, 10min, 15min, 30min
Note: Schwab does not support 60-minute intraday bars for periodType=day.
The maximum intraday resolution is 30min.

All functions return clean pandas DataFrames with a UTC datetime index.
All functions return empty DataFrames gracefully on any failure.

Architecture note:
  This module is intentionally separate from db.py. Greeks data and
  price data come from different sources and may diverge as the project
  grows (e.g. paid Greeks API + Schwab price, or vice versa).
  The backtest tab merges them at render time using timestamp alignment.
"""

import os
import time
import datetime
import warnings
import requests
import pandas as pd
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════

SCHWAB_BASE = "https://api.schwabapi.com/marketdata/v1"
ET          = ZoneInfo("America/New_York")

# Frequency config — maps user-facing label to Schwab API parameters
# Schwab /pricehistory valid minute frequencies: 1, 5, 10, 15, 30
# Note: 60-minute bars are NOT supported for intraday periodType=day
FREQ_CONFIG = {
    "1min":  {"frequencyType": "minute", "frequency": 1},
    "5min":  {"frequencyType": "minute", "frequency": 5},
    "10min": {"frequencyType": "minute", "frequency": 10},
    "15min": {"frequencyType": "minute", "frequency": 15},
    "30min": {"frequencyType": "minute", "frequency": 30},
}

# Market holidays — keep in sync with collector.py
MARKET_HOLIDAYS = {
    datetime.date(2025, 1,  1),
    datetime.date(2025, 1, 20),
    datetime.date(2025, 2, 17),
    datetime.date(2025, 4, 18),
    datetime.date(2025, 5, 26),
    datetime.date(2025, 6, 19),
    datetime.date(2025, 7,  4),
    datetime.date(2025, 9,  1),
    datetime.date(2025, 11, 27),
    datetime.date(2025, 12, 25),
    datetime.date(2026, 1,  1),
    datetime.date(2026, 1, 19),
    datetime.date(2026, 2, 16),
    datetime.date(2026, 4,  3),
    datetime.date(2026, 5, 25),
    datetime.date(2026, 6, 19),
    datetime.date(2026, 7,  3),
    datetime.date(2026, 9,  7),
    datetime.date(2026, 11, 26),
    datetime.date(2026, 12, 25),
}

# ══════════════════════════════════════════════════════════════════════════════
# TRADING DAY UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def is_trading_day(date: datetime.date) -> bool:
    """Return True if date is a US equity trading day."""
    return date.weekday() < 5 and date not in MARKET_HOLIDAYS


def prev_trading_day(date: datetime.date) -> datetime.date:
    """Return the most recent trading day before date."""
    d = date - datetime.timedelta(days=1)
    while not is_trading_day(d):
        d -= datetime.timedelta(days=1)
    return d


def next_trading_day(date: datetime.date) -> datetime.date:
    """Return the next trading day after date."""
    d = date + datetime.timedelta(days=1)
    while not is_trading_day(d):
        d += datetime.timedelta(days=1)
    return d


def get_window_dates(center_date: datetime.date) -> tuple[datetime.date,
                                                          datetime.date,
                                                          datetime.date]:
    """
    Return (day_before, center_date, day_after) as trading days.
    Skips weekends and holidays automatically.
    """
    before = prev_trading_day(center_date)
    after  = next_trading_day(center_date)
    return before, center_date, after

# ══════════════════════════════════════════════════════════════════════════════
# SCHWAB PRICE HISTORY
# ══════════════════════════════════════════════════════════════════════════════

def _date_to_ms(date: datetime.date, end_of_day: bool = False) -> int:
    """Convert a date to millisecond epoch timestamp (ET market hours)."""
    if end_of_day:
        dt = datetime.datetime.combine(date, datetime.time(16, 0),
                                       tzinfo=ET)
    else:
        dt = datetime.datetime.combine(date, datetime.time(9, 30),
                                       tzinfo=ET)
    return int(dt.timestamp() * 1000)


def _parse_candles(data: dict) -> pd.DataFrame:
    """
    Parse Schwab pricehistory JSON response into a clean OHLCV DataFrame.
    Datetime index is UTC-aware. Returns empty DataFrame if no candles.
    """
    candles = data.get("candles", [])
    if not candles:
        return pd.DataFrame()

    df = pd.DataFrame(candles)

    # Schwab returns datetime as millisecond epoch
    df["datetime"] = pd.to_datetime(df["datetime"], unit="ms", utc=True)
    df = df.set_index("datetime").sort_index()

    # Standardize column names to uppercase OHLCV
    df = df.rename(columns={
        "open":   "Open",
        "high":   "High",
        "low":    "Low",
        "close":  "Close",
        "volume": "Volume",
    })

    # Keep only OHLCV — drop any extra Schwab fields
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col not in df.columns:
            df[col] = float("nan")

    return df[["Open", "High", "Low", "Close", "Volume"]]


def get_intraday_bars(token: str,
                      symbol: str,
                      center_date: datetime.date | str,
                      frequency: str = "5min") -> pd.DataFrame:
    """
    Fetch intraday OHLCV bars for symbol spanning the day before,
    the center date, and the day after (3 trading days).

    Parameters:
      token       : valid Schwab access token
      symbol      : equity symbol e.g. "SPY"
      center_date : the date being backtested (date object or "YYYY-MM-DD")
      frequency   : "1min" | "5min" | "15min" | "1hr"

    Returns:
      DataFrame with UTC datetime index and columns Open/High/Low/Close/Volume.
      Also includes a 'session' column marking each row as
      "before" | "selected" | "after" for the chart to color differently.
      Returns empty DataFrame on any failure.

    Notes:
      - Market hours only (9:30am–4:00pm ET)
      - If center_date is today or in the future, only available bars returned
      - Schwab may not have data for very recent dates until EOD processing
    """
    if isinstance(center_date, str):
        center_date = datetime.date.fromisoformat(center_date)

    if frequency not in FREQ_CONFIG:
        warnings.warn(f"schwab_price: unknown frequency '{frequency}'. "
                      f"Use one of {list(FREQ_CONFIG.keys())}",
                      stacklevel=2)
        return pd.DataFrame()

    before, selected, after = get_window_dates(center_date)

    # Fetch window: start of day_before → end of day_after
    start_ms = _date_to_ms(before,  end_of_day=False)
    end_ms   = _date_to_ms(after,   end_of_day=True)

    freq_cfg = FREQ_CONFIG[frequency]

    params = {
        "symbol":        symbol,
        "periodType":    "day",
        "frequencyType": freq_cfg["frequencyType"],
        "frequency":     freq_cfg["frequency"],
        "startDate":     start_ms,
        "endDate":       end_ms,
        "needExtendedHoursData": "false",
    }

    for attempt in range(3):
        try:
            r = requests.get(
                f"{SCHWAB_BASE}/pricehistory",
                headers={"Authorization": f"Bearer {token}"},
                params=params,
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()
            break
        except requests.exceptions.HTTPError:
            if r.status_code in [429, 502, 503, 504] and attempt < 2:
                w = (attempt + 1) * 3
                print(f"  {r.status_code} on pricehistory {symbol} — retry in {w}s")
                time.sleep(w)
                continue
            warnings.warn(
                f"schwab_price: HTTP {r.status_code} fetching {symbol} "
                f"pricehistory: {r.text[:200]}",
                stacklevel=2
            )
            return pd.DataFrame()
        except Exception as e:
            warnings.warn(f"schwab_price: error fetching {symbol}: {e}",
                          stacklevel=2)
            return pd.DataFrame()

    df = _parse_candles(data)
    if df.empty:
        return df

    # Tag each bar with which session it belongs to
    def _tag_session(ts):
        d = ts.astimezone(ET).date()
        if d == before:   return "before"
        if d == selected: return "selected"
        if d == after:    return "after"
        return "other"

    df["session"]       = df.index.map(_tag_session)
    df["trading_date"]  = df.index.map(lambda ts: ts.astimezone(ET).date())

    return df


def get_single_day_bars(token: str,
                        symbol: str,
                        date: datetime.date | str,
                        frequency: str = "5min") -> pd.DataFrame:
    """
    Fetch intraday bars for a single trading day only.
    Convenience wrapper around get_intraday_bars for cases where
    only one day is needed (e.g. strategy entry/exit analysis).

    Returns DataFrame with UTC datetime index and OHLCV columns.
    """
    if isinstance(date, str):
        date = datetime.date.fromisoformat(date)

    if frequency not in FREQ_CONFIG:
        warnings.warn(f"schwab_price: unknown frequency '{frequency}'.",
                      stacklevel=2)
        return pd.DataFrame()

    freq_cfg = FREQ_CONFIG[frequency]
    start_ms = _date_to_ms(date, end_of_day=False)
    end_ms   = _date_to_ms(date, end_of_day=True)

    params = {
        "symbol":        symbol,
        "periodType":    "day",
        "frequencyType": freq_cfg["frequencyType"],
        "frequency":     freq_cfg["frequency"],
        "startDate":     start_ms,
        "endDate":       end_ms,
        "needExtendedHoursData": "false",
    }

    for attempt in range(3):
        try:
            r = requests.get(
                f"{SCHWAB_BASE}/pricehistory",
                headers={"Authorization": f"Bearer {token}"},
                params=params,
                timeout=30,
            )
            r.raise_for_status()
            df = _parse_candles(r.json())
            df["session"]      = "selected"
            df["trading_date"] = df.index.map(
                lambda ts: ts.astimezone(ET).date()
            )
            return df
        except requests.exceptions.HTTPError:
            if r.status_code in [429, 502, 503, 504] and attempt < 2:
                w = (attempt + 1) * 3
                time.sleep(w)
                continue
            warnings.warn(
                f"schwab_price: HTTP {r.status_code} on {symbol}: {r.text[:200]}",
                stacklevel=2
            )
            return pd.DataFrame()
        except Exception as e:
            warnings.warn(f"schwab_price: error: {e}", stacklevel=2)
            return pd.DataFrame()

    return pd.DataFrame()


def get_historical_volume(token: str,
                          symbol: str,
                          lookback_days: int = 30) -> pd.DataFrame:
    """
    Fetch daily OHLCV bars for the past N trading days.
    Used to compute 90th percentile volume for Y axis scaling
    and 30-day average volume for comparison annotation.

    Returns DataFrame with UTC datetime index and OHLCV columns.
    Returns empty DataFrame on failure.
    """
    params = {
        "symbol":        symbol,
        "periodType":    "month",
        "period":        1,
        "frequencyType": "daily",
        "frequency":     1,
        "needExtendedHoursData": "false",
    }
    for attempt in range(3):
        try:
            r = requests.get(
                f"{SCHWAB_BASE}/pricehistory",
                headers={"Authorization": f"Bearer {token}"},
                params=params,
                timeout=20,
            )
            r.raise_for_status()
            df = _parse_candles(r.json())
            if not df.empty:
                df["trading_date"] = df.index.map(
                    lambda ts: ts.astimezone(ET).date()
                )
            return df
        except requests.exceptions.HTTPError:
            if r.status_code in [429, 502, 503, 504] and attempt < 2:
                w = (attempt + 1) * 3
                time.sleep(w)
                continue
            warnings.warn(
                f"schwab_price: HTTP {r.status_code} fetching historical "
                f"volume for {symbol}: {r.text[:200]}",
                stacklevel=2
            )
            return pd.DataFrame()
        except Exception as e:
            warnings.warn(f"schwab_price get_historical_volume: {e}",
                          stacklevel=2)
            return pd.DataFrame()
    return pd.DataFrame()


def align_greeks_to_bars(bars: pd.DataFrame,
                         greeks: pd.DataFrame,
                         tolerance_minutes: int = 8) -> pd.DataFrame:
    """
    Merge intraday price bars with Greeks history on nearest timestamp.
    Greeks are collected every 15 minutes; bars may be 1/5/15/60 min.
    Uses merge_asof to align on nearest timestamp within tolerance.

    Parameters:
      bars              : DataFrame from get_intraday_bars()
      greeks            : DataFrame from db.get_summary_history()
      tolerance_minutes : max minutes between bar and Greeks timestamp

    Returns merged DataFrame. Greeks columns are suffixed with '_greeks'
    to avoid collisions with price columns. Rows with no Greeks match
    within tolerance have NaN in Greeks columns.

    This is the primary merge function for backtesting — it creates the
    combined dataset that strategy logic operates on.
    """
    if bars.empty or greeks.empty:
        return bars.copy()

    # Ensure both are UTC
    bars_idx   = bars.index
    greeks_idx = greeks.index

    if bars_idx.tz is None:
        bars_idx = bars_idx.tz_localize("UTC")
    if greeks_idx.tz is None:
        greeks_idx = greeks_idx.tz_localize("UTC")

    bars_reset   = bars.copy().reset_index()
    greeks_reset = greeks.copy().reset_index()

    bars_reset.columns   = [c if c != "datetime" else "datetime"
                             for c in bars_reset.columns]
    greeks_reset.columns = [c if c != "timestamp" else "datetime"
                             for c in greeks_reset.columns]

    bars_reset   = bars_reset.sort_values("datetime")
    greeks_reset = greeks_reset.sort_values("datetime")

    tolerance = pd.Timedelta(minutes=tolerance_minutes)

    merged = pd.merge_asof(
        bars_reset,
        greeks_reset.add_suffix("_greeks").rename(
            columns={"datetime_greeks": "datetime"}
        ),
        on="datetime",
        tolerance=tolerance,
        direction="nearest",
    )

    merged = merged.set_index("datetime").sort_index()
    return merged


# ══════════════════════════════════════════════════════════════════════════════
# QUICK TEST
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    print("schwab_price.py — Schwab Historical Price Data")
    print()

    try:
        from auth import get_valid_access_token
        token = get_valid_access_token(silent=True)
    except Exception as e:
        print(f"Auth failed: {e}")
        raise SystemExit(1)

    # Test with most recent completed trading day
    today  = datetime.date.today()
    target = prev_trading_day(today)
    before, selected, after = get_window_dates(target)

    print(f"Fetching SPY 5min bars for single day: {target}")
    df = get_single_day_bars(token, "SPY", target, frequency="5min")

    if df.empty:
        print("No data returned — market may be closed or date unavailable.")
    else:
        print(f"  {len(df)} bars")
        print(f"\nFirst bar:\n{df.head(1)}")
        print(f"\nLast bar:\n{df.tail(1)}")

    print("\nFrequency test — 30min:")
    df30 = get_single_day_bars(token, "SPY", target, frequency="30min")
    print(f"  {len(df30)} bars for {target}")

    print("\nFrequency test — 10min:")
    df10 = get_single_day_bars(token, "SPY", target, frequency="10min")
    print(f"  {len(df10)} bars for {target}")
