"""
db.py — Greeks Database Access Layer
=====================================
Read-only interface to greeks_history.db populated by collector.py.

Changes from v1:
  - Added get_latest_macro() — most recent macro_snapshot row
  - Added get_macro_history() — macro_snapshot over date range
  - Added get_yield_curve() — today + yesterday curves for MACRO tab
  - Added get_combined_signal() — latest macro regime + signal string
  - All existing functions unchanged

All functions return clean pandas DataFrames or scalar values.
All functions return empty DataFrames / None gracefully on failure.
"""

import os
import sqlite3
import datetime
import warnings
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════

DB_PATH = os.environ.get("GREEKS_DB_PATH", "greeks_history.db")

REGIME_STRONG_POS_THRESHOLD  =  2e9
REGIME_WEAK_POS_THRESHOLD    =  0.0
REGIME_FLIP_ZONE_PCT         =  0.005
REGIME_WEAK_NEG_THRESHOLD    = -2e9

# ══════════════════════════════════════════════════════════════════════════════
# CONNECTION
# ══════════════════════════════════════════════════════════════════════════════

def get_connection(path: str = None) -> sqlite3.Connection | None:
    db = path or DB_PATH
    if not os.path.exists(db):
        warnings.warn(f"db.py: database not found at '{db}'. "
                      "Start collector.py to begin building history.",
                      stacklevel=2)
        return None
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True,
                               check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        warnings.warn(f"db.py: could not connect to database: {e}",
                      stacklevel=2)
        return None


def _empty_summary() -> pd.DataFrame:
    cols = [
        "timestamp", "symbol", "spot",
        "net_GEX", "net_VannEX", "net_CharmEX",
        "gamma_flip", "call_wall", "put_wall", "max_pain",
        "total_oi", "total_volume", "iv_atm",
        "gex_0dte", "gex_1_7dte", "gex_8_30dte",
        "gex_31_90dte", "gex_91_180dte", "gex_180plus_dte",
        "regime",
    ]
    return pd.DataFrame(columns=cols)


def _empty_strikes() -> pd.DataFrame:
    cols = [
        "timestamp", "symbol", "spot", "strike", "dte", "dte_bucket",
        "GEX_call", "GEX_put", "GEX_net",
        "VannEX_call", "VannEX_put", "VannEX_net",
        "CharmEX_call", "CharmEX_put", "CharmEX_net",
        "total_oi", "total_volume", "iv_call", "iv_put",
    ]
    return pd.DataFrame(columns=cols)


def _empty_macro() -> pd.DataFrame:
    cols = [
        "timestamp",
        "tlt_price", "tlt_change", "tlt_chg_pct",
        "uso_price", "uso_change", "uso_chg_pct",
        "tnx_yield", "tyx_yield",
        "vix", "vix_change", "vix_chg_pct",
        "regime", "signal",
    ]
    return pd.DataFrame(columns=cols)

# ══════════════════════════════════════════════════════════════════════════════
# REGIME CLASSIFICATION
# ══════════════════════════════════════════════════════════════════════════════

def classify_regime(net_gex: float, spot: float, gamma_flip: float | None,
                    flip_zone_pct: float = REGIME_FLIP_ZONE_PCT) -> str:
    if gamma_flip is not None and spot > 0:
        pct_from_flip = abs(spot - gamma_flip) / spot
        if pct_from_flip <= flip_zone_pct:
            return "FLIP_ZONE"
    if net_gex >= REGIME_STRONG_POS_THRESHOLD:
        return "STRONG_POS"
    elif net_gex >= REGIME_WEAK_POS_THRESHOLD:
        return "WEAK_POS"
    elif net_gex >= REGIME_WEAK_NEG_THRESHOLD:
        return "WEAK_NEG"
    else:
        return "STRONG_NEG"

# ══════════════════════════════════════════════════════════════════════════════
# UTILITY QUERIES
# ══════════════════════════════════════════════════════════════════════════════

def get_available_symbols() -> list[str]:
    conn = get_connection()
    if conn is None:
        return []
    try:
        cur = conn.execute(
            "SELECT DISTINCT symbol FROM summary ORDER BY symbol"
        )
        return [row[0] for row in cur.fetchall()]
    except Exception as e:
        warnings.warn(f"db.py get_available_symbols: {e}", stacklevel=2)
        return []
    finally:
        conn.close()


def get_date_range(symbol: str) -> tuple[str | None, str | None]:
    conn = get_connection()
    if conn is None:
        return None, None
    try:
        cur = conn.execute(
            """
            SELECT MIN(DATE(timestamp)), MAX(DATE(timestamp))
            FROM summary WHERE symbol = ?
            """,
            (symbol,)
        )
        row = cur.fetchone()
        return (row[0], row[1]) if row else (None, None)
    except Exception as e:
        warnings.warn(f"db.py get_date_range: {e}", stacklevel=2)
        return None, None
    finally:
        conn.close()


def get_pull_timestamps(symbol: str,
                        start_date: str | datetime.date,
                        end_date:   str | datetime.date) -> list[str]:
    conn = get_connection()
    if conn is None:
        return []
    try:
        cur = conn.execute(
            """
            SELECT DISTINCT timestamp FROM summary
            WHERE symbol = ?
              AND DATE(timestamp) BETWEEN DATE(?) AND DATE(?)
            ORDER BY timestamp
            """,
            (symbol, str(start_date), str(end_date))
        )
        return [row[0] for row in cur.fetchall()]
    except Exception as e:
        warnings.warn(f"db.py get_pull_timestamps: {e}", stacklevel=2)
        return []
    finally:
        conn.close()

# ══════════════════════════════════════════════════════════════════════════════
# DASHBOARD QUERIES
# ══════════════════════════════════════════════════════════════════════════════

def get_latest_summary(symbol: str) -> dict | None:
    conn = get_connection()
    if conn is None:
        return None
    try:
        cur = conn.execute(
            """
            SELECT * FROM summary
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (symbol,)
        )
        row = cur.fetchone()
        if row is None:
            return None
        d = dict(row)
        d["regime"] = classify_regime(
            d.get("net_GEX", 0),
            d.get("spot", 0),
            d.get("gamma_flip"),
        )
        return d
    except Exception as e:
        warnings.warn(f"db.py get_latest_summary: {e}", stacklevel=2)
        return None
    finally:
        conn.close()


def get_max_pain(symbol: str, offset: int = 0) -> float | None:
    conn = get_connection()
    if conn is None:
        return None
    try:
        cur = conn.execute(
            """
            SELECT max_pain FROM summary
            WHERE symbol = ?
              AND max_pain IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 1 OFFSET ?
            """,
            (symbol, offset)
        )
        row = cur.fetchone()
        return float(row[0]) if row else None
    except Exception as e:
        warnings.warn(f"db.py get_max_pain: {e}", stacklevel=2)
        return None
    finally:
        conn.close()

# ══════════════════════════════════════════════════════════════════════════════
# MACRO QUERIES  (new in v2)
# ══════════════════════════════════════════════════════════════════════════════

def get_latest_macro() -> dict | None:
    """
    Return the most recent macro_snapshot row as a dict.
    Includes TLT, USO, VIX, TNX, TYX, regime, signal.
    Returns None if no macro data available yet.

    Used by the MACRO tab to populate the regime badge and data table.
    """
    conn = get_connection()
    if conn is None:
        return None
    try:
        cur = conn.execute(
            """
            SELECT * FROM macro_snapshot
            ORDER BY timestamp DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception as e:
        warnings.warn(f"db.py get_latest_macro: {e}", stacklevel=2)
        return None
    finally:
        conn.close()


def get_macro_history(start_date: str | datetime.date,
                      end_date:   str | datetime.date) -> pd.DataFrame:
    """
    Return all macro_snapshot rows over a date range.
    One row per collector pull cycle (~10 min intervals during market hours).
    Timestamp column parsed as UTC datetime and set as index.

    Used by the backtest engine to filter by macro regime.
    """
    conn = get_connection()
    if conn is None:
        return _empty_macro()
    try:
        df = pd.read_sql_query(
            """
            SELECT * FROM macro_snapshot
            WHERE DATE(timestamp) BETWEEN DATE(?) AND DATE(?)
            ORDER BY timestamp
            """,
            conn,
            params=(str(start_date), str(end_date)),
        )
        if df.empty:
            return _empty_macro()
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.set_index("timestamp").sort_index()
        return df
    except Exception as e:
        warnings.warn(f"db.py get_macro_history: {e}", stacklevel=2)
        return _empty_macro()
    finally:
        conn.close()


def get_combined_signal() -> dict | None:
    """
    Return the latest macro regime + GEX regime + combined signal.
    Merges most recent macro_snapshot with most recent summary row.

    Returns dict with keys:
      macro_regime, gex_regime, signal,
      tlt_price, tyx_yield, uso_price, vix,
      reason  (plain English explanation string)

    Returns None if either table has no data.
    Used by the MACRO tab regime badge.
    """
    macro   = get_latest_macro()
    summary = get_latest_summary("SPY")
    if not macro or not summary:
        return None

    macro_regime = macro.get("regime", "UNKNOWN")
    gex_regime   = summary.get("regime", "UNKNOWN")
    signal       = macro.get("signal", "NEUTRAL")

    tyx  = macro.get("tyx_yield")
    tlt  = macro.get("tlt_price")
    uso  = macro.get("uso_price")

    # Build plain English reason string
    parts = []
    if tyx:
        if tyx >= 5.0:
            parts.append(f"TYX {tyx:.2f}% — danger zone breach")
        elif tyx >= 4.75:
            parts.append(f"TYX {tyx:.2f}% approaching danger zone")
        else:
            parts.append(f"TYX {tyx:.2f}%")
    if uso:
        if uso >= 110:
            parts.append(f"Oil significantly elevated (USO ${uso:.2f})")
        elif uso >= 100:
            parts.append(f"Oil elevated (USO ${uso:.2f})")
    if tlt:
        if tlt <= 83.30 * 1.005:
            parts.append(f"TLT ${tlt:.2f} near 52-week low")
        elif tlt <= 84.76:
            parts.append(f"TLT ${tlt:.2f} below key level")
    reason = " | ".join(parts) if parts else "All macro indicators within normal range"

    return {
        "macro_regime": macro_regime,
        "gex_regime":   gex_regime,
        "signal":       signal,
        "tlt_price":    tlt,
        "tyx_yield":    tyx,
        "uso_price":    uso,
        "vix":          macro.get("vix"),
        "tnx_yield":    macro.get("tnx_yield"),
        "tlt_change":   macro.get("tlt_change"),
        "tlt_chg_pct":  macro.get("tlt_chg_pct"),
        "uso_change":   macro.get("uso_change"),
        "uso_chg_pct":  macro.get("uso_chg_pct"),
        "vix_change":   macro.get("vix_change"),
        "reason":       reason,
        "timestamp":    macro.get("timestamp"),
    }

# ══════════════════════════════════════════════════════════════════════════════
# YIELD CURVE QUERIES  (new in v2)
# ══════════════════════════════════════════════════════════════════════════════

def get_yield_curve(date: str | datetime.date | None = None) -> dict | None:
    """
    Return yield curve row for a specific date, or most recent if None.
    Returns dict with maturity keys: m3, m6, y1, y2, y5, y7, y10, y20, y30
    plus spread_10_2, spread_10_3m, and date.
    Returns None if no data available.

    Used by the MACRO tab to plot today's curve.
    """
    conn = get_connection()
    if conn is None:
        return None
    try:
        if date is None:
            cur = conn.execute(
                "SELECT * FROM yield_curve ORDER BY date DESC LIMIT 1"
            )
        else:
            cur = conn.execute(
                "SELECT * FROM yield_curve WHERE date = ? LIMIT 1",
                (str(date),)
            )
        row = cur.fetchone()
        return dict(row) if row else None
    except Exception as e:
        warnings.warn(f"db.py get_yield_curve: {e}", stacklevel=2)
        return None
    finally:
        conn.close()


def get_yield_curve_two_days() -> tuple[dict | None, dict | None]:
    """
    Return (today, yesterday) yield curve dicts for the MACRO tab.
    Today = most recent row. Yesterday = second most recent row.
    Either can be None if insufficient data.

    Used to draw today's curve (solid) vs yesterday's (ghost line).
    """
    conn = get_connection()
    if conn is None:
        return None, None
    try:
        cur = conn.execute(
            "SELECT * FROM yield_curve ORDER BY date DESC LIMIT 2"
        )
        rows = cur.fetchall()
        today     = dict(rows[0]) if len(rows) >= 1 else None
        yesterday = dict(rows[1]) if len(rows) >= 2 else None
        return today, yesterday
    except Exception as e:
        warnings.warn(f"db.py get_yield_curve_two_days: {e}", stacklevel=2)
        return None, None
    finally:
        conn.close()


def get_yield_curve_history(start_date: str | datetime.date,
                            end_date:   str | datetime.date) -> pd.DataFrame:
    """
    Return all yield curve rows over a date range as a DataFrame.
    Date column set as index. Used for historical curve comparison.
    Returns empty DataFrame if no data.
    """
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        df = pd.read_sql_query(
            """
            SELECT * FROM yield_curve
            WHERE date BETWEEN ? AND ?
            ORDER BY date
            """,
            conn,
            params=(str(start_date), str(end_date)),
        )
        if df.empty:
            return pd.DataFrame()
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df.set_index("date")
        return df
    except Exception as e:
        warnings.warn(f"db.py get_yield_curve_history: {e}", stacklevel=2)
        return pd.DataFrame()
    finally:
        conn.close()

# ══════════════════════════════════════════════════════════════════════════════
# BACKTESTING QUERIES  (unchanged from v1)
# ══════════════════════════════════════════════════════════════════════════════

def get_summary_history(symbol: str,
                        start_date: str | datetime.date,
                        end_date:   str | datetime.date) -> pd.DataFrame:
    conn = get_connection()
    if conn is None:
        return _empty_summary()
    try:
        df = pd.read_sql_query(
            """
            SELECT * FROM summary
            WHERE symbol = ?
              AND DATE(timestamp) BETWEEN DATE(?) AND DATE(?)
            ORDER BY timestamp
            """,
            conn,
            params=(symbol, str(start_date), str(end_date)),
        )
        if df.empty:
            return _empty_summary()
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.set_index("timestamp").sort_index()
        df["regime"] = df.apply(
            lambda r: classify_regime(
                r.get("net_GEX", 0),
                r.get("spot", 0),
                r.get("gamma_flip"),
            ),
            axis=1,
        )
        return df
    except Exception as e:
        warnings.warn(f"db.py get_summary_history: {e}", stacklevel=2)
        return _empty_summary()
    finally:
        conn.close()


def get_opening_snapshot(symbol: str,
                         date: str | datetime.date) -> dict | None:
    conn = get_connection()
    if conn is None:
        return None
    try:
        date_str = str(date)
        cur = conn.execute(
            """
            SELECT * FROM summary
            WHERE symbol = ?
              AND DATE(timestamp) = DATE(?)
              AND TIME(timestamp) >= '13:30:00'
            ORDER BY timestamp ASC
            LIMIT 1
            """,
            (symbol, date_str)
        )
        row = cur.fetchone()
        if row is None:
            cur = conn.execute(
                """
                SELECT * FROM summary
                WHERE symbol = ?
                  AND DATE(timestamp) = DATE(?)
                ORDER BY timestamp ASC
                LIMIT 1
                """,
                (symbol, date_str)
            )
            row = cur.fetchone()
        if row is None:
            return None
        d = dict(row)
        d["regime"] = classify_regime(
            d.get("net_GEX", 0),
            d.get("spot", 0),
            d.get("gamma_flip"),
        )
        return d
    except Exception as e:
        warnings.warn(f"db.py get_opening_snapshot: {e}", stacklevel=2)
        return None
    finally:
        conn.close()


def get_strike_history(symbol: str,
                       strike: float,
                       start_date: str | datetime.date,
                       end_date:   str | datetime.date,
                       dte_bucket: str | None = None) -> pd.DataFrame:
    conn = get_connection()
    if conn is None:
        return _empty_strikes()
    try:
        params = [symbol, strike, str(start_date), str(end_date)]
        bucket_clause = ""
        if dte_bucket:
            bucket_clause = "AND dte_bucket = ?"
            params.append(dte_bucket)
        df = pd.read_sql_query(
            f"""
            SELECT * FROM strike_data
            WHERE symbol = ?
              AND strike = ?
              AND DATE(timestamp) BETWEEN DATE(?) AND DATE(?)
              {bucket_clause}
            ORDER BY timestamp
            """,
            conn,
            params=params,
        )
        if df.empty:
            return _empty_strikes()
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.set_index("timestamp").sort_index()
        return df
    except Exception as e:
        warnings.warn(f"db.py get_strike_history: {e}", stacklevel=2)
        return _empty_strikes()
    finally:
        conn.close()


def get_gex_surface(symbol: str,
                    timestamp: str | datetime.datetime) -> pd.DataFrame:
    conn = get_connection()
    if conn is None:
        return _empty_strikes()
    try:
        ts_str = (timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
                  if isinstance(timestamp, datetime.datetime)
                  else str(timestamp))
        cur = conn.execute(
            """
            SELECT timestamp FROM strike_data
            WHERE symbol = ?
              AND ABS(strftime('%s', timestamp) - strftime('%s', ?)) <= 600
            ORDER BY ABS(strftime('%s', timestamp) - strftime('%s', ?))
            LIMIT 1
            """,
            (symbol, ts_str, ts_str)
        )
        row = cur.fetchone()
        if row is None:
            return _empty_strikes()
        closest_ts = row[0]
        df = pd.read_sql_query(
            """
            SELECT * FROM strike_data
            WHERE symbol = ? AND timestamp = ?
            ORDER BY strike
            """,
            conn,
            params=(symbol, closest_ts),
        )
        if df.empty:
            return _empty_strikes()
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df
    except Exception as e:
        warnings.warn(f"db.py get_gex_surface: {e}", stacklevel=2)
        return _empty_strikes()
    finally:
        conn.close()


def get_regime_history(symbol: str,
                       start_date: str | datetime.date,
                       end_date:   str | datetime.date,
                       flip_zone_pct: float = REGIME_FLIP_ZONE_PCT) -> pd.DataFrame:
    df = get_summary_history(symbol, start_date, end_date)
    if df.empty:
        return df
    if flip_zone_pct != REGIME_FLIP_ZONE_PCT:
        df["regime"] = df.apply(
            lambda r: classify_regime(
                r.get("net_GEX", 0),
                r.get("spot", 0),
                r.get("gamma_flip"),
                flip_zone_pct=flip_zone_pct,
            ),
            axis=1,
        )
    return df


def get_session_summary(symbol: str,
                        start_date: str | datetime.date,
                        end_date:   str | datetime.date) -> pd.DataFrame:
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        df = pd.read_sql_query(
            """
            SELECT * FROM summary
            WHERE symbol = ?
              AND DATE(timestamp) BETWEEN DATE(?) AND DATE(?)
            ORDER BY timestamp
            """,
            conn,
            params=(symbol, str(start_date), str(end_date)),
        )
        if df.empty:
            return pd.DataFrame()

        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df["date"]      = df["timestamp"].dt.date

        rows = []
        for date, group in df.groupby("date"):
            group = group.sort_values("timestamp")
            first = group.iloc[0]
            last  = group.iloc[-1]

            open_flip  = first.get("gamma_flip")
            open_spot  = first.get("spot", 0)
            close_spot = last.get("spot", 0)

            flip_tested = False
            if open_flip and open_flip > 0:
                spots       = group["spot"].values
                flip_tested = any(abs(s - open_flip) / open_flip <= 0.001
                                  for s in spots)

            rows.append({
                "date":               date,
                "symbol":             symbol,
                "open_spot":          open_spot,
                "open_net_gex":       first.get("net_GEX"),
                "open_gamma_flip":    open_flip,
                "open_max_pain":      first.get("max_pain"),
                "open_call_wall":     first.get("call_wall"),
                "open_put_wall":      first.get("put_wall"),
                "open_iv_atm":        first.get("iv_atm"),
                "open_regime":        classify_regime(
                                          first.get("net_GEX", 0),
                                          open_spot, open_flip,
                                      ),
                "close_spot":         close_spot,
                "close_net_gex":      last.get("net_GEX"),
                "close_gamma_flip":   last.get("gamma_flip"),
                "intraday_high_spot": group["spot"].max(),
                "intraday_low_spot":  group["spot"].min(),
                "closed_above_flip":  (open_flip is not None
                                       and close_spot > open_flip),
                "flip_tested":        flip_tested,
            })

        return pd.DataFrame(rows).set_index("date")
    except Exception as e:
        warnings.warn(f"db.py get_session_summary: {e}", stacklevel=2)
        return pd.DataFrame()
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# QUICK TEST
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("db.py — Greeks Database Access Layer v2")
    print(f"Database: {DB_PATH}")
    print()

    syms = get_available_symbols()
    if not syms:
        print("No data found. Run collector.py to begin building history.")
    else:
        print(f"Available symbols: {syms}")
        for sym in syms:
            start, end = get_date_range(sym)
            print(f"  {sym}: {start} → {end}")

    print("\nLatest macro snapshot:")
    macro = get_latest_macro()
    if macro:
        print(f"  TLT:    ${macro.get('tlt_price', 'N/A')}")
        print(f"  TYX:    {macro.get('tyx_yield', 'N/A')}%")
        print(f"  USO:    ${macro.get('uso_price', 'N/A')}")
        print(f"  VIX:    {macro.get('vix', 'N/A')}")
        print(f"  Regime: {macro.get('regime', 'N/A')}")
        print(f"  Signal: {macro.get('signal', 'N/A')}")
    else:
        print("  No macro data yet.")

    print("\nCombined signal:")
    sig = get_combined_signal()
    if sig:
        print(f"  Macro:  {sig['macro_regime']}")
        print(f"  GEX:    {sig['gex_regime']}")
        print(f"  Signal: {sig['signal']}")
        print(f"  Reason: {sig['reason']}")
    else:
        print("  No signal data yet.")

    print("\nYield curve (today + yesterday):")
    today_curve, yesterday_curve = get_yield_curve_two_days()
    if today_curve:
        print(f"  Today ({today_curve['date']}): "
              f"2Y={today_curve.get('y2')}% "
              f"10Y={today_curve.get('y10')}% "
              f"30Y={today_curve.get('y30')}%")
        print(f"  10Y-2Y spread: {today_curve.get('spread_10_2')}%")
    else:
        print("  No yield curve data yet.")
    if yesterday_curve:
        print(f"  Yesterday ({yesterday_curve['date']}): "
              f"2Y={yesterday_curve.get('y2')}% "
              f"10Y={yesterday_curve.get('y10')}% "
              f"30Y={yesterday_curve.get('y30')}%")