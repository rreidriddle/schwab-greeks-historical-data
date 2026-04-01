"""
query_examples.py
=================
Example queries for analyzing your collected Greeks data.
Run these interactively or build them into your backtest.
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

DB_PATH = "greeks_history.db"  # update if on NVMe

conn = sqlite3.connect(DB_PATH)

# ══════════════════════════════════════════════════════════════════════════════
# BASIC QUERIES
# ══════════════════════════════════════════════════════════════════════════════

def get_summary_history(symbol: str, days: int = 30) -> pd.DataFrame:
    """Get summary data for a symbol over the last N days."""
    return pd.read_sql(f"""
        SELECT * FROM summary
        WHERE symbol = '{symbol}'
        ORDER BY timestamp DESC
        LIMIT {days * 26}
    """, conn)


def get_strike_snapshot(symbol: str, timestamp: str) -> pd.DataFrame:
    """Get full strike-level data for a specific pull timestamp."""
    return pd.read_sql(f"""
        SELECT * FROM strike_data
        WHERE symbol = '{symbol}'
        AND timestamp = '{timestamp}'
        ORDER BY strike
    """, conn)


def get_latest_pull(symbol: str) -> pd.DataFrame:
    """Get the most recent pull for a symbol."""
    return pd.read_sql(f"""
        SELECT * FROM summary
        WHERE symbol = '{symbol}'
        ORDER BY timestamp DESC
        LIMIT 1
    """, conn)


# ══════════════════════════════════════════════════════════════════════════════
# BACKTEST HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def get_morning_charm(symbol: str, date_str: str) -> float | None:
    """
    Get the first charm reading of the day (around 9:30-10am).
    Used as the directional signal for the charm drift backtest.
    date_str format: 'YYYY-MM-DD'
    """
    df = pd.read_sql(f"""
        SELECT net_CharmEX, timestamp FROM summary
        WHERE symbol = '{symbol}'
        AND timestamp LIKE '{date_str}%'
        ORDER BY timestamp ASC
        LIMIT 1
    """, conn)
    if df.empty:
        return None
    return float(df["net_CharmEX"].values[0])


def get_gamma_flip_history(symbol: str) -> pd.DataFrame:
    """Track how gamma flip level has moved over time."""
    return pd.read_sql(f"""
        SELECT timestamp, spot, gamma_flip, net_GEX,
               gex_0dte, gex_1_7dte, gex_8_30dte
        FROM summary
        WHERE symbol = '{symbol}'
        AND gamma_flip IS NOT NULL
        ORDER BY timestamp
    """, conn)


def get_bucket_breakdown(symbol: str, timestamp: str) -> dict:
    """
    GEX breakdown by DTE bucket at a specific timestamp.
    Shows which expirations are driving dealer positioning.
    """
    df = pd.read_sql(f"""
        SELECT dte_bucket, SUM(GEX_net) as net_gex,
               SUM(VannEX_net) as net_vanna,
               SUM(CharmEX_net) as net_charm,
               SUM(total_oi) as total_oi
        FROM strike_data
        WHERE symbol = '{symbol}'
        AND timestamp = '{timestamp}'
        GROUP BY dte_bucket
        ORDER BY dte_bucket
    """, conn)
    return df


def get_wall_history(symbol: str) -> pd.DataFrame:
    """Track call wall and put wall levels over time."""
    return pd.read_sql(f"""
        SELECT timestamp, spot, call_wall, put_wall,
               max_pain, gamma_flip
        FROM summary
        WHERE symbol = '{symbol}'
        ORDER BY timestamp
    """, conn)


# ══════════════════════════════════════════════════════════════════════════════
# CHARM DRIFT BACKTEST STARTER
# ══════════════════════════════════════════════════════════════════════════════

def charm_drift_backtest(symbol: str = "SPY") -> pd.DataFrame:
    """
    Basic charm drift backtest:
    - Morning charm direction (positive/negative) at first pull
    - Compare to price change from open to 3pm close
    - Calculate accuracy of charm as directional signal
    
    Requires price data — extend this with Schwab price history API.
    """
    df = pd.read_sql(f"""
        SELECT DATE(timestamp) as date,
               MIN(timestamp)  as first_pull_ts,
               MAX(timestamp)  as last_pull_ts,
               AVG(spot)       as avg_spot,
               MIN(spot)       as open_spot,
               MAX(spot)       as high_spot,
               MIN(spot)       as low_spot,
               net_CharmEX
        FROM summary
        WHERE symbol = '{symbol}'
        GROUP BY DATE(timestamp)
        ORDER BY date
    """, conn)

    if df.empty:
        print("No data yet — run the collector during market hours first.")
        return df

    # Morning charm signal
    df["charm_signal"] = df["net_CharmEX"].apply(
        lambda x: "BEARISH" if x > 0 else "BULLISH"
    )

    print(f"\n{symbol} Charm Signal History")
    print(f"{'Date':<12} {'Morning Charm':>15} {'Signal':<10} {'Avg Spot':>10}")
    print("─" * 52)
    for _, r in df.iterrows():
        print(f"  {r['date']:<10}  {r['net_CharmEX']/1e6:>+12.4f}M  "
              f"{r['charm_signal']:<10}  ${r['avg_spot']:>8.2f}")

    return df


# ══════════════════════════════════════════════════════════════════════════════
# QUICK STATS
# ══════════════════════════════════════════════════════════════════════════════

def database_stats():
    """Show how much data you've collected so far."""
    for table in ["summary", "strike_data"]:
        count = pd.read_sql(f"SELECT COUNT(*) as n FROM {table}", conn)
        print(f"  {table:<15}: {count['n'].values[0]:>10,} rows")

    symbols = pd.read_sql(
        "SELECT symbol, COUNT(*) as pulls FROM summary GROUP BY symbol", conn)
    print(f"\n  Pulls per symbol:")
    for _, r in symbols.iterrows():
        print(f"    {r['symbol']:<6}: {r['pulls']:>6,} pulls")

    dates = pd.read_sql(
        "SELECT MIN(timestamp) as first, MAX(timestamp) as last FROM summary",
        conn)
    if not dates.empty and dates["first"].values[0]:
        print(f"\n  Date range: {dates['first'].values[0][:10]} "
              f"→ {dates['last'].values[0][:10]}")


if __name__ == "__main__":
    print("\n═" * 40)
    print("  Greeks Database Stats")
    print("═" * 40)
    database_stats()

    print("\n═" * 40)
    print("  Charm Signal History — SPY")
    print("═" * 40)
    charm_drift_backtest("SPY")
