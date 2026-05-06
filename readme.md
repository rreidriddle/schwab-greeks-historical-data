# schwab-greeks-historical-data

Collects and stores live options chain data from the Schwab MarketData API every 10 minutes during market hours. Computes Black-Scholes second-order Greeks (GEX, Vanna, Charm) per strike and writes them to a local SQLite database for historical analysis and backtesting.

This repo is the data pipeline. The companion dashboard repo ([schwab-greeks-dashboard](https://github.com/rreidriddle/schwab-greeks-dashboard)) reads from the database produced here via `db.py`.

## Files

| File | Purpose |
|---|---|
| `collector.py` | Main data collection loop. Fetches options chains every 10 minutes during market hours and writes to SQLite. |
| `db.py` | Read-only database access layer. Used by the dashboard to query Greeks history, regime classifications, macro signals, and yield curve data. |
| `schwab_price.py` | Fetches intraday OHLCV bars from Schwab's `/pricehistory` endpoint. Used by the dashboard backtest tab to plot price against historical GEX levels. |
| `auth.py` | Shared Schwab OAuth handler. Manages token refresh with file locking so the collector and dashboard can run simultaneously without conflicts. |
| `query-tool.py` | CLI utility for inspecting the database — available symbols, date ranges, latest summaries, and regime history. |

## Setup

**Requirements:** Python 3.11+, a Schwab developer account with MarketData API access.

```bash
pip install -r requirements.txt
```

Create a `.env` file in the project root:

```
SCHWAB_CLIENT_ID=your_client_id
SCHWAB_CLIENT_SECRET=your_client_secret
GREEKS_DB_PATH=greeks_history.db  # optional, defaults to local directory
```

On first run, `auth.py` will open a browser window to complete OAuth. After that, tokens are refreshed automatically.

```bash
python collector.py
```

The collector runs continuously, sleeping outside of market hours (9:30am–4:00pm ET, Mon–Fri) and waking automatically at the next open.

## Database Schema

Two tables are written on each collection cycle:

**`strike_data`** — per-strike Greeks for every expiration collected.

| Column | Description |
|---|---|
| `timestamp` | UTC timestamp of the collection |
| `symbol` | Underlying (SPY, QQQ, DIA, etc.) |
| `spot` | Underlying price at time of collection |
| `strike` | Strike price |
| `dte` | Days to expiration |
| `dte_bucket` | 0DTE / 1-7DTE / 8-30DTE / 31-90DTE / 91-180DTE / 180+DTE |
| `GEX_call/put/net` | Gamma exposure by side and net |
| `VannEX_call/put/net` | Vanna exposure by side and net |
| `CharmEX_call/put/net` | Charm exposure by side and net |
| `total_oi` | Open interest at that strike |
| `total_volume` | Volume at that strike |
| `iv_call / iv_put` | Implied volatility per side |

**`summary`** — top-level market structure snapshot per symbol per timestamp.

| Column | Description |
|---|---|
| `net_GEX / net_VannEX / net_CharmEX` | Aggregate exposure across all strikes |
| `gamma_flip` | Strike where net GEX crosses zero |
| `call_wall / put_wall` | Strikes with peak call and put GEX |
| `max_pain` | Strike that minimizes total OI loss at expiration |
| `iv_atm` | ATM implied volatility |
| `gex_0dte` through `gex_180plus_dte` | GEX broken out by DTE bucket |

## db.py — Access Layer

`db.py` is the interface between the database and the dashboard. It is read-only and designed to be imported — never run directly (though running it prints a diagnostic summary).

Key functions:

```python
get_latest_summary(symbol)           # Most recent snapshot for a symbol
get_summary_history(symbol, start, end)  # Summary rows across a date range
get_session_summary(symbol, start, end)  # Open/close regime and flip test per day
get_max_pain(symbol, offset)         # Max pain with optional day offset
get_date_range(symbol)               # First and last dates available for a symbol
get_latest_macro()                   # Most recent macro regime classification
get_yield_curve_two_days()           # Today and yesterday's Treasury yield curve
get_combined_signal()                # Merged GEX regime + macro regime signal
```

Regime classifications returned by `get_latest_summary` and related functions:

| Regime | Condition |
|---|---|
| `STRONG_POS` | Net GEX well above zero — dealers long gamma, dampening moves |
| `WEAK_POS` | Net GEX slightly positive |
| `FLIP_ZONE` | Net GEX near zero — unstable, dealers near neutral |
| `WEAK_NEG` | Net GEX slightly negative |
| `STRONG_NEG` | Net GEX well below zero — dealers short gamma, amplifying moves |

## schwab_price.py

Fetches intraday OHLCV bars from Schwab's `/pricehistory` endpoint. Returns a pandas DataFrame with a UTC datetime index. Supported intervals: 1min, 5min, 15min, 30min. Returns an empty DataFrame on any failure so the dashboard degrades gracefully if price data is unavailable.

This module is intentionally separate from `db.py`. Greeks data and price data come from different sources and may use different vendors as the project grows.

## Symbols

Default collection symbol: `SPY,`

The `SYMBOLS` list in `collector.py` can be modified. Strike range defaults to ±12% of spot. Collection window defaults to 45 DTE max — adjustable via the `toDate` parameter in `get_options_chain`.

## Notes

`greeks_history.db` is excluded from version control. The database grows roughly 50–150MB per month depending on market volatility and number of symbols collected. Point `GREEKS_DB_PATH` in your `.env` at an NVMe drive if storage speed is a concern.

Windows users: the collector uses `SetThreadExecutionState` to prevent the system from sleeping during market hours. This is released automatically on exit.