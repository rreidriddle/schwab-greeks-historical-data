# schwab-greeks-historical-data

Collects and stores live SPY options chain data from the Schwab MarketData API every 10 minutes during market hours. Computes Black-Scholes second-order Greeks (GEX, VannEX, CharmEX) per strike and writes them to a local SQLite database for historical analysis and backtesting.

This is the data pipeline. The companion dashboard ([black-scholes-greeks-dashboard](https://github.com/rreidriddle/black-scholes-greeks-dashboard)) reads from the database produced here for its BACKTEST tab. All live data in the dashboard is fetched independently — only the backtest requires this collector.

---

## Files

| File | Purpose |
|---|---|
| `collector.py` | Main collection loop — fetches SPY chain every 10 min during market hours, writes to SQLite |
| `greeks.py` | Pure Black-Scholes math — `parse_chain`, `aggregate`, structural levels (GEX flip, walls, max pain, ATM IV) |
| `auth.py` | Schwab OAuth handler — file-locked so the collector and dashboard can run simultaneously without token conflicts |
| `test_collector.py` | Dry-run test — exercises every function against the live API without writing to the database |
| `query-tool.py` | CLI utility for inspecting the database — available symbols, date ranges, latest summaries |

---

## Setup

**Requirements:** Python 3.12+, Charles Schwab developer account with MarketData API access.

```bash
git clone https://github.com/rreidriddle/schwab-greeks-historical-data.git
cd schwab-greeks-historical-data
pip install -r requirements.txt
```

Create a `.env` file in the project root:

```
SCHWAB_CLIENT_ID=your_client_id
SCHWAB_CLIENT_SECRET=your_client_secret
GREEKS_DB_PATH=E:\GreeksData\greeks_history.db   # optional, defaults to local directory
```

Authenticate once to cache your OAuth token:

```bash
python auth.py
```

Then start the collector:

```bash
python collector.py
```

The collector runs continuously, sleeping outside of market hours (9:30am–4:00pm ET, Mon–Fri) and waking automatically at the next open. It also ensures two scheduled pulls at 3:45pm and 4:00pm ET to capture end-of-day positioning regardless of the 10-minute cadence.

---

## Collection Behavior

- **Interval:** every 10 minutes during market hours
- **Scheduled pulls:** 3:45pm and 4:00pm ET always fire (captures close positioning)
- **Strike range:** ±12% of spot price
- **Max DTE:** full chain (no DTE cap at collection time)
- **Sleep prevention:** holds `SetThreadExecutionState` on Windows so the machine doesn't sleep during market hours
- **Concurrent fetching:** chain and macro quotes fetched concurrently to minimize latency per cycle

---

## Database Schema

Three tables written on each collection cycle:

**`strike_data`** — per-strike, per-expiry Greeks snapshot.

| Column | Description |
|---|---|
| `timestamp` | UTC timestamp of the collection |
| `symbol` | Underlying ticker (SPY) |
| `spot` | Underlying price at time of collection |
| `strike` | Strike price |
| `dte` | Days to expiration |
| `dte_bucket` | 0DTE / 1-7DTE / 8-30DTE / 31-90DTE / 91-180DTE / 180+DTE |
| `GEX_call / GEX_put / GEX_net` | Gamma exposure by side and net |
| `VannEX / VannEX_call / VannEX_put` | Vanna exposure by side and net |
| `CharmEX / CharmEX_call / CharmEX_put` | Charm exposure by side and net |
| `total_oi` | Open interest at that strike |
| `total_volume` | Volume at that strike |
| `iv_call / iv_put` | Implied volatility per side |

**`summary`** — top-level market structure per symbol per timestamp.

| Column | Description |
|---|---|
| `net_GEX / net_VannEX / net_CharmEX` | Aggregate exposure across all strikes |
| `gamma_flip` | Strike where net GEX crosses zero |
| `call_wall / put_wall` | Strikes with peak call and put GEX |
| `max_pain` | Strike that minimizes total OI loss at expiration |
| `iv_atm` | Front-month ATM implied volatility (closest expiry to 30 DTE) |
| `gex_regime` | STRONG_POS / WEAK_POS / FLIP_ZONE / WEAK_NEG / STRONG_NEG |
| `gex_0dte` through `gex_180plus` | Net GEX broken out by DTE bucket |

**`macro_snapshot`** — TLT and VIX snapshot per timestamp.

| Column | Description |
|---|---|
| `tlt_price / tlt_change / tlt_chg_pct` | TLT price and daily change |
| `vix / vix_change / vix_chg_pct` | VIX level and daily change |

---

## Testing

`test_collector.py` runs a full dry cycle against the live API without touching the database:

```bash
python test_collector.py
```

Nine steps: authentication, spot price, chain fetch, Greeks parsing, aggregation, structural levels (gamma flip, walls, max pain, ATM IV, regime), DB writer signature check, macro quotes, and DTE bucket distribution. Exits immediately on any failure with a clear error message.

---

## Notes

`greeks_history.db` is excluded from version control. The database grows roughly 50–150MB per month depending on market volatility. Point `GREEKS_DB_PATH` in your `.env` at an NVMe drive if storage speed is a concern.

`greeks.py` is an intentional duplicate of the same file in the dashboard repo — both are self-contained and independently versioned. The collector's copy uses `STRIKE_PCT=0.12` (±12% collection window); the dashboard uses `0.08` for display.

Windows: the collector holds `SetThreadExecutionState` during market hours to prevent system sleep. This is released automatically on exit or exception.
