"""
Schwab Greeks Historical Data Collector
========================================
Pulls live options chain data from Charles Schwab MarketData API
every 15 minutes during market hours (9:30am - 4:00pm ET, Mon-Fri).

Stores per-strike Greeks and summary data in SQLite for backtesting.

Symbols: SPY, QQQ, DIA, XSP, IWM
Greeks:  Gamma (GEX), Vanna, Charm
Storage: Two tables — strike_data (granular) + summary (top-level)

Place in the same folder as auth.py and options_greeks_dashboard.py.
All three programs share a single tokens.json via the file-locked auth module.
"""

from dotenv import load_dotenv
load_dotenv()

import os
import sys
import time
import sqlite3
import datetime
import logging
import requests
import numpy as np
import pandas as pd
from zoneinfo import ZoneInfo
from scipy.stats import norm
from concurrent.futures import ThreadPoolExecutor, as_completed

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

CLIENT_ID     = os.environ.get("SCHWAB_CLIENT_ID",     "YOUR_CLIENT_ID")
CLIENT_SECRET = os.environ.get("SCHWAB_CLIENT_SECRET", "YOUR_CLIENT_SECRET")
RISK_FREE     = 0.045
SCHWAB_BASE   = "https://api.schwabapi.com/marketdata/v1"
STRIKE_PCT    = 0.12
ET            = ZoneInfo("America/New_York")

SYMBOLS = ["SPY", "QQQ", "DIA", "XSP", "IWM"]

# DTE buckets — each expiration is tagged with its bucket label
DTE_BUCKETS = [
    (0,   0,   "0DTE"),
    (1,   7,   "1-7DTE"),
    (8,   30,  "8-30DTE"),
    (31,  90,  "31-90DTE"),
    (91,  180, "91-180DTE"),
    (181, 999, "180+DTE"),
]

# Storage path — point this at your NVMe drive if desired
# Windows example: "D:/GreeksData/greeks_history.db"
DB_PATH = os.environ.get("GREEKS_DB_PATH", "greeks_history.db")

# Pull interval in minutes
PULL_INTERVAL_MINUTES = 15

# Max parallel workers for concurrent symbol fetching
MAX_WORKERS = 3

# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("collector.log", encoding="utf-8"),
    ]
)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# DATABASE SETUP
# ══════════════════════════════════════════════════════════════════════════════

def init_db(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    conn.executescript("""
    CREATE TABLE IF NOT EXISTS strike_data (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp       TEXT    NOT NULL,
        symbol          TEXT    NOT NULL,
        spot            REAL    NOT NULL,
        strike          REAL    NOT NULL,
        dte             REAL    NOT NULL,
        dte_bucket      TEXT    NOT NULL,
        GEX_call        REAL,
        GEX_put         REAL,
        GEX_net         REAL,
        VannEX_call     REAL,
        VannEX_put      REAL,
        VannEX_net      REAL,
        CharmEX_call    REAL,
        CharmEX_put     REAL,
        CharmEX_net     REAL,
        total_oi        INTEGER,
        total_volume    INTEGER,
        iv_call         REAL,
        iv_put          REAL
    );

    CREATE TABLE IF NOT EXISTS summary (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp           TEXT    NOT NULL,
        symbol              TEXT    NOT NULL,
        spot                REAL    NOT NULL,
        net_GEX             REAL,
        net_VannEX          REAL,
        net_CharmEX         REAL,
        gamma_flip          REAL,
        call_wall           REAL,
        put_wall            REAL,
        max_pain            REAL,
        total_oi            INTEGER,
        total_volume        INTEGER,
        iv_atm              REAL,
        gex_0dte            REAL,
        gex_1_7dte          REAL,
        gex_8_30dte         REAL,
        gex_31_90dte        REAL,
        gex_91_180dte       REAL,
        gex_180plus_dte     REAL
    );

    CREATE INDEX IF NOT EXISTS idx_strike_ts     ON strike_data (timestamp, symbol);
    CREATE INDEX IF NOT EXISTS idx_strike_sym    ON strike_data (symbol, strike);
    CREATE INDEX IF NOT EXISTS idx_strike_bucket ON strike_data (symbol, dte_bucket);
    CREATE INDEX IF NOT EXISTS idx_summary_ts    ON summary (timestamp, symbol);
    CREATE INDEX IF NOT EXISTS idx_summary_sym   ON summary (symbol);
    """)
    conn.commit()
    log.info(f"Database ready: {path}")
    return conn

# ══════════════════════════════════════════════════════════════════════════════
# MARKET HOURS CHECK
# ══════════════════════════════════════════════════════════════════════════════

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

def is_market_open() -> bool:
    now   = datetime.datetime.now(ET)
    today = now.date()
    if now.weekday() >= 5:
        return False
    if today in MARKET_HOLIDAYS:
        return False
    market_open  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0,  second=0, microsecond=0)
    return market_open <= now < market_close


def seconds_until_open() -> int:
    now = datetime.datetime.now(ET)
    candidate = now.replace(hour=9, minute=30, second=0, microsecond=0)
    if candidate > now and now.weekday() < 5 and now.date() not in MARKET_HOLIDAYS:
        return int((candidate - now).total_seconds())
    next_day = now + datetime.timedelta(days=1)
    while next_day.weekday() >= 5 or next_day.date() in MARKET_HOLIDAYS:
        next_day += datetime.timedelta(days=1)
    next_open = next_day.replace(hour=9, minute=30, second=0, microsecond=0)
    return int((next_open - now).total_seconds())

# ══════════════════════════════════════════════════════════════════════════════
# SCHWAB API
# ══════════════════════════════════════════════════════════════════════════════

def get_spot(token: str, symbol: str) -> float | None:
    try:
        r = requests.get(
            f"{SCHWAB_BASE}/quotes",
            headers={"Authorization": f"Bearer {token}"},
            params={"symbols": symbol},
            timeout=10,
        )
        r.raise_for_status()
        qd    = r.json()
        inner = qd.get(symbol, list(qd.values())[0] if qd else {})
        return (inner.get("quote", {}).get("lastPrice")
                or inner.get("lastPrice")
                or inner.get("mark"))
    except Exception as e:
        log.warning(f"  Spot fetch failed for {symbol}: {e}")
        return None


def get_options_chain(token: str, symbol: str, spot: float) -> dict | None:
    headers   = {"Authorization": f"Bearer {token}"}
    from_date = datetime.date.today().strftime("%Y-%m-%d")
    to_date   = (datetime.date.today() +
                 datetime.timedelta(days=45)).strftime("%Y-%m-%d")
    params = {
        "symbol":           symbol,
        "contractType":     "ALL",
        "includeQuotes":    "TRUE",
        "optionType":       "ALL",
        "range":            "ALL",
        "fromDate":         from_date,
        "toDate":           to_date,
        "strikePriceAbove": round(spot * (1 - STRIKE_PCT), 2),
        "strikePriceBelow": round(spot * (1 + STRIKE_PCT), 2),
    }
    for attempt in range(3):
        try:
            r = requests.get(
                f"{SCHWAB_BASE}/chains",
                headers=headers, params=params, timeout=45,
            )
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError:
            if r.status_code in [429, 502, 503, 504] and attempt < 2:
                w = (attempt + 1) * 3
                log.warning(f"  {r.status_code} on {symbol} — retry in {w}s")
                time.sleep(w)
                continue
            log.error(f"  Chain fetch failed {symbol}: {r.status_code}")
            return None
        except Exception as e:
            log.error(f"  Chain fetch error {symbol}: {e}")
            return None

# ══════════════════════════════════════════════════════════════════════════════
# BLACK-SCHOLES
# ══════════════════════════════════════════════════════════════════════════════

def _d1(S, K, T, r, s): return (np.log(S/K) + (r + 0.5*s**2)*T) / (s*np.sqrt(T))
def _d2(S, K, T, r, s): return _d1(S,K,T,r,s) - s*np.sqrt(T)

def calc_gamma(S, K, T, r, s):
    return norm.pdf(_d1(S,K,T,r,s)) / (S * s * np.sqrt(T))

def calc_vanna(S, K, T, r, s):
    return -norm.pdf(_d1(S,K,T,r,s)) * _d2(S,K,T,r,s) / s

def calc_charm(S, K, T, r, s, call):
    d1  = _d1(S,K,T,r,s); d2 = _d2(S,K,T,r,s)
    raw = -norm.pdf(d1) * (2*r*T - d2*s*np.sqrt(T)) / (2*T*s*np.sqrt(T))
    return raw/365 if call else (raw + 2*r*norm.cdf(-d1))/365

# ══════════════════════════════════════════════════════════════════════════════
# DTE BUCKETING
# ══════════════════════════════════════════════════════════════════════════════

def get_dte_bucket(dte: float) -> str:
    for lo, hi, label in DTE_BUCKETS:
        if lo <= dte <= hi:
            return label
    return "180+DTE"

# ══════════════════════════════════════════════════════════════════════════════
# PARSE CHAIN
# ══════════════════════════════════════════════════════════════════════════════

def parse_chain(chain: dict, r: float = RISK_FREE) -> pd.DataFrame:
    S    = chain["underlyingPrice"]
    rows = []

    for side, exp_map in [
        ("call", chain.get("callExpDateMap", {})),
        ("put",  chain.get("putExpDateMap",  {})),
    ]:
        call = (side == "call")
        for exp_key, strikes in exp_map.items():
            try:    dte = float(exp_key.split(":")[1])
            except: continue
            T = dte / 365
            if T <= 0: continue

            bucket = get_dte_bucket(dte)

            for ks, contracts in strikes.items():
                K = float(ks)
                if abs(K - S) / S > STRIKE_PCT: continue

                c     = contracts[0]
                iv    = c.get("volatility", 0)
                if not iv or iv <= 0: continue
                sigma = iv / 100
                oi    = c.get("openInterest", 0) or 0
                vol   = c.get("totalVolume",  0) or 0
                if oi < 1: continue

                try:
                    g  = calc_gamma(S, K, T, r, sigma)
                    va = calc_vanna(S, K, T, r, sigma)
                    ch = calc_charm(S, K, T, r, sigma, call)
                except: continue

                mult = oi * 100
                sign = 1 if call else -1

                rows.append({
                    "strike":       K,
                    "dte":          dte,
                    "dte_bucket":   bucket,
                    "type":         side,
                    "oi":           oi,
                    "volume":       vol,
                    "iv":           sigma,
                    "GEX_call":     g  * mult * S if call     else 0,
                    "GEX_put":     -g  * mult * S if not call else 0,
                    "VannEX_call":  va * mult      if call     else 0,
                    "VannEX_put":  -va * mult      if not call else 0,
                    "VannEX":       sign * va * mult,
                    "CharmEX_call": ch * mult      if call     else 0,
                    "CharmEX_put": -ch * mult      if not call else 0,
                    "CharmEX":      sign * ch * mult,
                })

    return pd.DataFrame(rows)

# ══════════════════════════════════════════════════════════════════════════════
# AGGREGATE
# ══════════════════════════════════════════════════════════════════════════════

def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "GEX_call", "GEX_put",
        "VannEX", "VannEX_call", "VannEX_put",
        "CharmEX", "CharmEX_call", "CharmEX_put",
        "oi", "volume",
    ]
    a = (df.groupby(["strike", "dte", "dte_bucket"])[cols]
           .sum()
           .reset_index()
           .sort_values(["strike", "dte"]))
    a["GEX_net"] = a["GEX_call"] + a["GEX_put"]

    iv_pivot = (df.pivot_table(
                    index=["strike", "dte"],
                    columns="type",
                    values="iv",
                    aggfunc="first")
                  .reset_index()
                  .rename(columns={"call": "iv_call", "put": "iv_put"}))
    iv_pivot = iv_pivot[["strike", "dte"] +
                        [c for c in ["iv_call", "iv_put"] if c in iv_pivot.columns]]
    a = a.merge(iv_pivot, on=["strike", "dte"], how="left")
    return a

# ══════════════════════════════════════════════════════════════════════════════
# STRUCTURAL LEVEL CALCULATIONS
# ══════════════════════════════════════════════════════════════════════════════

def calc_gamma_flip(agg: pd.DataFrame) -> float | None:
    by_strike = agg.groupby("strike")["GEX_net"].sum().reset_index()
    pos = by_strike[by_strike["GEX_net"] > 0]["strike"]
    neg = by_strike[by_strike["GEX_net"] < 0]["strike"]
    if pos.empty or neg.empty:
        return None
    return round((pos.min() + neg.max()) / 2, 2)


def calc_call_wall(agg: pd.DataFrame) -> float | None:
    by_strike = agg.groupby("strike")["GEX_call"].sum()
    if by_strike.empty:
        return None
    return float(by_strike.idxmax())


def calc_put_wall(agg: pd.DataFrame) -> float | None:
    by_strike = agg.groupby("strike")["GEX_put"].sum()
    if by_strike.empty:
        return None
    return float(by_strike.idxmin())


def calc_max_pain(df: pd.DataFrame) -> float | None:
    strikes = df["strike"].unique()
    if len(strikes) == 0:
        return None
    pain = {}
    for s in strikes:
        calls     = df[df["type"] == "call"]
        puts      = df[df["type"] == "put"]
        call_pain = ((s - calls["strike"]).clip(lower=0) * calls["oi"]).sum()
        put_pain  = ((puts["strike"] - s).clip(lower=0) * puts["oi"]).sum()
        pain[s]   = call_pain + put_pain
    return float(min(pain, key=pain.get))


def calc_atm_iv(df: pd.DataFrame, spot: float) -> float | None:
    calls = df[df["type"] == "call"].copy()
    if calls.empty:
        return None
    calls["dist"] = (calls["strike"] - spot).abs()
    closest = calls.nsmallest(1, "dist")
    return float(closest["iv"].values[0]) if not closest.empty else None

# ══════════════════════════════════════════════════════════════════════════════
# DATABASE WRITERS
# ══════════════════════════════════════════════════════════════════════════════

def write_strike_data(conn: sqlite3.Connection, ts: str,
                      symbol: str, spot: float, agg: pd.DataFrame):
    rows = []
    for _, r in agg.iterrows():
        rows.append((
            ts, symbol, spot,
            r["strike"], r["dte"], r["dte_bucket"],
            r.get("GEX_call"),  r.get("GEX_put"),  r.get("GEX_net"),
            r.get("VannEX_call"), r.get("VannEX_put"), r.get("VannEX"),
            r.get("CharmEX_call"), r.get("CharmEX_put"), r.get("CharmEX"),
            int(r.get("oi", 0)), int(r.get("volume", 0)),
            r.get("iv_call"), r.get("iv_put"),
        ))
    conn.executemany("""
        INSERT INTO strike_data (
            timestamp, symbol, spot, strike, dte, dte_bucket,
            GEX_call, GEX_put, GEX_net,
            VannEX_call, VannEX_put, VannEX_net,
            CharmEX_call, CharmEX_put, CharmEX_net,
            total_oi, total_volume,
            iv_call, iv_put
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()


def write_summary(conn: sqlite3.Connection, ts: str,
                  symbol: str, spot: float,
                  agg: pd.DataFrame, df_raw: pd.DataFrame):
    bucket_gex = {}
    for _, label in [(lo, lbl) for lo, hi, lbl in DTE_BUCKETS]:
        subset = agg[agg["dte_bucket"] == label]["GEX_net"].sum()
        bucket_gex[label] = float(subset)

    conn.execute("""
        INSERT INTO summary (
            timestamp, symbol, spot,
            net_GEX, net_VannEX, net_CharmEX,
            gamma_flip, call_wall, put_wall, max_pain,
            total_oi, total_volume, iv_atm,
            gex_0dte, gex_1_7dte, gex_8_30dte,
            gex_31_90dte, gex_91_180dte, gex_180plus_dte
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        ts, symbol, spot,
        float(agg["GEX_net"].sum()),
        float(agg["VannEX"].sum()),
        float(agg["CharmEX"].sum()),
        calc_gamma_flip(agg),
        calc_call_wall(agg),
        calc_put_wall(agg),
        calc_max_pain(df_raw),
        int(agg["oi"].sum()),
        int(agg["volume"].sum()),
        calc_atm_iv(df_raw, spot),
        bucket_gex.get("0DTE",       0),
        bucket_gex.get("1-7DTE",     0),
        bucket_gex.get("8-30DTE",    0),
        bucket_gex.get("31-90DTE",   0),
        bucket_gex.get("91-180DTE",  0),
        bucket_gex.get("180+DTE",    0),
    ))
    conn.commit()

# ══════════════════════════════════════════════════════════════════════════════
# FETCH ONE SYMBOL (used by ThreadPoolExecutor)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_symbol(token: str, symbol: str) -> tuple[str, float | None, pd.DataFrame | None]:
    """
    Fetch spot + chain for a single symbol.
    Returns (symbol, spot, df_raw) — spot/df_raw are None on failure.
    A short sleep between quote and chain helps avoid back-to-back 429s
    on the same symbol without adding unnecessary delay between symbols.
    """
    spot = get_spot(token, symbol)
    if not spot:
        log.warning(f"  Skipping {symbol} — no spot price")
        return symbol, None, None

    time.sleep(1)  # brief pause between quote and chain for same symbol

    chain = get_options_chain(token, symbol, spot)
    if not chain:
        log.warning(f"  Skipping {symbol} — no chain data")
        return symbol, spot, None

    df_raw = parse_chain(chain)
    if df_raw.empty:
        log.warning(f"  Skipping {symbol} — empty parsed chain")
        return symbol, spot, None

    return symbol, spot, df_raw

# ══════════════════════════════════════════════════════════════════════════════
# SINGLE PULL CYCLE
# ══════════════════════════════════════════════════════════════════════════════

def run_pull(conn: sqlite3.Connection, token: str):
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info(f"Pull started — {ts}")

    results = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_symbol, token, sym): sym for sym in SYMBOLS}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                symbol, spot, df_raw = future.result()
                results[symbol] = (spot, df_raw)
            except Exception as e:
                log.error(f"  Unexpected error fetching {sym}: {e}")
                results[sym] = (None, None)

    # Write results in deterministic order
    for symbol in SYMBOLS:
        spot, df_raw = results.get(symbol, (None, None))
        if spot is None or df_raw is None:
            continue

        agg = aggregate(df_raw)
        write_strike_data(conn, ts, symbol, spot, agg)
        write_summary(conn, ts, symbol, spot, agg, df_raw)

        net_gex   = agg["GEX_net"].sum()
        net_vanna = agg["VannEX"].sum()
        net_charm = agg["CharmEX"].sum()
        log.info(f"  {symbol} ${spot:.2f} | "
                 f"GEX {net_gex/1e9:+.3f}B | "
                 f"Vanna {net_vanna/1e3:+.0f}K | "
                 f"Charm {net_charm/1e6:+.4f}M | "
                 f"OI {agg['oi'].sum():,.0f}")

    log.info(f"Pull complete — {len(SYMBOLS)} symbols\n")

# ══════════════════════════════════════════════════════════════════════════════
# MAIN LOOP
# ══════════════════════════════════════════════════════════════════════════════

def main():

    import ctypes
    ES_CONTINUOUS      = 0x80000000
    ES_SYSTEM_REQUIRED = 0x00000001
    ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)
    if CLIENT_ID == "YOUR_CLIENT_ID":
        log.error("No Schwab credentials found. Add to .env file.")
        sys.exit(1)

    log.info("═" * 55)
    log.info("  Schwab Greeks Historical Data Collector")
    log.info(f"  Symbols : {', '.join(SYMBOLS)}")
    log.info(f"  Interval: {PULL_INTERVAL_MINUTES} minutes")
    log.info(f"  Database: {DB_PATH}")
    log.info("═" * 55)

    conn = init_db(DB_PATH)

    from auth import get_valid_access_token

    try:
        while True:
            if not is_market_open():
                secs = seconds_until_open()
                hrs  = secs // 3600
                mins = (secs % 3600) // 60
                log.info(f"Market closed — sleeping {hrs}h {mins}m until next open")
                time.sleep(60)
                continue

            try:
                token = get_valid_access_token(silent=True)
            except Exception as e:
                log.error(f"Auth failed: {e}")
                time.sleep(60)
                continue

            run_pull(conn, token)

            log.info(f"Next pull in {PULL_INTERVAL_MINUTES} minutes...")
            time.sleep(PULL_INTERVAL_MINUTES * 60)

    except KeyboardInterrupt:
        log.info("Collector stopped by user.")
    finally:
        ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS)
        log.info("Sleep prevention cleared.")



if __name__ == "__main__":
    main()
