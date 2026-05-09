"""
Schwab Greeks Historical Data Collector
========================================
Pulls live options chain data from Charles Schwab MarketData API
every 10 minutes during market hours (9:30am - 4:00pm ET, Mon-Fri).

Stores SPY Greeks (strike_data, summary) and TLT/VIX snapshots
(macro_snapshot) for backtesting. Yield curve, macro regime, and
combined signal are handled entirely by the dashboard.
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
import pandas as pd
from zoneinfo import ZoneInfo

from greeks import (
    DTE_BUCKETS, parse_chain, aggregate,
    calc_gamma_flip, calc_call_wall, calc_put_wall,
    calc_max_pain, calc_atm_iv, calc_gex_regime,
)

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

CLIENT_ID     = os.environ.get("SCHWAB_CLIENT_ID",     "YOUR_CLIENT_ID")
CLIENT_SECRET = os.environ.get("SCHWAB_CLIENT_SECRET", "YOUR_CLIENT_SECRET")
RISK_FREE     = 0.045
SCHWAB_BASE   = "https://api.schwabapi.com/marketdata/v1"
STRIKE_PCT    = 0.12
ET            = ZoneInfo("America/New_York")

SYMBOL = "SPY"

DB_PATH = os.environ.get("GREEKS_DB_PATH", "greeks_history.db")

PULL_INTERVAL_MINUTES = 10

SCHEDULED_PULLS = [
    datetime.time(15, 45),
    datetime.time(16,  0),
]
SCHEDULED_PULL_WINDOW = 90

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

    CREATE TABLE IF NOT EXISTS macro_snapshot (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp   TEXT    NOT NULL,
        tlt_price   REAL,
        tlt_change  REAL,
        tlt_chg_pct REAL,
        vix         REAL,
        vix_change  REAL,
        vix_chg_pct REAL
    );

    CREATE INDEX IF NOT EXISTS idx_strike_ts     ON strike_data (timestamp, symbol);
    CREATE INDEX IF NOT EXISTS idx_strike_sym    ON strike_data (symbol, strike);
    CREATE INDEX IF NOT EXISTS idx_strike_bucket ON strike_data (symbol, dte_bucket);
    CREATE INDEX IF NOT EXISTS idx_summary_ts    ON summary (timestamp, symbol);
    CREATE INDEX IF NOT EXISTS idx_summary_sym   ON summary (symbol);
    CREATE INDEX IF NOT EXISTS idx_macro_ts      ON macro_snapshot (timestamp);
    """)
    conn.commit()

    log.info(f"Database ready: {path}")
    return conn

# ══════════════════════════════════════════════════════════════════════════════
# MARKET HOURS
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
    return market_open <= now <= market_close


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


def is_scheduled_pull_due(last_scheduled: dict) -> bool:
    now   = datetime.datetime.now(ET)
    today = now.date()
    for t in SCHEDULED_PULLS:
        key = f"{today}_{t.strftime('%H%M')}"
        if last_scheduled.get(key):
            continue
        scheduled_dt = datetime.datetime.combine(today, t, tzinfo=ET)
        diff = abs((now - scheduled_dt).total_seconds())
        if diff <= SCHEDULED_PULL_WINDOW:
            last_scheduled[key] = True
            return True
    return False

# ══════════════════════════════════════════════════════════════════════════════
# SCHWAB API — OPTIONS
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
# SCHWAB API — MACRO QUOTES
# ══════════════════════════════════════════════════════════════════════════════

def get_macro_quotes(token: str) -> dict:
    result = {
        "tlt_price": None, "tlt_change": None, "tlt_chg_pct": None,
        "vix":       None, "vix_change": None, "vix_chg_pct": None,
    }
    try:
        r = requests.get(
            f"{SCHWAB_BASE}/quotes",
            headers={"Authorization": f"Bearer {token}"},
            params={"symbols": "TLT,$VIX"},
            timeout=15,
        )
        r.raise_for_status()
        qd = r.json()

        def _extract(sym):
            inner = qd.get(sym, {})
            quote = inner.get("quote", inner)
            last  = quote.get("lastPrice") or quote.get("mark")
            prev  = quote.get("closePrice") or last
            if not last:
                return None, None, None
            chg     = last - (prev or last)
            chg_pct = (chg / prev * 100) if prev else 0
            return last, chg, chg_pct

        tlt_p, tlt_c, tlt_pct = _extract("TLT")
        vix_p, vix_c, vix_pct = _extract("$VIX")
        result.update({
            "tlt_price": tlt_p, "tlt_change": tlt_c, "tlt_chg_pct": tlt_pct,
            "vix":       vix_p, "vix_change": vix_c, "vix_chg_pct": vix_pct,
        })
    except Exception as e:
        log.warning(f"  Macro quotes fetch failed: {e}")

    return result

# ══════════════════════════════════════════════════════════════════════════════
# DATABASE WRITERS
# ══════════════════════════════════════════════════════════════════════════════

def write_strike_data(conn: sqlite3.Connection, ts: str,
                      symbol: str, spot: float, agg: pd.DataFrame):
    rows = []
    for _, r in agg.iterrows():
        iv_call = r.get("iv_call")
        iv_put  = r.get("iv_put")
        rows.append((
            ts, symbol, spot,
            r["strike"], r["dte"], r["dte_bucket"],
            r.get("GEX_call"),    r.get("GEX_put"),    r.get("GEX_net"),
            r.get("VannEX_call"), r.get("VannEX_put"), r.get("VannEX"),
            r.get("CharmEX_call"),r.get("CharmEX_put"),r.get("CharmEX"),
            int(r.get("oi", 0)), int(r.get("volume", 0)),
            None if (iv_call is None or pd.isna(iv_call)) else float(iv_call),
            None if (iv_put  is None or pd.isna(iv_put))  else float(iv_put),
        ))
    conn.executemany("""
        INSERT INTO strike_data (
            timestamp, symbol, spot, strike, dte, dte_bucket,
            GEX_call, GEX_put, GEX_net,
            VannEX_call, VannEX_put, VannEX_net,
            CharmEX_call, CharmEX_put, CharmEX_net,
            total_oi, total_volume, iv_call, iv_put
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
        bucket_gex.get("0DTE",      0),
        bucket_gex.get("1-7DTE",    0),
        bucket_gex.get("8-30DTE",   0),
        bucket_gex.get("31-90DTE",  0),
        bucket_gex.get("91-180DTE", 0),
        bucket_gex.get("180+DTE",   0),
    ))
    conn.commit()


def write_macro_snapshot(conn: sqlite3.Connection, ts: str, macro: dict):
    conn.execute("""
        INSERT INTO macro_snapshot (
            timestamp,
            tlt_price, tlt_change, tlt_chg_pct,
            vix, vix_change, vix_chg_pct
        ) VALUES (?,?,?,?,?,?,?)
    """, (
        ts,
        macro.get("tlt_price"), macro.get("tlt_change"), macro.get("tlt_chg_pct"),
        macro.get("vix"),       macro.get("vix_change"), macro.get("vix_chg_pct"),
    ))
    conn.commit()

# ══════════════════════════════════════════════════════════════════════════════
# SINGLE PULL CYCLE
# ══════════════════════════════════════════════════════════════════════════════

def run_pull(conn: sqlite3.Connection, token: str):
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info(f"Pull started — {ts}")

    # ── SPY options ────────────────────────────────────────────────────────────
    spot = get_spot(token, SYMBOL)
    if not spot:
        log.warning(f"  Skipping {SYMBOL} — no spot price")
        return

    time.sleep(1)
    chain = get_options_chain(token, SYMBOL, spot)
    if not chain:
        log.warning(f"  Skipping {SYMBOL} — no chain data")
        return

    df_raw = parse_chain(chain)
    if df_raw.empty:
        log.warning(f"  Skipping {SYMBOL} — empty parsed chain")
        return

    agg        = aggregate(df_raw)
    gamma_flip = calc_gamma_flip(agg)
    gex_regime = calc_gex_regime(agg, spot, gamma_flip)

    write_strike_data(conn, ts, SYMBOL, spot, agg)
    write_summary(conn, ts, SYMBOL, spot, agg, df_raw)

    log.info(f"  {SYMBOL} ${spot:.2f} | "
             f"GEX {agg['GEX_net'].sum()/1e9:+.3f}B | "
             f"Vanna {agg['VannEX'].sum()/1e3:+.0f}K | "
             f"Charm {agg['CharmEX'].sum()/1e6:+.4f}M | "
             f"Flip ${gamma_flip} | Regime: {gex_regime}")

    # ── TLT + VIX snapshot ─────────────────────────────────────────────────────
    time.sleep(1)
    macro = get_macro_quotes(token)
    write_macro_snapshot(conn, ts, macro)

    log.info(f"  Macro: TLT=${macro.get('tlt_price','?')} | "
             f"VIX={macro.get('vix','?')}")

    log.info("Pull complete\n")

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
    log.info(f"  Symbol  : {SYMBOL}")
    log.info(f"  Interval: {PULL_INTERVAL_MINUTES} minutes")
    log.info(f"  Sched   : 3:45 PM ET + 4:00 PM ET guaranteed")
    log.info(f"  Database: {DB_PATH}")
    log.info("═" * 55)

    conn           = init_db(DB_PATH)
    last_pull_time = 0
    last_scheduled = {}

    from auth import get_valid_access_token

    try:
        while True:
            if not is_market_open():
                secs = seconds_until_open()
                hrs  = secs // 3600
                mins = (secs % 3600) // 60
                log.info(f"Market closed — sleeping {hrs}h {mins}m until next open")
                last_scheduled = {}
                time.sleep(60)
                continue

            now           = time.time()
            elapsed       = now - last_pull_time
            interval_due  = elapsed >= PULL_INTERVAL_MINUTES * 60
            scheduled_due = is_scheduled_pull_due(last_scheduled)

            if not interval_due and not scheduled_due:
                time.sleep(15)
                continue

            if scheduled_due and not interval_due:
                log.info("Scheduled pull triggered (end-of-day)")

            try:
                token = get_valid_access_token(silent=True)
            except Exception as e:
                log.error(f"Auth failed: {e}")
                time.sleep(60)
                continue

            run_pull(conn, token)
            last_pull_time = time.time()

            log.info(f"Next pull in {PULL_INTERVAL_MINUTES} min "
                     f"(or at next scheduled time)...")
            time.sleep(15)

    except KeyboardInterrupt:
        log.info("Collector stopped by user.")
    finally:
        ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS)
        log.info("Sleep prevention cleared.")


if __name__ == "__main__":
    main()
