"""
Schwab Greeks Historical Data Collector
========================================
Pulls live options chain data from Charles Schwab MarketData API
every 10 minutes during market hours (9:30am - 4:00pm ET, Mon-Fri).

Changes from v1:
  - SPY only (removed QQQ, DIA, XSP, IWM)
  - 10-minute pull interval (was 15)
  - Guaranteed scheduled pulls at 3:45 PM and 4:00 PM ET
  - Fixed IV null bug in write_strike_data()
  - Macro data collection integrated (TLT, USO, VIX, TNX, TYX, FVX, IRX)
  - Yield curve fetched from Treasury.gov on startup + once per session
  - Macro regime + combined signal computed and stored each pull
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
from xml.etree import ElementTree

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

DTE_BUCKETS = [
    (0,   0,   "0DTE"),
    (1,   7,   "1-7DTE"),
    (8,   30,  "8-30DTE"),
    (31,  90,  "31-90DTE"),
    (91,  180, "91-180DTE"),
    (181, 999, "180+DTE"),
]

DB_PATH = os.environ.get("GREEKS_DB_PATH", "greeks_history.db")

PULL_INTERVAL_MINUTES = 10

SCHEDULED_PULLS = [
    datetime.time(15, 45),
    datetime.time(16,  0),
]
SCHEDULED_PULL_WINDOW = 90  # seconds

REGIME_THRESHOLDS = {
    "tyx_orange":  4.75,
    "tyx_red":     5.00,
    "oil_yellow":  85.0,
    "oil_orange": 100.0,
    "oil_red":    110.0,
    "tlt_52w_low": 83.30,
    "tlt_key":     84.76,
}

TREASURY_XML_URL = (
    "https://home.treasury.gov/resource-center/data-chart-center/"
    "interest-rates/pages/xml?data=daily_treasury_yield_curve&field_tdr_date_value_month="
)

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
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp    TEXT    NOT NULL,
        tlt_price    REAL,
        tlt_change   REAL,
        tlt_chg_pct  REAL,
        uso_price    REAL,
        uso_change   REAL,
        uso_chg_pct  REAL,
        tnx_yield    REAL,
        tyx_yield    REAL,
        fvx_yield    REAL,
        irx_yield    REAL,
        vix          REAL,
        vix_change   REAL,
        vix_chg_pct  REAL,
        regime       TEXT,
        signal       TEXT
    );

    CREATE TABLE IF NOT EXISTS yield_curve (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        date         TEXT    NOT NULL,
        m3           REAL,
        m6           REAL,
        y1           REAL,
        y2           REAL,
        y5           REAL,
        y7           REAL,
        y10          REAL,
        y20          REAL,
        y30          REAL,
        spread_10_2  REAL,
        spread_10_3m REAL
    );

    CREATE INDEX IF NOT EXISTS idx_strike_ts     ON strike_data (timestamp, symbol);
    CREATE INDEX IF NOT EXISTS idx_strike_sym    ON strike_data (symbol, strike);
    CREATE INDEX IF NOT EXISTS idx_strike_bucket ON strike_data (symbol, dte_bucket);
    CREATE INDEX IF NOT EXISTS idx_summary_ts    ON summary (timestamp, symbol);
    CREATE INDEX IF NOT EXISTS idx_summary_sym   ON summary (symbol);
    CREATE INDEX IF NOT EXISTS idx_macro_ts      ON macro_snapshot (timestamp);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_yield_date ON yield_curve (date);
    """)
    conn.commit()

    # Migrate existing DBs that predate fvx_yield / irx_yield columns
    for col in ("fvx_yield", "irx_yield"):
        try:
            conn.execute(f"ALTER TABLE macro_snapshot ADD COLUMN {col} REAL")
            conn.commit()
            log.info(f"  Migration: added {col} column to macro_snapshot")
        except sqlite3.OperationalError:
            pass  # column already exists

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
    """
    Fetch TLT, USO, $VIX, $TNX, $TYX, $FVX, $IRX in a single quotes call.
    Returns dict with all macro values. Any failed field returns None.
    """
    symbols = ["TLT", "USO", "$VIX", "$TNX", "$TYX", "$FVX", "$IRX"]
    result  = {
        "tlt_price": None, "tlt_change": None, "tlt_chg_pct": None,
        "uso_price": None, "uso_change": None, "uso_chg_pct": None,
        "vix":       None, "vix_change": None, "vix_chg_pct": None,
        "tnx_yield": None, "tyx_yield":  None,
        "fvx_yield": None, "irx_yield":  None,
    }
    try:
        r = requests.get(
            f"{SCHWAB_BASE}/quotes",
            headers={"Authorization": f"Bearer {token}"},
            params={"symbols": ",".join(symbols)},
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
        uso_p, uso_c, uso_pct = _extract("USO")
        vix_p, vix_c, vix_pct = _extract("$VIX")

        result.update({
            "tlt_price": tlt_p, "tlt_change": tlt_c, "tlt_chg_pct": tlt_pct,
            "uso_price": uso_p, "uso_change": uso_c, "uso_chg_pct": uso_pct,
            "vix":       vix_p, "vix_change": vix_c, "vix_chg_pct": vix_pct,
        })

        # Index yields — Schwab returns as x10 (e.g. 44.4 = 4.44%)
        for key, sym in [
            ("tnx_yield", "$TNX"),
            ("tyx_yield", "$TYX"),
            ("fvx_yield", "$FVX"),
            ("irx_yield", "$IRX"),
        ]:
            inner = qd.get(sym, {})
            quote = inner.get("quote", inner)
            raw   = quote.get("lastPrice") or quote.get("mark")
            if raw:
                result[key] = round(raw / 10, 4)

    except Exception as e:
        log.warning(f"  Macro quotes fetch failed: {e}")

    return result

# ══════════════════════════════════════════════════════════════════════════════
# MACRO REGIME CLASSIFICATION
# ══════════════════════════════════════════════════════════════════════════════

def classify_macro_regime(tlt_price: float | None,
                          tyx_yield: float | None,
                          uso_price: float | None,
                          tlt_prev:  float | None = None) -> str:
    t = REGIME_THRESHOLDS

    # RED — any single trigger
    if tyx_yield and tyx_yield >= t["tyx_red"]:
        return "RED"
    if uso_price and uso_price >= t["oil_red"]:
        return "RED"
    if tlt_price and tlt_price <= t["tlt_52w_low"] * 1.005:
        return "RED"

    # ORANGE
    if tyx_yield and tyx_yield >= t["tyx_orange"]:
        return "ORANGE"
    if uso_price and uso_price >= t["oil_orange"]:
        return "ORANGE"
    if tlt_price and tlt_prev and tlt_price < tlt_prev:
        return "ORANGE"

    # YELLOW
    if uso_price and uso_price >= t["oil_yellow"]:
        return "YELLOW"
    if tyx_yield and tyx_yield >= 4.50:
        return "YELLOW"

    return "GREEN"


def build_regime_reason(tlt_price: float | None,
                        tyx_yield: float | None,
                        uso_price: float | None) -> str:
    t     = REGIME_THRESHOLDS
    parts = []
    if tyx_yield:
        if tyx_yield >= t["tyx_red"]:
            parts.append(f"TYX {tyx_yield:.2f}% — danger zone breach")
        elif tyx_yield >= t["tyx_orange"]:
            parts.append(f"TYX {tyx_yield:.2f}% approaching danger zone")
        else:
            parts.append(f"TYX {tyx_yield:.2f}%")
    if uso_price:
        if uso_price >= t["oil_red"]:
            parts.append(f"Oil significantly elevated (USO ${uso_price:.2f})")
        elif uso_price >= t["oil_orange"]:
            parts.append(f"Oil elevated (USO ${uso_price:.2f})")
    if tlt_price:
        if tlt_price <= t["tlt_52w_low"] * 1.005:
            parts.append(f"TLT ${tlt_price:.2f} near 52-week low")
        elif tlt_price <= t["tlt_key"]:
            parts.append(f"TLT ${tlt_price:.2f} below key level")
    return " | ".join(parts) if parts else "All macro indicators within normal range"


def build_combined_signal(macro_regime: str, gex_regime: str) -> str:
    gex_bearish = gex_regime in ("WEAK_NEG", "STRONG_NEG", "FLIP_ZONE")
    matrix = {
        ("GREEN",  True):  "LONG BIAS",
        ("GREEN",  False): "CONFLICTED — REDUCE SIZE",
        ("YELLOW", True):  "NEUTRAL — WAIT FOR CONFIRMATION",
        ("YELLOW", False): "NEUTRAL — WAIT FOR CONFIRMATION",
        ("ORANGE", True):  "SHORT BIAS",
        ("ORANGE", False): "CONFLICTED — MACRO VS GREEKS",
        ("RED",    True):  "HIGH CONVICTION SHORT",
        ("RED",    False): "CAUTION — MACRO FIGHTING GREEKS",
    }
    return matrix.get((macro_regime, gex_bearish), "NEUTRAL")

# ══════════════════════════════════════════════════════════════════════════════
# TREASURY YIELD CURVE
# ══════════════════════════════════════════════════════════════════════════════

def fetch_yield_curve(target_date: datetime.date | None = None) -> dict | None:
    base_date = target_date or datetime.date.today()
    months_to_try = [
        base_date.strftime("%Y%m"),
        (base_date.replace(day=1) - datetime.timedelta(days=1)).strftime("%Y%m"),
    ]
    for month_str in months_to_try:
        url = TREASURY_XML_URL + month_str
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            root = ElementTree.fromstring(r.content)
            ns = {
                "m":    "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
                "d":    "http://schemas.microsoft.com/ado/2007/08/dataservices",
                "atom": "http://www.w3.org/2005/Atom",
            }
            entries = root.findall("atom:entry/atom:content/m:properties", ns)
            if not entries:
                continue

            best = entries[-1]  # Treasury returns oldest first, newest last
            date_el = best.find("d:NEW_DATE", ns)
            actual_date = date_el.text[:10] if (date_el is not None and date_el.text) else base_date.isoformat()

            def _val(tag):
                el = best.find(f"d:{tag}", ns)
                if el is not None and el.text:
                    try: return float(el.text)
                    except: pass
                return None

            m3  = _val("BC_1MONTH") or _val("BC_3MONTH")
            m6  = _val("BC_6MONTH")
            y1  = _val("BC_1YEAR")
            y2  = _val("BC_2YEAR")
            y5  = _val("BC_5YEAR")
            y7  = _val("BC_7YEAR")
            y10 = _val("BC_10YEAR")
            y20 = _val("BC_20YEAR")
            y30 = _val("BC_30YEAR")

            spread_10_2  = round(y10 - y2,  4) if (y10 and y2)  else None
            spread_10_3m = round(y10 - m3,  4) if (y10 and m3)  else None

            log.info(f"  Yield curve ({actual_date}): 2Y={y2}% 10Y={y10}% 30Y={y30}%")
            return {
                "date": actual_date,
                "m3": m3, "m6": m6, "y1": y1, "y2": y2,
                "y5": y5, "y7": y7, "y10": y10, "y20": y20, "y30": y30,
                "spread_10_2": spread_10_2, "spread_10_3m": spread_10_3m,
            }
        except Exception as e:
            log.warning(f"  Yield curve fetch failed ({month_str}): {e}")
            continue
    return None


def write_yield_curve(conn: sqlite3.Connection, data: dict):
    conn.execute("""
        INSERT OR REPLACE INTO yield_curve
            (date, m3, m6, y1, y2, y5, y7, y10, y20, y30,
             spread_10_2, spread_10_3m)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        data["date"],
        data.get("m3"),  data.get("m6"),  data.get("y1"),
        data.get("y2"),  data.get("y5"),  data.get("y7"),
        data.get("y10"), data.get("y20"), data.get("y30"),
        data.get("spread_10_2"), data.get("spread_10_3m"),
    ))
    conn.commit()

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
    d1  = _d1(S,K,T,r,s)
    d2  = _d2(S,K,T,r,s)
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
                  .reset_index())

    rename_map = {}
    if "call" in iv_pivot.columns: rename_map["call"] = "iv_call"
    if "put"  in iv_pivot.columns: rename_map["put"]  = "iv_put"
    iv_pivot = iv_pivot.rename(columns=rename_map)

    for col in ["iv_call", "iv_put"]:
        if col not in iv_pivot.columns:
            iv_pivot[col] = np.nan

    a = a.merge(iv_pivot[["strike", "dte", "iv_call", "iv_put"]],
                on=["strike", "dte"], how="left")
    return a

# ══════════════════════════════════════════════════════════════════════════════
# STRUCTURAL CALCULATIONS
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
    return float(by_strike.idxmax()) if not by_strike.empty else None


def calc_put_wall(agg: pd.DataFrame) -> float | None:
    by_strike = agg.groupby("strike")["GEX_put"].sum()
    return float(by_strike.idxmin()) if not by_strike.empty else None


def calc_max_pain(df: pd.DataFrame) -> float | None:
    strikes = df["strike"].unique()
    if len(strikes) == 0:
        return None
    pain  = {}
    calls = df[df["type"] == "call"]
    puts  = df[df["type"] == "put"]
    for s in strikes:
        call_pain = ((s - calls["strike"]).clip(lower=0) * calls["oi"]).sum()
        put_pain  = ((puts["strike"] - s).clip(lower=0)  * puts["oi"]).sum()
        pain[s]   = call_pain + put_pain
    return float(min(pain, key=pain.get))


def calc_atm_iv(df: pd.DataFrame, spot: float) -> float | None:
    calls = df[df["type"] == "call"].copy()
    if calls.empty:
        return None
    calls["dist"] = (calls["strike"] - spot).abs()
    closest = calls.nsmallest(1, "dist")
    return float(closest["iv"].values[0]) if not closest.empty else None


def calc_gex_regime(agg: pd.DataFrame, spot: float,
                    gamma_flip: float | None) -> str:
    net_gex = float(agg["GEX_net"].sum())
    if gamma_flip and spot > 0:
        if abs(spot - gamma_flip) / spot <= 0.005:
            return "FLIP_ZONE"
    if net_gex >= 2e9:  return "STRONG_POS"
    if net_gex >= 0:    return "WEAK_POS"
    if net_gex >= -2e9: return "WEAK_NEG"
    return "STRONG_NEG"

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


def write_macro_snapshot(conn: sqlite3.Connection, ts: str,
                         macro: dict, regime: str, signal: str):
    conn.execute("""
        INSERT INTO macro_snapshot (
            timestamp,
            tlt_price, tlt_change, tlt_chg_pct,
            uso_price, uso_change, uso_chg_pct,
            tnx_yield, tyx_yield, fvx_yield, irx_yield,
            vix, vix_change, vix_chg_pct,
            regime, signal
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        ts,
        macro.get("tlt_price"),  macro.get("tlt_change"),  macro.get("tlt_chg_pct"),
        macro.get("uso_price"),  macro.get("uso_change"),  macro.get("uso_chg_pct"),
        macro.get("tnx_yield"),  macro.get("tyx_yield"),
        macro.get("fvx_yield"),  macro.get("irx_yield"),
        macro.get("vix"),        macro.get("vix_change"),  macro.get("vix_chg_pct"),
        regime, signal,
    ))
    conn.commit()

# ══════════════════════════════════════════════════════════════════════════════
# SINGLE PULL CYCLE
# ══════════════════════════════════════════════════════════════════════════════

def run_pull(conn: sqlite3.Connection, token: str, session_state: dict):
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

    # ── Macro quotes ───────────────────────────────────────────────────────────
    time.sleep(1)
    macro = get_macro_quotes(token)

    macro_regime = classify_macro_regime(
        tlt_price = macro.get("tlt_price"),
        tyx_yield = macro.get("tyx_yield"),
        uso_price = macro.get("uso_price"),
        tlt_prev  = session_state.get("last_tlt_price"),
    )

    if macro.get("tlt_price"):
        session_state["last_tlt_price"] = macro["tlt_price"]

    signal = build_combined_signal(macro_regime, gex_regime)
    write_macro_snapshot(conn, ts, macro, macro_regime, signal)

    log.info(f"  Macro: TLT=${macro.get('tlt_price','?')} | "
             f"TYX={macro.get('tyx_yield','?')}% | "
             f"FVX={macro.get('fvx_yield','?')}% | "
             f"IRX={macro.get('irx_yield','?')}% | "
             f"USO=${macro.get('uso_price','?')} | "
             f"Regime={macro_regime} | Signal={signal}")

    # ── Yield curve — once per session ─────────────────────────────────────────
    today = datetime.date.today()
    if session_state.get("yield_curve_fetched_date") != today:
        log.info("  Fetching Treasury yield curve...")
        curve = fetch_yield_curve()
        if curve:
            write_yield_curve(conn, curve)
            session_state["yield_curve_fetched_date"] = today
            log.info(f"  Yield curve stored for {curve['date']}")
        else:
            log.warning("  Yield curve fetch failed — will retry next pull")

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
    session_state  = {}
    last_pull_time = 0
    last_scheduled = {}

    # Pre-market: fetch yield curve immediately on startup so the dashboard
    # has data before the first market-hours pull at 9:30 ET.
    log.info("Fetching yield curve on startup...")
    curve = fetch_yield_curve()
    if curve:
        write_yield_curve(conn, curve)
        session_state["yield_curve_fetched_date"] = datetime.date.today()
        log.info(f"  Yield curve ready: 2Y={curve.get('y2')}% 10Y={curve.get('y10')}%")
    else:
        log.warning("  Startup yield curve fetch failed — will retry on first pull")

    from auth import get_valid_access_token

    try:
        while True:
            if not is_market_open():
                secs = seconds_until_open()
                hrs  = secs // 3600
                mins = (secs % 3600) // 60
                log.info(f"Market closed — sleeping {hrs}h {mins}m until next open")
                session_state  = {}
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

            run_pull(conn, token, session_state)
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
