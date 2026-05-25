"""
Black-Scholes Greeks — pure math module.

Shared between collector.py and the dashboard (intentional duplicate).
No I/O, no API calls, no database access.
"""

import numpy as np
import pandas as pd
from scipy.stats import norm

RISK_FREE  = 0.045
STRIKE_PCT = 0.12

DTE_BUCKETS = [
    (0,   0,   "0DTE"),
    (1,   7,   "1-7DTE"),
    (8,   30,  "8-30DTE"),
    (31,  90,  "31-90DTE"),
    (91,  180, "91-180DTE"),
    (181, 999, "180+DTE"),
]

# ── Black-Scholes ──────────────────────────────────────────────────────────────

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

# ── DTE bucketing ──────────────────────────────────────────────────────────────

def get_dte_bucket(dte: float) -> str:
    for lo, hi, label in DTE_BUCKETS:
        if lo <= dte <= hi:
            return label
    return "180+DTE"

# ── Chain parsing ──────────────────────────────────────────────────────────────

def parse_chain(chain: dict,
                r: float = RISK_FREE,
                strike_pct: float = STRIKE_PCT) -> pd.DataFrame:
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
            T = max(dte, 0.5) / 365
            bucket = get_dte_bucket(dte)
            for ks, contracts in strikes.items():
                K = float(ks)
                if abs(K - S) / S > strike_pct: continue
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

    call_oi = (df[df["type"] == "call"]
               .groupby(["strike", "dte"])[["oi", "volume"]]
               .sum()
               .rename(columns={"oi": "call_oi", "volume": "call_volume"})
               .reset_index())
    put_oi  = (df[df["type"] == "put"]
               .groupby(["strike", "dte"])[["oi", "volume"]]
               .sum()
               .rename(columns={"oi": "put_oi", "volume": "put_volume"})
               .reset_index())

    return (a.merge(iv_pivot[["strike", "dte", "iv_call", "iv_put"]],
                    on=["strike", "dte"], how="left")
              .merge(call_oi, on=["strike", "dte"], how="left")
              .merge(put_oi,  on=["strike", "dte"], how="left")
              .fillna({"call_oi": 0, "put_oi": 0, "call_volume": 0, "put_volume": 0}))

# ── Structural levels ──────────────────────────────────────────────────────────

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


def calc_atm_iv(df: pd.DataFrame, spot: float,
                target_dte: float = 30.0) -> float | None:
    calls = df[df["type"] == "call"].copy()
    if calls.empty:
        return None
    # Pick the expiry closest to target_dte (default 30-day front month),
    # then find the ATM strike within that expiry.
    unique_dtes = calls["dte"].unique()
    best_dte    = unique_dtes[int((abs(unique_dtes - target_dte)).argmin())]
    front       = calls[calls["dte"] == best_dte].copy()
    front["dist"] = (front["strike"] - spot).abs()
    closest = front.nsmallest(1, "dist")
    return float(closest["iv"].values[0]) if not closest.empty else None


def calc_iv_skew(df: pd.DataFrame, spot: float,
                 target_dte: float = 30.0,
                 band: float = 0.05) -> float | None:
    """
    Simplified risk-reversal skew at the expiry nearest to target_dte.
    OTM put IV (band/2 to band below spot) minus OTM call IV (band/2 to band above spot).
    Positive = put vol premium (normal). Near-zero or negative = unusual.
    """
    if df.empty:
        return None
    unique_dtes = df["dte"].unique()
    best_dte    = unique_dtes[int(np.abs(unique_dtes - target_dte).argmin())]
    exp         = df[df["dte"] == best_dte]

    otm_puts  = exp[(exp["type"] == "put") &
                    (exp["strike"] >= spot * (1 - band)) &
                    (exp["strike"] <= spot * (1 - band / 2))]
    otm_calls = exp[(exp["type"] == "call") &
                    (exp["strike"] >= spot * (1 + band / 2)) &
                    (exp["strike"] <= spot * (1 + band))]

    if otm_puts.empty or otm_calls.empty:
        return None
    return float(otm_puts["iv"].mean() - otm_calls["iv"].mean())


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
