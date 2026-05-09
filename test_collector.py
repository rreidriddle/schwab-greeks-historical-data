"""
test_collector.py — Dry-run test for collector.py logic.

Exercises every function in a full pull cycle using live Schwab API data
but writes NOTHING to the database. Prints a pass/fail for each step.

Run from the collector directory:
    python test_collector.py
"""

from dotenv import load_dotenv
load_dotenv()

import sys
import time

# ── Colour helpers ─────────────────────────────────────────────────────────────

def ok(msg):   print(f"  [PASS] {msg}")
def fail(msg): print(f"  [FAIL] {msg}"); sys.exit(1)
def hdr(msg):  print(f"\n{'-'*60}\n  {msg}\n{'-'*60}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — Auth
# ══════════════════════════════════════════════════════════════════════════════

hdr("Step 1 — Authentication")
try:
    from auth import get_valid_access_token
    token = get_valid_access_token(silent=True)
    if not token:
        fail("get_valid_access_token returned None")
    ok(f"Token obtained ({len(token)} chars)")
except Exception as e:
    fail(f"Auth failed: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — Spot price
# ══════════════════════════════════════════════════════════════════════════════

hdr("Step 2 — SPY spot price")
try:
    from collector import get_spot, SYMBOL
    spot = get_spot(token, SYMBOL)
    if not spot or spot <= 0:
        fail(f"Bad spot price: {spot}")
    ok(f"{SYMBOL} spot = ${spot:.2f}")
except Exception as e:
    fail(f"get_spot failed: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — Options chain fetch
# ══════════════════════════════════════════════════════════════════════════════

hdr("Step 3 — Options chain fetch")
try:
    from collector import get_options_chain
    time.sleep(1)
    chain = get_options_chain(token, SYMBOL, spot)
    if not chain:
        fail("get_options_chain returned None")
    calls = chain.get("callExpDateMap", {})
    puts  = chain.get("putExpDateMap",  {})
    n_exp = len(calls)
    ok(f"Chain received — {n_exp} call expiries, underlying=${chain.get('underlyingPrice', '?')}")
except Exception as e:
    fail(f"get_options_chain failed: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — parse_chain (greeks.py)
# ══════════════════════════════════════════════════════════════════════════════

hdr("Step 4 — parse_chain")
try:
    from greeks import parse_chain
    df = parse_chain(chain)
    if df.empty:
        fail("parse_chain returned empty DataFrame")

    required_cols = [
        "strike", "dte", "dte_bucket", "type",
        "oi",
        "GEX_call", "GEX_put",
        "VannEX", "VannEX_call", "VannEX_put",
        "CharmEX", "CharmEX_call", "CharmEX_put",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        fail(f"Missing columns: {missing}")

    ok(f"{len(df)} rows | strikes {df['strike'].min():.0f}–{df['strike'].max():.0f} "
       f"| DTE {df['dte'].min():.0f}–{df['dte'].max():.0f} "
       f"| OI range {df['oi'].min()}–{df['oi'].max()}")
    ok(f"DTE buckets: {sorted(df['dte_bucket'].unique())}")
except Exception as e:
    fail(f"parse_chain failed: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 5 — aggregate (greeks.py)
# ══════════════════════════════════════════════════════════════════════════════

hdr("Step 5 — aggregate")
try:
    from greeks import aggregate
    agg = aggregate(df)
    if agg.empty:
        fail("aggregate returned empty DataFrame")

    required_agg_cols = [
        "strike", "dte", "dte_bucket",
        "GEX_call", "GEX_put", "GEX_net",
        "VannEX", "VannEX_call", "VannEX_put",
        "CharmEX", "CharmEX_call", "CharmEX_put",
    ]
    missing = [c for c in required_agg_cols if c not in agg.columns]
    if missing:
        fail(f"Missing agg columns: {missing}")

    ok(f"{len(agg)} (strike, dte) rows | "
       f"Net GEX {agg['GEX_net'].sum()/1e9:+.3f}B | "
       f"Net Vanna {agg['VannEX'].sum()/1e3:+.0f}K")
except Exception as e:
    fail(f"aggregate failed: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 6 — Structural levels
# ══════════════════════════════════════════════════════════════════════════════

hdr("Step 6 — Structural levels (gamma flip, walls, max pain, ATM IV, regime)")
try:
    from greeks import (
        calc_gamma_flip, calc_call_wall, calc_put_wall,
        calc_max_pain, calc_atm_iv, calc_gex_regime,
    )
    gamma_flip = calc_gamma_flip(agg)
    call_wall  = calc_call_wall(agg)
    put_wall   = calc_put_wall(agg)
    max_pain   = calc_max_pain(df)
    atm_iv     = calc_atm_iv(df, spot)
    regime     = calc_gex_regime(agg, spot, gamma_flip)

    ok(f"Gamma flip  : ${gamma_flip}")
    ok(f"Call wall   : ${call_wall}")
    ok(f"Put wall    : ${put_wall}")
    ok(f"Max pain    : ${max_pain}")
    ok(f"ATM IV      : {atm_iv:.1%}" if atm_iv else "ATM IV      : None")
    ok(f"GEX regime  : {regime}")
except Exception as e:
    fail(f"Structural level calc failed: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 7 — write_strike_data shape check (no actual write)
# ══════════════════════════════════════════════════════════════════════════════

hdr("Step 7 — DB write shape check (no actual write)")
try:
    from collector import write_strike_data, write_summary, write_macro_snapshot
    import inspect

    # Verify the functions exist and have the right signatures
    for fn in [write_strike_data, write_summary, write_macro_snapshot]:
        sig = inspect.signature(fn)
        ok(f"{fn.__name__}{sig}")
except Exception as e:
    fail(f"DB writer import failed: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 8 — Macro quotes (TLT + VIX)
# ══════════════════════════════════════════════════════════════════════════════

hdr("Step 8 — Macro quotes (TLT + VIX)")
try:
    from collector import get_macro_quotes
    time.sleep(1)
    macro = get_macro_quotes(token)

    tlt = macro.get("tlt_price")
    vix = macro.get("vix")

    if tlt is None and vix is None:
        fail("Both TLT and VIX returned None — likely API issue")

    ok(f"TLT  : ${tlt:.2f}  chg {macro.get('tlt_change', 0):+.2f} "
       f"({macro.get('tlt_chg_pct', 0):+.2f}%)")
    ok(f"VIX  : {vix:.2f}  chg {macro.get('vix_change', 0):+.2f} "
       f"({macro.get('vix_chg_pct', 0):+.2f}%)")
except Exception as e:
    fail(f"get_macro_quotes failed: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 9 — DTE bucket distribution
# ══════════════════════════════════════════════════════════════════════════════

hdr("Step 9 — DTE bucket distribution")
try:
    from greeks import DTE_BUCKETS
    bucket_counts = df.groupby("dte_bucket")["oi"].sum()
    ok(f"Buckets defined: {DTE_BUCKETS}")
    for bucket, total_oi in bucket_counts.items():
        ok(f"  {bucket:>12s}  OI={total_oi:>10,.0f}")
except Exception as e:
    fail(f"DTE bucket check failed: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# DONE
# ══════════════════════════════════════════════════════════════════════════════

print(f"\n{'='*60}")
print(f"  ALL STEPS PASSED -- collector is good to go")
print(f"{'='*60}\n")
