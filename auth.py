"""
auth.py — Schwab OAuth2 Authentication Module
=============================================
Handles the full 3-legged OAuth2 flow for the Charles Schwab API.

First run:
  - Opens your browser to Schwab's login page
  - You log in with your Schwab BROKERAGE credentials (not developer portal)
  - Schwab redirects to a blank page — you paste that full URL back here
  - Tokens are saved to tokens.json for future runs

Subsequent runs:
  - Reads tokens.json automatically
  - Refreshes access token silently (expires every 30 min)
  - Refresh token lasts 7 days — re-run login flow if it expires
"""

import os
import json
import time
import base64
import webbrowser
import requests
from dotenv import load_dotenv

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────────
CLIENT_ID     = os.environ.get("SCHWAB_CLIENT_ID")
CLIENT_SECRET = os.environ.get("SCHWAB_CLIENT_SECRET")
REDIRECT_URI  = os.environ.get("SCHWAB_REDIRECT_URI", "https://127.0.0.1")
TOKENS_FILE   = "tokens.json"

AUTH_URL      = "https://api.schwabapi.com/v1/oauth/authorize"
TOKEN_URL     = "https://api.schwabapi.com/v1/oauth/token"

# ── Helpers ────────────────────────────────────────────────────────────────────

def _basic_header() -> str:
    credentials = f"{CLIENT_ID}:{CLIENT_SECRET}"
    encoded     = base64.b64encode(credentials.encode()).decode()
    return f"Basic {encoded}"


def _save_tokens(token_data: dict):
    token_data["saved_at"] = time.time()
    with open(TOKENS_FILE, "w") as f:
        json.dump(token_data, f, indent=2)
    print(f"Tokens saved to {TOKENS_FILE}")


def _load_tokens() -> dict | None:
    if not os.path.exists(TOKENS_FILE):
        return None
    with open(TOKENS_FILE, "r") as f:
        return json.load(f)


def _is_access_token_expired(tokens: dict) -> bool:
    saved_at   = tokens.get("saved_at", 0)
    expires_in = tokens.get("expires_in", 1800)
    elapsed    = time.time() - saved_at
    return elapsed >= (expires_in - 60)


def _is_refresh_token_expired(tokens: dict) -> bool:
    saved_at   = tokens.get("saved_at", 0)
    elapsed    = time.time() - saved_at
    return elapsed >= (604800 - 3600)


def get_authorization_url() -> str:
    params = (
        f"response_type=code"
        f"&client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        f"&scope=readonly"
    )
    return f"{AUTH_URL}?{params}"


def exchange_code_for_tokens(authorization_code: str) -> dict:
    headers = {
        "Authorization": _basic_header(),
        "Content-Type":  "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type":   "authorization_code",
        "code":         authorization_code,
        "redirect_uri": REDIRECT_URI,
    }
    resp = requests.post(TOKEN_URL, headers=headers, data=data, timeout=15)
    if resp.status_code != 200:
        print(f"    Token exchange failed: {resp.status_code}")
        print(f"    Response: {resp.text}")
        raise Exception("Failed to exchange authorization code for tokens.")
    tokens = resp.json()
    _save_tokens(tokens)
    return tokens


def refresh_access_token(refresh_token: str) -> dict:
    headers = {
        "Authorization": _basic_header(),
        "Content-Type":  "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type":    "refresh_token",
        "refresh_token": refresh_token,
    }
    resp = requests.post(TOKEN_URL, headers=headers, data=data, timeout=15)
    if resp.status_code != 200:
        print(f"    Token refresh failed: {resp.status_code}")
        print(f"    Response: {resp.text}")
        raise Exception("Failed to refresh access token. Re-run login flow.")
    tokens = resp.json()
    _save_tokens(tokens)
    print("Access token refreshed successfully.")
    return tokens


def get_valid_access_token(silent=False) -> str:
    """
    Returns a valid access token. 
    If silent=True, suppresses print statements (useful for background threads).
    """
    if not CLIENT_ID or not CLIENT_SECRET:
        raise EnvironmentError("SCHWAB_CLIENT_ID or SCHWAB_CLIENT_SECRET not found in .env")

    tokens = _load_tokens()

    if tokens:
        # Check if refresh token is dead (7 days)
        if _is_refresh_token_expired(tokens):
            if not silent: print("Refresh token expired (>7 days). Re-authenticating...")
            return _run_login_flow()
            
        # Check if access token is still good (30 mins)
        if not _is_access_token_expired(tokens):
            if not silent:
                expiry = tokens.get("saved_at", 0) + tokens.get("expires_in", 1800)
                mins = int((expiry - time.time()) / 60)
                print(f"Access token valid — expires in ~{mins} minutes.")
            return tokens["access_token"]
            
        # Access token expired, but refresh token is still good
        if not silent: print("Access token expired — refreshing...")
        try:
            new_tokens = refresh_access_token(tokens["refresh_token"])
            return new_tokens["access_token"]
        except Exception as e:
            if not silent: print(f"Refresh failed: {e}")
            return _run_login_flow()

    if not silent: print("No tokens found — starting login flow...")
    return _run_login_flow()


def _run_login_flow() -> str:
    auth_url = get_authorization_url()

    print("\n" + "═" * 60)
    print("  SCHWAB AUTHENTICATION")
    print("═" * 60)
    print("Opening Schwab login page in your browser...")
    print("  → Log in with your SCHWAB BROKERAGE credentials")
    print("    (Not your Developer Portal credentials)")
    print("After logging in, Schwab will redirect you to a")
    print("  blank page. The URL will look like:")
    print("  https://127.0.0.1/?code=LONG_CODE_HERE&session=...")
    print("Copy that ENTIRE URL and paste it below.")
    print("═" * 60 + "\n")

    try:
        webbrowser.open(auth_url)
        print("Browser opened automatically.")
    except Exception:
        print("Could not open browser automatically.")
        print(f"Manually open this URL:\n  {auth_url}\n")

    print()
    redirected_url = input("  Paste the full redirect URL here and press Enter:\n  > ").strip()

    try:
        from urllib.parse import urlparse, parse_qs, unquote
        parsed    = urlparse(redirected_url)
        params    = parse_qs(parsed.query)
        auth_code = unquote(params["code"][0])
    except (IndexError, KeyError):
        raise ValueError(
            "Could not extract authorization code from URL.\n"
            "Make sure you pasted the complete redirect URL."
        )

    print(f"Authorization code extracted.")
    print("Exchanging code for tokens...\n")

    tokens = exchange_code_for_tokens(auth_code)

    print("Authentication complete!")
    print(f"  Access token expires in: {tokens.get('expires_in', 1800) // 60} minutes")
    print("  Refresh token lasts: 7 days")
    print("  Future runs will authenticate automatically.\n")

    return tokens["access_token"]


if __name__ == "__main__":
    print("Testing Schwab authentication...\n")
    try:
        token = get_valid_access_token()
        print(f"Got access token: {token[:20]}...{token[-10:]}")
        print("\nAuthentication working correctly.")
        print("You can now run options_greeks_analyzer.py")
    except Exception as e:
        print(f"Authentication failed: {e}")