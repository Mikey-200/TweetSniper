"""
╔══════════════════════════════════════════════════════════════════════╗
║                TweetSniper — Polymarket Elon Tweet Bot               ║
║          Automated trading of Elon Musk tweet-count markets          ║
╚══════════════════════════════════════════════════════════════════════╝

Single-file production bot. Start with: python bot.py

Authentication: email/Magic.link wallet (signature_type=1).
   - POLYGON_PRIVATE_KEY  = private key exported from Polymarket
   - PROXY_WALLET_ADDRESS = your Polymarket proxy wallet address
     (found on polymarket.com → Deposit dialog)

If you switch to MetaMask/EOA: set signature_type=0, remove funder=.
If you use browser proxy contract wallet: set signature_type=2.
"""

# ──────────────────────────────────────────────────────────────────────
# SECTION 1 — IMPORTS
# ──────────────────────────────────────────────────────────────────────
import asyncio
import csv
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx
import subprocess
import websockets
from dotenv import load_dotenv
from web3 import Web3

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    OrderArgs, OrderType, BalanceAllowanceParams,
    TradeParams, OpenOrderParams, BookParams,
)
from py_clob_client.order_builder.constants import BUY, SELL

load_dotenv()

# ──────────────────────────────────────────────────────────────────────
# SECTION 2 — LOGGING
# ──────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("tweetsniper.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("TweetSniper")

# ──────────────────────────────────────────────────────────────────────
# SECTION 3 — CONFIGURATION (env vars + defaults)
# ──────────────────────────────────────────────────────────────────────

# === Wallet / Auth ===
PRIVATE_KEY        = os.getenv("POLYGON_PRIVATE_KEY", "")
PROXY_WALLET       = os.getenv("PROXY_WALLET_ADDRESS", "")
ALCHEMY_RPC_URL    = os.getenv("ALCHEMY_RPC_URL", "")

# === Telegram ===
TG_BOT_TOKEN       = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID         = int(os.getenv("TG_CHAT_ID", "0"))

# === Timing ===
MARKET_POLL_SECS       = int(os.getenv("MARKET_POLL_SECS", "60"))
TP_BACKUP_POLL_SECS    = int(os.getenv("TP_BACKUP_POLL_SECS", "300"))
FILL_ALERT_SECS        = int(os.getenv("FILL_ALERT_SECS", "120"))
STALE_CANCEL_SECS      = int(os.getenv("STALE_CANCEL_SECS", "1200"))
MARKET_AGE_MINUTES     = int(os.getenv("MARKET_AGE_MINUTES", "60"))
DAILY_SUMMARY_UTC_HOUR = int(os.getenv("DAILY_SUMMARY_UTC_HOUR", "20"))
ONGOING_RESCAN_SECS    = int(os.getenv("ONGOING_RESCAN_SECS", "600"))

# === Market Scanning ===
SCAN_ONGOING_MARKETS   = os.getenv("SCAN_ONGOING_MARKETS", "true").lower() == "true"
ELON_KEYWORD           = os.getenv("ELON_KEYWORD", "elon").lower()
# Real market questions from CLI output say "post" AND "tweet" — include both
# Example: "Will Elon Musk post 60-79 tweets from April 10 to April 17, 2026?"
ELON_TWEET_KEYWORDS    = {"elon", "elonmusk", "@elonmusk"}  # question must match at least one
TWEET_COUNT_KEYWORDS   = {"tweet", "post", "times"}          # AND at least one of these

# === Order Parameters ===
ORDER_SIZE_USD         = float(os.getenv("ORDER_SIZE_USD", "1.0"))
MIN_BUY_PRICE          = float(os.getenv("MIN_BUY_PRICE", "0.01"))   # skip tiny/garbage buckets
MAX_BUY_PRICE          = float(os.getenv("MAX_BUY_PRICE", "0.30"))
MAX_BUY_PRICE_ONGOING  = float(os.getenv("MAX_BUY_PRICE_ONGOING", "0.20"))
MAX_SPREAD             = float(os.getenv("MAX_SPREAD", "0.25"))
EMPTY_BOOK_RETRIES     = int(os.getenv("EMPTY_BOOK_RETRIES", "6"))
FALLBACK_GTC_PRICE     = float(os.getenv("FALLBACK_GTC_PRICE", "0.25"))
BUCKETS_TO_BUY         = int(os.getenv("BUCKETS_TO_BUY", "4"))
SKIP_MARGIN_MULTIPLIER = float(os.getenv("SKIP_MARGIN_MULTIPLIER", "1.5"))

# === Position Management ===
MAX_MARKETS_PER_CYCLE  = int(os.getenv("MAX_MARKETS_PER_CYCLE",  "2"))   # max markets to enter per scan cycle
STOP_LOSS_PCT          = float(os.getenv("STOP_LOSS_PCT",         "0.60")) # cut loss when down 60% (price at 40% of entry)

# === Take-Profit Multipliers (per slot) ===
TP_SLOTS = [
    float(os.getenv("TP_SLOT_0", "2.0")),
    float(os.getenv("TP_SLOT_1", "2.0")),
    float(os.getenv("TP_SLOT_2", "2.0")),
    float(os.getenv("TP_SLOT_3", "2.0")),
]

# === Dry Run ===
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

# === API URLs ===
CLOB_HOST        = "https://clob.polymarket.com"
GAMMA_API        = "https://gamma-api.polymarket.com"
XTRACKER_API     = "https://xtracker.polymarket.com/api"
WS_URL           = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
POLYMARKET_BASE  = "https://polymarket.com"

# === On-Chain ===
USDC_ADDRESS       = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # USDC.e (bridged)
USDC_NATIVE_ADDRESS= "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"  # native USDC on Polygon
USDC_DECIMALS      = 6
ERC20_ABI = [
    {
        "name": "transfer",
        "type": "function",
        "inputs": [
            {"name": "recipient", "type": "address"},
            {"name": "amount",    "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "nonpayable",
    },
    {
        "name": "balanceOf",
        "type": "function",
        "inputs": [{"name": "account", "type": "address"}],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
    },
]

# === Trade CSV ===
CSV_FILE = "trades.csv"
CSV_COLUMNS = [
    "session_num", "timestamp_utc", "market_question", "bucket", "slot",
    "buy_price", "size_shares", "cost_usd", "tp_target", "tp_mult",
    "buy_order_id", "buy_status", "sell_price", "sell_order_id",
    "profit_usd", "profit_pct", "spread_at_entry", "is_fallback_gtc",
]

# ──────────────────────────────────────────────────────────────────────
# SECTION 4 — IN-MEMORY STATE
# ──────────────────────────────────────────────────────────────────────

# Markets already seen/dispatched this session (by market_id)
seen_market_ids: set = set()

# Markets where we've already placed orders (key = question[:30])
open_positions_by_market: dict = {}

# All open positions awaiting TP or fill: {order_id → position_dict}
open_positions: dict = {}

# Session order registry: {session_num (int) → order_id (str)}
order_registry: dict = {}

# Sequential session counter
session_counter = 0

# Session P&L accumulators
pnl_summary = {
    "total_invested": 0.0,
    "total_returned": 0.0,
    "trades_placed":  0,
    "trades_closed":  0,
    "wins": 0,
    "losses": 0,
}

# For daily summary scheduling
_last_summary_day: Optional[int] = None

# ──────────────────────────────────────────────────────────────────────
# SECTION 5 — CLOB CLIENT SETUP
# ──────────────────────────────────────────────────────────────────────

def _build_clob_client() -> Optional[ClobClient]:
    """Build and authenticate the Polymarket CLOB client.

    Uses signature_type=2 for Polymarket proxy wallets.
    This is the "proxy" signature type used by ALL email/social login
    accounts on Polymarket. The polymarket-cli source confirms:
      "proxy" → SignatureType::Proxy → signature_type=2 in py-clob-client

    Your exported private key IS the signing key; PROXY_WALLET is the
    separate proxy smart contract wallet that holds your USDC balance.
    """
    if not PRIVATE_KEY or not PROXY_WALLET:
        log.warning("POLYGON_PRIVATE_KEY or PROXY_WALLET_ADDRESS not set — "
                    "trading disabled, read-only mode.")
        return None
    try:
        c = ClobClient(
            host=CLOB_HOST,
            key=PRIVATE_KEY,
            chain_id=137,            # Polygon Mainnet
            signature_type=2,        # proxy wallet (email/social login accounts)
            funder=PROXY_WALLET,     # proxy wallet address that holds your USDC
        )
        creds = c.create_or_derive_api_creds()
        c.set_api_creds(creds)
        log.info("CLOB client authenticated ✓ (proxy: %s…)", PROXY_WALLET[:10])
        return c
    except Exception as e:
        log.error("Failed to build CLOB client: %s", e)
        return None


clob: Optional[ClobClient] = _build_clob_client()


async def run_clob(fn, *args, **kwargs):
    """Run a synchronous CLOB call in a thread executor to avoid blocking."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))


# ──────────────────────────────────────────────────────────────────────
# SECTION 6 — BALANCE HELPERS
# ──────────────────────────────────────────────────────────────────────

async def get_proxy_balance() -> float:
    """Get USDC trading balance from Polymarket's CLOB API.

    Uses get_balance_allowance() — the correct method name in py-clob-client.
    Returns the 'balance' or 'allowance' field depending on what the API returns.
    """
    if clob is None:
        return 0.0
    try:
        params = BalanceAllowanceParams(signature_type=2)  # proxy wallet type
        raw = await run_clob(clob.get_balance_allowance, params)
        if isinstance(raw, dict):
            # Try common field names returned by the API
            for key in ("balance", "allowance", "USDC", "usdc"):
                if key in raw and raw[key] is not None:
                    return float(raw[key])
            log.warning("get_balance_allowance keys: %s", list(raw.keys()))
        elif isinstance(raw, (int, float)):
            return float(raw)
        elif isinstance(raw, str):
            try:
                return float(raw)
            except ValueError:
                pass
        log.warning("Unknown get_balance_allowance format: %s — %s", type(raw), str(raw)[:120])
        return 0.0
    except Exception as e:
        log.error("get_proxy_balance error: %s", e)
        return 0.0


async def get_eoa_usdc_balance() -> float:
    """Get USDC balance (USDC.e + native) at both signing key AND proxy wallet addresses."""
    if not ALCHEMY_RPC_URL or not PRIVATE_KEY:
        return 0.0
    try:
        w3 = Web3(Web3.HTTPProvider(ALCHEMY_RPC_URL))
        from eth_account import Account
        acct = Account.from_key(PRIVATE_KEY)
        # Check both the EOA (signing key) and the proxy wallet address
        addresses_to_check = [acct.address]
        if PROXY_WALLET and PROXY_WALLET.lower() != acct.address.lower():
            addresses_to_check.append(Web3.to_checksum_address(PROXY_WALLET))
        total = 0.0
        for addr in addresses_to_check:
            for token_addr in (USDC_ADDRESS, USDC_NATIVE_ADDRESS):
                usdc = w3.eth.contract(
                    address=Web3.to_checksum_address(token_addr), abi=ERC20_ABI
                )
                raw = usdc.functions.balanceOf(addr).call()
                total += raw / (10 ** USDC_DECIMALS)
        return total
    except Exception as e:
        log.error("get_eoa_usdc_balance error: %s", e)
        return 0.0


# ──────────────────────────────────────────────────────────────────────
# SECTION 7 — XTRACKER INTEGRATION
# ──────────────────────────────────────────────────────────────────────

_xtracker_cache: Optional[list] = None   # all trackings (raw, no stats)
_xtracker_cache_ts: float = 0.0           # unix timestamp of last fetch
_XTRACKER_CACHE_TTL = 600                 # refresh every 10 minutes


async def _load_xtracker_trackings() -> list:
    """Load all XTracker tracking periods for @elonmusk, with 10-min cache.

    The API returns a flat list with id, title, startDate, endDate, marketLink.
    marketLink is the key field — it contains the Polymarket event URL:
      "https://polymarket.com/event/elon-musk-of-tweets-april-10-april-17"
    We use this to match each Polymarket market to its EXACT tracking period.
    """
    global _xtracker_cache, _xtracker_cache_ts
    now = time.time()
    if _xtracker_cache is not None and (now - _xtracker_cache_ts) < _XTRACKER_CACHE_TTL:
        return _xtracker_cache

    try:
        async with httpx.AsyncClient(timeout=15) as http:
            r = await http.get(
                f"{XTRACKER_API}/users/elonmusk/trackings",
                params={"platform": "X"},
            )
            r.raise_for_status()
            body = r.json()
            trackings = body.get("data", body) if isinstance(body, dict) else body
            if isinstance(trackings, list):
                _xtracker_cache = trackings
                _xtracker_cache_ts = now
                log.info("XTracker: loaded %d tracking periods", len(trackings))
                return trackings
    except Exception as e:
        log.error("XTracker load_trackings error: %s", e)

    return _xtracker_cache or []


async def fetch_elon_pace(market_slug: str = "") -> Optional[dict]:
    """Fetch Elon's tweet pace for a SPECIFIC market period.

    CRITICAL FIX: The old code picked ONE "current" tracking and used it
    for ALL markets. This was wrong — when the bot evaluated the Apr 10-17
    market it was using the Apr 14-21 tracking's stats (87 tweets, 5d left)
    instead of the correct Apr 10-17 stats (283 tweets, 1d left).

    NEW APPROACH:
    1. Load all trackings (cached 10 min)
    2. Match by marketLink slug first (exact match)
    3. Fall back to date-range overlap if no slug match
    4. Fetch per-tracking stats from /trackings/{id}?includeStats=true

    Args:
        market_slug: The Polymarket event slug, e.g.
                     "elon-musk-of-tweets-april-10-april-17"
                     Used to find the exact XTracker period for this market.
    """
    try:
        trackings = await _load_xtracker_trackings()
        if not trackings:
            return None

        tracking = None

        # ── Step 1: match by marketLink slug (most reliable) ─────────────
        if market_slug:
            for t in trackings:
                link = t.get("marketLink", "") or ""
                # link = "https://polymarket.com/event/elon-musk-of-tweets-april-10-april-17"
                # slug = "elon-musk-of-tweets-april-10-april-17"
                if link.rstrip("/").endswith(market_slug):
                    tracking = t
                    log.debug("XTracker: slug match '%s' → tracking '%s'",
                              market_slug, t.get("title", ""))
                    break

        # ── Step 2: fall back to date-range overlap ───────────────────────
        if tracking is None:
            now_utc = datetime.now(timezone.utc)
            for t in trackings:
                try:
                    start = datetime.fromisoformat(
                        t.get("startDate", "").replace("Z", "+00:00"))
                    end = datetime.fromisoformat(
                        t.get("endDate", "").replace("Z", "+00:00"))
                    if start <= now_utc <= end:
                        tracking = t
                        log.debug("XTracker: date-range match → tracking '%s'",
                                  t.get("title", ""))
                        break
                except Exception:
                    continue

        # ── Step 3: last resort — most recently started ───────────────────
        if tracking is None:
            now_utc = datetime.now(timezone.utc)
            started = [
                t for t in trackings
                if t.get("startDate", "") <= now_utc.isoformat()
            ]
            if started:
                tracking = sorted(started,
                                  key=lambda t: t.get("startDate", ""),
                                  reverse=True)[0]
                log.warning("XTracker: using most-recent fallback '%s'",
                            tracking.get("title", ""))

        if tracking is None:
            log.warning("XTracker: no matching tracking found for slug='%s'",
                        market_slug)
            return None

        tracking_id = tracking["id"]
        log.info("XTracker: fetching stats for '%s' (id=%s)",
                 tracking.get("title", ""), tracking_id)

        # ── Step 4: fetch stats for this specific tracking ────────────────
        async with httpx.AsyncClient(timeout=15) as http:
            r2 = await http.get(
                f"{XTRACKER_API}/trackings/{tracking_id}",
                params={"includeStats": "true"},
            )
            r2.raise_for_status()
            body2 = r2.json()

        data  = body2.get("data", body2) if isinstance(body2, dict) else body2
        stats = data.get("stats", {})

        # API fields verified from live response:
        #   stats.total          → cumulative tweet count so far
        #   stats.daysElapsed    → coarse integer days (NOT precise enough)
        #   stats.daysRemaining  → also coarse — only whole days!
        #   stats.percentComplete → 0-100
        #
        # CRITICAL: XTracker reports daysRemaining=1 for a market that may
        # only have 14 hours left (e.g. if it closes at 16:00 UTC today).
        # We MUST compute real hours from endDate/startDate timestamps.
        total        = float(stats.get("total", 0))
        pct_complete = float(stats.get("percentComplete", 0))
        start_date   = data.get("startDate", tracking.get("startDate", ""))
        end_date     = data.get("endDate",   tracking.get("endDate",   ""))

        # ── Compute precise elapsed / remaining in HOURS ─────────────────
        now_utc = datetime.now(timezone.utc)
        try:
            start_dt = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
            end_dt   = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
            hours_elapsed   = max(0.01, (now_utc - start_dt).total_seconds() / 3600)
            hours_remaining = max(0.0,  (end_dt   - now_utc).total_seconds() / 3600)
        except Exception:
            # Fall back to XTracker's coarse day counts
            hours_elapsed   = float(stats.get("daysElapsed",   1)) * 24
            hours_remaining = float(stats.get("daysRemaining", 0)) * 24

        # Hourly rate (more accurate near end-of-market than daily rate)
        hourly_avg = total / hours_elapsed
        daily_avg  = hourly_avg * 24   # for display only

        # Early-period bias (sparse data in first 2h)
        bias      = 0.5 * hourly_avg * 24 if hours_elapsed < 48 else 0.0
        projected = total + (hourly_avg * hours_remaining) + bias

        if   projected < 200:  tier = "🟢 Low"
        elif projected < 400:  tier = "🟡 Medium"
        elif projected < 600:  tier = "🟠 High"
        else:                  tier = "🔴 Very High"

        log.info(
            "XTracker: total=%d  elapsed=%.1fh  remaining=%.1fh  "
            "rate=%.2f/h  projected=%d  pct=%.0f%%",
            total, hours_elapsed, hours_remaining,
            hourly_avg, projected, pct_complete,
        )

        return {
            "tracking_id":    tracking_id,
            "title":          tracking.get("title", ""),
            "start_date":     start_date,
            "end_date":       end_date,
            "total":          total,
            "hourly_avg":     hourly_avg,
            "daily_avg":      daily_avg,
            "hours_elapsed":  hours_elapsed,
            "hours_remaining": hours_remaining,
            # Legacy keys kept so existing code using days_* still works
            "days_elapsed":   hours_elapsed / 24,
            "days_remaining": hours_remaining / 24,
            "projected":      projected,
            "pct_complete":   pct_complete,
            "tier":           tier,
            "has_bias":       bias > 0,
        }

    except Exception as e:
        log.error("fetch_elon_pace error: %s", e)
        return None



# ──────────────────────────────────────────────────────────────────────

# SECTION 8 — BUCKET STRATEGY
# ──────────────────────────────────────────────────────────────────────

def parse_bucket_label(label: str) -> tuple:
    """Parse a bucket outcome label into (low_bound, high_bound) integers.

    Handles all formats observed on Polymarket:
      "220-239"    → (220, 239)
      "580+"       → (580, 9999)
      "320+"       → (320, 9999)
      "<20"        → (0, 19)
      "Under 100"  → (0, 99)
      "100-119"    → (100, 119)
      "Other"      → None (skip)
    """
    label = label.strip()

    # Range: "220-239"
    m = re.match(r"^(\d+)\s*[-–]\s*(\d+)$", label)
    if m:
        return int(m.group(1)), int(m.group(2))

    # Open-ended high: "580+" or "320+"
    m = re.match(r"^(\d+)\+$", label)
    if m:
        return int(m.group(1)), 9999

    # Less than: "<20" or "< 20"
    m = re.match(r"^<\s*(\d+)$", label)
    if m:
        return 0, int(m.group(1)) - 1

    # "Under 100"
    m = re.match(r"^[Uu]nder\s+(\d+)$", label)
    if m:
        return 0, int(m.group(1)) - 1

    # Plain number: "100"
    m = re.match(r"^(\d+)$", label)
    if m:
        v = int(m.group(1))
        return v, v

    return None  # unparseable — will be skipped


def select_buckets(tokens: list, pace: dict) -> list:
    """Apply the expert bucket selection strategy.

    Strategy (from Polymarket research by Terry Lee, March 2026):
    1. Parse all bucket labels into numeric ranges
    2. Skip bucket[0] (lowest range — Elon never tweets that little)
    3. Apply SKIP_MARGIN logic: skip bucket if projected > ceiling + width*SKIP_MARGIN
    4. Find CENTER bucket (range that contains projected_final)
    5. Select CENTER + up to (BUCKETS_TO_BUY-1) buckets above center
    6. Buy the CENTER bucket first (highest probability per research)

    Args:
        tokens: list of {token_id, outcome (label)} dicts from Gamma API
        pace: dict from fetch_elon_pace()

    Returns:
        list of (token_id, label, slot_index, low_bound, high_bound)
    """
    projected = pace["projected"]

    # Parse all tokens into (low, high, token_id, label)
    parsed = []
    for t in tokens:
        label = t.get("outcome", "")
        bounds = parse_bucket_label(label)
        if bounds is None:
            continue
        low, high = bounds
        parsed.append((low, high, t["token_id"], label))

    # Sort by lower bound ascending
    parsed.sort(key=lambda x: x[0])

    if not parsed:
        return []

    # Always skip bucket[0] (the lowest range — Elon's never that quiet)
    candidates = parsed[1:]

    # Apply SKIP_MARGIN logic
    # Skip a bucket if projected > bucket_ceiling + (bucket_width × SKIP_MARGIN)
    # This prevents buying buckets whose ceiling is well below projection
    filtered = []
    for low, high, token_id, label in candidates:
        if high == 9999:
            # Open-ended bucket (e.g. "580+") — never skip this
            filtered.append((low, high, token_id, label))
            continue
        bucket_width = high - low + 1
        skip_threshold = high + (bucket_width * SKIP_MARGIN_MULTIPLIER)
        if projected <= skip_threshold:
            filtered.append((low, high, token_id, label))
        else:
            log.debug("Skip bucket '%s': projected %.0f > threshold %.0f",
                      label, projected, skip_threshold)

    if not filtered:
        log.warning("All buckets skipped by SKIP_MARGIN logic — falling back to center")
        filtered = candidates  # fallback: use all candidates

    # Find CENTER bucket: the one whose range contains projected_final
    center_idx = None
    for i, (low, high, token_id, label) in enumerate(filtered):
        if low <= projected <= high or (high == 9999 and projected >= low):
            center_idx = i
            break

    # If no bucket contains projected exactly, find nearest above projected
    if center_idx is None:
        for i, (low, high, token_id, label) in enumerate(filtered):
            if low > projected:
                center_idx = i
                break

    # Fallback: take the highest bucket
    if center_idx is None:
        center_idx = len(filtered) - 1

    # Select center + up to (BUCKETS_TO_BUY-1) buckets above (upward bias)
    start = center_idx
    end   = min(center_idx + BUCKETS_TO_BUY, len(filtered))
    selected = filtered[start:end]

    # Build result with slot index
    result = []
    for slot_idx, (low, high, token_id, label) in enumerate(selected):
        result.append((token_id, label, slot_idx, low, high))

    return result


def describe_bucket_analysis(tokens: list, pace: dict) -> str:
    """Generate a human-readable bucket analysis for the pre-buy alert."""
    projected = pace["projected"]
    lines = []

    parsed = []
    for t in tokens:
        label = t.get("outcome", "")
        bounds = parse_bucket_label(label)
        if bounds is None:
            continue
        low, high = bounds
        parsed.append((low, high, t.get("price", 0.0), label))
    parsed.sort(key=lambda x: x[0])

    for i, (low, high, price_hint, label) in enumerate(parsed):
        if i == 0:
            lines.append(f"  ⛔ {label:15s}  [skipped — always too low]")
            continue
        if high == 9999:
            skip_threshold = None
        else:
            width = high - low + 1
            skip_threshold = high + (width * SKIP_MARGIN_MULTIPLIER)

        if skip_threshold is not None and projected > skip_threshold:
            lines.append(f"  ❌ {label:15s}  [skip: proj {projected:.0f} > thresh {skip_threshold:.0f}]")
        elif low <= projected <= high or (high == 9999 and projected >= low):
            lines.append(f"  🎯 {label:15s}  [CENTER — projection {projected:.0f} is here]")
        else:
            lines.append(f"  ✅ {label:15s}  [within range]")

    return "\n".join(lines[:20])  # cap UI length


# ──────────────────────────────────────────────────────────────────────
# SECTION 9 — MARKET FETCHING (Gamma API + CLI)
# ──────────────────────────────────────────────────────────────────────


async def fetch_elon_markets_cli() -> list:
    """Search for Elon tweet-count markets and GROUP them into multi-bucket events.

    WHY GROUPING IS NEEDED:
    Polymarket structures Elon tweet markets as INDIVIDUAL Yes/No markets:

      "Will Elon Musk post 540-559 tweets from April 10 to April 17?" -> Yes/No
      "Will Elon Musk post 560-579 tweets from April 10 to April 17?" -> Yes/No
      "Will Elon Musk post 580+ tweets from April 10 to April 17?"    -> Yes/No

    The strategy engine needs ALL BUCKETS together to pick the right one.
    We GROUP all individual markets for the same date period into one synthetic
    multi-bucket event dict, so process_market() works correctly.

    GROUPING LOGIC:
    Group key = endDate[:10]  (all bucket markets for same week end same day)
    Bucket label = parsed from question: "post 540-559 tweets" -> "540-559"
    YES token = clobTokenIds[0]  (first entry is always the YES token)
    YES price = outcomePrices[0]

    SYNTHETIC EVENT FORMAT (same as native multi-bucket market):
      id:            "event_2026-04-17"
      question:      "Elon Musk tweets: April 10 - April 17, 2026"
      endDate:       "2026-04-17T16:00:00Z"
      slug:          "elon-musk-of-tweets-april-10-april-17"
      clobTokenIds:  '["0xABC","0xDEF","0xGHI"]'  <- YES tokens per bucket
      outcomes:      '["540-559","560-579","580+"]' <- bucket labels
      outcomePrices: '["0.15","0.08","0.05"]'       <- YES prices

    WINDOWS ENCODING FIX:
    CLI outputs UTF-8 JSON. Windows subprocess defaults to cp1252.
    Must set encoding='utf-8' or crashes with UnicodeDecodeError 0x9d.

    TIMEOUT NOTE:
    CLI with --limit 50 takes >30s. Use --limit 20 with 45s timeout.
    """
    individual_markets: dict = {}  # market_id -> market dict

    # Verified: "elon musk post" returns current live markets.
    # "elon musk tweet" returns ONLY old closed markets.
    search_terms = ["elon musk post"]

    for term in search_terms:
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda t=term: subprocess.run(
                    ["polymarket", "-o", "json", "markets", "search",
                     t, "--limit", "20"],
                    capture_output=True,
                    text=True,
                    encoding="utf-8",   # MUST: CLI outputs UTF-8, not cp1252
                    errors="replace",   # don't crash on edge-case chars
                    timeout=45,         # CLI is slow; needs ~20-30s
                )
            )
            if not result.stdout.strip():
                log.warning("CLI returned empty for '%s' (exit=%d stderr=%s)",
                            term, result.returncode, result.stderr[:100])
                continue

            raw = result.stdout.strip().lstrip('\ufeff')  # strip BOM
            try:
                markets = json.loads(raw)
            except (json.JSONDecodeError, ValueError) as e:
                log.warning("CLI JSON parse error for '%s': %s | first200: %s",
                            term, e, raw[:200])
                continue

            if not isinstance(markets, list):
                continue

            for m in markets:
                mid = m.get("id", "")
                if not mid:
                    continue
                q = (m.get("question", "") or "").lower()
                if not any(kw in q for kw in ELON_TWEET_KEYWORDS):
                    continue
                if not re.search(r'\d', q):
                    continue
                if m.get("closed", True):        # skip resolved
                    continue
                if not m.get("acceptingOrders", False):  # must be open
                    continue
                individual_markets[mid] = m

        except Exception as e:
            log.warning("CLI search failed for '%s': %s", term, e)

    log.info("CLI: %d live individual Elon markets before grouping",
             len(individual_markets))
    if not individual_markets:
        return []

    # GROUP into synthetic multi-bucket events:
    # All bucket markets for one week share the same endDate.
    event_groups: dict = {}

    for m in individual_markets.values():
        q        = m.get("question", "")
        end_date = m.get("endDate", "")
        end_key  = end_date[:10]

        # Parse bucket label from question
        # "post 540-559 tweets" -> "540-559"
        # "post 580+ tweets"    -> "580+"
        # "have 0-19 tweets"    -> "0-19"
        bucket_match = re.search(
            r'(?:post|have)\s+([\d]+(?:[-\u2013][\d]+|\+)?)\s+tweets?',
            q, re.IGNORECASE
        )
        if not bucket_match:
            log.debug("Could not parse bucket from: %s", q[:70])
            continue
        bucket_label = bucket_match.group(1).replace('\u2013', '-')

        # YES token = clobTokenIds[0], YES price = outcomePrices[0]
        try:
            token_ids  = json.loads(m.get("clobTokenIds",  "[]") or "[]")
            prices_raw = json.loads(m.get("outcomePrices", "[]") or "[]")
            yes_token  = token_ids[0]  if token_ids  else None
            yes_price  = float(prices_raw[0]) if prices_raw else 0.0
        except Exception:
            continue
        if not yes_token:
            continue

        # Strip bucket suffix from slug to get parent event slug
        # "elon-musk-of-tweets-april-10-april-17-540-559"
        #   -> "elon-musk-of-tweets-april-10-april-17"
        slug = m.get("slug", "")
        slug_prefix = re.sub(r'-[\d]+(?:-[\d]+|plus)?$', '', slug)

        if end_key not in event_groups:
            date_match = re.search(
                r'from\s+(.+?)\s+to\s+(.+?),?\s*(\d{4})',
                q, re.IGNORECASE
            )
            if date_match:
                event_q = (f"Elon Musk tweets: {date_match.group(1).strip()} "
                           f"- {date_match.group(2).strip()}, "
                           f"{date_match.group(3)}")
            else:
                event_q = f"Elon Musk tweet count (week ending {end_key})"

            event_groups[end_key] = {
                "id":              f"event_{end_key}",
                "question":        event_q,
                "endDate":         end_date,
                "slug":            slug_prefix,
                "closed":          False,
                "acceptingOrders": True,
                "buckets":         [],
            }

        event_groups[end_key]["buckets"].append({
            "label":     bucket_label,
            "yes_token": yes_token,
            "yes_price": yes_price,
        })

    # Build synthetic market dicts compatible with process_market()
    result_events = []
    for end_key, event in event_groups.items():
        buckets = event.get("buckets", [])
        if not buckets:
            continue

        # Sort buckets by lower bound ascending
        def _parse_low(b: dict) -> int:
            m2 = re.match(r'^(\d+)', b["label"])
            return int(m2.group(1)) if m2 else 9999
        buckets.sort(key=_parse_low)

        labels     = [b["label"]     for b in buckets]
        yes_toks   = [b["yes_token"] for b in buckets]
        yes_prices = [str(b["yes_price"]) for b in buckets]

        synthetic = {
            "id":            event["id"],
            "question":      event["question"],
            "endDate":       event["endDate"],
            "slug":          event["slug"],
            "closed":        False,
            "acceptingOrders": True,
            # process_market() reads these three via json.loads():
            "clobTokenIds":  json.dumps(yes_toks),
            "outcomes":      json.dumps(labels),
            "outcomePrices": json.dumps(yes_prices),
        }
        result_events.append(synthetic)
        log.info("Event '%s' (%s): %d buckets [%s]",
                 event["question"][:45], end_key, len(buckets),
                 ", ".join(labels[:5]) + ("..." if len(labels) > 5 else ""))

    result_events.sort(key=lambda ev: ev.get("endDate", "9999"))
    log.info("CLI grouped %d event(s) from %d individual markets",
             len(result_events), len(individual_markets))
    return result_events



async def fetch_elon_markets(
    active_only: bool = True,
    max_age_minutes: Optional[int] = None,
) -> list:
    """Fetch Elon tweet-count markets from the Gamma API.

    VERIFIED against real Gamma API JSON from the installed polymarket-cli:

    Real question format (April 2026):
      "Will Elon Musk post 60-79 tweets from April 10 to April 17, 2026?"
      "Will Elon Musk post 200-219 tweets from April 10 to April 17, 2026?"

    Key field names (ACTUAL from live API, confirmed by CLI output):
      - question:        the market question (str)
      - active:          bool (always true even for closed -- UNRELIABLE, use acceptingOrders)
      - closed:          bool (resolved markets are closed=True)
      - acceptingOrders: bool — TRUE means market is live and tradeable
      - clobTokenIds:    JSON-encoded string: '["0xABC...","0xDEF..."]'
      - outcomes:        JSON-encoded string: '["Yes","No"]'  OR  '["60-79","80-99",...]'
      - outcomePrices:   JSON-encoded string: '["0.15","0.85"]'
      - endDate:         ISO datetime string (market resolution time)
      - createdAt:       ISO datetime string
      - slug:            URL slug for Polymarket link
      - tags:            null (tags are NOT populated in search results)

    FILTERING STRATEGY:
      - Match 'elon' OR '@elonmusk' in question (case-insensitive)
      - AND match 'tweet' OR 'post' OR 'times' in question
      - AND acceptingOrders=True (market is live)
      - AND closed=False (not resolved)

    Args:
        active_only:     Only return markets with acceptingOrders=True
        max_age_minutes: If set, only return markets created within this window
    """
    markets = []
    cursor = ""
    cutoff_dt = None
    if max_age_minutes is not None:
        cutoff_dt = datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)

    async with httpx.AsyncClient(timeout=20) as http:
        while True:
            # Gamma API parameters - verified from CLI source (commands/markets.rs)
            params = {
                "closed":  "false",   # exclude resolved markets
                "limit":   "100",
                "_order":  "createdAt",
                "_sort":   "DESC",
            }
            if cursor:
                params["next_cursor"] = cursor

            try:
                r = await http.get(f"{GAMMA_API}/markets", params=params)
                r.raise_for_status()
                body = r.json()
            except Exception as e:
                log.error("Gamma API fetch error: %s", e)
                break

            # Gamma API returns a plain JSON list — NOT a wrapped {data:[...]} response
            # Confirmed: CLI commands/markets.rs calls client.markets() which returns Vec<Market>
            if isinstance(body, dict):
                items = body.get("data", [])
                next_cursor = body.get("next_cursor", "")
            else:
                items = body          # plain list (most common)
                next_cursor = ""

            if not items:
                break

            stop_early = False
            for market in items:
                # Age gate: stop scanning when we hit markets older than max_age
                if cutoff_dt is not None:
                    created_raw = market.get("createdAt", market.get("created_at", ""))
                    if created_raw:
                        try:
                            created_dt = datetime.fromisoformat(
                                created_raw.replace("Z", "+00:00")
                            )
                            if created_dt < cutoff_dt:
                                stop_early = True
                                break
                        except Exception:
                            pass

                # FILTER 1: Question must mention Elon
                # Real questions: "Will Elon Musk post X tweets..."
                #                 "Will @elonmusk have X tweets..."
                question = (market.get("question", "") or "").lower()
                has_elon = any(kw in question for kw in ELON_TWEET_KEYWORDS)
                if not has_elon:
                    continue

                # FILTER 2: Must be a tweet-COUNT market (not "will he tweet about X")
                # Real bucket markets have numbers in their question (e.g. "60-79 tweets")
                has_count = any(kw in question for kw in TWEET_COUNT_KEYWORDS)
                if not has_count:
                    continue

                # FILTER 3: Must have numeric bucket ranges in the question
                # This catches "60-79 tweets" but not "will elon tweet today?" style
                if not re.search(r'\d+', question):
                    continue

                # FILTER 4: Must be actively tradeable
                # 'active=True, closed=True' happens for recently closed markets
                # 'acceptingOrders=True' is the definitive "market is live" flag
                if market.get("closed", False):
                    continue
                if active_only and market.get("acceptingOrders") is False:
                    continue

                markets.append(market)

            if stop_early:
                break

            # Pagination: Gamma API uses next_cursor for pagination
            if not next_cursor or next_cursor in ("LTE=", "", "MA=="):
                break
            cursor = next_cursor

    return markets


# ──────────────────────────────────────────────────────────────────────
# SECTION 10 — TRADE EXECUTION PIPELINE
# ──────────────────────────────────────────────────────────────────────

async def send_message(app: Application, text: str, **kwargs) -> None:
    """Send a Telegram message with error handling."""
    try:
        await app.bot.send_message(
            chat_id=TG_CHAT_ID,
            text=text,
            parse_mode="HTML",
            **kwargs,
        )
    except Exception as e:
        log.error("Telegram send_message error: %s", e)


async def send_pre_buy_alert(app: Application, market: dict, pace: Optional[dict],
                              tokens: list, planned: list, is_ongoing: bool) -> None:
    """Send a clean, simple pre-trade announcement before placing any orders."""
    question = market.get("question", "Unknown")
    end_raw  = (market.get("endDate") or "")[:10]
    slug     = market.get("slug", "")
    pm_link  = f"https://polymarket.com/event/{slug}" if slug else POLYMARKET_BASE
    mode_tag = "🔄" if is_ongoing else "🆕"

    # Pace line — show hourly rate, daily avg, and precise hours remaining
    if pace:
        proj       = int(pace["projected"])
        total_tw   = int(pace["total"])
        hrs_rem    = pace["hours_remaining"]
        hrly       = pace["hourly_avg"]
        daily      = pace["daily_avg"]
        bias_tag   = " (+early bias)" if pace["has_bias"] else ""
        # Choose hours or minutes for display based on urgency
        if hrs_rem < 1:
            time_left = f"{int(hrs_rem * 60)}min left"
        elif hrs_rem < 24:
            time_left = f"{hrs_rem:.1f}h left"
        else:
            time_left = f"{hrs_rem/24:.1f}d left"
        pace_line = (
            f"📊 Pace: <b>{total_tw}</b> tweets  ·  "
            f"{hrly:.1f}/hr ({daily:.0f}/day)  ·  "
            f"projected <b>{proj}{bias_tag}</b>  ·  {time_left}"
        )
    else:
        pace_line = "📊 Pace: unavailable"

    # Planned buckets line
    if planned:
        bucket_list = "  ".join(
            f"<b>{lbl}</b> ${price:.3f}" for lbl, price in planned
        )
        buckets_line = f"🎯 Buying: {bucket_list}"
        cost = ORDER_SIZE_USD * len(planned)
        cost_line = f"💸 Total: ${cost:.2f}"
    else:
        buckets_line = "⚠️ No buckets selected"
        cost_line = ""

    lines = [
        f"{mode_tag} <b>Market found</b> — <a href='{pm_link}'>{question[:60]}</a>",
        f"⏰ Ends: {end_raw}",
        pace_line,
        buckets_line,
        cost_line,
        "⏳ Placing orders now…",
    ]
    await send_message(app, "\n".join(l for l in lines if l))


def init_csv():
    """Create trades.csv with header if it doesn't exist."""
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()


def append_csv_row(row: dict) -> None:
    """Append a new trade row to CSV."""
    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writerow({col: row.get(col, "") for col in CSV_COLUMNS})


def rewrite_csv(rows: list) -> None:
    """Rewrite the entire CSV (used on TP hit to update a row)."""
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def load_csv_rows() -> list:
    """Load all CSV rows as list of dicts."""
    if not os.path.exists(CSV_FILE):
        return []
    with open(CSV_FILE, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


async def fetch_orderbook_with_retry(token_id: str) -> Optional[dict]:
    """Fetch orderbook for a token, retrying if empty.

    Brand-new markets have no liquidity yet. Retry every 5s up to
    EMPTY_BOOK_RETRIES (6) times = 30s max.

    Returns: {"asks": [...], "bids": [...]} or None on total failure.
    """
    for attempt in range(EMPTY_BOOK_RETRIES + 1):
        try:
            book = await run_clob(clob.get_order_book, token_id)
            asks = getattr(book, "asks", []) or []
            bids = getattr(book, "bids", []) or []

            # Convert OrderSummary objects to dicts if needed
            def to_price(item):
                if hasattr(item, "price"):
                    return float(item.price)
                if isinstance(item, dict):
                    return float(item.get("price", 0))
                return float(item)

            asks_prices = sorted([to_price(a) for a in asks])
            bids_prices = sorted([to_price(b) for b in bids], reverse=True)

            if asks_prices:
                return {"asks": asks_prices, "bids": bids_prices}

            if attempt < EMPTY_BOOK_RETRIES:
                log.info("Empty orderbook for %s (attempt %d/%d) — retrying in 5s…",
                         token_id[:12], attempt + 1, EMPTY_BOOK_RETRIES)
                await asyncio.sleep(5)
        except Exception as e:
            log.error("fetch_orderbook error for %s: %s", token_id[:12], e)
            if attempt < EMPTY_BOOK_RETRIES:
                await asyncio.sleep(5)

    return None  # all retries exhausted


async def process_market(app: Application, market: dict,
                         is_ongoing: bool = False) -> bool:
    """Full trade execution pipeline for a detected Elon tweet market.

    Returns True if at least one order was placed, False otherwise.
    The scanner uses this return value to stop processing further markets
    once one has been successfully traded (soonest-first strategy).
    """
    global session_counter

    question = market.get("question", "Unknown")
    market_key = question[:30]

    # Duplicate guard: skip if we already have a position in this market
    if market_key in open_positions_by_market:
        log.debug("Already traded market: %s", market_key)
        return

    # Mark as seen to prevent re-dispatch before orders are placed
    market_id = market.get("id", market.get("conditionId", ""))
    seen_market_ids.add(market_id)

    log.info("Processing market: %s (ongoing=%s)", question[:60], is_ongoing)

    # ─── EXTRACT OUTCOME TOKENS FROM GAMMA API RESPONSE ─────────────────
    # VERIFIED from live CLI output:
    #
    # clobTokenIds = '["0xABC...","0xDEF..."]'  ← JSON-encoded string of token IDs
    # outcomes     = '["60-79","80-99",...]'     ← JSON-encoded string of bucket labels
    # outcomePrices= '["0.15","0.85"]'          ← JSON-encoded string of prices
    #
    # These are STRINGS containing JSON arrays, not actual arrays.
    # Must parse them with json.loads().
    #
    # For multi-bucket markets, outcomes contains the bucket ranges:
    #   ["0-19", "20-39", "40-59", "60-79", "80-99", "100-119", "120+"]
    # For simple Yes/No, outcomes = ["Yes", "No"]
    # We skip Yes/No markets (not bucket-count markets)

    clob_token_ids_raw = market.get("clobTokenIds", "[]")
    outcomes_raw       = market.get("outcomes", "[]")
    prices_raw         = market.get("outcomePrices", "[]")

    # Parse the JSON-encoded strings
    try:
        clob_ids = json.loads(clob_token_ids_raw) if isinstance(clob_token_ids_raw, str) else (clob_token_ids_raw or [])
    except (json.JSONDecodeError, TypeError):
        clob_ids = []

    try:
        outcome_labels = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else (outcomes_raw or [])
    except (json.JSONDecodeError, TypeError):
        outcome_labels = []

    try:
        prices = json.loads(prices_raw) if isinstance(prices_raw, str) else (prices_raw or [])
    except (json.JSONDecodeError, TypeError):
        prices = []

    if not clob_ids or not outcome_labels:
        log.warning("No CLOB token IDs or outcomes for: %s", question[:60])
        return False

    # Skip simple Yes/No markets — these are not bucket markets
    if len(outcome_labels) <= 2 and set(lbl.lower() for lbl in outcome_labels) <= {"yes", "no"}:
        log.info("Skipping Yes/No market: %s", question[:60])
        return False

    # Build normalized token list
    # Each entry: {token_id, outcome (bucket label), price}
    tokens = []
    for i, token_id in enumerate(clob_ids):
        label = outcome_labels[i] if i < len(outcome_labels) else f"bucket_{i}"
        try:
            price = float(prices[i]) if i < len(prices) else 0.0
        except (ValueError, TypeError):
            price = 0.0
        tokens.append({
            "token_id": token_id,
            "outcome":  label,
            "price":    price,
        })

    if not tokens:
        log.warning("Could not parse tokens for: %s", question[:60])
        return False

    # Filter out garbage/near-zero-price buckets (e.g. $0.001 = no liquidity)
    tokens = [t for t in tokens if t["price"] >= MIN_BUY_PRICE]
    if not tokens:
        log.info("All buckets filtered out (price < $%.3f) for: %s",
                 MIN_BUY_PRICE, question[:60])
        return False

    log.info("Market has %d buckets: %s", len(tokens),
             ", ".join(t["outcome"] for t in tokens[:6]))

    # Fetch live XTracker pace for THIS SPECIFIC MARKET (matched by slug)
    # Each Polymarket event has its own XTracker tracking period. Passing the
    # slug ensures we use Apr 10-17 stats for the Apr 10-17 market, not the
    # generic "current" period which may be a different week.
    market_slug = market.get("slug", "")
    pace = await fetch_elon_pace(market_slug=market_slug)

    # Step 3: Balance gate & bucket selection
    price_cap = MAX_BUY_PRICE_ONGOING if is_ongoing else MAX_BUY_PRICE

    if pace:
        selected_tokens = select_buckets(tokens, pace)
    else:
        log.warning("No pace data — picking first %d tokens by price", BUCKETS_TO_BUY)
        selected_tokens = [
            (t["token_id"], t.get("outcome", ""), i, 0, 9999)
            for i, t in enumerate(tokens[:BUCKETS_TO_BUY])
        ]

    # ── IMPROVEMENT #3: Skip CENTER bucket if YES price > $0.50 ─────────
    # If the market has already priced in a bucket too heavily (>50¢), buying
    # it gives <2× return even if it resolves YES. Skip to the next bucket up.
    filtered_tokens = []
    for tok in selected_tokens:
        token_id, label, slot_idx, low, high = tok
        # Quick price check via token's outcomePrices entry
        token_price = next(
            (float(t["price"]) for t in tokens if t["token_id"] == token_id), 0.0
        )
        if token_price > 0.50 and slot_idx == 0:  # slot 0 = CENTER bucket
            log.info("CENTER bucket '%s' skipped (price=%.3f > 0.50)", label, token_price)
            await send_message(app,
                f"⏭️ CENTER bucket <b>{label}</b> skipped "
                f"(price ${token_price:.3f} > $0.50 — already priced in)")
            continue
        filtered_tokens.append(tok)
    selected_tokens = filtered_tokens
    # ─────────────────────────────────────────────────────────────────────

    if not selected_tokens:
        await send_message(app,
            f"⚠️ No valid buckets for <b>{question[:60]}</b>")
        return False

    # Pre-trade announcement: tell the user WHAT we're about to buy
    planned = []
    for token_id, label, slot_idx, low, high in selected_tokens:
        token_price = next(
            (float(t["price"]) for t in tokens if t["token_id"] == token_id), 0.0
        )
        planned.append((label, token_price))
    await send_pre_buy_alert(app, market, pace, tokens, planned, is_ongoing)

    if clob is None:
        await send_message(app,
            "⚠️ CLOB not configured — set POLYGON_PRIVATE_KEY + PROXY_WALLET_ADDRESS")
        return False

    if DRY_RUN:
        await send_message(app, "🔂 [DRY RUN] — orders skipped.")
        return False

    num_to_buy = len(selected_tokens)
    required = ORDER_SIZE_USD * num_to_buy
    balance  = await get_proxy_balance()

    if balance < required:
        await send_message(app,
            f"💸 <b>Insufficient balance</b>\n"
            f"  Need ${required:.2f}  ·  Have ${balance:.2f}\n"
            f"  Deposit USDC to your proxy wallet to start trading.")
        return False  # signal: no funds, try next market

    # Steps 4-7: Per-bucket execution
    placed_any = False
    for token_id, label, slot_idx, low, high in selected_tokens:
        tp_mult = TP_SLOTS[slot_idx] if slot_idx < len(TP_SLOTS) else 2.0

        # Step 5: Fetch orderbook (with retry for empty books)
        book = await fetch_orderbook_with_retry(token_id)
        is_fallback_gtc = False

        if book is None or not book.get("asks"):
            # Step 6: Fallback GTC order at predefined price
            log.warning("No asks after retries for %s — placing GTC fallback", label)
            is_fallback_gtc = True
            exec_price = FALLBACK_GTC_PRICE
            best_ask   = FALLBACK_GTC_PRICE
            spread     = 0.0
        else:
            best_ask = book["asks"][0]
            best_bid = book["bids"][0] if book["bids"] else 0.0
            spread   = best_ask - best_bid

            # Spread guard
            if spread > MAX_SPREAD:
                await send_message(app,
                    f"📉 Bucket <b>{label}</b> skipped — spread ${spread:.3f} > ${MAX_SPREAD}")
                continue

            # Price cap guard
            if best_ask > price_cap:
                await send_message(app,
                    f"💰 Bucket <b>{label}</b> skipped — ask ${best_ask:.3f} > cap ${price_cap:.3f}")
                continue

            # Simulate market order: place limit at ask + 0.01 (taker)
            exec_price = min(best_ask + 0.01, price_cap)

        # size is in SHARES, not USD
        size_shares = ORDER_SIZE_USD / best_ask
        tp_target   = best_ask * tp_mult

        try:
            order_args = OrderArgs(
                token_id=token_id,
                price=round(exec_price, 4),
                size=round(size_shares, 4),
                side=BUY,
            )
            signed = await run_clob(clob.create_order, order_args)
            order_type = OrderType.GTC if is_fallback_gtc else OrderType.GTC
            resp = await run_clob(clob.post_order, signed, order_type)

            order_id = resp.get("orderID", resp.get("id", f"local_{int(time.time())}"))
        except Exception as e:
            log.error("Order placement failed for %s: %s", label, e)
            await send_message(app,
                f"❌ Order failed for bucket <b>{label}</b>:\n<code>{str(e)[:200]}</code>")
            continue

        # Register position
        session_counter += 1
        snum = session_counter
        ts   = datetime.now(timezone.utc).isoformat()

        position = {
            "session_num":    snum,
            "order_id":       order_id,
            "token_id":       token_id,
            "market_question": question,
            "market_key":     market_key,
            "bucket":         label,
            "slot":           slot_idx,
            "buy_price":      best_ask,
            "exec_price":     exec_price,
            "size_shares":    size_shares,
            "cost_usd":       ORDER_SIZE_USD,
            "tp_target":      tp_target,
            "tp_mult":        tp_mult,
            "buy_order_id":   order_id,
            "buy_status":     "OPEN",
            "placed_at":      time.time(),
            "spread":         spread,
            "is_fallback_gtc": is_fallback_gtc,
        }

        open_positions[order_id] = position
        order_registry[snum]     = order_id

        # Mark market as traded (duplicate guard)
        if market_key not in open_positions_by_market:
            open_positions_by_market[market_key] = order_id
            placed_any = True

        # Update P&L
        pnl_summary["total_invested"] += ORDER_SIZE_USD
        pnl_summary["trades_placed"]  += 1

        # CSV log
        row = {
            "session_num":     snum,
            "timestamp_utc":   ts,
            "market_question": question,
            "bucket":          label,
            "slot":            slot_idx,
            "buy_price":       round(best_ask, 4),
            "size_shares":     round(size_shares, 4),
            "cost_usd":        round(ORDER_SIZE_USD, 4),
            "tp_target":       round(tp_target, 4),
            "tp_mult":         tp_mult,
            "buy_order_id":    order_id,
            "buy_status":      "OPEN",
            "sell_price":      "",
            "sell_order_id":   "",
            "profit_usd":      "",
            "profit_pct":      "",
            "spread_at_entry": round(spread, 4),
            "is_fallback_gtc": is_fallback_gtc,
        }
        append_csv_row(row)

        gtc_tag = " ·GTC" if is_fallback_gtc else ""
        await send_message(app,
            f"✅ <b>#{snum}</b>  {label}  @ ${best_ask:.3f}{gtc_tag}\n"
            f"   TP → ${tp_target:.3f} ({tp_mult}×)  ·  Cost ${ORDER_SIZE_USD:.2f}\n"
            f"   <code>{order_id[:24]}</code>")

        # Subscribe WebSocket to this token for real-time TP
        # (handled by ws_price_monitor via open_positions dict)

        # Throttle between bucket orders
        await asyncio.sleep(0.5)

    return placed_any  # True = at least one order placed this run


# ──────────────────────────────────────────────────────────────────────
# SECTION 11 — TAKE-PROFIT SYSTEM
# ──────────────────────────────────────────────────────────────────────

async def execute_tp(order_id: str, app: Application,
                     trigger_price: Optional[float] = None) -> None:
    """Execute a take-profit sell order.

    CRITICAL: pop the position FIRST (before placing sell order) to prevent
    double-execution when WebSocket and backup poll both trigger simultaneously.
    """
    pos = open_positions.pop(order_id, None)
    if pos is None:
        # Already handled — normal case when WS + backup both fire
        return

    # Remove from market guard too
    market_key = pos.get("market_key", "")
    open_positions_by_market.pop(market_key, None)

    tp_target   = pos["tp_target"]
    sell_price  = trigger_price or tp_target
    token_id    = pos["token_id"]
    size_shares = pos["size_shares"]
    buy_price   = pos["buy_price"]
    snum        = pos["session_num"]

    sell_order_id = ""
    profit_usd    = 0.0
    profit_pct    = 0.0

    try:
        if clob and not DRY_RUN:
            sell_args = OrderArgs(
                token_id=token_id,
                price=round(sell_price, 4),
                size=round(size_shares, 4),
                side=SELL,
            )
            signed = await run_clob(clob.create_order, sell_args)
            resp   = await run_clob(clob.post_order, signed, OrderType.GTC)
            sell_order_id = resp.get("orderID", resp.get("id", ""))

        profit_usd = (sell_price - buy_price) * size_shares
        profit_pct = ((sell_price / buy_price) - 1) * 100 if buy_price > 0 else 0.0

        # Update P&L session tracker
        pnl_summary["total_returned"] += (sell_price * size_shares)
        pnl_summary["trades_closed"]  += 1
        if profit_usd > 0:
            pnl_summary["wins"] += 1
        else:
            pnl_summary["losses"] += 1

        # Update CSV row
        rows = load_csv_rows()
        for row in rows:
            if str(row.get("buy_order_id", "")) == order_id:
                row["sell_price"]    = round(sell_price, 4)
                row["sell_order_id"] = sell_order_id
                row["profit_usd"]    = round(profit_usd, 4)
                row["profit_pct"]    = round(profit_pct, 2)
                row["buy_status"]    = "CLOSED"
                break
        rewrite_csv(rows)

        emoji = "🚀" if profit_usd > 0 else "💥"
        dry_note = " [DRY RUN]" if DRY_RUN else ""
        await send_message(app,
            f"{emoji} <b>TP HIT{dry_note} — #{snum}  {pos['bucket']}</b>\n"
            f"   ${buy_price:.3f} → ${sell_price:.3f}  ·  "
            f"<b>${profit_usd:+.2f} ({profit_pct:+.1f}%)</b>")
        log.info("TP executed for order #%d | P&L: $%.4f (%.1f%%)",
                 snum, profit_usd, profit_pct)
    except Exception as e:
        log.error("execute_tp error for order %s: %s", order_id, e)
        await send_message(app,
            f"❌ TP execution failed for order #{snum}:\n<code>{str(e)[:200]}</code>")


async def execute_stop_loss(order_id: str, app: Application,
                            trigger_price: float) -> None:
    """Execute a stop-loss sell order when position is down STOP_LOSS_PCT.

    Logic mirrors execute_tp but:
    - Sells at best bid (market sell) to exit immediately
    - Tagged as STOP_LOSS in CSV and P&L
    - Sends a different notification message

    Race-safe: pops position FIRST to prevent double-execution.
    """
    pos = open_positions.pop(order_id, None)
    if pos is None:
        return  # already handled (race between WS + backup poll)

    market_key = pos.get("market_key", "")
    open_positions_by_market.pop(market_key, None)

    token_id    = pos["token_id"]
    size_shares = pos["size_shares"]
    buy_price   = pos["buy_price"]
    snum        = pos["session_num"]
    sell_price  = trigger_price  # current best bid — already at loss

    sell_order_id = ""
    loss_usd = 0.0
    loss_pct = 0.0

    try:
        if clob and not DRY_RUN:
            # Sell at current best bid — place limit slightly below to ensure fill
            sell_price_limit = max(round(trigger_price - 0.01, 4), 0.01)
            sell_args = OrderArgs(
                token_id=token_id,
                price=sell_price_limit,
                size=round(size_shares, 4),
                side=SELL,
            )
            signed = await run_clob(clob.create_order, sell_args)
            resp   = await run_clob(clob.post_order, signed, OrderType.GTC)
            sell_order_id = resp.get("orderID", resp.get("id", ""))

        loss_usd = (sell_price - buy_price) * size_shares
        loss_pct = ((sell_price / buy_price) - 1) * 100 if buy_price > 0 else 0.0

        pnl_summary["total_returned"] += (sell_price * size_shares)
        pnl_summary["trades_closed"]  += 1
        pnl_summary["losses"]         += 1

        rows = load_csv_rows()
        for row in rows:
            if str(row.get("buy_order_id", "")) == order_id:
                row["sell_price"]    = round(sell_price, 4)
                row["sell_order_id"] = sell_order_id
                row["profit_usd"]    = round(loss_usd, 4)
                row["profit_pct"]    = round(loss_pct, 2)
                row["buy_status"]    = "STOP_LOSS"
                break
        rewrite_csv(rows)

        dry_note = " [DRY RUN]" if DRY_RUN else ""
        await send_message(app,
            f"🛑 <b>STOP-LOSS{dry_note} — #{snum}  {pos['bucket']}</b>\n"
            f"   Entry ${buy_price:.3f} → Exit ${sell_price:.3f}  ·  "
            f"<b>${loss_usd:+.2f} ({loss_pct:+.1f}%)</b>\n"
            f"   Position closed to protect remaining capital.")
        log.warning("Stop-loss executed for order #%d | loss: $%.4f (%.1f%%)",
                    snum, loss_usd, loss_pct)
    except Exception as e:
        log.error("execute_stop_loss error for order %s: %s", order_id, e)
        # Re-add position if sell failed so we can retry
        open_positions[order_id] = pos
        open_positions_by_market[market_key] = order_id
        await send_message(app,
            f"⚠️ Stop-loss order FAILED for #{snum} — position kept open:\n"
            f"<code>{str(e)[:200]}</code>")


# ──────────────────────────────────────────────────────────────────────
# SECTION 12 — WEBSOCKET TP MONITOR (primary, real-time)
# ──────────────────────────────────────────────────────────────────────

async def ws_price_monitor(app: Application) -> None:
    """WebSocket subscription to real-time price feed.

    Connects to wss://ws-subscriptions-clob.polymarket.com/ws/market
    Subscribes to all tokens in open_positions.
    On book update: computes midpoint and checks TP trigger.
    Reconnects with exponential backoff on disconnect.
    """
    backoff = 1
    while True:
        token_ids = [pos["token_id"] for pos in open_positions.values()]
        if not token_ids:
            await asyncio.sleep(10)
            continue

        log.info("WS: Connecting to price feed for %d tokens…", len(token_ids))
        try:
            async with websockets.connect(WS_URL, ping_interval=30) as ws:
                # Subscribe to market channel
                sub_msg = json.dumps({
                    "assets_ids": token_ids,
                    "type":       "market",
                })
                await ws.send(sub_msg)
                log.info("WS: Subscribed to %d tokens", len(token_ids))
                backoff = 1  # reset on successful connect

                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=60)
                    except asyncio.TimeoutError:
                        # Send ping to keep alive
                        await ws.send(json.dumps({"type": "ping"}))
                        continue

                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    # Process book update events
                    if not isinstance(msg, list):
                        msg = [msg]
                    for event in msg:
                        asset_id = event.get("asset_id", event.get("token_id", ""))
                        asks_raw = event.get("asks", [])
                        bids_raw = event.get("bids", [])

                        if not asks_raw and not bids_raw:
                            continue

                        def first_price(lst):
                            if not lst:
                                return None
                            item = lst[0]
                            if isinstance(item, dict):
                                return float(item.get("price", 0))
                            return float(item)

                        best_ask = first_price(asks_raw)
                        best_bid = first_price(bids_raw)

                        if best_ask is None and best_bid is None:
                            continue

                        # Compute midpoint
                        if best_ask is not None and best_bid is not None:
                            midpoint = (best_ask + best_bid) / 2
                        elif best_ask is not None:
                            midpoint = best_ask
                        else:
                            midpoint = best_bid

                        # Check all positions for this token — TP and Stop-Loss
                        for order_id, pos in list(open_positions.items()):
                            if pos["token_id"] != asset_id:
                                continue
                            buy_price  = pos["buy_price"]
                            sl_trigger = buy_price * (1.0 - STOP_LOSS_PCT)
                            if midpoint >= pos["tp_target"]:
                                log.info("WS TP trigger: order_id=%s mid=%.4f tp=%.4f",
                                         order_id, midpoint, pos["tp_target"])
                                asyncio.create_task(execute_tp(order_id, app, midpoint))
                            elif midpoint <= sl_trigger and buy_price > 0:
                                log.warning("WS SL trigger: order_id=%s mid=%.4f sl=%.4f",
                                            order_id, midpoint, sl_trigger)
                                asyncio.create_task(
                                    execute_stop_loss(order_id, app, best_bid or midpoint))

                    # Re-subscribe if new positions appeared
                    new_ids = [pos["token_id"] for pos in open_positions.values()]
                    if set(new_ids) != set(token_ids):
                        token_ids = new_ids
                        sub_msg = json.dumps({"assets_ids": new_ids, "type": "market"})
                        await ws.send(sub_msg)
                        log.debug("WS: Re-subscribed to %d tokens", len(new_ids))

        except (websockets.ConnectionClosed, OSError, Exception) as e:
            log.warning("WS disconnected: %s — reconnecting in %ds…", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)  # exponential backoff, cap at 60s


# ──────────────────────────────────────────────────────────────────────
# SECTION 13 — BACKUP TP POLL (REST fallback)
# ──────────────────────────────────────────────────────────────────────

async def tp_backup_poll(app: Application) -> None:
    """REST-based TP poll every TP_BACKUP_POLL_SECS (default 5 min).

    Safety net in case the WebSocket missed a price update.
    Calls the same execute_tp() — race-safety guaranteed by pop-first.
    """
    await asyncio.sleep(30)  # Let WS get established first
    while True:
        await asyncio.sleep(TP_BACKUP_POLL_SECS)
        if not open_positions or clob is None:
            continue
        log.debug("TP backup poll: checking %d positions…", len(open_positions))
        for order_id, pos in list(open_positions.items()):
            try:
                book = await run_clob(clob.get_order_book, pos["token_id"])
                asks = getattr(book, "asks", []) or []
                bids = getattr(book, "bids", []) or []
                if not asks or not bids:
                    continue

                def price_of(item):
                    if hasattr(item, "price"):
                        return float(item.price)
                    if isinstance(item, dict):
                        return float(item.get("price", 0))
                    return float(item)

                best_ask = min(price_of(a) for a in asks)
                best_bid = max(price_of(b) for b in bids)
                midpoint = (best_ask + best_bid) / 2

                buy_price   = pos["buy_price"]
                sl_trigger  = buy_price * (1.0 - STOP_LOSS_PCT)

                if midpoint >= pos["tp_target"]:
                    log.info("REST TP trigger: order_id=%s mid=%.4f tp=%.4f",
                             order_id, midpoint, pos["tp_target"])
                    await execute_tp(order_id, app, midpoint)
                elif midpoint <= sl_trigger and buy_price > 0:
                    log.warning("REST SL trigger: order_id=%s mid=%.4f sl=%.4f (entry=%.4f)",
                                order_id, midpoint, sl_trigger, buy_price)
                    await execute_stop_loss(order_id, app, best_bid)
            except Exception as e:
                log.debug("tp_backup_poll error for %s: %s", order_id[:12], e)


# ──────────────────────────────────────────────────────────────────────
# SECTION 14 — FILL MONITOR
# ──────────────────────────────────────────────────────────────────────

async def fill_monitor(app: Application) -> None:
    """Monitor open orders for fills. Alert on stalls, auto-cancel stale orders."""
    await asyncio.sleep(60)
    while True:
        await asyncio.sleep(60)
        now = time.time()
        for order_id, pos in list(open_positions.items()):
            age = now - pos.get("placed_at", now)
            snum = pos["session_num"]

            # Alert if unfilled for > FILL_ALERT_SECS (2 min)
            if age > FILL_ALERT_SECS and pos.get("fill_alerted") != True:
                pos["fill_alerted"] = True
                kb = InlineKeyboardMarkup([[
                    InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_order_{snum}"),
                    InlineKeyboardButton("💰 Force TP", callback_data=f"tp_order_{snum}"),
                ]])
                try:
                    await app.bot.send_message(
                        chat_id=TG_CHAT_ID,
                        text=(
                            f"⏳ <b>Order #{snum} unfilled for {int(age//60)}m</b>\n"
                            f"  Bucket: {pos['bucket']}\n"
                            f"  Price:  ${pos['exec_price']:.4f}\n"
                            f"  ID:     <code>{order_id}</code>"
                        ),
                        parse_mode="HTML",
                        reply_markup=kb,
                    )
                except Exception as e:
                    log.error("fill_monitor send error: %s", e)

            # Auto-cancel after STALE_CANCEL_SECS (20 min)
            if age > STALE_CANCEL_SECS:
                log.warning("Auto-cancelling stale order #%d (%s)", snum, order_id)
                try:
                    if clob and not DRY_RUN:
                        await run_clob(clob.cancel, order_id)
                    open_positions.pop(order_id, None)
                    market_key = pos.get("market_key", "")
                    open_positions_by_market.pop(market_key, None)
                    # Remove from registry
                    order_registry.pop(snum, None)
                    await send_message(app,
                        f"🚫 <b>Order #{snum} auto-cancelled</b> (stale after "
                        f"{STALE_CANCEL_SECS//60}min)\n"
                        f"  Bucket: {pos['bucket']}")
                except Exception as e:
                    log.error("Auto-cancel error for %s: %s", order_id, e)


# ──────────────────────────────────────────────────────────────────────
# SECTION 15 — MARKET SCANNERS
# ──────────────────────────────────────────────────────────────────────

async def _build_market_scan_report(markets: list, label: str) -> str:
    """Build a Telegram message summarising the top-3 markets found."""
    if not markets:
        return f"🔍 <b>{label}</b>\n  No active Elon tweet markets found right now."

    lines = [f"🔍 <b>{label}</b> — {len(markets)} market(s) found\n"]
    for i, m in enumerate(markets[:3]):
        q       = m.get("question", "Unknown")[:70]
        end_raw = (m.get("endDate") or "")[:10]
        slug    = m.get("slug", "")
        pm_url  = f"https://polymarket.com/event/{slug}" if slug else "https://polymarket.com"
        # parse outcomes to show bucket count
        try:
            outcomes = json.loads(m.get("outcomes", "[]"))
        except Exception:
            outcomes = []
        n_buckets = len(outcomes)
        lines.append(
            f"{'🥇' if i==0 else '🥈' if i==1 else '🥉'} <b>{q}</b>\n"
            f"   ⏰ Ends: {end_raw}  |  🪣 {n_buckets} buckets\n"
            f"   🔗 <a href='{pm_url}'>View on Polymarket</a>\n"
        )
    if len(markets) > 3:
        lines.append(f"   …and {len(markets)-3} more.")
    return "\n".join(lines)


async def fast_market_scanner(app: Application) -> None:
    """Fast scanner: polls for new Elon markets every MARKET_POLL_SECS.

    SEQUENTIAL EXECUTION: Markets are processed one at a time, soonest-ending
    first. Once a market places at least one order, the loop stops for this
    cycle. This prevents the bot from blowing the whole balance on many markets
    and focuses capital on the highest-urgency opportunity.
    """
    log.info("Fast scanner started (interval=%ds, age_cap=%dmin)",
             MARKET_POLL_SECS, MARKET_AGE_MINUTES)
    first_run = True
    while True:
        try:
            # Try CLI first (more reliable), fall back to HTTP
            markets = await fetch_elon_markets_cli()
            if not markets:
                markets = await fetch_elon_markets(
                    active_only=True,
                    max_age_minutes=MARKET_AGE_MINUTES,
                )

            new_markets = []
            for market in markets:
                market_id = market.get("id", market.get("conditionId", ""))
                if not market_id or market_id in seen_market_ids:
                    continue
                new_markets.append(market)

            # Only send a Telegram message on first run or when new markets found
            if first_run:
                report = await _build_market_scan_report(
                    markets, "Market Status (Initial Scan)"
                )
                await send_message(app, report)
                first_run = False
            elif new_markets:
                report = await _build_market_scan_report(new_markets, "🚨 NEW Market Detected")
                await send_message(app, report)

            # ── POSITION GATE ─────────────────────────────────────────────────
            # Don't open new trades until ALL existing positions are closed.
            # This ensures we wait for TP or stop-loss before risking more capital.
            if open_positions:
                log.info("Fast scan: %d open position(s) — gated, not trading.",
                         len(open_positions))
                await asyncio.sleep(MARKET_POLL_SECS)
                continue
            # ────────────────────────────────────────────────────────────────

            # Process new markets SEQUENTIALLY, soonest-expiring first.
            # Trade up to MAX_MARKETS_PER_CYCLE markets then stop.
            markets_this_cycle = 0
            for market in new_markets:  # already sorted soonest-first by CLI
                market_id = market.get("id", market.get("conditionId", ""))
                seen_market_ids.add(market_id)
                log.info("Fast scan: processing '%s'",
                         market.get("question", "")[:60])
                traded = await process_market(app, market, is_ongoing=False)
                if traded:
                    markets_this_cycle += 1
                    if markets_this_cycle >= MAX_MARKETS_PER_CYCLE:
                        log.info("Fast scan: reached MAX_MARKETS_PER_CYCLE (%d) — stopping.",
                                 MAX_MARKETS_PER_CYCLE)
                        break

        except Exception as e:
            log.error("fast_market_scanner error: %s", e)
        await asyncio.sleep(MARKET_POLL_SECS)


async def ongoing_market_scanner(app: Application) -> None:
    """Ongoing scanner: checks ALL active Elon markets every ONGOING_RESCAN_SECS.

    SEQUENTIAL EXECUTION: Processes markets one at a time, soonest-ending first.
    Stops as soon as one market places orders. Only moves to the next market
    if the current one fails (no valid buckets / price cap / no balance).
    """
    if not SCAN_ONGOING_MARKETS:
        log.info("Ongoing scanner disabled (SCAN_ONGOING_MARKETS=false)")
        return

    log.info("Ongoing scanner started (interval=%ds)", ONGOING_RESCAN_SECS)
    await asyncio.sleep(20)  # Let fast scanner run first on startup

    while True:
        try:
            # CLI is primary source
            markets = await fetch_elon_markets_cli()
            if not markets:
                markets = await fetch_elon_markets(active_only=True, max_age_minutes=None)
                markets.sort(key=lambda m: m.get("endDate", "9999"))

            # ── POSITION GATE ────────────────────────────────────────────────
            if open_positions:
                log.info("Ongoing scan: %d open position(s) — gated.",
                         len(open_positions))
                await asyncio.sleep(ONGOING_RESCAN_SECS)
                continue
            # ───────────────────────────────────────────────────────────────

            # Send top-3 report
            report = await _build_market_scan_report(markets, "Ongoing Market Scan")
            await send_message(app, report)

            # Filter to markets we haven't traded yet
            tradeable = []
            for market in markets:  # already sorted soonest-first
                market_id  = market.get("id", market.get("conditionId", ""))
                question   = market.get("question", "")
                market_key = question[:30]
                if market_key in open_positions_by_market:
                    continue
                if market_id in seen_market_ids:
                    continue
                tradeable.append(market)

            if not tradeable:
                await send_message(app, "💤 No new markets to trade this cycle.")
            else:
                await send_message(app,
                    f"⏳ Evaluating <b>{len(tradeable)}</b> market(s) — "
                    f"trading up to {MAX_MARKETS_PER_CYCLE}, soonest-first…")
                markets_this_cycle = 0
                for market in tradeable:
                    market_id = market.get("id", market.get("conditionId", ""))
                    seen_market_ids.add(market_id)
                    log.info("Ongoing scan: processing '%s'",
                             market.get("question", "")[:60])
                    traded = await process_market(app, market, is_ongoing=True)
                    if traded:
                        markets_this_cycle += 1
                        if markets_this_cycle >= MAX_MARKETS_PER_CYCLE:
                            log.info("Ongoing scan: reached MAX_MARKETS_PER_CYCLE (%d).",
                                     MAX_MARKETS_PER_CYCLE)
                            break

        except Exception as e:
            log.error("ongoing_market_scanner error: %s", e)

        await asyncio.sleep(ONGOING_RESCAN_SECS)


# ──────────────────────────────────────────────────────────────────────
# SECTION 16 — DAILY SUMMARY JOB
# ──────────────────────────────────────────────────────────────────────

async def daily_summary_job(app: Application) -> None:
    """Send a daily P&L summary at DAILY_SUMMARY_UTC_HOUR (default 20:00 UTC)."""
    global _last_summary_day
    while True:
        now = datetime.now(timezone.utc)
        if (now.hour == DAILY_SUMMARY_UTC_HOUR
                and _last_summary_day != now.day
                and pnl_summary["trades_placed"] > 0):
            _last_summary_day = now.day
            net = pnl_summary["total_returned"] - pnl_summary["total_invested"]
            roi = (net / pnl_summary["total_invested"] * 100
                   if pnl_summary["total_invested"] > 0 else 0)
            await send_message(app,
                f"📊 <b>Daily P&amp;L Summary</b> — {now.strftime('%Y-%m-%d UTC')}\n"
                f"  Trades placed:  {pnl_summary['trades_placed']}\n"
                f"  Trades closed:  {pnl_summary['trades_closed']}\n"
                f"  Wins / Losses:  {pnl_summary['wins']} / {pnl_summary['losses']}\n"
                f"  Total invested: ${pnl_summary['total_invested']:.2f}\n"
                f"  Total returned: ${pnl_summary['total_returned']:.2f}\n"
                f"  Net P&amp;L:       <b>${net:+.2f} ({roi:+.1f}%)</b>")
        await asyncio.sleep(60)


# ──────────────────────────────────────────────────────────────────────
# SECTION 17 — ON-CHAIN DEPOSIT
# ──────────────────────────────────────────────────────────────────────

async def do_deposit(amount_usd: float, app: Application) -> None:
    """Transfer USDC.e from EOA wallet to Polymarket proxy wallet on-chain.

    Requires:
      - ALCHEMY_RPC_URL for Polygon mainnet connection
      - POLYGON_PRIVATE_KEY (the EOA/signing key)
      - PROXY_WALLET_ADDRESS (destination)
      - Small MATIC balance in EOA for gas (~$0.01 per tx)
    """
    if not ALCHEMY_RPC_URL or not PRIVATE_KEY or not PROXY_WALLET:
        await send_message(app,
            "⚠️ Deposit requires ALCHEMY_RPC_URL, POLYGON_PRIVATE_KEY, "
            "and PROXY_WALLET_ADDRESS in .env")
        return
    try:
        from eth_account import Account
        w3   = Web3(Web3.HTTPProvider(ALCHEMY_RPC_URL))
        acct = Account.from_key(PRIVATE_KEY)
        usdc = w3.eth.contract(
            address=Web3.to_checksum_address(USDC_ADDRESS), abi=ERC20_ABI
        )
        amount_raw = int(amount_usd * (10 ** USDC_DECIMALS))
        nonce = w3.eth.get_transaction_count(acct.address)
        gas_price = w3.eth.gas_price

        tx = usdc.functions.transfer(
            Web3.to_checksum_address(PROXY_WALLET),
            amount_raw,
        ).build_transaction({
            "chainId":   137,
            "from":      acct.address,
            "nonce":     nonce,
            "gasPrice":  gas_price,
            "gas":       100_000,
        })
        signed = acct.sign_transaction(tx)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        tx_hex  = tx_hash.hex()

        await send_message(app,
            f"💳 <b>Deposit Sent</b>\n"
            f"  Amount: ${amount_usd:.2f} USDC\n"
            f"  To:     <code>{PROXY_WALLET}</code>\n"
            f"  Tx:     <a href='https://polygonscan.com/tx/{tx_hex}'>"
            f"{tx_hex[:20]}…</a>\n"
            f"⏳ Waiting for confirmation…")

        # Wait for receipt
        loop = asyncio.get_event_loop()
        receipt = await loop.run_in_executor(
            None, lambda: w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        )
        status = "✅ Confirmed" if receipt.status == 1 else "❌ Failed"
        await send_message(app,
            f"{status} — Block {receipt.blockNumber}\n"
            f"Gas used: {receipt.gasUsed}")
    except Exception as e:
        log.error("Deposit error: %s", e)
        await send_message(app, f"❌ Deposit failed:\n<code>{str(e)[:300]}</code>")


# ──────────────────────────────────────────────────────────────────────
# SECTION 18 — TELEGRAM COMMAND HANDLERS
# ──────────────────────────────────────────────────────────────────────

def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Orders",     callback_data="menu_orders"),
            InlineKeyboardButton("💰 Balance",     callback_data="menu_balance"),
            InlineKeyboardButton("📈 P&L",         callback_data="menu_pnl"),
        ],
        [
            InlineKeyboardButton("🐦 Elon Pace",  callback_data="menu_pace"),
            InlineKeyboardButton("🔍 Markets",    callback_data="menu_markets"),
            InlineKeyboardButton("⚙️ Status",     callback_data="menu_status"),
        ],
        [
            InlineKeyboardButton("🔄 Force Scan", callback_data="menu_scan"),
            InlineKeyboardButton("💳 Deposit",    callback_data="menu_deposit"),
            InlineKeyboardButton("🏧 Withdraw",   callback_data="menu_withdraw"),
        ],
    ])


async def start_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Send main menu."""
    dry_tag  = "  ⚠️ <b>TEST MODE</b> — no real orders placed\n" if DRY_RUN else ""
    bal      = await get_proxy_balance()
    mode_tag = "⚠️ TEST" if DRY_RUN else "🔴 LIVE"
    await update.message.reply_text(
        f"🎯 <b>TweetSniper</b>  ·  {mode_tag}\n"
        f"{dry_tag}"
        f"Balance: <b>${bal:.2f} USDC</b>\n"
        f"Positions open: <b>{len(open_positions)}</b>\n"
        f"Markets tracked: <b>{len(seen_market_ids)}</b>\n"
        f"\nTap a button 👇",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard(),
    )


async def orders_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """List all open orders with per-order Cancel/TP buttons."""
    msg = update.message or update.callback_query.message
    if not open_positions:
        await msg.reply_text("📭 No open positions.", parse_mode="HTML")
        return

    for order_id, pos in list(open_positions.items()):
        snum = pos["session_num"]
        age  = int(time.time() - pos.get("placed_at", time.time()))
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton(f"❌ Cancel #{snum}", callback_data=f"cancel_order_{snum}"),
            InlineKeyboardButton(f"💰 TP #{snum}",     callback_data=f"tp_order_{snum}"),
        ]])
        await msg.reply_text(
            f"📌 <b>Order #{snum}</b>\n"
            f"  Market: {pos['market_question'][:50]}\n"
            f"  Bucket: {pos['bucket']}\n"
            f"  Buy:    ${pos['buy_price']:.4f} | TP: ${pos['tp_target']:.4f}\n"
            f"  Shares: {pos['size_shares']:.2f} | Cost: ${pos['cost_usd']:.2f}\n"
            f"  Age:    {age//60}m {age%60}s\n"
            f"  ID:     <code>{order_id}</code>",
            parse_mode="HTML",
            reply_markup=kb,
        )


async def balance_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Show proxy wallet and EOA USDC balances with diagnostics."""
    msg = update.message or update.callback_query.message
    proxy_bal = await get_proxy_balance()
    eoa_bal   = await get_eoa_usdc_balance()

    # Also get raw API response for diagnostics
    raw_info = ""
    if clob is not None:
        try:
            params = BalanceAllowanceParams(signature_type=2)
            raw = await run_clob(clob.get_balance_allowance, params)
            raw_info = f"\n  Raw API: <code>{str(raw)[:100]}</code>"
        except Exception as e:
            raw_info = f"\n  Raw API error: <code>{str(e)[:80]}</code>"

    from eth_account import Account
    try:
        eoa_addr = Account.from_key(PRIVATE_KEY).address if PRIVATE_KEY else "?"
    except Exception:
        eoa_addr = "invalid key"

    await msg.reply_text(
        f"💰 <b>Balances</b>\n"
        f"  Proxy (trading): <b>${proxy_bal:.2f}</b>\n"
        f"  EOA on-chain:    <b>${eoa_bal:.2f}</b>{raw_info}\n\n"
        f"  Proxy addr: <code>{PROXY_WALLET[:24]}…</code>\n"
        f"  EOA addr:   <code>{eoa_addr[:24]}…</code>",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard(),
    )


async def pnl_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Show session P&L summary."""
    msg = update.message or update.callback_query.message
    net = pnl_summary["total_returned"] - pnl_summary["total_invested"]
    roi = (net / pnl_summary["total_invested"] * 100
           if pnl_summary["total_invested"] > 0 else 0)
    wins    = pnl_summary["wins"]
    losses  = pnl_summary["losses"]
    winrate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0
    net_emoji = "📈" if net >= 0 else "📉"
    await msg.reply_text(
        f"{net_emoji} <b>P&amp;L Summary</b>\n"
        f"  Trades:  {pnl_summary['trades_placed']} placed · {pnl_summary['trades_closed']} closed · {len(open_positions)} open\n"
        f"  Win rate: {winrate:.0f}%  ({wins}W / {losses}L)\n"
        f"  Invested: ${pnl_summary['total_invested']:.2f}\n"
        f"  Net P&amp;L: <b>${net:+.2f} ({roi:+.1f}%)</b>",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard(),
    )


async def pace_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Show live Elon tweet pace from XTracker."""
    msg  = update.message or update.callback_query.message
    pace = await fetch_elon_pace()  # generic current period for /pace command
    if not pace:
        await msg.reply_text("⚠️ XTracker unavailable — try again shortly.",
                             parse_mode="HTML", reply_markup=main_menu_keyboard())
        return
    bias_tag = " +bias" if pace["has_bias"] else ""
    proj     = int(pace["projected"])
    total    = int(pace["total"])
    hrs_el   = pace["hours_elapsed"]
    hrs_rem  = pace["hours_remaining"]
    hrly     = pace["hourly_avg"]
    daily    = pace["daily_avg"]
    period   = f"{pace['start_date'][:10]} → {pace['end_date'][:10]}"
    if hrs_rem < 1:
        time_left = f"{int(hrs_rem*60)}min"
    elif hrs_rem < 24:
        time_left = f"{hrs_rem:.1f}h"
    else:
        time_left = f"{hrs_rem/24:.1f}d"
    await msg.reply_text(
        f"🐦 <b>Elon Tweet Pace</b>  ·  {pace['tier']}\n"
        f"  So far:    <b>{total} tweets</b>  ({hrs_el:.0f}h elapsed)\n"
        f"  Rate:      <b>{hrly:.1f}/hr</b>  ({daily:.0f}/day)\n"
        f"  Projected: <b>{proj}{bias_tag}</b>  ({time_left} left)\n"
        f"  Period:    {period}",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard(),
    )


async def status_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Show bot config and health."""
    msg    = update.message or update.callback_query.message
    clob_s = "✅" if clob else "❌"
    mode_s = "⚠️ TEST" if DRY_RUN else "🔴 LIVE"
    await msg.reply_text(
        f"⚙️ <b>Bot Status</b>  ·  {mode_s}\n"
        f"  CLOB:      {clob_s}   Positions: {len(open_positions)}   Seen: {len(seen_market_ids)}\n"
        f"  Order:     ${ORDER_SIZE_USD:.2f} × {BUCKETS_TO_BUY} buckets\n"
        f"  Max price: ${MAX_BUY_PRICE:.2f} new  /  ${MAX_BUY_PRICE_ONGOING:.2f} ongoing\n"
        f"  TP target: {TP_SLOTS[0]}×   Spread cap: ${MAX_SPREAD:.2f}\n"
        f"  Scan:      every {MARKET_POLL_SECS}s  (ongoing: {ONGOING_RESCAN_SECS}s)\n"
        f"  Wallet:    <code>{PROXY_WALLET[:20]}…</code>",
        parse_mode="HTML",
        reply_markup=main_menu_keyboard(),
    )


async def markets_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Show current active Elon tweet markets (top 3 by end date)."""
    msg = update.message or update.callback_query.message
    await msg.reply_text("🔍 Fetching active Elon tweet markets…", parse_mode="HTML")
    markets = await fetch_elon_markets_cli()
    if not markets:
        markets = await fetch_elon_markets(active_only=True, max_age_minutes=None)
        markets.sort(key=lambda m: m.get("endDate", "9999"))
    report = await _build_market_scan_report(markets, "Active Elon Tweet Markets")
    await msg.reply_text(report, parse_mode="HTML")


async def scan_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Force an immediate market scan cycle."""
    msg = update.message or update.callback_query.message
    await msg.reply_text("🔄 Running immediate market scan…", parse_mode="HTML")
    markets = await fetch_elon_markets_cli()
    if not markets:
        markets = await fetch_elon_markets(active_only=True, max_age_minutes=None)
        markets.sort(key=lambda m: m.get("endDate", "9999"))
    report = await _build_market_scan_report(markets, "Forced Scan Results")
    await msg.reply_text(report, parse_mode="HTML")
    # Process any unseen tradeable markets
    dispatched = 0
    for market in markets:
        market_id  = market.get("id", market.get("conditionId", ""))
        question   = market.get("question", "")
        market_key = question[:30]
        if market_key in open_positions_by_market:
            continue
        if market_id in seen_market_ids:
            continue
        seen_market_ids.add(market_id)
        asyncio.create_task(process_market(ctx.application, market, is_ongoing=True))
        dispatched += 1
    if dispatched:
        await msg.reply_text(
            f"✅ Dispatched <b>{dispatched}</b> markets for trading "
            f"(soonest-first).",
            parse_mode="HTML",
        )
    else:
        await msg.reply_text(
            "💤 No new markets to trade — all already seen or in positions.",
            parse_mode="HTML",
        )


async def cancel_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """/cancel N — cancel order #N by session number."""
    try:
        snum = int(ctx.args[0])
    except (IndexError, ValueError):
        await update.message.reply_text("Usage: /cancel &lt;session_num&gt;", parse_mode="HTML")
        return
    await _cancel_order_by_snum(snum, update.message, ctx.application)


async def mtp_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """/mtp N — manually trigger take-profit for order #N."""
    try:
        snum = int(ctx.args[0])
    except (IndexError, ValueError):
        await update.message.reply_text("Usage: /mtp &lt;session_num&gt;", parse_mode="HTML")
        return
    order_id = order_registry.get(snum)
    if not order_id or order_id not in open_positions:
        await update.message.reply_text(f"Order #{snum} not found.", parse_mode="HTML")
        return
    pos = open_positions[order_id]
    await update.message.reply_text(
        f"💰 Triggering manual TP for #{snum} at ${pos['tp_target']:.4f}…",
        parse_mode="HTML",
    )
    await execute_tp(order_id, ctx.application, pos["tp_target"])


async def deposit_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """/deposit <amount> — send USDC from EOA to proxy wallet."""
    try:
        amount = float(ctx.args[0])
    except (IndexError, ValueError):
        await update.message.reply_text(
            "Usage: /deposit &lt;amount&gt;  e.g. /deposit 50", parse_mode="HTML")
        return
    if amount <= 0:
        await update.message.reply_text("Amount must be positive.", parse_mode="HTML")
        return
    await update.message.reply_text(
        f"💳 Initiating deposit of ${amount:.2f} USDC…", parse_mode="HTML")
    await do_deposit(amount, ctx.application)


async def withdraw_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """/withdraw — show withdrawal instructions."""
    msg = update.message or update.callback_query.message
    proxy_bal = await get_proxy_balance()
    await msg.reply_text(
        f"🏧 <b>Withdrawal Instructions</b>\n\n"
        f"Proxy wallet balance: <b>${proxy_bal:.4f} USDC</b>\n\n"
        f"⚠️ The Polymarket proxy wallet is a smart contract managed "
        f"by Polymarket — you cannot drain it directly.\n\n"
        f"<b>To withdraw:</b>\n"
        f"1. Visit <a href='https://polymarket.com/portfolio'>polymarket.com/portfolio</a>\n"
        f"2. Click <b>Withdraw</b>\n"
        f"3. Enter amount and confirm with your email/wallet\n"
        f"4. Funds arrive in your EOA wallet\n\n"
        f"Proxy wallet: <code>{PROXY_WALLET}</code>",
        parse_mode="HTML",
    )


async def _cancel_order_by_snum(snum: int, msg, app: Application) -> None:
    """Helper to cancel an order by session number."""
    order_id = order_registry.get(snum)
    if not order_id or order_id not in open_positions:
        await msg.reply_text(f"Order #{snum} not found or already closed.")
        return
    pos = open_positions.pop(order_id, {})
    market_key = pos.get("market_key", "")
    open_positions_by_market.pop(market_key, None)
    order_registry.pop(snum, None)
    try:
        if clob and not DRY_RUN:
            await run_clob(clob.cancel, order_id)
        await msg.reply_text(
            f"✅ Order #{snum} cancelled\n"
            f"  Bucket: {pos.get('bucket', '')}\n"
            f"  ID: <code>{order_id}</code>",
            parse_mode="HTML",
        )
    except Exception as e:
        await msg.reply_text(f"❌ Cancel error: {str(e)[:200]}")


# ──────────────────────────────────────────────────────────────────────
# SECTION 19 — CALLBACK QUERY HANDLER
# ──────────────────────────────────────────────────────────────────────

async def button_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle all inline button taps."""
    q    = update.callback_query
    data = q.data
    await q.answer()  # dismiss loading spinner

    # Main menu routing
    if data == "menu_orders":
        await orders_cmd(update, ctx)
    elif data == "menu_balance":
        await balance_cmd(update, ctx)
    elif data == "menu_pnl":
        await pnl_cmd(update, ctx)
    elif data == "menu_pace":
        await pace_cmd(update, ctx)
    elif data == "menu_status":
        await status_cmd(update, ctx)
    elif data == "menu_markets":
        await markets_cmd(update, ctx)
    elif data == "menu_scan":
        await scan_cmd(update, ctx)
    elif data == "menu_deposit":
        await q.message.reply_text(
            "💳 Use /deposit &lt;amount&gt; to fund your proxy wallet.\n"
            "Example: /deposit 50",
            parse_mode="HTML",
            reply_markup=main_menu_keyboard(),
        )
    elif data == "menu_withdraw":
        await withdraw_cmd(update, ctx)

    # Per-order actions
    elif data.startswith("cancel_order_"):
        snum = int(data.split("_")[-1])
        await _cancel_order_by_snum(snum, q.message, ctx.application)
    elif data.startswith("tp_order_"):
        snum = int(data.split("_")[-1])
        order_id = order_registry.get(snum)
        if not order_id or order_id not in open_positions:
            await q.message.reply_text(f"Order #{snum} not found.")
            return
        pos = open_positions[order_id]
        await q.message.reply_text(
            f"💰 Triggering manual TP for #{snum}…", parse_mode="HTML")
        await execute_tp(order_id, ctx.application, pos["tp_target"])


# ──────────────────────────────────────────────────────────────────────
# SECTION 20 — APPLICATION STARTUP
# ──────────────────────────────────────────────────────────────────────

async def post_init(app: Application) -> None:
    """Launch all background tasks after the bot is initialized."""
    log.info("Starting TweetSniper background tasks…")
    init_csv()

    dry_note = " [DRY RUN]" if DRY_RUN else ""
    try:
        await app.bot.send_message(
            chat_id=TG_CHAT_ID,
            text=(
                f"🚀 <b>TweetSniper Started{dry_note}</b>\n"
                f"  Proxy wallet: <code>{PROXY_WALLET[:20]}…</code>\n"
                f"  Order size:   ${ORDER_SIZE_USD:.2f}/bucket (max {BUCKETS_TO_BUY} buckets)\n"
                f"  TP at:        {TP_SLOTS[0]}×\n"
                f"  Fast scan:    every {MARKET_POLL_SECS}s\n"
                f"  Ongoing scan: {'every ' + str(ONGOING_RESCAN_SECS) + 's' if SCAN_ONGOING_MARKETS else 'disabled'}\n"
                f"  CLOB:         {'✅ authenticated' if clob else '❌ not connected'}\n"
                f"\n⏳ Fetching current Elon tweet markets…\n"
                f"Commands: /markets /scan /pace /orders /status"
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        log.warning("Could not send startup message: %s", e)

    # Send an immediate market report on startup
    try:
        markets = await fetch_elon_markets_cli()
        if not markets:
            markets = await fetch_elon_markets(active_only=True, max_age_minutes=None)
            markets.sort(key=lambda m: m.get("endDate", "9999"))
        report = await _build_market_scan_report(markets, "Startup Market Report")
        await app.bot.send_message(
            chat_id=TG_CHAT_ID,
            text=report,
            parse_mode="HTML",
        )
    except Exception as e:
        log.warning("Could not send startup market report: %s", e)

    asyncio.create_task(fast_market_scanner(app))
    asyncio.create_task(ongoing_market_scanner(app))
    asyncio.create_task(ws_price_monitor(app))
    asyncio.create_task(tp_backup_poll(app))
    asyncio.create_task(fill_monitor(app))
    asyncio.create_task(daily_summary_job(app))
    log.info("All background tasks launched ✓")


def main() -> None:
    """Entry point. Build the Telegram Application and start polling."""
    if not TG_BOT_TOKEN:
        log.error("TG_BOT_TOKEN not set — cannot start bot.")
        sys.exit(1)

    log.info("Building Telegram application…")
    app = (
        Application.builder()
        .token(TG_BOT_TOKEN)
        .connect_timeout(30.0)       # allow 30s to reach api.telegram.org
        .read_timeout(30.0)          # allow 30s for slow responses
        .write_timeout(30.0)
        .pool_timeout(30.0)
        .post_init(post_init)
        .build()
    )

    # Register command handlers
    app.add_handler(CommandHandler("start",    start_cmd))
    app.add_handler(CommandHandler("orders",   orders_cmd))
    app.add_handler(CommandHandler("balance",  balance_cmd))
    app.add_handler(CommandHandler("pnl",      pnl_cmd))
    app.add_handler(CommandHandler("pace",     pace_cmd))
    app.add_handler(CommandHandler("status",   status_cmd))
    app.add_handler(CommandHandler("cancel",   cancel_cmd))
    app.add_handler(CommandHandler("mtp",      mtp_cmd))
    app.add_handler(CommandHandler("deposit",  deposit_cmd))
    app.add_handler(CommandHandler("withdraw", withdraw_cmd))
    app.add_handler(CommandHandler("markets",  markets_cmd))
    app.add_handler(CommandHandler("scan",     scan_cmd))

    # Inline button handler
    app.add_handler(CallbackQueryHandler(button_handler))

    log.info("TweetSniper is running… (Ctrl+C to stop)")
    app.run_polling(
        drop_pending_updates=True,
        bootstrap_retries=10,   # retry up to 10× if network is flaky at startup
    )


if __name__ == "__main__":
    main()
