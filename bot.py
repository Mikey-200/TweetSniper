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
import math
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from statistics import NormalDist
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
    OrderArgs, OrderType, BalanceAllowanceParams, AssetType,
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
MIN_BUY_PRICE          = float(os.getenv("MIN_BUY_PRICE", "0.10"))   # skip buckets below 10¢
MAX_BUY_PRICE          = float(os.getenv("MAX_BUY_PRICE", "0.30"))
MAX_BUY_PRICE_ONGOING  = float(os.getenv("MAX_BUY_PRICE_ONGOING", "0.20"))
MAX_SPREAD             = float(os.getenv("MAX_SPREAD", "0.25"))
EMPTY_BOOK_RETRIES     = int(os.getenv("EMPTY_BOOK_RETRIES", "6"))
FALLBACK_GTC_PRICE     = float(os.getenv("FALLBACK_GTC_PRICE", "0.25"))
BUCKETS_TO_BUY         = int(os.getenv("BUCKETS_TO_BUY", "3"))
SKIP_MARGIN_MULTIPLIER = float(os.getenv("SKIP_MARGIN_MULTIPLIER", "1.5"))

# === Position Management ===
MAX_MARKETS_PER_CYCLE  = int(os.getenv("MAX_MARKETS_PER_CYCLE",  "2"))   # max markets to enter per scan cycle
MAX_OPEN_ORDERS        = int(os.getenv("MAX_OPEN_ORDERS",         "6"))   # hard cap on concurrent open positions
MIN_MARKET_AGE_HOURS   = float(os.getenv("MIN_MARKET_AGE_HOURS",  "12"))  # 12h gate: don't enter until market is this old
MIN_CONFIDENCE_PCT     = float(os.getenv("MIN_CONFIDENCE_PCT",    "60"))  # skip buckets below this fused-confidence %
KELLY_FRACTION         = float(os.getenv("KELLY_FRACTION",        "0.25")) # fraction of full Kelly (0.25 = quarter-Kelly)
MARKET_HISTORY_FILE    = os.getenv("MARKET_HISTORY_FILE", "market_history.json")
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
    "token_id", "market_key",   # added for cross-restart position restore
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

# Token IDs already bought (ever) — populated from CLOB trade history on startup
# Prevents duplicate buys across redeploys
traded_token_ids: set = set()

# Sequential session counter
session_counter = 0

# ── Single-market focus lock (Method 4) ──────────────────────────────────────────────
# Bot focuses on ONE market at a time (earliest-expiring).
# A lock is set when we first encounter a market. While locked,
# all other markets are ignored. Lock clears when market ends.
_locked_market_id:       str           = ""    # Gamma/Polymarket market ID
_locked_market_end_dt:   Optional[datetime] = None   # UTC end time of locked market
_locked_market_title:    str           = ""    # human-readable (for notifications)
_locked_market_start_dt: Optional[datetime] = None   # to compute market age hours
_monitored_notified_ids: set           = set()  # IDs we've sent the 12h-gate notice for

# ── Method 5: dynamic prior strength (empirical Bayes) ────────────────────────
_dynamic_beta0: float = 0.0  # 0 = use default; computed from market history when > 0

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

    Uses signature_type=1 (POLY_PROXY) for email/Magic.link accounts.

    Confirmed via py_order_utils source:
      EOA             = 0  (raw private key, no proxy)
      POLY_PROXY      = 1  (Magic.link / email login wallet) ← CORRECT FOR US
      POLY_GNOSIS_SAFE= 2  (browser extension + Gnosis Safe)

    The polymarket-cli uses DEFAULT_SIGNATURE_TYPE="proxy" → SignatureType::Proxy
    which the CLOB identifies as integer 1 (POLY_PROXY).
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
            signature_type=1,        # POLY_PROXY — email/Magic.link accounts
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
    """Get USDC trading balance — reads on-chain from Alchemy.

    The CLOB API's get_balance_allowance() returns 'Invalid asset type' for
    email/magic-link proxy accounts. The on-chain balance at the proxy wallet
    is the real source of truth and works reliably via Alchemy RPC.
    """
    return await get_eoa_usdc_balance()


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

# Historical rate cache (EWMA of past completed periods)
_hist_rate_cache: Optional[float] = None  # tweets/hr (EWMA)
_hist_rate_cache_ts: float = 0.0
_HIST_RATE_CACHE_TTL = 86400              # recompute every 24 hours
_HIST_RATE_DEFAULT   = 1.4               # fallback if no history yet (tweets/hr)
_BAYESIAN_BETA0      = 48.0              # prior equivalent hours (strength of prior)
_CREDIBILITY_K       = 30.0              # tweets needed for 50% credibility weight


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


async def compute_historical_rate() -> float:
    """Auto-compute Elon's historical tweet rate (tweets/hr) from past XTracker periods.

    Fetches the last 5 COMPLETED tracking periods, computes each period's rate,
    then returns an EWMA-weighted average (most recent weighted highest, α=0.4).

    Fully automatic — no user input needed. Cached for 24 hours.
    Falls back to _HIST_RATE_DEFAULT (1.4/hr) if data unavailable.
    """
    global _hist_rate_cache, _hist_rate_cache_ts
    now = time.time()
    if _hist_rate_cache is not None and (now - _hist_rate_cache_ts) < _HIST_RATE_CACHE_TTL:
        return _hist_rate_cache

    trackings = await _load_xtracker_trackings()
    now_utc = datetime.now(timezone.utc)

    # Find completed trackings (endDate in the past)
    completed = []
    for t in trackings:
        try:
            end = datetime.fromisoformat(t.get("endDate", "").replace("Z", "+00:00"))
            start = datetime.fromisoformat(t.get("startDate", "").replace("Z", "+00:00"))
            if end < now_utc:
                duration_hrs = max(1.0, (end - start).total_seconds() / 3600)
                completed.append((end, duration_hrs, t))
        except Exception:
            continue

    # Sort by end date — most recent first
    completed.sort(key=lambda x: x[0], reverse=True)
    completed = completed[:7]  # use last 7 completed periods

    if not completed:
        log.warning("compute_historical_rate: no completed periods found — using default %.2f/hr",
                    _HIST_RATE_DEFAULT)
        _hist_rate_cache = _HIST_RATE_DEFAULT
        _hist_rate_cache_ts = now
        return _HIST_RATE_DEFAULT

    # Fetch stats for each completed period and compute rates
    rates = []
    async with httpx.AsyncClient(timeout=15) as http:
        for end_dt, duration_hrs, t in completed:
            try:
                tracking_id = t["id"]
                r = await http.get(
                    f"{XTRACKER_API}/trackings/{tracking_id}",
                    params={"includeStats": "true"},
                )
                r.raise_for_status()
                body = r.json()
                data = body.get("data", body) if isinstance(body, dict) else body
                stats = data.get("stats", {})
                total = float(stats.get("total", 0))
                if total > 0 and duration_hrs > 0:
                    rate = total / duration_hrs
                    rates.append(rate)
                    log.debug("Historical period '%s': %.0f tweets / %.0fh = %.2f/hr",
                              t.get("title", ""), total, duration_hrs, rate)
            except Exception as e:
                log.debug("compute_historical_rate: skipping period %s: %s", t.get("id", "?"), e)
                continue

    if not rates:
        log.warning("compute_historical_rate: all fetches failed — using default")
        _hist_rate_cache = _HIST_RATE_DEFAULT
        _hist_rate_cache_ts = now
        return _HIST_RATE_DEFAULT

    # EWMA: most recent period gets highest weight
    # rates[0] = most recent, rates[-1] = oldest
    alpha = 0.4
    ewma = rates[0]
    for r in rates[1:]:
        ewma = alpha * ewma + (1 - alpha) * r

    log.info("Historical EWMA rate: %.3f/hr from %d periods (individual: %s)",
             ewma, len(rates), [f"{r:.2f}" for r in rates])
    _hist_rate_cache = ewma
    _hist_rate_cache_ts = now
    return ewma


def compute_bucket_probabilities(tokens: list, mu: float, sigma: float) -> list:
    """Compute P(bucket wins) for every token bucket using Normal approximation.

    Args:
        tokens:  list of {token_id, outcome, price} from process_market
        mu:      projected final tweet count (point estimate)
        sigma:   standard deviation of projection uncertainty

    Returns:
        list of (prob, token_id, label, low, high) sorted by prob descending
    """
    if sigma <= 0:
        sigma = 1.0  # safety guard

    nd = NormalDist(mu=mu, sigma=sigma)
    results = []

    for t in tokens:
        label = t.get("outcome", "")
        bounds = parse_bucket_label(label)
        if bounds is None:
            continue
        lo, hi = bounds

        # P(lo ≤ X ≤ hi) with continuity correction
        if hi == 9999:
            # Open-ended bucket: P(X ≥ lo - 0.5)
            prob = 1.0 - nd.cdf(lo - 0.5)
        else:
            prob = nd.cdf(hi + 0.5) - nd.cdf(lo - 0.5)

        prob = max(0.0, min(1.0, prob))
        results.append((prob, t["token_id"], label, lo, hi))

    # Sort by probability descending
    results.sort(key=lambda x: x[0], reverse=True)
    return results


# ──────────────────────────────────────────────────────────────────────
# SECTION 7b — HELPER ENGINES (History · Kelly · Confidence · Reasoning)
# ──────────────────────────────────────────────────────────────────────

def load_market_history() -> dict:
    """Load market history JSON. Returns {beta0, markets:[...]}."""
    if not os.path.exists(MARKET_HISTORY_FILE):
        return {"beta0": 0.0, "markets": []}
    try:
        with open(MARKET_HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.warning("load_market_history error: %s", e)
        return {"beta0": 0.0, "markets": []}


def save_market_history(history: dict) -> None:
    """Persist market history JSON to disk."""
    try:
        with open(MARKET_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2, default=str)
    except Exception as e:
        log.error("save_market_history error: %s", e)


def compute_dynamic_beta0_from_history(markets: list) -> float:
    """Method 5 — Empirical Bayes: compute optimal prior strength β₀.

    β₀ = mean_rate / variance_between_periods

    Intuition: if Elon's rate is very consistent across weeks (low variance),
    we need less live data to be confident → smaller β₀. If it varies a lot,
    we need more data before trusting live observations → larger β₀.

    Requires ≥ 3 completed markets. Returns 0.0 (use default) if insufficient.
    """
    import statistics as _stats
    rates = [m["rate_per_hour"] for m in markets if m.get("rate_per_hour", 0) > 0]
    if len(rates) < 3:
        return 0.0
    try:
        mean_r = _stats.mean(rates)
        var_r  = _stats.variance(rates)
        if var_r < 0.001:
            return 48.0  # very stable pace → use standard prior
        beta0 = mean_r / var_r
        beta0 = max(8.0, min(168.0, beta0))
        log.info("Method 5: β₀=%.1f from %d periods (mean=%.2f/hr, var=%.4f)",
                 beta0, len(rates), mean_r, var_r)
        return beta0
    except Exception as e:
        log.warning("compute_dynamic_beta0 error: %s", e)
        return 0.0


async def record_completed_market(tracking_id: str, title: str,
                                  total_tweets: float, hours: float) -> None:
    """Method 5 — Record a completed market and recompute β₀ from history."""
    global _dynamic_beta0
    if hours <= 0 or total_tweets <= 0:
        return
    rate    = total_tweets / hours
    history = load_market_history()
    markets = history.get("markets", [])
    if any(m.get("tracking_id") == tracking_id for m in markets):
        return   # already recorded
    markets.append({
        "tracking_id":  tracking_id,
        "title":        title,
        "total_tweets": round(total_tweets, 0),
        "hours":        round(hours, 2),
        "rate_per_hour": round(rate, 4),
        "recorded_at":  datetime.now(timezone.utc).isoformat(),
    })
    markets = markets[-20:]   # keep last 20 markets
    new_beta0 = compute_dynamic_beta0_from_history(markets)
    history   = {"beta0": new_beta0, "markets": markets}
    save_market_history(history)
    if new_beta0 > 0:
        _dynamic_beta0 = new_beta0
        await send_message(None,
            f"🧬 <b>Bot self-updated</b> (Method 5)\n"
            f"  Recorded: <i>{title[:50]}</i> — {int(total_tweets)} tweets, {rate:.2f}/hr\n"
            f"  New prior strength: β₀ = {new_beta0:.1f}h  "
            f"  (from {len(markets)} markets)")
    log.info("Method 5: recorded '%s': %.0f tweets / %.0fh = %.3f/hr",
             title[:40], total_tweets, hours, rate)


def kelly_bet_size(p_fused: float, price: float,
                   balance: float, max_bet: float) -> float:
    """Kelly Criterion: compute optimal bet size, floored at $1, capped at max_bet.

    Returns 0.0 to skip the bet when edge is negative.

    Uses KELLY_FRACTION (default 0.25 = quarter-Kelly) — conservative multiplier
    that protects bankroll even when our probability estimate is slightly off.

    Formula:  edge    = p_fused × (1/price) − 1       (expected value per $1)
              f_full  = edge / (1/price − 1)           (full Kelly fraction)
              f_safe  = f_full × KELLY_FRACTION        (scaled down for safety)
              bet     = balance × f_safe               (absolute bet size)

    Example with $5 balance, price=$0.12, p_fused=0.65:
      edge   = 0.65/0.12 − 1 = 4.42
      f_full = 4.42 / 7.33   = 0.60  (60% full Kelly)
      f_safe = 0.60 × 0.25   = 0.15  (15% quarter-Kelly)
      bet    = $5 × 0.15     = $0.75 → rounds up to $1 minimum
    """
    if price <= 0 or price >= 1 or p_fused <= 0:
        return max_bet  # no data → use full size
    b    = (1.0 / price) - 1.0   # net odds per $1 staked
    if b <= 0:
        return 0.0
    edge = p_fused * (1.0 + b) - 1.0   # = p_fused/price − 1
    if edge <= 0:
        return 0.0   # negative edge → skip → protect capital
    f_full = edge / b
    f_safe = f_full * KELLY_FRACTION    # quarter-Kelly when KELLY_FRACTION=0.25
    bet    = balance * f_safe
    bet    = min(bet, max_bet)
    if bet < 1.0:
        return 1.0 if edge > 0.08 else 0.0  # only pay $1 minimum if edge is meaningful
    return round(bet, 2)


def confidence_score(p_fused: float, cred_z: float,
                     market_age_hrs: float, has_mismatch: bool) -> int:
    """Compute overall decision confidence 0–100.

    base  = fused probability × 100
    ±adj  = credibility factor (Z) adjustment
    +bonus= market age (older = more data = more reliable)
    −pen  = period mismatch (tracking doesn't match market)
    """
    base       = p_fused * 100
    z_adj      = (cred_z - 0.5) * 20          # −10 … +10
    age_bonus  = min(10.0, max(0.0, (market_age_hrs - 12) / 3.6))   # +0 … +10
    mismatch_p = -15.0 if has_mismatch else 0.0
    return max(5, min(95, int(base + z_adj + age_bonus + mismatch_p)))


def format_entry_reason(label: str, p_fused: float, p_sabp: float, p_market: float,
                        cred_z: float, pace: dict, bet_usd: float, conf: int) -> str:
    """Plain-English reasoning for entering a bucket."""
    proj    = int(pace.get("projected", 0))
    sigma   = pace.get("sigma", 0.0)
    total   = int(pace.get("total", 0))
    hrs_el  = pace.get("hours_elapsed", 0)
    proj_lo = max(0, proj - int(sigma))
    proj_hi = proj + int(sigma)
    edge    = p_sabp - p_market
    edge_str = (
        f"we see {edge*100:.0f}pt edge above market ✨" if edge > 0.12 else
        f"slight edge vs market (+{edge*100:.0f}pt)"   if edge > 0.04 else
        "aligned with market"                           if edge >= -0.04 else
        f"market disagrees (caution, -{abs(edge)*100:.0f}pt)"
    )
    data_str = (
        f"Low live data ({total} tweets / {hrs_el:.0f}h) — anchored to history" if cred_z < 0.2 else
        f"Mixed: {cred_z:.0%} live + {(1-cred_z):.0%} history"                 if cred_z < 0.6 else
        f"Strong live data ({cred_z:.0%} weight)"
    )
    return (
        f"🧠 <b>Why buying {label}:</b>\n"
        f"  Projection: {proj} tweets (range {proj_lo}–{proj_hi})\n"
        f"  Bucket chance: <b>{p_fused*100:.0f}%</b> — {edge_str}\n"
        f"  Data quality: {data_str}\n"
        f"  Bet size: <b>${bet_usd:.2f}</b>  ·  <b>Confidence: {conf}/100</b>"
    )


def format_skip_reason(label: str, reason: str, conf: int = 0) -> str:
    """Plain-English reasoning for skipping a bucket."""
    conf_tag = f"  (confidence: {conf}/100)" if conf > 0 else ""
    return f"⏭ <b>Skipping {label}:</b> {reason}{conf_tag}"


def format_monitor_message(title: str, market_age_hrs: float,
                           pace: Optional[dict]) -> str:
    """Notification sent when 12h gate blocks entry."""
    wait_hrs = max(0.0, MIN_MARKET_AGE_HOURS - market_age_hrs)
    pace_line = ""
    if pace:
        proj   = int(pace.get("projected", 0))
        total  = int(pace.get("total", 0))
        cred_z = pace.get("credibility_z", 0.0)
        pace_line = (
            f"\n  Early data: {total} tweets so far ({cred_z:.0%} reliable)"
            f"\n  Early estimate: ~{proj} tweets"
        )
    return (
        f"📍 <b>Monitoring</b> — {title[:50]}\n"
        f"  Market is only {market_age_hrs:.1f}h old — need {MIN_MARKET_AGE_HOURS:.0f}h minimum\n"
        f"  ⏳ Will re-assess in ~{wait_hrs:.1f}h"
        f"{pace_line}"
    )


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
            tracking_duration_hrs = max(1.0, (end_dt - start_dt).total_seconds() / 3600)
        except Exception:
            hours_elapsed         = float(stats.get("daysElapsed",   1)) * 24
            hours_remaining       = float(stats.get("daysRemaining", 0)) * 24
            tracking_duration_hrs = hours_elapsed + hours_remaining

        # ── SABP: Period mismatch detection ──────────────────────────────
        # If the tracking covers a much longer period than the market (e.g.
        # a monthly tracking matched to a weekly market), the tweet count
        # and hourly_avg will be wrong. Detect and flag this.
        # We use the market's own endDate if we know it, otherwise we check
        # the tracking's own duration vs a typical weekly market (~168 hrs).
        period_mismatch = False
        expected_market_hrs = 168.0  # typical weekly Elon market
        if tracking_duration_hrs > expected_market_hrs * 2.5:
            period_mismatch = True
            log.warning(
                "SABP: Period mismatch detected — tracking covers %.0fh but market "
                "is typically %.0fh. Using historical rate only.",
                tracking_duration_hrs, expected_market_hrs,
            )

        # ── SABP: Auto-compute λ_hist (EWMA of past periods) ─────────────
        lambda_hist = await compute_historical_rate()

        # ── SABP: Bayesian Gamma-Poisson posterior rate ───────────────────
        # Prior:     λ ~ Gamma(α₀, β₀)  where α₀ = λ_hist × β₀
        # Posterior: λ | data ~ Gamma(α₀ + n, β₀ + h_e)
        # Posterior mean: λ̂ = (α₀ + n) / (β₀ + h_e)
        beta0  = _dynamic_beta0 if _dynamic_beta0 > 0 else _BAYESIAN_BETA0  # Method 5: adaptive
        alpha0 = lambda_hist * beta0              # prior pseudo-count

        if period_mismatch:
            # Tracking doesn't match market — ignore observed rate entirely
            lambda_posterior = lambda_hist
            lambda_obs       = lambda_hist        # display only
            credibility_z    = 0.0
        else:
            lambda_obs       = total / hours_elapsed
            lambda_posterior = (alpha0 + total) / (beta0 + hours_elapsed)
            # Credibility factor: how much we trust current data vs history
            credibility_z    = total / (total + _CREDIBILITY_K)

        # ── SABP: Projection with uncertainty (σ) ────────────────────────
        # Point estimate: n + λ̂ × h_r
        # Uncertainty: σ = √(λ̂ × h_r)  — Poisson variance of future tweets
        projected_remaining = lambda_posterior * hours_remaining
        projected           = total + projected_remaining
        sigma               = math.sqrt(max(1.0, projected_remaining))

        # Keep legacy bias key False (Bayesian prior replaces the old bias)
        daily_avg = lambda_posterior * 24

        if   projected < 200:  tier = "🟢 Low"
        elif projected < 400:  tier = "🟡 Medium"
        elif projected < 600:  tier = "🟠 High"
        else:                  tier = "🔴 Very High"

        log.info(
            "SABP: total=%d  h_e=%.1f  h_r=%.1f  "
            "λ_hist=%.2f  λ_obs=%.2f  λ̂=%.2f  Z=%.2f  "
            "projected=%.0f±%.0f  mismatch=%s",
            total, hours_elapsed, hours_remaining,
            lambda_hist, lambda_obs, lambda_posterior, credibility_z,
            projected, sigma, period_mismatch,
        )

        return {
            "tracking_id":      tracking_id,
            "title":            tracking.get("title", ""),
            "start_date":       start_date,
            "end_date":         end_date,
            "total":            total,
            "hourly_avg":       lambda_posterior,   # posterior rate (was raw rate)
            "daily_avg":        daily_avg,
            "hours_elapsed":    hours_elapsed,
            "hours_remaining":  hours_remaining,
            "days_elapsed":     hours_elapsed / 24,
            "days_remaining":   hours_remaining / 24,
            "projected":        projected,
            "sigma":            sigma,
            "pct_complete":     pct_complete,
            "tier":             tier,
            "has_bias":         False,
            # SABP diagnostics
            "lambda_hist":      lambda_hist,
            "lambda_obs":       lambda_obs,
            "lambda_posterior": lambda_posterior,
            "credibility_z":    credibility_z,
            "period_mismatch":  period_mismatch,
        }

    except Exception as e:
        log.error("fetch_elon_pace error: %s", e)
        return None




# ──────────────────────────────────────────────────────────────────────
# SECTION 8 — BUCKET STRATEGY
# ──────────────────────────────────────────────────────────────────────

def parse_bucket_label(label: str) -> Optional[tuple]:
    """Parse a bucket outcome label into (low_bound, high_bound) integers.

    Handles all formats observed on Polymarket:
      "220-239"    -> (220, 239)
      "580+"       -> (580, 9999)
      "320+"       -> (320, 9999)
      "<20"        -> (0, 19)
      "Under 100"  -> (0, 99)
      "100-119"    -> (100, 119)
      "Other"      -> None (skip)
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
    """SABP + Method 3: fused probability bucket selection.

    Fuses SABP Bayesian probability with market's implied probability (price),
    weighted by credibility Z. Applies Kelly/confidence skip. Returns 7-tuples:
        (token_id, label, slot_idx, low, high, p_fused, confidence)
    """
    projected = pace["projected"]
    sigma     = pace.get("sigma", 0.0)
    cred_z    = pace.get("credibility_z", 0.0)
    mismatch  = pace.get("period_mismatch", False)
    market_age = pace.get("hours_elapsed", MIN_MARKET_AGE_HOURS)

    # Parse all tokens, preserving price
    parsed = []
    for t in tokens:
        label  = t.get("outcome", "")
        bounds = parse_bucket_label(label)
        if bounds is None:
            continue
        low, high = bounds
        parsed.append((low, high, t["token_id"], label, t.get("price", 0.0)))
    parsed.sort(key=lambda x: x[0])

    if not parsed:
        return []

    # Skip bucket[0] (lowest — Elon never tweets that little)
    candidates = parsed[1:] if len(parsed) > 1 else parsed

    if sigma > 0:
        # Build token dicts for SABP probability computation
        cand_tokens = [
            {"token_id": tid, "outcome": lbl}
            for (lo, hi, tid, lbl, _price) in candidates
        ]
        price_map = {tid: price for (lo, hi, tid, lbl, price) in candidates}
        sabp_probs = compute_bucket_probabilities(cand_tokens, mu=projected, sigma=sigma)

        # Fuse: p_fused = Z × p_sabp + (1-Z) × p_market
        fused = []
        for (p_sabp, token_id, label, lo, hi) in sabp_probs:
            p_market = max(0.01, min(0.99, price_map.get(token_id, p_sabp)))
            p_fused  = cred_z * p_sabp + (1.0 - cred_z) * p_market
            conf     = confidence_score(p_fused, cred_z, market_age, mismatch)
            if p_fused * 100 >= MIN_CONFIDENCE_PCT:
                fused.append((p_fused, p_sabp, p_market, token_id, label, lo, hi, conf))

        fused.sort(key=lambda x: x[0], reverse=True)
        top = fused[:BUCKETS_TO_BUY]

        if not top:
            # Confidence gate blocked all — relax and take top SABP
            log.warning("select_buckets: all below %.0f%% confidence — relaxing gate",
                        MIN_CONFIDENCE_PCT)
            for (p_sabp, token_id, label, lo, hi) in sabp_probs[:BUCKETS_TO_BUY]:
                p_market = price_map.get(token_id, p_sabp)
                p_fused  = cred_z * p_sabp + (1.0 - cred_z) * p_market
                conf     = confidence_score(p_fused, cred_z, market_age, mismatch)
                top.append((p_fused, p_sabp, p_market, token_id, label, lo, hi, conf))

        result = []
        for slot_idx, (p_fused, p_sabp, p_market, token_id, label, lo, hi, conf) in enumerate(top):
            result.append((token_id, label, slot_idx, lo, hi, p_fused, conf))

        log.info("SABP+M3: μ=%.0f σ=%.1f Z=%.0f%% → %s",
                 projected, sigma, cred_z*100,
                 [(f"{lbl}({pf*100:.0f}%)") for (pf, *_, lbl, lo, hi, c) in top])
        return result

    # Legacy fallback (no sigma)
    log.warning("select_buckets: no sigma — using center-based fallback")
    center_idx = None
    cands_simple = [(lo, hi, tid, lbl) for (lo, hi, tid, lbl, _p) in candidates]
    for i, (lo, hi, tid, lbl) in enumerate(cands_simple):
        if lo <= projected <= hi or (hi == 9999 and projected >= lo):
            center_idx = i
            break
    if center_idx is None:
        for i, (lo, hi, tid, lbl) in enumerate(cands_simple):
            if lo > projected:
                center_idx = i
                break
    if center_idx is None:
        center_idx = len(cands_simple) - 1
    selected = [cands_simple[center_idx]]
    if center_idx - 1 >= 0:
        selected.append(cands_simple[center_idx - 1])
    if center_idx - 2 >= 0:
        selected.append(cands_simple[center_idx - 2])
    return [(tid, lbl, si, lo, hi, 0.0, 50)
            for si, (lo, hi, tid, lbl) in enumerate(selected)]


def describe_bucket_analysis(tokens: list, pace: dict) -> str:
    """Generate a probability-annotated bucket analysis for the pre-buy alert."""
    projected = pace["projected"]
    sigma     = pace.get("sigma", 0.0)
    lines = []

    if sigma > 0:
        nd = NormalDist(mu=projected, sigma=sigma)

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
            lines.append(f"  ⛔ {label:15s}  [always skipped]")
            continue

        # Compute probability if we have sigma
        if sigma > 0:
            if high == 9999:
                prob = 1.0 - nd.cdf(low - 0.5)
            else:
                prob = nd.cdf(high + 0.5) - nd.cdf(low - 0.5)
            prob_str = f"  {prob*100:4.1f}%"
        else:
            prob_str = ""

        if low <= projected <= high or (high == 9999 and projected >= low):
            lines.append(f"  🎯 {label:15s}{prob_str}  [PEAK — proj {projected:.0f} here]")
        elif sigma > 0 and prob > 0.10:
            lines.append(f"  ✅ {label:15s}{prob_str}  [in range]")
        elif sigma > 0 and prob < 0.02:
            lines.append(f"  ⬜ {label:15s}{prob_str}  [unlikely]")
        else:
            lines.append(f"  ✅ {label:15s}{prob_str}")

    return "\n".join(lines[:20])  # cap UI length


# ──────────────────────────────────────────────────────────────────────
# SECTION 9 — MARKET FETCHING (Gamma API + CLI)
# ──────────────────────────────────────────────────────────────────────


def normalize_token_id(token_id: str) -> str:
    """Convert hex token ID (0x...) to decimal string as required by the CLOB API.

    The Gamma HTTP API returns token IDs in decimal format:
      '8501497159083948713316135768103773293754490207922884688769443031624417212426'

    The polymarket-cli formats them in hex using Rust's default Display:
      '0xff3726cc58c499da70c5f9e7e5b99a76a39e3f3...'

    The CLOB's get_order_book and post_order endpoints ONLY accept decimal format.
    """
    if isinstance(token_id, str) and token_id.lower().startswith("0x"):
        try:
            return str(int(token_id, 16))
        except ValueError:
            pass
    return token_id


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
            yes_token  = normalize_token_id(token_ids[0])  if token_ids  else None
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
    """Send a clear pre-trade announcement with SABP diagnostics."""
    question = market.get("question", "Unknown")
    end_raw  = (market.get("endDate") or "")[:10]
    slug     = market.get("slug", "")
    pm_link  = f"https://polymarket.com/event/{slug}" if slug else POLYMARKET_BASE
    mode_tag = "🔄" if is_ongoing else "🆕"

    if pace:
        proj       = int(pace["projected"])
        total_tw   = int(pace["total"])
        hrs_rem    = pace["hours_remaining"]
        # Rates
        lam_post   = pace.get("lambda_posterior", pace["hourly_avg"])
        lam_hist   = pace.get("lambda_hist", 0.0)
        lam_obs    = pace.get("lambda_obs",  lam_post)
        cred_z     = pace.get("credibility_z", 0.0)
        sigma      = pace.get("sigma", 0.0)
        mismatch   = pace.get("period_mismatch", False)

        if hrs_rem < 1:
            time_left = f"{int(hrs_rem * 60)}min left"
        elif hrs_rem < 24:
            time_left = f"{hrs_rem:.1f}h left"
        else:
            time_left = f"{hrs_rem/24:.1f}d left"

        # Credibility label
        if cred_z < 0.2:
            cred_label = "history-anchored"
        elif cred_z < 0.6:
            cred_label = "blending"
        else:
            cred_label = "data-driven"

        mismatch_tag = "  ⚠️ Estimate based on history only (period mismatch)" if mismatch else ""
        proj_lo = max(0, proj - int(sigma))
        proj_hi = proj + int(sigma)

        if cred_z < 0.2:
            conf_label = f"Low — only {total} tweets seen so far, using history"
        elif cred_z < 0.6:
            conf_label = f"Medium — {cred_z:.0%} live data + {(1-cred_z):.0%} history"
        else:
            conf_label = f"High — {cred_z:.0%} from live data"

        pace_line = (
            f"📊 <b>{total_tw}</b> tweets so far  ·  {time_left}\n"
            f"   Tweeting at <b>{lam_post:.1f}/hr</b>  ·  Confidence: {conf_label}\n"
            f"   Projected: <b>{proj} tweets</b>  (likely range: {proj_lo}–{proj_hi}){mismatch_tag}"
        )
    else:
        pace_line = "📊 Pace: unavailable"

    # Planned buckets — with probabilities if available
    if planned:
        if pace and pace.get("sigma", 0) > 0:
            mu    = pace["projected"]
            sigma = pace["sigma"]
            nd    = NormalDist(mu=mu, sigma=sigma)
            def bucket_prob(lbl):
                bounds = parse_bucket_label(lbl)
                if not bounds:
                    return ""
                lo, hi = bounds
                p = (1 - nd.cdf(lo - 0.5)) if hi == 9999 else (nd.cdf(hi + 0.5) - nd.cdf(lo - 0.5))
                return f"({p*100:.0f}%)"
            bucket_list = "  ".join(
                f"<b>{lbl}</b>{bucket_prob(lbl)} ${price:.3f}"
                for lbl, price in planned
            )
        else:
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
    global session_counter, _locked_market_id, _locked_market_end_dt
    global _locked_market_title, _locked_market_start_dt, _monitored_notified_ids

    question   = market.get("question", "Unknown")
    market_key = question[:30]
    market_id  = market.get("id", market.get("conditionId", ""))

    # ── SINGLE-MARKET FOCUS LOCK (Method 4) ────────────────────────────────────
    now_utc = datetime.now(timezone.utc)
    if _locked_market_id and _locked_market_id != market_id:
        # We're locked onto a different market — ignore this one entirely
        log.debug("Locked on %s — ignoring %s", _locked_market_title[:30], question[:30])
        return False

    # Set the lock the first time we see this market
    if not _locked_market_id:
        _locked_market_id    = market_id
        _locked_market_title = question
        # Parse market start date from pace or market dict
        start_raw = market.get("startDate") or market.get("created_at", "")
        try:
            _locked_market_start_dt = datetime.fromisoformat(
                start_raw.replace("Z", "+00:00")
            )
        except Exception:
            _locked_market_start_dt = now_utc
        end_raw = market.get("endDate") or ""
        try:
            _locked_market_end_dt = datetime.fromisoformat(end_raw.replace("Z", "+00:00"))
        except Exception:
            _locked_market_end_dt = None
        log.info("Locked onto market: %s", question[:60])

    # ── 12-HOUR ENTRY GATE (Method 4) ───────────────────────────────────────
    # Don't enter until the market has been running for MIN_MARKET_AGE_HOURS.
    # This ensures we have real data before committing capital.
    if _locked_market_start_dt:
        market_age_hrs = (now_utc - _locked_market_start_dt).total_seconds() / 3600
    else:
        market_age_hrs = MIN_MARKET_AGE_HOURS  # assume old enough if unknown

    if market_age_hrs < MIN_MARKET_AGE_HOURS:
        # Gate blocked — notify once then wait
        if market_id not in _monitored_notified_ids:
            _monitored_notified_ids.add(market_id)
            # Fetch early pace for context
            early_pace = await fetch_elon_pace(market_slug=market.get("slug", ""))
            await send_message(app, format_monitor_message(
                question, market_age_hrs, early_pace))
        return False  # not ready to trade yet
    # ──────────────────────────────────────────────────────────────────────

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

    clob_ids = [normalize_token_id(t) for t in clob_ids]

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
            (t["token_id"], t.get("outcome", ""), i, 0, 9999, 0.0, 50)
            for i, t in enumerate(tokens[:BUCKETS_TO_BUY])
        ]

    # ── IMPROVEMENT #3: Skip CENTER bucket if YES price > $0.50 ─────────
    # If the market has already priced in a bucket too heavily (>50¢), buying
    # it gives <2× return even if it resolves YES. Skip to the next bucket up.
    filtered_tokens = []
    for tok in selected_tokens:
        token_id, label, slot_idx, low, high, p_fused, conf = tok[:5] + (tok[5] if len(tok) > 5 else 0.0,) + (tok[6] if len(tok) > 6 else 50,)
        token_price = next(
            (float(t["price"]) for t in tokens if t["token_id"] == token_id), 0.0
        )
        if token_price > 0.50 and slot_idx == 0:  # slot 0 = CENTER bucket
            reason = f"price ${token_price:.3f} > $0.50 — return < 2x, not worth the risk"
            await send_message(app, format_skip_reason(label, reason, conf))
            log.info("CENTER bucket '%s' skipped: %s", label, reason)
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
    for tok in selected_tokens:
        token_id, label = tok[0], tok[1]
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
    for tok in selected_tokens:
        token_id, label, slot_idx = tok[0], tok[1], tok[2]
        low, high = tok[3], tok[4]
        p_fused    = tok[5] if len(tok) > 5 else 0.0
        bucket_conf = tok[6] if len(tok) > 6 else 50
        tp_mult = TP_SLOTS[slot_idx] if slot_idx < len(TP_SLOTS) else 2.0

        # Hard cap: never hold more than MAX_OPEN_ORDERS simultaneously
        if len(open_positions) >= MAX_OPEN_ORDERS:
            await send_message(app,
                f"🚫 Open-order cap reached ({MAX_OPEN_ORDERS}) — "
                f"skipping bucket <b>{label}</b> until a position closes.")
            break

        # Duplicate guard: skip if this token was already bought (any session)
        if token_id in traded_token_ids:
            log.info("Skipping %s — already traded token %s…", label, token_id[:16])
            continue

        # Per-bucket balance check (balance changes after each fill)
        current_balance = await get_proxy_balance()
        if current_balance < ORDER_SIZE_USD:
            await send_message(app,
                f"💸 Balance too low to buy bucket <b>{label}</b> — "
                f"need ${ORDER_SIZE_USD:.2f}, have ${current_balance:.2f}. "
                f"Skipping remaining buckets.")
            break  # stop trying more buckets this cycle

        # Step 5: Fetch orderbook (with retry for empty books)
        book = await fetch_orderbook_with_retry(token_id)
        is_fallback_gtc = False

        if book is None:
            # Total fetch failure — token_id likely invalid or market not live yet.
            # Do NOT place an order; just skip this bucket.
            log.warning("Orderbook fetch failed entirely for %s (token: %s…) — skipping",
                        label, token_id[:16])
            await send_message(app,
                f"⚠️ Bucket <b>{label}</b> skipped — orderbook unreachable\n"
                f"  Token: <code>{token_id[:20]}…</code>")
            continue

        if not book.get("asks"):
            # Orderbook returned but has no asks — GTC fallback at preset price
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

        # ── Kelly Criterion sizing ──────────────────────────────────────────────
        # bet_usd: Kelly-optimal per-bucket bet, floored at $1, capped at ORDER_SIZE_USD
        kelly_usd = kelly_bet_size(p_fused, best_ask, current_balance, ORDER_SIZE_USD)
        if kelly_usd == 0.0:
            reason = (
                f"negative expected value at ${best_ask:.3f} "
                f"with {p_fused*100:.0f}% estimated chance — protecting capital"
            )
            await send_message(app, format_skip_reason(label, reason, bucket_conf))
            log.info("Kelly skip: %s (p_fused=%.2f, price=%.3f)", label, p_fused, best_ask)
            continue
        bet_usd = kelly_usd

        # ── Entry reasoning before placing ──────────────────────────────────────
        if pace:
            p_sabp   = pace.get("sabp_prob_"+label, p_fused)  # best-effort
            p_market = best_ask  # market's implied probability
            await send_message(app, format_entry_reason(
                label, p_fused, p_fused, p_market,
                pace.get("credibility_z", 0.0), pace, bet_usd, bucket_conf
            ))

        # size in SHARES, not USD
        raw_shares  = bet_usd / best_ask
        size_shares = math.ceil(raw_shares * 10_000) / 10_000
        tp_target   = best_ask * tp_mult

        log.info("Placing order: bucket=%s token=%s… price=%.4f size=%.4f",
                 label, token_id[:16], exec_price, size_shares)
        try:
            order_args = OrderArgs(
                token_id=token_id,
                price=round(exec_price, 4),
                size=round(size_shares, 4),
                side=BUY,
            )
            signed = await run_clob(clob.create_order, order_args)
            order_type = OrderType.GTC
            resp = await run_clob(clob.post_order, signed, order_type)

            order_id = resp.get("orderID", resp.get("id", f"local_{int(time.time())}"))
        except Exception as e:
            log.error("Order placement failed for %s (token %s…): %s",
                      label, token_id[:16], e)
            await send_message(app,
                f"❌ Order failed for bucket <b>{label}</b>:\n"
                f"  Token: <code>{token_id[:24]}…</code>\n"
                f"  Error: <code>{str(e)[:200]}</code>")
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

        # Mark this token as bought — prevents re-buying across restarts
        traded_token_ids.add(token_id)

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
            "token_id":        token_id,
            "market_key":      market_key,
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

        emoji    = "🚀" if profit_usd > 0 else "💥"
        dry_note = " [DRY RUN]" if DRY_RUN else ""
        mult_achieved = sell_price / buy_price if buy_price > 0 else 1.0
        await send_message(app,
            f"{emoji} <b>Profit taken{dry_note} — #{snum}  {pos['bucket']}</b>\n"
            f"  Why: price reached {mult_achieved:.1f}× entry (our target was {pos['tp_mult']:.1f}×)\n"
            f"  ${buy_price:.3f} → ${sell_price:.3f}  ·  "
            f"<b>${profit_usd:+.2f} ({profit_pct:+.1f}%)</b>\n"
            f"  👍 Confidence this was right: 90/100")
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
        loss_from_entry_pct = abs(loss_pct)
        await send_message(app,
            f"🛑 <b>Stop-loss triggered{dry_note} — #{snum}  {pos['bucket']}</b>\n"
            f"  Why: price fell {loss_from_entry_pct:.0f}% from entry "
            f"(threshold is {int(STOP_LOSS_PCT*100)}% drop)\n"
            f"  Market likely moved against this bucket — cutting loss now\n"
            f"  ${buy_price:.3f} → ${sell_price:.3f}  ·  "
            f"<b>${loss_usd:+.2f} ({loss_pct:+.1f}%)</b>\n"
            f"  🛡 Confidence this was right: 80/100")
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
                    "type":       "MARKET",
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
                        sub_msg = json.dumps({"assets_ids": new_ids, "type": "MARKET"})
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
    """List all open positions with per-order manual TP and SL buttons."""
    msg = update.message or update.callback_query.message
    if not open_positions:
        await msg.reply_text(
            "📭 No open positions right now."
            "\n\nWhen a market is found and orders are placed, they'll appear here.",
            parse_mode="HTML",
        )
        return

    for order_id, pos in list(open_positions.items()):
        snum = pos["session_num"]
        age  = int(time.time() - pos.get("placed_at", time.time()))
        age_str = f"{age//3600}h {(age%3600)//60}m" if age >= 3600 else f"{age//60}m {age%60}s"
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton(f"💰 Take Profit #{snum}", callback_data=f"tp_order_{snum}"),
            InlineKeyboardButton(f"🛑 Cut Loss #{snum}",   callback_data=f"sl_order_{snum}"),
        ]])
        current_tp  = pos['tp_target']
        buy_p       = pos['buy_price']
        stop_p      = round(buy_p * (1.0 - STOP_LOSS_PCT), 4)
        await msg.reply_text(
            f"📌 <b>Position #{snum}</b>  —  {age_str} old\n"
            f"  Market: {pos['market_question'][:50]}\n"
            f"  Bucket: <b>{pos['bucket']}</b>\n"
            f"  Entry:  ${buy_p:.4f}  │  Shares: {pos['size_shares']:.2f}  │  Cost: ${pos['cost_usd']:.2f}\n"
            f"  TP:     ${current_tp:.4f}  │  Stop:  ${stop_p:.4f}\n"
            f"  Order:  <code>{order_id[:26]}…</code>",
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
            params = BalanceAllowanceParams(signature_type=1)
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
        f"💰 <b>Balances (on-chain)</b>\n"
        f"  Trading balance: <b>${proxy_bal:.2f} USDC</b>\n\n"
        f"  Proxy wallet: <code>{PROXY_WALLET[:24]}…</code>\n"
        f"  EOA (signing): <code>{eoa_addr[:24]}…</code>\n\n"
        f"  {'✅ Sufficient to trade' if proxy_bal >= ORDER_SIZE_USD else '⚠️ Insufficient — deposit more USDC'}",
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
    """Show Elon tweet pace in plain English — focuses on the current weekly market."""
    msg = update.message or update.callback_query.message

    # Load all XTracker tracking periods
    trackings = await _load_xtracker_trackings()
    if not trackings:
        await msg.reply_text("⚠️ XTracker unavailable — try again shortly.",
                             parse_mode="HTML", reply_markup=main_menu_keyboard())
        return

    now_utc = datetime.now(timezone.utc)

    # Classify each tracking as active-weekly, active-monthly, or completed
    active_weekly  = []   # running markets shorter than 20 days
    completed_list = []   # finished markets

    for t in trackings:
        try:
            start = datetime.fromisoformat(t.get("startDate", "").replace("Z", "+00:00"))
            end   = datetime.fromisoformat(t.get("endDate",   "").replace("Z", "+00:00"))
            dur_hrs = max(1.0, (end - start).total_seconds() / 3600)
            hrs_rem = (end - now_utc).total_seconds() / 3600
            if start <= now_utc <= end and dur_hrs < 480:   # active, < 20 days
                active_weekly.append((hrs_rem, dur_hrs, t))
            elif end < now_utc:
                completed_list.append((end, t))
        except Exception:
            continue

    # Primary: soonest-ending weekly market (most urgent/relevant)
    active_weekly.sort(key=lambda x: x[0])
    primary_t = active_weekly[0][2] if active_weekly else None

    if primary_t is None:
        await msg.reply_text(
            "📭 No active weekly Elon tweet market found right now.\n"
            "The bot will pick one up when it appears.",
            parse_mode="HTML", reply_markup=main_menu_keyboard())
        return

    # Fetch SABP pace data for that tracking
    slug = (primary_t.get("marketLink", "") or "").rstrip("/").split("/")[-1]
    pace = await fetch_elon_pace(market_slug=slug)
    if not pace:
        await msg.reply_text("⚠️ Could not fetch pace data — try again shortly.",
                             parse_mode="HTML", reply_markup=main_menu_keyboard())
        return

    # ── Plain-English values ──────────────────────────────────────────────
    total    = int(pace["total"])
    proj     = int(pace["projected"])
    sigma    = pace.get("sigma", 0.0)
    hrs_el   = pace["hours_elapsed"]
    hrs_rem  = pace["hours_remaining"]
    speed    = pace.get("lambda_posterior", pace["hourly_avg"])  # best rate estimate
    hist_spd = pace.get("lambda_hist",  speed)
    raw_spd  = pace.get("lambda_obs",   speed)
    cred_z   = pace.get("credibility_z", 0.0)
    mismatch = pace.get("period_mismatch", False)
    title    = primary_t.get("title", "Current market")

    proj_lo = max(0, proj - int(sigma))
    proj_hi = proj + int(sigma)

    if hrs_rem < 1:
        time_left = f"{int(hrs_rem*60)}min"
    elif hrs_rem < 24:
        time_left = f"{hrs_rem:.1f} hours"
    else:
        time_left = f"{hrs_rem/24:.1f} days"

    # Activity level without emoji codes
    tier_text = {
        "🟢 Low":       "🟢 Low activity",
        "🟡 Medium":    "🟡 Medium activity",
        "🟠 High":      "🟠 High activity",
        "🔴 Very High": "🔴 Very high activity",
    }.get(pace["tier"], pace["tier"])

    # Confidence explanation in plain English
    if cred_z < 0.2:
        conf_line = (
            f"  Low — only {total} tweets so far, relying mostly on history.\n"
            f"  Historical average: <b>{hist_spd:.1f}/hr</b>"
        )
    elif cred_z < 0.6:
        conf_line = (
            f"  Medium — blending live data ({cred_z:.0%}) with history ({(1-cred_z):.0%}).\n"
            f"  Live pace: <b>{raw_spd:.1f}/hr</b>  ·  Hist. avg: <b>{hist_spd:.1f}/hr</b>"
        )
    else:
        conf_line = (
            f"  High — enough tweets seen to trust live data ({cred_z:.0%}).\n"
            f"  Live pace: <b>{raw_spd:.1f}/hr</b>  ·  Hist. avg: <b>{hist_spd:.1f}/hr</b>"
        )

    mismatch_warn = (
        "\n⚠️ <b>Note:</b> Only a monthly tracker is available right now.\n"
        "The estimate below uses historical averages, not live data."
    ) if mismatch else ""

    # ── Build main output ─────────────────────────────────────────────────
    lines = [
        f"🐦 <b>Elon Tweet Pace</b>  ·  {tier_text}",
        f"📅 <b>{title}</b>",
        f"⏱ {hrs_el:.0f}h elapsed  ·  {time_left} left",
        mismatch_warn,
        "",
        f"<b>Tweets so far:</b>  {total}",
        f"<b>Tweeting speed:</b>  <b>{speed:.1f}/hr</b>  ({speed*24:.0f}/day)",
        f"<b>Confidence:</b>",
        conf_line,
        "",
        f"🎯 <b>Predicted total:  {proj} tweets</b>",
        f"   Likely range: {proj_lo} – {proj_hi} tweets",
    ]

    # ── Other analytics: most recently COMPLETED market ───────────────────
    completed_list.sort(key=lambda x: x[0], reverse=True)
    if completed_list:
        recent_t = completed_list[0][1]
        try:
            async with httpx.AsyncClient(timeout=10) as http:
                r = await http.get(
                    f"{XTRACKER_API}/trackings/{recent_t['id']}",
                    params={"includeStats": "true"},
                )
                r.raise_for_status()
                body = r.json()
            rdata  = body.get("data", body) if isinstance(body, dict) else body
            rstats = rdata.get("stats", {})
            r_total = float(rstats.get("total", 0))
            r_start = datetime.fromisoformat(rdata.get("startDate", "").replace("Z", "+00:00"))
            r_end   = datetime.fromisoformat(rdata.get("endDate",   "").replace("Z", "+00:00"))
            r_dur   = max(1.0, (r_end - r_start).total_seconds() / 3600)
            r_rate  = r_total / r_dur
            r_title = recent_t.get("title", "Previous market")
            r_date  = r_end.strftime("%b %d")
            lines += [
                "",
                "📊 <b>Last completed market</b>",
                f"  <i>{r_title}</i>  (ended {r_date})",
                f"  Total tweets: <b>{int(r_total)}</b>  ·  Avg speed: <b>{r_rate:.1f}/hr</b> ({r_rate*24:.0f}/day)",
            ]
        except Exception as e:
            log.debug("pace_cmd: could not fetch recent completed stats: %s", e)

    await msg.reply_text(
        "\n".join(l for l in lines if l is not None),
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
            await q.message.reply_text(f"Order #{snum} not found or already closed.")
            return
        pos = open_positions[order_id]
        await q.message.reply_text(
            f"💰 Taking profit on #{snum} ({pos['bucket']}) at ${pos['tp_target']:.4f}…",
            parse_mode="HTML")
        await execute_tp(order_id, ctx.application, pos["tp_target"])

    elif data.startswith("sl_order_"):
        snum = int(data.split("_")[-1])
        order_id = order_registry.get(snum)
        if not order_id or order_id not in open_positions:
            await q.message.reply_text(f"Order #{snum} not found or already closed.")
            return
        pos = open_positions[order_id]
        token_id = pos["token_id"]
        # Fetch current best bid for market-sell price
        try:
            book = await fetch_orderbook_with_retry(token_id)
            sell_price = book["bids"][0] if (book and book.get("bids")) else pos["buy_price"] * 0.85
        except Exception:
            sell_price = pos["buy_price"] * 0.85
        await q.message.reply_text(
            f"🛑 Cutting loss on #{snum} ({pos['bucket']}) at ${sell_price:.4f}…",
            parse_mode="HTML")
        await execute_stop_loss(order_id, ctx.application, sell_price)


# ──────────────────────────────────────────────────────────────────────
# SECTION 20 — APPLICATION STARTUP
# ──────────────────────────────────────────────────────────────────────

async def load_traded_token_ids() -> int:
    """Populate traded_token_ids from CLOB trade history + open orders on startup.

    Sources (in priority order):
    1. CSV file  — fast, no API call, covers all sessions with token_id column
    2. CLOB open orders  — clob.get_orders()  → asset_id per order
    3. CLOB trade history — clob.get_trades() → asset per completed trade

    Returns total count of token IDs loaded.
    """
    global traded_token_ids

    # ── Source 1: CSV (instant, no network) ─────────────────────────────
    for row in load_csv_rows():
        tid = row.get("token_id", "").strip()
        if tid:
            traded_token_ids.add(tid)

    # ── Source 2: CLOB open orders ───────────────────────────────────────
    if clob:
        try:
            open_orders = await run_clob(clob.get_orders)
            for o in open_orders:
                tid = str(o.get("asset_id") or o.get("token_id") or "").strip()
                if tid:
                    traded_token_ids.add(tid)
            log.info("Loaded %d traded token IDs from CLOB open orders", len(open_orders))
        except Exception as e:
            log.warning("Could not load open orders for dedup: %s", e)

        # ── Source 3: CLOB trade history ─────────────────────────────────
        try:
            trades = await run_clob(clob.get_trades)
            for t in trades:
                # asset_id is the conditional token ID for this trade
                # NOTE: t.get('market') is the condition ID (0x hex), NOT the token ID
                tid = str(t.get("asset_id", "")).strip()
                if tid:
                    traded_token_ids.add(tid)
            log.info("Loaded %d traded token IDs from CLOB trade history", len(trades))
        except Exception as e:
            log.warning("Could not load trade history for dedup: %s", e)

    total = len(traded_token_ids)
    log.info("traded_token_ids populated: %d unique token IDs (no re-buy guard active)", total)
    return total


async def restore_positions_from_csv() -> int:
    """On startup, rebuild open_positions from CSV rows with buy_status=OPEN.

    Each row is verified against the CLOB API to confirm the order still exists
    and hasn't been filled/cancelled outside the bot.  Returns the count of
    successfully restored positions.
    """
    global session_counter
    rows = load_csv_rows()
    restored = 0

    for row in rows:
        if row.get("buy_status", "") != "OPEN":
            continue

        order_id  = row.get("buy_order_id", "")
        token_id  = row.get("token_id", "")
        if not order_id or not token_id:
            log.info("Skipping CSV row — missing order_id or token_id: %s", row)
            continue
        if order_id in open_positions:
            continue  # already restored

        # Verify order is still open on the CLOB
        try:
            live = await run_clob(clob.get_order, order_id)
            status = (live.get("status") or "").upper()
            # CLOB status values: LIVE (open), MATCHED (filled), CANCELED/CANCELLED
            if status in ("MATCHED", "CANCELED", "CANCELLED"):
                log.info("Skip restore for %s — CLOB status=%s", order_id[:16], status)
                continue
        except Exception as e:
            log.warning("Could not verify order %s on CLOB: %s — restoring anyway", order_id[:16], e)

        try:
            snum       = int(row.get("session_num", 0) or 0)
            buy_price  = float(row.get("buy_price",  0) or 0)
            size_shares= float(row.get("size_shares", 0) or 0)
            tp_target  = float(row.get("tp_target",  buy_price * 2.0) or buy_price * 2.0)
            tp_mult    = float(row.get("tp_mult",    2.0) or 2.0)
            market_key = row.get("market_key", row.get("market_question", "")[:30])
            cost_usd   = float(row.get("cost_usd",   ORDER_SIZE_USD) or ORDER_SIZE_USD)
        except (ValueError, TypeError):
            log.warning("Bad numeric data in CSV row, skipping: %s", row)
            continue

        position = {
            "session_num":     snum,
            "order_id":        order_id,
            "token_id":        token_id,
            "market_question": row.get("market_question", "Restored position"),
            "market_key":      market_key,
            "bucket":          row.get("bucket", "?"),
            "slot":            int(row.get("slot", 0) or 0),
            "buy_price":       buy_price,
            "exec_price":      buy_price,
            "size_shares":     size_shares,
            "cost_usd":        cost_usd,
            "tp_target":       tp_target,
            "tp_mult":         tp_mult,
            "buy_order_id":    order_id,
            "buy_status":      "OPEN",
            "placed_at":       time.time(),  # age resets on restart — cosmetic only
            "spread":          0.0,
            "is_fallback_gtc": False,
        }
        open_positions[order_id] = position
        order_registry[snum]     = order_id
        if market_key not in open_positions_by_market:
            open_positions_by_market[market_key] = order_id
        if snum > session_counter:
            session_counter = snum
        restored += 1
        log.info("Restored position #%d — %s @ $%.4f",
                 snum, row.get("bucket", "?"), buy_price)

    return restored


async def sync_positions_from_clob() -> int:
    """Fallback: fetch ALL open orders from the CLOB and restore any that
    aren't already in open_positions (e.g. placed before the CSV had token_id).

    Uses 2× buy price as default TP since we can't recover original strategy metadata.
    Returns count of newly restored positions.
    """
    global session_counter
    if not clob:
        return 0

    try:
        live_orders = await run_clob(clob.get_orders)
    except Exception as e:
        log.warning("sync_positions_from_clob: get_orders failed: %s", e)
        return 0

    restored = 0
    for o in live_orders:
        order_id = o.get("id", "")
        if not order_id or order_id in open_positions:
            continue
        status = (o.get("status") or "").upper()
        if status in ("MATCHED", "CANCELED", "CANCELLED"):
            continue
        if (o.get("side") or "").upper() != "BUY":
            continue

        token_id = str(o.get("asset_id") or o.get("token_id") or "")
        if not token_id:
            continue

        try:
            buy_price   = float(o.get("price", 0) or 0)
            # original_size = total shares ordered; size_matched = filled so far
            # For a live GTC order not yet filled: size_matched=0, original_size=full
            size_shares = float(o.get("original_size", 0) or o.get("size_matched", 0) or 1.0)
            if buy_price <= 0:
                continue
        except (ValueError, TypeError):
            continue

        session_counter += 1
        snum       = session_counter
        tp_target  = round(buy_price * 2.0, 4)
        market_key = f"clob_restore_{token_id[:12]}"

        position = {
            "session_num":     snum,
            "order_id":        order_id,
            "token_id":        token_id,
            "market_question": "Restored from CLOB (pre-fix)",
            "market_key":      market_key,
            "bucket":          "unknown (restored)",
            "slot":            0,
            "buy_price":       buy_price,
            "exec_price":      buy_price,
            "size_shares":     size_shares,
            "cost_usd":        ORDER_SIZE_USD,
            "tp_target":       tp_target,
            "tp_mult":         2.0,
            "buy_order_id":    order_id,
            "buy_status":      "OPEN",
            "placed_at":       time.time(),
            "spread":          0.0,
            "is_fallback_gtc": False,
        }
        open_positions[order_id] = position
        order_registry[snum]     = order_id
        open_positions_by_market[market_key] = order_id
        restored += 1
        log.info("CLOB sync restored order %s @ $%.4f TP=$%.4f",
                 order_id[:16], buy_price, tp_target)

    return restored


async def post_init(app: Application) -> None:
    """Launch all background tasks after the bot is initialized."""
    log.info("Starting TweetSniper background tasks…")
    init_csv()

    # Load dedup set FIRST so no token is re-bought on restart
    await load_traded_token_ids()

    # Restore open positions from CSV (primary) then CLOB (fallback)
    csv_restored  = await restore_positions_from_csv()
    clob_restored = await sync_positions_from_clob()
    total_restored = csv_restored + clob_restored
    restore_note = (
        f"\n  ♻️ Restored {total_restored} open position(s) "
        f"({csv_restored} from CSV, {clob_restored} from CLOB)"
        if total_restored else ""
    )

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
                f"  CLOB:         {'✅ authenticated' if clob else '❌ not connected'}"
                f"{restore_note}\n"
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
