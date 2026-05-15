# TweetSniper — How the Bot Works

A complete, plain-English breakdown of every step this bot takes from startup to trade exit.

---

## Overview

TweetSniper is an automated trading bot that bets on Polymarket prediction markets related to **how many times Elon Musk will tweet in a given week**. It uses live tweet pace data, Bayesian probability modeling, and Kelly Criterion bet sizing to find and execute high-confidence trades — all managed through Telegram.

---

## 1. Startup Sequence

When the bot starts (either locally or on Fly.io):

1. **Loads environment variables** — private key, Telegram token, proxy wallet address, Alchemy RPC URL, and all trading config values.
2. **Builds the CLOB client** — authenticates with Polymarket's order book using your private key and proxy wallet (signature type 1 = Magic.link/email account). If auth fails, the bot runs in read-only mode.
3. **Loads market history** (`/app/data/market_history.json`) — a record of past Elon markets used to calibrate the Bayesian prior. Survives redeploys via the Fly.io persistent volume.
4. **Restores open positions** from `trades.csv` — if the bot restarted mid-trade, it picks up where it left off.
5. **Checks CLOB trade history** — queries Polymarket for any tokens already bought to prevent buying the same bucket twice.
6. **Sends a startup market report** to Telegram — lists all currently tracked Elon markets.
7. **Launches 6 background tasks** running concurrently:

| Task | What it does | Frequency |
|---|---|---|
| `fast_market_scanner` | Finds new markets, executes trades | Every 60 seconds |
| `ongoing_market_scanner` | Re-evaluates locked market for new entry | Every 10 minutes |
| `ws_price_monitor` | WebSocket: watches live price changes for TP/SL | Continuous |
| `tp_backup_poll` | Backup TP/SL check via REST API | Every 5 minutes |
| `fill_monitor` | Checks if placed orders have been filled | Every 2 minutes |
| `daily_summary_job` | Sends P&L summary | Every day at 8pm UTC |

---

## 2. Market Discovery

Every 60 seconds, the scanner:

1. Queries **Polymarket's Gamma API** for all active prediction markets.
2. Filters for markets where the question contains:
   - **"elon"** or **"elonmusk"** or **"@elonmusk"**
   - AND **"tweet"** or **"post"** or **"times"**
3. Skips any Yes/No binary markets — only processes **bucket markets** (e.g., "60–79 tweets", "80–99 tweets").
4. Sorts remaining markets by **end date — earliest expiring first**.
5. Passes the first market to the trade pipeline.

---

## 3. Single-Market Focus Lock (Method 4)

The bot only trades **one market at a time** (`MAX_MARKETS_PER_CYCLE = 1`).

- The first time it sees a market, it **locks onto it** — all other markets are completely ignored.
- The lock stays until the market ends (resolved) or expires.
- Once the lock clears, the bot picks the next earliest market.

**Why?** Concentrating capital on the highest-confidence opportunity gives better expected return than spreading across multiple uncertain markets simultaneously.

---

## 4. The 12-Hour Entry Gate

Even after locking onto a market, the bot will **not enter** until the market has been running for at least **12 hours** (`MIN_MARKET_AGE_HOURS = 12`).

**Why?** In the first 12 hours there's very little tweet data, making the pace estimate unreliable. Waiting means the Bayesian model has real data to work with, and the true bucket becomes clearer.

During the waiting period:
- The bot sends a Telegram notification: *"Monitoring — waiting 12h before entry"*
- It continues monitoring and updating its probability estimates every cycle
- No capital is at risk

---

## 5. Tweet Pace Estimation (SABP Model)

Once the gate opens, the bot estimates Elon's current tweet rate using the **SABP (Self-Adjusting Bayesian Probability)** model:

### Step A: Get Live Pace
Queries **XTracker API** (`xtracker.polymarket.com`) for the specific tracking period that matches the current market. This gives:
- **Total tweets so far** in this period
- **Hours elapsed** in this period
- **Live rate** = total ÷ hours_elapsed (tweets/hour)

### Step B: Get Historical Rate
Computes an **EWMA (Exponentially Weighted Moving Average)** of Elon's rate from the last 7 completed tracking periods. More recent periods get higher weight (α = 0.4).

### Step C: Credibility Weighting
Combines live rate and historical rate using **Bayesian credibility theory**:

```
credibility Z = n / (n + β₀)

where:
  n  = tweets so far in this market
  β₀ = prior strength (default 48h, auto-adjusted by Method 5)

blended_rate = Z × live_rate + (1-Z) × historical_rate
```

- Early in the market (few tweets): Z is low → historical rate dominates → stable estimate
- Later in the market (many tweets): Z rises → live data dominates → responsive estimate

### Step D: Project Final Count
```
projected_tweets = tweets_so_far + (blended_rate × hours_remaining)
sigma = uncertainty band (√projected for Poisson distribution)
```

---

## 6. Bucket Probability Calculation

Using the projected tweet count and sigma, the bot computes the probability that Elon lands in each bucket (e.g., 60–79, 80–99, 100–119):

```
P(bucket) = CDF(bucket_high) − CDF(bucket_low)
```

using a **Normal distribution** centred on the projection with standard deviation = sigma.

This gives a **model probability** for each bucket (e.g., 60–79 bucket = 45% likely).

The bot then **fuses** this with the market's current price:
```
p_fused = credibility_weight × p_sabp + (1 − weight) × market_price
```

This means: the more live tweet data we have, the more we trust our own model over the crowd's price.

---

## 7. Bucket Selection

The bot ranks all buckets by fused probability and selects the **top 2** (`BUCKETS_TO_BUY = 2`).

Before selecting, it filters out:
- Buckets priced above **$0.25** (`MAX_BUY_PRICE`) — too expensive, low return
- Buckets priced below **$0.10** (`MIN_BUY_PRICE`) — too illiquid
- Buckets with fused confidence below **60%** (`MIN_CONFIDENCE_PCT`)
- The centre bucket if its price exceeds $0.50 (return would be < 2×)
- Buckets already bought in this session

If fewer than 2 buckets survive the filter, the bot buys only those that pass.

---

## 8. Kelly Criterion Bet Sizing

For each selected bucket, the bet size is calculated using **Quarter-Kelly**:

```
b       = (1 / market_price) − 1        # net odds per $1 staked
edge    = p_fused × (1 + b) − 1         # expected value per $1
f_full  = edge / b                       # full Kelly fraction
f_safe  = f_full × 0.25                 # quarter-Kelly (KELLY_FRACTION)
bet_usd = account_balance × f_safe
```

- **Minimum bet:** $1.00 (only placed if edge > 8%)
- **Maximum bet:** `ORDER_SIZE_USD` = $1.00 by default (overridable via env var)
- **Zero bet:** if edge is negative, the bot skips that bucket entirely

Quarter-Kelly is used because our probability estimates are imperfect — being conservative prevents ruin even if the model is slightly wrong.

---

## 9. Pre-Trade Announcement

Before placing any order, the bot sends a Telegram message explaining:
- Which market it's entering
- Which bucket(s) it's buying and why
- The model probability vs. market price (our edge)
- The projected tweet count and uncertainty range
- The bet size and take-profit target

---

## 10. Balance Check

```
required = ORDER_SIZE_USD × number_of_selected_buckets
```

If `trading_balance < required` → bot skips trading this cycle and sends a Telegram alert.

The balance is checked:
1. **CLOB API first** (`get_balance_allowance(COLLATERAL)`) — sees funds inside Polymarket's exchange
2. **On-chain fallback** via Alchemy RPC — sees raw USDC at the proxy wallet

---

## 11. Order Placement

For each selected bucket, the bot:

1. **Fetches the order book** (with up to 6 retries for empty books)
2. **Calculates the best available ask price** from the book
3. If spread is acceptable (≤ `MAX_SPREAD = $0.25`): places a **limit order** at the best ask
4. If the book is empty or spread too wide: places a **GTC (Good-Till-Cancelled) limit order** at the fallback price (`FALLBACK_GTC_PRICE = $0.25`)
5. **Records the trade** to `/app/data/trades.csv` (persisted on Fly.io volume)
6. Sends a Telegram confirmation with order ID and fill details
7. Adds the token ID to `traded_token_ids` to prevent future duplicate buys

---

## 12. Position Monitoring

After entry, 3 systems monitor open positions simultaneously:

### WebSocket Price Monitor (`ws_price_monitor`)
- Subscribes to real-time price feed for each held token
- On every price tick, checks for **Take Profit** or **Stop Loss** trigger

### Backup REST Poll (`tp_backup_poll`)
- Every 5 minutes, queries CLOB REST API for current prices
- Safety net in case WebSocket disconnects

### Fill Monitor (`fill_monitor`)
- Every 2 minutes, checks if a placed order has been filled
- Sends Telegram alert when fill confirmed

---

## 13. Exit Logic

### Take Profit
```
Trigger: current_price ≥ entry_price × TP_multiplier (default 2.0×)
Action:  place sell order at market price, close position
```

Example: Bought at $0.12 → Sell when price hits $0.24 → 100% profit.

### Stop Loss
```
Trigger: current_price ≤ entry_price × (1 − STOP_LOSS_PCT)
         (default STOP_LOSS_PCT = 0.60 → triggers when down 60%)
Action:  place sell order at market price, cut loss
```

Example: Bought at $0.15 → Sell if price drops to $0.06 → limit loss to 60%.

### Market Expiry
If the market ends before TP or SL triggers, Polymarket auto-resolves. Winning positions pay out 1 USDC per share. Losing positions go to zero.

On close, the bot:
1. Updates the trade row in `trades.csv` with sell price, profit/loss %, outcome
2. Sends a Telegram close notification with P&L
3. Frees the market lock so the next market can be processed
4. Updates `market_history.json` for future Bayesian calibration (Method 5)

---

## 14. Self-Learning (Method 5 — Empirical Bayes)

After each market resolves, the bot:
1. Records the actual tweet count and rate for that period
2. Stores up to the last 20 completed markets in `market_history.json`
3. Recomputes the Bayesian prior strength β₀ from historical variance
4. Sends a Telegram notification: *"🧬 Bot self-updated"*

The more markets it has seen, the more accurate its prior becomes.

---

## 15. Telegram Commands

| Command | What it shows |
|---|---|
| `/start` | Main menu with balance, open positions, markets tracked |
| `/balance` | Detailed balance breakdown (trading balance + wallet addresses) |
| `/orders` | All currently open positions |
| `/pnl` | Session P&L — invested, returned, win rate |
| `/pace` | Live tweet pace, projection, confidence, last completed market |
| `/markets` | All currently tracked Elon markets with details |
| `/status` | Bot health — CLOB status, config values, mode (LIVE/TEST) |
| `/scan` | Force an immediate market scan |
| `/cancel` | Cancel all open orders |
| `/mtp` | Manual take-profit — force-sell a position |
| `/deposit` | Shows your proxy wallet deposit address |
| `/withdraw` | Instructions for withdrawing funds |

---

## 16. Key Config Values (Tunable via Fly.io Secrets)

| Variable | Default | What it controls |
|---|---|---|
| `ORDER_SIZE_USD` | $1.00 | Max bet per bucket |
| `BUCKETS_TO_BUY` | 2 | How many buckets per market |
| `MAX_BUY_PRICE` | $0.25 | Max price allowed (25¢) |
| `MIN_BUY_PRICE` | $0.10 | Min price allowed (10¢) |
| `MIN_CONFIDENCE_PCT` | 60% | Skip buckets below this confidence |
| `KELLY_FRACTION` | 0.25 | Quarter-Kelly conservatism factor |
| `STOP_LOSS_PCT` | 60% | Cut loss when down this much |
| `MIN_MARKET_AGE_HOURS` | 12h | Entry gate — wait this long before buying |
| `MARKET_POLL_SECS` | 60s | How often the scanner runs |
| `DRY_RUN` | false | Set to `true` for simulation mode (no real orders) |

---

## 17. Data Files (Persistent on Fly.io Volume)

| File | Location | Purpose |
|---|---|---|
| `trades.csv` | `/app/data/trades.csv` | Full history of every trade |
| `market_history.json` | `/app/data/market_history.json` | Bayesian self-learning history |

Both survive redeploys because they're stored on the `tweetsniper_data` Fly.io volume (1GB, encrypted, daily snapshots).

---

## The Full Trade Lifecycle (Summary)

```
Bot starts
    ↓
Scan markets every 60s → Find Elon tweet bucket markets
    ↓
Lock onto earliest-expiring market
    ↓
Wait 12 hours (entry gate) → Monitor pace while waiting
    ↓
Gate opens → Run SABP model → Compute bucket probabilities
    ↓
Select top 2 buckets (confidence > 60%, price < $0.25)
    ↓
Calculate Kelly bet size → Check balance
    ↓
Send pre-trade Telegram alert → Place CLOB order
    ↓
Monitor WebSocket price feed continuously
    ↓
    ├── Price doubles  → TAKE PROFIT → Sell → Record win
    ├── Price −60%    → STOP LOSS → Sell → Record loss
    └── Market ends   → Auto-resolved by Polymarket
    ↓
Update trades.csv → Update market_history.json (self-learn)
    ↓
Unlock → Move to next earliest market → Repeat
```
