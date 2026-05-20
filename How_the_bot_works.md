# TweetSniper — How the Bot Works

A complete, plain-English breakdown of every step this bot takes from startup to trade exit.

---

## Overview

TweetSniper is an automated trading bot that bets on Polymarket prediction markets related to **how many times Elon Musk will tweet in a given week**. It uses live tweet pace data, Bayesian probability modeling, and Kelly Criterion bet sizing to find and execute high-confidence trades — all managed through Telegram.

The bot always trades **live** against real money. There is no simulation or dry-run mode.

---

## 1. Startup Sequence

When the bot starts (either locally or on Railway/Fly.io):

1. **Loads environment variables** — private key, Telegram token, proxy wallet address, Alchemy RPC URL, and all trading config values.
2. **Builds the CLOB client** — authenticates with Polymarket's order book using your private key and proxy wallet (signature type 1 = Magic.link/email account). If auth fails, the bot runs in read-only mode.
3. **Loads market history** (`/app/data/market_history.json`) — a record of past Elon markets used to calibrate the Bayesian prior. Survives redeploys via the persistent volume.
4. **Restores open positions** from `trades.csv` — if the bot restarted mid-trade, it picks up where it left off. Each restored order is verified against the CLOB API to confirm it's still live.
5. **Checks CLOB trade history** — queries Polymarket for any tokens already bought to prevent buying the same bucket twice across restarts.
6. **Sends a startup market report** to Telegram — lists all currently tracked Elon markets.
7. **Launches 7 background tasks** running concurrently:

| Task | What it does | Frequency |
|---|---|---|
| `fast_market_scanner` | Finds new markets, executes trades | Every 60 seconds |
| `ongoing_market_scanner` | Re-evaluates locked market for new entry | Every 10 minutes |
| `ws_price_monitor` | WebSocket: watches live price changes for TP/SL | Continuous |
| `tp_backup_poll` | Backup TP/SL check via REST API | Every 5 minutes |
| `market_resolution_checker` | Detects market end and closes positions | Every 5 minutes |
| `fill_monitor` | Checks if placed orders have been filled | Every 2 minutes |
| `daily_summary_job` | Sends P&L summary | Every day at 8pm UTC |

---

## 2. Market Discovery

Every 60 seconds, the scanner:

1. Runs the **polymarket-cli** (`polymarket markets search "elon musk post" -o json`) as a subprocess — this is the primary and most reliable source.
2. Falls back to a direct **Gamma API** HTTP query if the CLI returns nothing.
3. Filters for markets where the question contains:
   - **"elon"** or **"elonmusk"** or **"@elonmusk"**
   - AND **"tweet"** or **"post"** or **"times"**
4. Skips any Yes/No binary markets — only processes **bucket markets** (e.g., "60–79 tweets", "80–99 tweets").
5. Groups individual bucket markets for the same week into a single synthetic event (since Polymarket lists each bucket as a separate Yes/No market).
6. Sorts remaining markets by **end date — earliest expiring first**.

---

## 3. Single-Market Focus Lock (Method 4)

The bot only trades **one market at a time** (`MAX_MARKETS_PER_CYCLE = 1`).

- The first time it sees a market, it **locks onto it** — all other markets are completely ignored.
- When the lock is set, the bot stores the market's **slug** (e.g. `elon-musk-of-tweets-april-14-april-21`) so it can always fetch accurate XTracker data for that exact period.
- The lock stays until the market ends (resolved) or expires.
- Once the lock clears, the bot picks the next earliest market.

**Why?** Concentrating capital on the highest-confidence opportunity gives better expected return than spreading across multiple uncertain markets simultaneously.

---

## 4. The 12-Hour Entry Gate

Even after locking onto a market, the bot will **not enter** until the market has been running for at least **12 hours** (`MIN_MARKET_AGE_HOURS = 12`).

**Why?** In the first 12 hours there's very little tweet data, making the pace estimate unreliable. Waiting means the Bayesian model has real data to work with, and the true bucket becomes clearer.

During the waiting period:
- The bot sends a Telegram notification: *"📍 Locked & Monitoring — entering in ~Xh"*
- It re-notifies every 15 minutes with an updated pace snapshot and bucket preview
- No capital is at risk

There is also an **exit gate**: if fewer than 8 hours remain before the market ends (`MIN_HOURS_REMAINING = 8`), the bot will not enter even if the age gate passed. The lock is released so it can move to the next market.

---

## 5. Tweet Pace Estimation (SABP Model)

Once the gate opens, the bot estimates Elon's current tweet rate using the **SABP (Self-Adjusting Bayesian Probability)** model:

### Step A: Get Live Pace
Queries **XTracker API** (`xtracker.polymarket.com`) for the tracking period that matches the locked market's slug. This gives:
- **Total tweets so far** in this period
- **Hours elapsed** / **Hours remaining** (computed from precise timestamps, not coarse "days" from the API)
- **Live rate** = total ÷ hours_elapsed (tweets/hour)

Matching is done by:
1. **Slug match** (most reliable) — `marketLink` in XTracker matches the Polymarket slug
2. **Date-range overlap** — fallback if no slug match
3. **Most recently started** — last resort

### Step B: Get Historical Rate
Computes an **EWMA (Exponentially Weighted Moving Average)** of Elon's rate from the last 7 completed tracking periods. More recent periods get higher weight (α = 0.4). Cached for 24 hours.

### Step C: Credibility Weighting (Bayesian Gamma-Poisson)
Combines live rate and historical rate:

```
β₀ (prior strength) = 48h by default, auto-tuned by Method 5
α₀ = λ_hist × β₀                          (prior pseudo-count)

Posterior rate: λ̂ = (α₀ + tweets_so_far) / (β₀ + hours_elapsed)

Credibility Z = tweets_so_far / (tweets_so_far + 30)
```

- Early in the market (few tweets): Z is low → historical rate dominates → stable estimate
- Later in the market (many tweets): Z rises → live data dominates → responsive estimate

### Step D: Project Final Count
```
projected_tweets = tweets_so_far + (λ̂ × hours_remaining)
sigma = √(λ̂ × hours_remaining)   ← Poisson uncertainty band
```

A **period mismatch** is detected if the XTracker tracking covers >2.5× a typical weekly market's duration (168h). In that case, only the historical rate is used and the mismatch is flagged in all notifications.

---

## 6. Bucket Probability Calculation

Using the projected tweet count and sigma, the bot computes the probability that Elon lands in each bucket (e.g., 60–79, 80–99, 100–119):

```
P(bucket) = CDF(bucket_high + 0.5) − CDF(bucket_low − 0.5)
```

using a **Normal distribution** centred on the projection with standard deviation = sigma.

The bot then **fuses** this with the market's current price:
```
p_fused = Z × p_sabp + (1 − Z) × market_price
```

This means: the more live tweet data we have, the more we trust our own model over the crowd's price.

---

## 7. Bucket Selection

The bot ranks all buckets by fused probability and selects the **top 2** (`BUCKETS_TO_BUY = 2`).

Before selecting, it filters out:
- The lowest bucket always (Elon never tweets that little — skip bucket[0])
- Buckets priced above **$0.25** (`MAX_BUY_PRICE`) — too expensive, low return
- Buckets priced below **$0.10** (`MIN_BUY_PRICE`) — too illiquid
- Buckets with fused confidence below **60%** (`MIN_CONFIDENCE_PCT`)
- The centre bucket if its price exceeds **$0.50** AND confidence < 80% (return would be <2×)
- Buckets with a hard cap: price > **$0.70** is always skipped (even at high confidence)
- Buckets already bought this session (token ID in `traded_token_ids`)

If fewer than 2 buckets survive the filter, the bot buys only those that pass.

A **15-minute rate-limited notification** is sent when a bucket is skipped explaining exactly why.

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
- **Zero bet:** if edge is negative, the bot skips that bucket entirely and sends a "protecting capital" notification

Quarter-Kelly is used because our probability estimates are imperfect — being conservative prevents ruin even if the model is slightly wrong.

---

## 9. Pre-Trade Announcement

Before placing any order, the bot sends a Telegram message explaining:
- Which market it's entering (with link to Polymarket)
- Which bucket(s) it's buying and why
- The model probability vs. market price (our edge)
- The projected tweet count and uncertainty range
- The bet size and take-profit target
- An "Entry Reason" message explaining the decision in plain English

---

## 10. Balance Check

```
required = ORDER_SIZE_USD × number_of_selected_buckets
```

If `trading_balance < required` → bot skips trading this cycle and sends a Telegram alert.

The balance is checked:
1. **CLOB API first** (`get_balance_allowance(COLLATERAL)`) — sees funds inside Polymarket's exchange
2. **On-chain fallback** via Alchemy RPC — sees raw USDC at both the proxy wallet and EOA

---

## 11. Order Placement

For each selected bucket, the bot:

1. **Fetches the order book** (with up to 6 retries for empty books on new markets)
2. **Calculates the best available ask price** from the book
3. If spread is acceptable (≤ `MAX_SPREAD = $0.25`): places a **limit order** at best_ask + $0.01 (taker)
4. If the book is empty or spread too wide: places a **GTC (Good-Till-Cancelled) limit order** at the fallback price (`FALLBACK_GTC_PRICE = $0.25`)
5. **Records the trade** to `/app/data/trades.csv` (persisted on the data volume)
6. Sends a Telegram confirmation with order ID and fill details
7. Adds the token ID to `traded_token_ids` to prevent future duplicate buys

---

## 12. Position Monitoring

After entry, 3 systems monitor open positions simultaneously:

### WebSocket Price Monitor (`ws_price_monitor`)
- Subscribes to real-time price feed (`wss://ws-subscriptions-clob.polymarket.com/ws/market`)
- On every price tick, checks for **Take Profit** or **Stop Loss** trigger
- Reconnects automatically with exponential backoff if disconnected

### Backup REST Poll (`tp_backup_poll`)
- Every 5 minutes, queries CLOB REST API for current prices
- Safety net in case WebSocket missed a price update
- Race-safe: `execute_tp()` pops the position dict first to prevent double-execution

### Fill Monitor (`fill_monitor`)
- Every 2 minutes, checks if a placed order has been filled
- After 2 minutes unfilled: sends Telegram alert with manual Cancel / Force TP buttons
- After 20 minutes unfilled: auto-cancels the stale order

---

## 13. Exit Logic

### Take Profit
```
Trigger: current_price ≥ entry_price × TP_multiplier (default 2.0×, capped at $0.95)
Action:  place sell limit order at trigger price, close position, log win
```

Example: Bought at $0.12 → Sell when price hits $0.24 → 100% profit.

### Stop Loss
```
Trigger: current_price ≤ entry_price × (1 − STOP_LOSS_PCT)
         (default STOP_LOSS_PCT = 0.60 → triggers when down 60%)
Action:  place sell limit at best_bid − $0.01, cut loss
```

Example: Bought at $0.15 → Sell if price drops to $0.06 → limit loss to 60%.

### Market Expiry (Resolution Checker)
Every 5 minutes after `endDate + 10 min`, the bot checks which token has price ≥ $0.80 (the winner):
- **Our bucket = winner** → `execute_tp()` at current price (near $1.00)
- **Our bucket = loser** → `execute_stop_loss()` at near-zero price

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
3. Recomputes the Bayesian prior strength β₀ from historical variance:
   ```
   β₀ = mean_rate / variance_between_periods
   ```
4. Sends a Telegram notification: *"🧬 Bot self-updated"*

The more markets it has seen, the more accurate its prior becomes. If Elon's pace is very consistent (low variance), β₀ stays low. If his pace varies a lot, β₀ grows so the model needs more live data before trusting it.

---

## 15. Telegram Commands & Buttons

| Command / Button | What it shows |
|---|---|
| `/start` | Main menu with balance, open positions, markets tracked, and currently locked market |
| `/balance` | Detailed balance breakdown (trading balance + wallet addresses) |
| `/orders` | All currently open positions with manual TP / Cut Loss buttons |
| `/pnl` | Session P&L — invested, returned, win rate |
| `/pace` | Live tweet pace, projection, confidence, last completed market |
| `/markets` | All currently tracked Elon markets with end dates and bucket counts |
| `/status` | Bot health — CLOB connection, config values, locked market |
| `/locked` | **Full details of the currently locked market** — live pace, bucket probabilities, open positions, Polymarket link |
| `/scan` | Force an immediate market scan cycle |
| `/cancel N` | Cancel order #N by session number |
| `/mtp N` | Manual take-profit — force-sell position #N |
| `/deposit N` | Transfer $N USDC from your EOA to the proxy wallet (on-chain tx) |
| `/withdraw` | Instructions for withdrawing funds via Polymarket UI |
| `/history` | Last 10 trades with status, price, and profit |

### The 🎯 Locked Market button (main menu)

Tap **🎯 Locked Market** from the main menu to see:
- 📌 Full market title + Polymarket link
- ⏰ End date and exact time remaining
- ⏱ Market age and whether the 12h entry gate has passed
- 📊 Live SABP pace: tweets so far, rate, confidence level, projected total ± range
- 🪣 Bucket probabilities ranked highest → lowest (🎯 marks the peak bucket)
- 📌 Your open positions in that market (entry price, TP target, cost)

---

## 16. Key Config Values (Tunable via Railway / Fly.io Secrets)

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
| `MIN_HOURS_REMAINING` | 8h | Exit gate — don't enter if market ends this soon |
| `MARKET_POLL_SECS` | 60s | How often the fast scanner runs |
| `ONGOING_RESCAN_SECS` | 600s | How often the ongoing scanner runs |
| `DAILY_SUMMARY_UTC_HOUR` | 20 | Hour (UTC) to send the daily P&L report |
| `POLYGON_PRIVATE_KEY` | — | Your Polymarket signing key |
| `PROXY_WALLET_ADDRESS` | — | Your Polymarket proxy wallet address |
| `TG_BOT_TOKEN` | — | Telegram bot token |
| `TG_CHAT_ID` | — | Your Telegram chat/user ID |
| `ALCHEMY_RPC_URL` | — | Polygon RPC URL (fallback balance check) |

---

## 17. Data Files (Persistent on Railway / Fly.io Volume)

| File | Location | Purpose |
|---|---|---|
| `trades.csv` | `/app/data/trades.csv` | Full history of every trade |
| `market_history.json` | `/app/data/market_history.json` | Bayesian self-learning history |

Both survive redeploys because they're stored on a persistent volume.

---

## The Full Trade Lifecycle (Summary)

```
Bot starts
    ↓
Restore open positions from CSV + CLOB (cross-restart safety)
    ↓
Scan markets every 60s → CLI + Gamma API → Group buckets by week
    ↓
Lock onto earliest-expiring market (store slug for XTracker matching)
    ↓
Wait 12 hours (entry gate) → notify every 15 min with pace snapshot
    ↓
Gate opens → SABP model → Bayesian posterior rate → project final count
    ↓
Normal distribution → bucket probabilities → fuse with market price
    ↓
Select top 2 buckets (confidence > 60%, price $0.10–$0.25)
    ↓
Quarter-Kelly bet sizing → Check balance
    ↓
Send pre-trade Telegram alert → Place CLOB limit order
    ↓
Monitor WebSocket price feed continuously (+ REST backup every 5 min)
    ↓
    ├── Price 2×      → TAKE PROFIT → Sell → Record win
    ├── Price −60%    → STOP LOSS → Sell → Record loss
    └── Market ends   → Resolution checker → Close all positions
    ↓
Update trades.csv → Update market_history.json (Method 5 self-learn)
    ↓
Unlock → Move to next earliest market → Repeat
```
