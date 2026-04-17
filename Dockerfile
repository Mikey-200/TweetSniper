# ─────────────────────────────────────────────────────────────────────
# TweetSniper — Dockerfile
#
# Two-stage build:
#   Stage 1 (builder)  — compiles the polymarket Rust CLI binary
#   Stage 2 (runtime)  — lean Python image with the compiled binary
# ─────────────────────────────────────────────────────────────────────

# ── Stage 1: Build polymarket-cli ─────────────────────────────────────
FROM rust:latest AS builder

RUN apt-get update && apt-get install -y \
    pkg-config libssl-dev build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY polymarket-cli/ ./polymarket-cli/

WORKDIR /build/polymarket-cli
RUN cargo build --release


# ── Stage 2: Python runtime ───────────────────────────────────────────
FROM python:3.12-slim

# Install minimal system deps
RUN apt-get update && apt-get install -y \
    libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy compiled CLI binary from builder
COPY --from=builder /build/polymarket-cli/target/release/polymarket \
     /usr/local/bin/polymarket
RUN chmod +x /usr/local/bin/polymarket

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot source
COPY bot.py .
COPY .env.example .

# Railway injects env vars — no .env file needed in container
ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "bot.py"]
