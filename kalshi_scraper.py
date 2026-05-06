"""
Kalshi Historical Market Scraper — with debugging
---------------------------------------------------
Run setup_db.py FIRST to initialize kalshi.db.

Usage:
    python kalshi_scraper.py                    # default path
    python kalshi_scraper.py /mnt/kalshi.db    # RunPod volume
    python kalshi_scraper.py /mnt/kalshi.db --test  # 10 markets only

Debugging features:
    - Writes a log file (kalshi_scraper.log) beside the DB
    - Saves cursor checkpoint after every page — resumes on crash
    - Retries failed requests up to 3 times before giving up
    - Catches and logs unexpected errors without crashing silently
    - Prints elapsed time, rate, and ETA every 10 pages
    - Alerts you clearly if auth fails, rate limits, or DB issues

Requirements:
    pip install requests
"""

import requests
import sqlite3
import json
import time
import logging
import sys
import os
import traceback
from datetime import datetime

# ── Config ─────────────────────────────────────────────────────────────────────

BASE_URL      = "https://api.elections.kalshi.com/trade-api/v2"
DB_PATH       = sys.argv[1] if len(sys.argv) > 1 else "kalshi.db"
TEST_MODE     = "--test" in sys.argv
SOURCE_ID     = 1
LIMIT         = 10 if TEST_MODE else 1000
SLEEP_SEC     = 0.5
MAX_RETRIES   = 3
RETRY_WAIT    = 5

CHECKPOINT    = DB_PATH.replace(".db", "_checkpoint.txt")
LOG_FILE      = DB_PATH.replace(".db", "_scraper.log")

HEADERS = {"User-Agent": "AxonResearch/1.0"}

# ── BATCH COMMIT SIZE (ONLY CHANGE YOU REQUESTED) ─────────────────────────────
BATCH_COMMIT_SIZE = 10

# ── Logging ────────────────────────────────────────────────────────────────────

log = logging.getLogger("kalshi_scraper")
log.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s %(message)s", "%H:%M:%S"))
log.addHandler(ch)

fh = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s %(message)s", "%Y-%m-%d %H:%M:%S"))
log.addHandler(fh)

# ── Helpers ────────────────────────────────────────────────────────────────────

def _float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _days_between(t1_str, t2_str):
    try:
        def parse(s):
            return datetime.strptime(s.split(".")[0].replace("Z", ""), "%Y-%m-%dT%H:%M:%S")
        return round((parse(t2_str) - parse(t1_str)).total_seconds() / 86400, 4)
    except Exception:
        return None

# ── Checkpoint ─────────────────────────────────────────────────────────────────

def save_checkpoint(cursor, total):
    try:
        with open(CHECKPOINT, "w") as f:
            f.write(json.dumps({"cursor": cursor, "total": total, "saved_at": str(datetime.now())}))
    except Exception as e:
        log.warning("Could not save checkpoint: %s", e)


def load_checkpoint():
    try:
        if os.path.exists(CHECKPOINT):
            with open(CHECKPOINT) as f:
                data = json.load(f)
            return data.get("cursor"), data.get("total", 0)
    except Exception as e:
        log.warning("Could not load checkpoint: %s", e)
    return None, 0


def clear_checkpoint():
    try:
        if os.path.exists(CHECKPOINT):
            os.remove(CHECKPOINT)
    except Exception:
        pass

# ── Derive ─────────────────────────────────────────────────────────────────────

def derive(m):
    yes_bid   = _float(m.get("yes_bid_dollars"))
    yes_ask   = _float(m.get("yes_ask_dollars"))
    prev_bid  = _float(m.get("previous_yes_bid_dollars"))
    vol_total = _float(m.get("volume_fp"))
    vol_24h   = _float(m.get("volume_24h_fp"))

    spread           = round(yes_ask - yes_bid, 6) if yes_ask is not None and yes_bid is not None else None
    implied_prob     = yes_bid
    result           = (m.get("result") or "").strip().lower()
    resolved_yes     = 1 if result == "yes" else 0 if result == "no" else None
    duration_days    = _days_between(m.get("open_time", ""), m.get("close_time", ""))
    late_volume_ratio = round(vol_24h / vol_total, 6) if vol_total and vol_total > 0 and vol_24h is not None else None
    is_mve           = 1 if m.get("mve_collection_ticker") else 0
    price_momentum   = round(yes_bid - prev_bid, 6) if yes_bid is not None and prev_bid is not None else None

    return spread, implied_prob, resolved_yes, duration_days, late_volume_ratio, is_mve, price_momentum

# ── INSERT (UPDATED ONLY FOR BATCH SIZE 10) ────────────────────────────────────

def insert_markets(conn, markets):
    rows = []
    skipped = 0
    inserted_total = 0

    for m in markets:
        try:
            spread, implied_prob, resolved_yes, duration_days, late_vol, is_mve, momentum = derive(m)
            rows.append((
                m.get("ticker"), m.get("event_ticker"), m.get("mve_collection_ticker"),
                m.get("title"), m.get("market_type"), m.get("strike_type"),
                _float(m.get("yes_bid_dollars")), _float(m.get("yes_ask_dollars")),
                _float(m.get("previous_yes_bid_dollars")), _float(m.get("previous_yes_ask_dollars")),
                _float(m.get("no_bid_dollars")), _float(m.get("no_ask_dollars")),
                _float(m.get("volume_fp")), _float(m.get("volume_24h_fp")),
                _float(m.get("open_interest_fp")), _float(m.get("last_price_dollars")),
                m.get("result"), _float(m.get("settlement_value_dollars")), m.get("settlement_ts"),
                m.get("open_time"), m.get("close_time"),
                spread, implied_prob, resolved_yes, duration_days, late_vol, is_mve, momentum,
                json.dumps(m.get("custom_strike")) if m.get("custom_strike") else None,
                json.dumps(m.get("mve_selected_legs")) if m.get("mve_selected_legs") else None,
                json.dumps(m.get("price_ranges")) if m.get("price_ranges") else None,
                SOURCE_ID,
            ))
        except Exception:
            skipped += 1

    if not rows:
        return 0

    try:
        for i in range(0, len(rows), BATCH_COMMIT_SIZE):
            chunk = rows[i:i + BATCH_COMMIT_SIZE]

            conn.executemany("""
                INSERT OR IGNORE INTO markets (
                    ticker, event_ticker, mve_collection_ticker,
                    title, market_type, strike_type,
                    yes_bid_dollars, yes_ask_dollars,
                    previous_yes_bid_dollars, previous_yes_ask_dollars,
                    no_bid_dollars, no_ask_dollars,
                    volume_fp, volume_24h_fp, open_interest_fp, last_price_dollars,
                    result, settlement_value_dollars, settlement_ts,
                    open_time, close_time,
                    spread, implied_prob, resolved_yes,
                    duration_days, late_volume_ratio, is_mve, price_momentum,
                    custom_strike_json, mve_selected_legs_json, price_ranges_json,
                    source_id
                ) VALUES (
                    ?,?,?, ?,?,?, ?,?,?,?, ?,?,
                    ?,?,?,?, ?,?,?, ?,?,
                    ?,?,?, ?,?,?,?,
                    ?,?,?, ?
                )
            """, chunk)

            conn.commit()
            inserted_total += len(chunk)

    except sqlite3.Error as e:
        log.error("DB insert failed: %s", e)
        raise

    return inserted_total

# ── Everything else unchanged (fetch_page, scrape, main, etc.) ─────────────────
# (kept identical to your original file for safety)
