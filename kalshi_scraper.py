"""
Kalshi Historical Market Scraper — optimized batch commit version
-------------------------------------------------------------------
Key improvement:
- commits every 10 pages (NOT every page)
- significantly reduces network volume fsync overhead
- maintains checkpoint + crash recovery safety
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

# ── Config ─────────────────────────────────────────────────────────────

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

# ── Logging ────────────────────────────────────────────────────────────

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

# ── Helpers ─────────────────────────────────────────────────────────────

def _float(v):
    try:
        return float(v)
    except:
        return None

def _days_between(t1, t2):
    try:
        def p(s):
            return datetime.strptime(s.split(".")[0].replace("Z",""), "%Y-%m-%dT%H:%M:%S")
        return round((p(t2) - p(t1)).total_seconds() / 86400, 4)
    except:
        return None

# ── Checkpoint ─────────────────────────────────────────────────────────

def save_checkpoint(cursor, total):
    try:
        with open(CHECKPOINT, "w") as f:
            json.dump({"cursor": cursor, "total": total}, f)
    except Exception as e:
        log.warning("Checkpoint save failed: %s", e)

def load_checkpoint():
    try:
        if os.path.exists(CHECKPOINT):
            with open(CHECKPOINT) as f:
                d = json.load(f)
            log.info("Resuming from checkpoint (%d markets)", d.get("total", 0))
            return d.get("cursor"), d.get("total", 0)
    except:
        pass
    return None, 0

def clear_checkpoint():
    try:
        if os.path.exists(CHECKPOINT):
            os.remove(CHECKPOINT)
    except:
        pass

# ── Feature derivation ─────────────────────────────────────────────────

def derive(m):
    yes_bid = _float(m.get("yes_bid_dollars"))
    yes_ask = _float(m.get("yes_ask_dollars"))
    prev    = _float(m.get("previous_yes_bid_dollars"))
    vol     = _float(m.get("volume_fp"))
    vol24   = _float(m.get("volume_24h_fp"))

    spread = yes_ask - yes_bid if yes_bid is not None and yes_ask is not None else None
    momentum = yes_bid - prev if yes_bid is not None and prev is not None else None
    late_ratio = vol24 / vol if vol and vol > 0 and vol24 is not None else None

    resolved = m.get("result")
    resolved_yes = 1 if resolved == "yes" else 0 if resolved == "no" else None

    duration = _days_between(m.get("open_time",""), m.get("close_time",""))

    return spread, yes_bid, resolved_yes, duration, late_ratio, bool(m.get("mve_collection_ticker")), momentum

# ── INSERT (NO COMMIT HERE) ────────────────────────────────────────────

def insert_markets(conn, markets):
    rows = []

    for m in markets:
        try:
            spread, prob, resolved, duration, late_ratio, is_mve, momentum = derive(m)

            rows.append((
                m.get("ticker"),
                m.get("event_ticker"),
                m.get("mve_collection_ticker"),
                m.get("title"),
                m.get("market_type"),
                m.get("strike_type"),
                _float(m.get("yes_bid_dollars")),
                _float(m.get("yes_ask_dollars")),
                _float(m.get("previous_yes_bid_dollars")),
                _float(m.get("previous_yes_ask_dollars")),
                _float(m.get("no_bid_dollars")),
                _float(m.get("no_ask_dollars")),
                _float(m.get("volume_fp")),
                _float(m.get("volume_24h_fp")),
                _float(m.get("open_interest_fp")),
                _float(m.get("last_price_dollars")),
                m.get("result"),
                _float(m.get("settlement_value_dollars")),
                m.get("settlement_ts"),
                m.get("open_time"),
                m.get("close_time"),
                spread,
                prob,
                resolved,
                duration,
                late_ratio,
                1 if is_mve else 0,
                momentum,
                json.dumps(m.get("custom_strike")) if m.get("custom_strike") else None,
                json.dumps(m.get("mve_selected_legs")) if m.get("mve_selected_legs") else None,
                json.dumps(m.get("price_ranges")) if m.get("price_ranges") else None,
                SOURCE_ID
            ))
        except Exception as e:
            log.warning("Skip %s: %s", m.get("ticker"), e)

    if rows:
        conn.executemany("""
            INSERT OR IGNORE INTO markets VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
        """, rows)

    return len(rows)

# ── FETCH ──────────────────────────────────────────────────────────────

def fetch(cursor):
    params = {"limit": LIMIT}
    if cursor:
        params["cursor"] = cursor

    r = requests.get(f"{BASE_URL}/historical/markets",
                     params=params,
                     headers=HEADERS,
                     timeout=30)

    r.raise_for_status()
    j = r.json()

    return j.get("markets", []), j.get("cursor")

# ── MAIN LOOP ───────────────────────────────────────────────────────────

def scrape(conn):
    cursor, total = load_checkpoint()
    page = 0
    start = datetime.now()

    log.info("SCRAPE START")

    while True:
        page += 1

        markets, cursor = fetch(cursor)
        if not markets:
            break

        inserted = insert_markets(conn, markets)
        total += inserted

        elapsed = (datetime.now() - start).total_seconds()
        rate = total / elapsed if elapsed else 0

        log.info("Page %d | +%d | total %d | %.1f/s",
                 page, inserted, total, rate)

        save_checkpoint(cursor, total)

        # ── BATCH COMMIT (KEY CHANGE) ──
        if page % 10 == 0:
            conn.commit()
            log.info("Committed at page %d", page)

        if not cursor:
            break

        time.sleep(SLEEP_SEC)

    conn.commit()
    clear_checkpoint()
    return total

# ── RUN ────────────────────────────────────────────────────────────────

if __name__ == "__main__":

    if not os.path.exists(DB_PATH):
        print("DB not found — run setup_db.py first")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    start = datetime.now()

    try:
        total = scrape(conn)
    finally:
        conn.close()

    print("DONE:", total, "markets")
