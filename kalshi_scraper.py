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
TEST_MODE     = "--test" in sys.argv       # pull 10 markets and stop
SOURCE_ID     = 1
LIMIT         = 10 if TEST_MODE else 1000
SLEEP_SEC     = 0.5
MAX_RETRIES   = 3                          # retry failed requests this many times
RETRY_WAIT    = 5                          # seconds between retries

# Checkpoint file — saves cursor so scraper can resume after a crash
CHECKPOINT    = DB_PATH.replace(".db", "_checkpoint.txt")

# Log file — written beside the DB
LOG_FILE      = DB_PATH.replace(".db", "_scraper.log")

# Add your API key if required
# HEADERS = {"Authorization": "Bearer YOUR_API_KEY"}
HEADERS = {"User-Agent": "AxonResearch/1.0"}


# ── Logging ────────────────────────────────────────────────────────────────────
# Logs to BOTH terminal and a persistent log file

log = logging.getLogger("kalshi_scraper")
log.setLevel(logging.DEBUG)

# Terminal handler — INFO and above
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s %(message)s", "%H:%M:%S"))
log.addHandler(ch)

# File handler — DEBUG and above (captures everything)
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


# ── Checkpoint (crash recovery) ────────────────────────────────────────────────

def save_checkpoint(cursor, total):
    """Save cursor to file so we can resume if the script crashes."""
    try:
        with open(CHECKPOINT, "w") as f:
            f.write(json.dumps({"cursor": cursor, "total": total, "saved_at": str(datetime.now())}))
        log.debug("Checkpoint saved — cursor: %s", cursor[:20] if cursor else "None")
    except Exception as e:
        log.warning("Could not save checkpoint: %s", e)


def load_checkpoint():
    """Load cursor from checkpoint file if it exists."""
    try:
        if os.path.exists(CHECKPOINT):
            with open(CHECKPOINT) as f:
                data = json.load(f)
            cursor = data.get("cursor")
            total  = data.get("total", 0)
            saved  = data.get("saved_at", "unknown")
            log.info("Resuming from checkpoint — %d markets already scraped (saved %s)", total, saved)
            return cursor, total
    except Exception as e:
        log.warning("Could not load checkpoint: %s — starting fresh", e)
    return None, 0


def clear_checkpoint():
    """Delete checkpoint file after a successful complete scrape."""
    try:
        if os.path.exists(CHECKPOINT):
            os.remove(CHECKPOINT)
            log.info("Checkpoint cleared — scrape was complete")
    except Exception as e:
        log.warning("Could not clear checkpoint: %s", e)


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


# ── Insert ─────────────────────────────────────────────────────────────────────

def insert_markets(conn, markets):
    """Insert batch — skips duplicates on ticker primary key."""
    rows = []
    skipped = 0

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
                json.dumps(m.get("custom_strike"))     if m.get("custom_strike")     else None,
                json.dumps(m.get("mve_selected_legs")) if m.get("mve_selected_legs") else None,
                json.dumps(m.get("price_ranges"))      if m.get("price_ranges")      else None,
                SOURCE_ID,
            ))
        except Exception as e:
            skipped += 1
            log.warning("Skipped market %s — derive error: %s", m.get("ticker", "?"), e)
            log.debug("Full market that failed: %s", json.dumps(m))

    if not rows:
        log.warning("No valid rows to insert in this batch")
        return 0

    try:
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
        """, rows)
        conn.commit()
    except sqlite3.Error as e:
        log.error("DB insert failed: %s", e)
        log.debug("Traceback: %s", traceback.format_exc())
        raise

    if skipped:
        log.warning("Batch: %d inserted, %d skipped due to errors", len(rows), skipped)

    return len(rows)


# ── Request with retry ─────────────────────────────────────────────────────────

def fetch_page(cursor, attempt=1):
    """
    Fetch one page from the API.
    Retries up to MAX_RETRIES times on network errors.
    Returns (markets list, next cursor) or raises on unrecoverable error.
    """
    params = {"limit": LIMIT}
    if cursor:
        params["cursor"] = cursor

    try:
        resp = requests.get(
            f"{BASE_URL}/historical/markets",
            params=params,
            headers=HEADERS,
            timeout=30
        )

        # ── Handle specific HTTP errors clearly ──────────
        if resp.status_code == 401:
            log.error("=" * 50)
            log.error("AUTH FAILED (401) — Kalshi requires an API key")
            log.error("Add your key to HEADERS at the top of this file:")
            log.error('  HEADERS = {"Authorization": "Bearer YOUR_KEY"}')
            log.error("=" * 50)
            raise SystemExit(1)

        if resp.status_code == 403:
            log.error("ACCESS DENIED (403) — check your API permissions")
            raise SystemExit(1)

        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 30))
            log.warning("RATE LIMITED (429) — sleeping %ds before retry", wait)
            time.sleep(wait)
            return fetch_page(cursor, attempt)   # retry same page

        if resp.status_code == 500:
            log.error("KALSHI SERVER ERROR (500) — their side, not ours")
            raise requests.exceptions.RequestException("Server error 500")

        resp.raise_for_status()

        data    = resp.json()
        markets = data.get("markets", [])
        next_cursor = data.get("cursor")

        log.debug("Page fetched — %d markets, next_cursor: %s",
                  len(markets), (next_cursor[:20] + "...") if next_cursor else "None")

        return markets, next_cursor

    except requests.exceptions.Timeout:
        log.warning("Request timed out (attempt %d/%d)", attempt, MAX_RETRIES)
        if attempt < MAX_RETRIES:
            log.info("Retrying in %ds...", RETRY_WAIT * attempt)
            time.sleep(RETRY_WAIT * attempt)
            return fetch_page(cursor, attempt + 1)
        log.error("Max retries reached — giving up on this page")
        raise

    except requests.exceptions.ConnectionError:
        log.warning("Connection error (attempt %d/%d)", attempt, MAX_RETRIES)
        if attempt < MAX_RETRIES:
            log.info("Retrying in %ds...", RETRY_WAIT * attempt)
            time.sleep(RETRY_WAIT * attempt)
            return fetch_page(cursor, attempt + 1)
        log.error("Max retries reached — network appears down")
        raise

    except requests.exceptions.RequestException as e:
        log.error("Request failed: %s", e)
        raise


# ── Scraper ────────────────────────────────────────────────────────────────────

def scrape(conn):
    """
    Main scrape loop.
    Resumes from checkpoint if one exists.
    Saves checkpoint after every page.
    """
    if TEST_MODE:
        log.info("TEST MODE — pulling %d markets then stopping", LIMIT)

    # Resume from checkpoint if available
    cursor, total = load_checkpoint()
    page  = 0
    start = datetime.now()

    log.info("=" * 55)
    log.info("SCRAPE STARTED — %s", start.strftime("%Y-%m-%d %H:%M:%S"))
    log.info("DB path:   %s", os.path.abspath(DB_PATH))
    log.info("Log file:  %s", os.path.abspath(LOG_FILE))
    log.info("Resuming:  %s", "yes" if total > 0 else "fresh start")
    log.info("=" * 55)

    try:
        while True:
            page += 1

            # ── Fetch ─────────────────────────────────────────
            try:
                markets, next_cursor = fetch_page(cursor)
            except SystemExit:
                raise
            except Exception as e:
                log.error("Fetch failed after retries: %s", e)
                log.error("Saving checkpoint — restart script to resume from page %d", page)
                save_checkpoint(cursor, total)
                return total

            if not markets:
                log.info("No markets returned — scrape complete")
                break

            # ── Insert ────────────────────────────────────────
            try:
                inserted = insert_markets(conn, markets)
            except sqlite3.Error as e:
                log.error("DB write failed on page %d: %s", page, e)
                log.error("Saving checkpoint — restart to resume")
                save_checkpoint(cursor, total)
                return total

            total += inserted

            # ── Progress ──────────────────────────────────────
            elapsed = (datetime.now() - start).total_seconds()
            rate    = total / elapsed if elapsed > 0 else 0
            log.info(
                "Page %3d | +%4d | total: %6d | %.0f markets/sec",
                page, inserted, total, rate
            )

            # ── Checkpoint ────────────────────────────────────
            save_checkpoint(next_cursor, total)
            cursor = next_cursor

            if not cursor:
                log.info("No cursor returned — reached end of data")
                break

            if TEST_MODE:
                log.info("TEST MODE complete — stopping after first page")
                break

            time.sleep(SLEEP_SEC)

    except KeyboardInterrupt:
        log.warning("Interrupted by user (Ctrl+C)")
        log.warning("Checkpoint saved — restart to resume from page %d", page)
        save_checkpoint(cursor, total)
        return total

    except Exception as e:
        log.error("Unexpected crash: %s", e)
        log.error("Traceback: %s", traceback.format_exc())
        log.error("Checkpoint saved — restart to resume")
        save_checkpoint(cursor, total)
        return total

    # Clean finish — remove checkpoint
    clear_checkpoint()
    return total


# ── Summary ────────────────────────────────────────────────────────────────────

def print_summary(conn, elapsed):
    print("\n" + "=" * 55)
    print("SCRAPE COMPLETE")
    print("=" * 55)

    total    = conn.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
    resolved = conn.execute("SELECT COUNT(*) FROM markets WHERE resolved_yes IS NOT NULL").fetchone()[0]
    yes_rate = conn.execute("SELECT AVG(resolved_yes) FROM markets WHERE resolved_yes IS NOT NULL").fetchone()[0]
    mve_ct   = conn.execute("SELECT COUNT(*) FROM markets WHERE is_mve = 1").fetchone()[0]

    print(f"Total markets:      {total:>8,}")
    print(f"Resolved:           {resolved:>8,}")
    print(f"YES resolution:     {(yes_rate or 0)*100:>7.1f}%")
    print(f"MVE combo markets:  {mve_ct:>8,}")
    print(f"Time elapsed:       {elapsed:.1f}s")
    print(f"Log file:           {os.path.abspath(LOG_FILE)}")

    print("\nBy market type:")
    rows = conn.execute("""
        SELECT market_type, COUNT(*) as n, AVG(resolved_yes) as yr
        FROM markets GROUP BY market_type ORDER BY n DESC
    """).fetchall()
    for r in rows:
        print(f"  {(r[0] or 'unknown'):<20} {r[1]:>6,}   {(r[2] or 0)*100:.1f}% YES")

    print("\nTop 5 by volume:")
    rows = conn.execute("""
        SELECT ticker, title, volume_fp, implied_prob, resolved_yes
        FROM markets WHERE volume_fp IS NOT NULL
        ORDER BY volume_fp DESC LIMIT 5
    """).fetchall()
    for r in rows:
        res = "YES" if r[4] == 1 else "NO" if r[4] == 0 else "?"
        print(f"  [{res}] vol={r[2]:>10,.0f}  prob={r[3] or 0:.2f}  {r[1][:48]}")

    print("\nDerived feature coverage:")
    for col in ["spread", "implied_prob", "duration_days", "late_volume_ratio", "price_momentum"]:
        ct  = conn.execute(f"SELECT COUNT(*) FROM markets WHERE {col} IS NOT NULL").fetchone()[0]
        pct = (ct / total * 100) if total > 0 else 0
        print(f"  {col:<22} {ct:>8,}  ({pct:.1f}%)")

    print("=" * 55 + "\n")


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":

    if not os.path.exists(DB_PATH):
        log.error("DB not found: %s", DB_PATH)
        log.error("Run setup_db.py first")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row

    start = datetime.now()

    try:
        total = scrape(conn)
    except SystemExit:
        conn.close()
        sys.exit(1)
    except Exception as e:
        log.error("Fatal error in main: %s", e)
        log.error(traceback.format_exc())
        conn.close()
        sys.exit(1)

    elapsed = round((datetime.now() - start).total_seconds(), 1)
    log.info("Done — %d markets in %.1fs", total, elapsed)

    print_summary(conn, elapsed)
    conn.close()
