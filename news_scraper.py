"""
RSS News Feed Scraper — Async Staggered
-----------------------------------------
Staggers feed checks across the poll interval so something
is always being fetched. Minimizes latency for signal confirmation.

Stores ALL articles — keyword match is a tag, not a filter.
Deduplicates via seen_stories table — survives restarts.

Usage:
    python news_scraper.py /workspace/kalshi.db          # run continuously
    python news_scraper.py /workspace/kalshi.db --once    # single pass then exit

Requirements:
    pip install aiohttp feedparser
"""

import aiohttp
import asyncio
import feedparser
import sqlite3
import sys
import os
import logging
import time
from datetime import datetime

# ── Config ─────────────────────────────────────────────────────────────────────

DB_PATH = sys.argv[1] if len(sys.argv) > 1 else "kalshi.db"
ONCE_MODE = "--once" in sys.argv
POLL_INTERVAL = 7
LOG_FILE = DB_PATH.replace(".db", "_news.log")

# RSS feeds — source_id must match sources table
FEEDS = [
    {"source_id": 2, "name": "Reuters Top",       "url": "https://feeds.reuters.com/reuters/topNews"},
    {"source_id": 2, "name": "Reuters Business",   "url": "https://feeds.reuters.com/reuters/businessNews"},
    {"source_id": 2, "name": "Reuters Politics",   "url": "https://feeds.reuters.com/reuters/politicsNews"},
    {"source_id": 3, "name": "AP Top News",        "url": "https://rss.apnews.com/apnews/topnews"},
    {"source_id": 3, "name": "AP Politics",         "url": "https://rss.apnews.com/apnews/politics"},
    {"source_id": 4, "name": "BBC Top",             "url": "https://feeds.bbci.co.uk/news/rss.xml"},
    {"source_id": 4, "name": "BBC Business",        "url": "https://feeds.bbci.co.uk/news/business/rss.xml"},
    {"source_id": 4, "name": "BBC World",           "url": "https://feeds.bbci.co.uk/news/world/rss.xml"},
]

# Keywords — tagging only, no filtering
KEYWORDS = [
    "fed", "rate", "election", "president", "vote", "congress",
    "inflation", "gdp", "jobs", "unemployment", "recession",
    "trade", "tariff", "sanction", "war", "military",
    "court", "ruling", "supreme", "ban", "law", "bill",
    "crypto", "bitcoin", "ethereum", "sec", "regulate",
    "earthquake", "hurricane", "storm", "wildfire", "flood",
    "nba", "nfl", "mlb", "nhl", "championship", "playoff",
    "earnings", "revenue", "profit", "loss", "merger", "acquisition",
    "oil", "opec", "gas", "energy", "climate",
    "china", "russia", "ukraine", "iran", "north korea",
]

HEADERS = {"User-Agent": "AxonResearch/1.0"}

# ── Logging ────────────────────────────────────────────────────────────────────

log = logging.getLogger("news_scraper")
log.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s %(message)s", "%H:%M:%S"))
log.addHandler(ch)

fh = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s %(message)s", "%Y-%m-%d %H:%M:%S"))
log.addHandler(fh)

# ── Schema update ──────────────────────────────────────────────────────────────

def ensure_keyword_column(conn):
    try:
        conn.execute("ALTER TABLE news_articles ADD COLUMN keyword_match INTEGER DEFAULT 0")
        conn.commit()
        log.info("Added keyword_match column to news_articles")
    except:
        pass

# ── Dedup ──────────────────────────────────────────────────────────────────────

def load_seen_ids(conn):
    rows = conn.execute("SELECT id FROM seen_stories").fetchall()
    return set(r[0] for r in rows)

def mark_seen(conn, story_id, source_id):
    conn.execute(
        "INSERT OR IGNORE INTO seen_stories (id, source_id) VALUES (?, ?)",
        (story_id, source_id)
    )

# ── Keyword tagger ─────────────────────────────────────────────────────────────

def check_keywords(title, summary=""):
    text = (title + " " + summary).lower()
    return 1 if any(kw in text for kw in KEYWORDS) else 0

# ── Insert ─────────────────────────────────────────────────────────────────────

def insert_article(conn, article):
    conn.execute("""
        INSERT OR IGNORE INTO news_articles
        (id, source_id, headline, summary, published_at, keyword_match)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        article["id"],
        article["source_id"],
        article["headline"],
        article["summary"],
        article["published_at"],
        article["keyword_match"],
    ))

def store_articles(conn, articles, seen_ids):
    """Write a batch of articles to DB and update seen set."""
    new_count = 0
    keyword_count = 0

    for article in articles:
        insert_article(conn, article)
        mark_seen(conn, article["id"], article["source_id"])
        seen_ids.add(article["id"])
        new_count += 1
        if article["keyword_match"]:
            keyword_count += 1

    if new_count > 0:
        conn.commit()

    return new_count, keyword_count

# ── Async feed fetching ───────────────────────────────────────────────────────

async def fetch_feed(session, feed_config, seen_ids):
    """Fetch and parse one RSS feed asynchronously."""
    url = feed_config["url"]
    source_id = feed_config["source_id"]
    name = feed_config["name"]

    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                log.warning("%s returned status %d", name, resp.status)
                return []
            text = await resp.text()
    except asyncio.TimeoutError:
        log.warning("%s timed out", name)
        return []
    except Exception as e:
        log.warning("Failed to fetch %s: %s", name, e)
        return []

    feed = feedparser.parse(text)
    new_articles = []

    for entry in feed.entries:
        story_id = entry.get("id") or entry.get("link")
        if not story_id:
            continue
        if story_id in seen_ids:
            continue

        title = entry.get("title", "")
        summary = entry.get("summary", "")
        published = entry.get("published", entry.get("updated", ""))

        new_articles.append({
            "id": story_id,
            "source_id": source_id,
            "headline": title,
            "summary": summary[:500],
            "published_at": published,
            "keyword_match": check_keywords(title, summary),
        })

    return new_articles

# ── Staggered polling ─────────────────────────────────────────────────────────

async def run_staggered(conn, seen_ids, session):
    """
    Staggers feeds across the poll interval.
    With 8 feeds and 7s interval, one feed fires every ~0.875s.
    Something is always being checked.
    """
    num_feeds = len(FEEDS)
    stagger_delay = POLL_INTERVAL / num_feeds  # ~0.875s per feed

    tasks = []
    for i, feed_config in enumerate(FEEDS):
        # Delay each feed by its position in the stagger
        delay = i * stagger_delay
        task = asyncio.create_task(
            staggered_fetch(session, feed_config, seen_ids, conn, delay)
        )
        tasks.append(task)

    results = await asyncio.gather(*tasks, return_exceptions=True)

    total_new = 0
    total_keyword = 0
    for result in results:
        if isinstance(result, tuple):
            total_new += result[0]
            total_keyword += result[1]

    return total_new, total_keyword


async def staggered_fetch(session, feed_config, seen_ids, conn, delay):
    """Wait for stagger delay, then fetch and store."""
    if delay > 0:
        await asyncio.sleep(delay)

    articles = await fetch_feed(session, feed_config, seen_ids)

    if articles:
        new_count, keyword_count = store_articles(conn, articles, seen_ids)
        log.info("%s: +%d articles (%d keyword) [stagger +%.1fs]",
                 feed_config["name"], new_count, keyword_count, delay)
        return new_count, keyword_count

    return 0, 0

# ── Batch polling (for --once mode) ────────────────────────────────────────────

async def run_batch(conn, seen_ids):
    """Fetch all feeds at once — used for --once mode."""
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        tasks = [fetch_feed(session, feed, seen_ids) for feed in FEEDS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    all_articles = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            log.warning("Feed %s failed: %s", FEEDS[i]["name"], result)
            continue
        if result:
            log.info("%s: +%d articles", FEEDS[i]["name"], len(result))
            all_articles.extend(result)

    return store_articles(conn, all_articles, seen_ids)

# ── Main loop ──────────────────────────────────────────────────────────────────

async def run_continuous(conn):
    seen_ids = load_seen_ids(conn)
    cycle = 0

    log.info("=" * 55)
    log.info("NEWS SCRAPER STARTED (async staggered)")
    log.info("DB: %s", os.path.abspath(DB_PATH))
    log.info("Feeds: %d", len(FEEDS))
    log.info("Stagger: %.2fs between feeds", POLL_INTERVAL / len(FEEDS))
    log.info("Cycle interval: %ds", POLL_INTERVAL)
    log.info("Max latency: ~%.1fs", POLL_INTERVAL / len(FEEDS))
    log.info("Keywords: %d (tagging only)", len(KEYWORDS))
    log.info("Mode: %s", "single pass" if ONCE_MODE else "continuous")
    log.info("Seen stories loaded: %d", len(seen_ids))
    log.info("Storing ALL articles")
    log.info("=" * 55)

    if ONCE_MODE:
        new, hits = await run_batch(conn, seen_ids)
        total = conn.execute("SELECT COUNT(*) FROM news_articles").fetchone()[0]
        log.info("Single pass | +%d new (%d keyword) | total: %d", new, hits, total)
        print_summary(conn, seen_ids)
        return

    try:
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            while True:
                cycle += 1
                start = time.time()

                new, hits = await run_staggered(conn, seen_ids, session)

                elapsed = round(time.time() - start, 2)
                total = conn.execute("SELECT COUNT(*) FROM news_articles").fetchone()[0]
                log.info("Cycle %d | +%d new (%d keyword) | total: %d | %.2fs | seen: %d",
                         cycle, new, hits, total, elapsed, len(seen_ids))

                # Wait remaining time in the interval
                remaining = max(0, POLL_INTERVAL - elapsed)
                if remaining > 0:
                    await asyncio.sleep(remaining)

    except KeyboardInterrupt:
        log.info("Stopped by user")

    print_summary(conn, seen_ids)


def print_summary(conn, seen_ids):
    total = conn.execute("SELECT COUNT(*) FROM news_articles").fetchone()[0]
    tagged = conn.execute("SELECT COUNT(*) FROM news_articles WHERE keyword_match = 1").fetchone()[0]
    by_source = conn.execute("""
        SELECT s.name, COUNT(*) as n
        FROM news_articles a
        JOIN sources s ON a.source_id = s.id
        GROUP BY s.name
        ORDER BY n DESC
    """).fetchall()

    print("\n" + "=" * 55)
    print("NEWS SCRAPER SUMMARY")
    print("=" * 55)
    print(f"Total articles:    {total:,}")
    print(f"Keyword matches:   {tagged:,} ({(tagged/total*100) if total > 0 else 0:.1f}%)")
    print(f"Seen stories:      {len(seen_ids):,}")
    print(f"\nLatency profile:")
    print(f"  Poll interval:   {POLL_INTERVAL}s")
    print(f"  Feeds:           {len(FEEDS)}")
    print(f"  Stagger gap:     {POLL_INTERVAL/len(FEEDS):.2f}s")
    print(f"  Max latency:     ~{POLL_INTERVAL/len(FEEDS):.1f}s")
    print("\nBy source:")
    for row in by_source:
        print(f"  {row[0]:<25} {row[1]:>6,}")
    print("=" * 55 + "\n")

# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not os.path.exists(DB_PATH):
        log.error("DB not found: %s — run setup_db.py first", DB_PATH)
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    ensure_keyword_column(conn)
    asyncio.run(run_continuous(conn))
    conn.close()
