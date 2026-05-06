"""
Kalshi Database Schema Setup
------------------------------
Run this once to initialize kalshi.db on your network volume.
Safe to re-run — all statements use IF NOT EXISTS.

Usage:
    python setup_db.py                        # default path
    python setup_db.py /mnt/kalshi.db        # custom path

Requirements:
    sqlite3 — built into Python, no install needed
"""

import sqlite3
import sys
import os
from datetime import datetime

# ── Config ─────────────────────────────────────────────────────────────────────

DB_PATH = sys.argv[1] if len(sys.argv) > 1 else "kalshi.db"

# ── Schema ─────────────────────────────────────────────────────────────────────

SCHEMA = """

-- ─────────────────────────────────────────────
-- SOURCES
-- Registry of all data sources.
-- Add a new row here to register a new source.
-- No schema changes ever needed to add a source.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sources (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,          -- "Reuters RSS", "Kalshi API"
    type            TEXT,                   -- "rss", "websocket", "rest", "scrape"
    url             TEXT,                   -- source endpoint
    protocol        TEXT,                   -- "http", "websocket"
    active          INTEGER DEFAULT 1,      -- 1 = running, 0 = paused
    added_at        TEXT DEFAULT (datetime('now'))
);

-- Seed default sources
INSERT OR IGNORE INTO sources (id, name, type, url, protocol)
VALUES
    (1, 'Kalshi API',    'rest',      'https://api.elections.kalshi.com/trade-api/v2', 'http'),
    (2, 'Reuters RSS',   'rss',       'https://feeds.reuters.com/reuters/topNews',     'http'),
    (3, 'AP Wire RSS',   'rss',       'https://rss.apnews.com/apnews/topnews',         'http'),
    (4, 'BBC RSS',       'rss',       'https://feeds.bbci.co.uk/news/rss.xml',         'http'),
    (5, 'Twitter/X',     'websocket', 'wss://api.twitter.com/2/tweets/search/stream',  'websocket'),
    (6, 'SEC EDGAR',     'rss',       'https://www.sec.gov/cgi-bin/browse-edgar',      'http');


-- ─────────────────────────────────────────────
-- MARKETS
-- Core Kalshi historical market data.
-- One row per resolved market.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS markets (
    -- Identity
    ticker                      TEXT PRIMARY KEY,
    event_ticker                TEXT,
    mve_collection_ticker       TEXT,           -- MVE group ID, null if not a combo market

    -- Description
    title                       TEXT,
    market_type                 TEXT,           -- "binary", "scalar"
    strike_type                 TEXT,           -- "custom", "standard"

    -- YES side pricing
    yes_bid_dollars             REAL,
    yes_ask_dollars             REAL,
    previous_yes_bid_dollars    REAL,
    previous_yes_ask_dollars    REAL,

    -- NO side pricing
    no_bid_dollars              REAL,
    no_ask_dollars              REAL,

    -- Volume + liquidity
    volume_fp                   REAL,           -- total contracts traded
    volume_24h_fp               REAL,           -- contracts in last 24h
    open_interest_fp            REAL,           -- outstanding contracts
    last_price_dollars          REAL,           -- final traded price

    -- Resolution
    result                      TEXT,           -- "yes" or "no"
    settlement_value_dollars    REAL,           -- final payout
    settlement_ts               TEXT,           -- when it settled

    -- Timing
    open_time                   TEXT,
    close_time                  TEXT,

    -- ── Derived fields (computed on insert) ──────────────
    spread                      REAL,           -- yes_ask - yes_bid
    implied_prob                REAL,           -- yes_bid = market's probability estimate
    resolved_yes                INTEGER,        -- 1 = YES, 0 = NO, NULL = unresolved
    duration_days               REAL,           -- close_time - open_time in days
    late_volume_ratio           REAL,           -- volume_24h / volume_fp
    is_mve                      INTEGER,        -- 1 if combo market, 0 if standard
    price_momentum              REAL,           -- yes_bid - previous_yes_bid

    -- ── Raw JSON backup ──────────────────────────────────
    -- Nested fields stored as text — queryable via json_extract() if needed
    custom_strike_json          TEXT,           -- custom_strike object
    mve_selected_legs_json      TEXT,           -- mve_selected_legs array
    price_ranges_json           TEXT,           -- price_ranges array

    -- ── Meta ─────────────────────────────────────────────
    source_id                   INTEGER REFERENCES sources(id) DEFAULT 1,
    scraped_at                  TEXT DEFAULT (datetime('now'))
);


-- ─────────────────────────────────────────────
-- NEWS ARTICLES
-- All scraped news stories from all sources.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS news_articles (
    id              TEXT PRIMARY KEY,       -- URL or API-provided ID
    source_id       INTEGER REFERENCES sources(id),
    headline        TEXT,
    summary         TEXT,
    published_at    TEXT,
    scraped_at      TEXT DEFAULT (datetime('now'))
);


-- ─────────────────────────────────────────────
-- NEWS MARKET LINKS
-- Maps a news story to relevant markets.
-- Built by the NLP classifier.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS news_market_links (
    article_id      TEXT REFERENCES news_articles(id),
    market_ticker   TEXT REFERENCES markets(ticker),
    confidence      REAL,                   -- LLM confidence score 0.0-1.0
    direction       TEXT,                   -- "yes_favored", "no_favored", "unclear"
    created_at      TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (article_id, market_ticker)
);


-- ─────────────────────────────────────────────
-- MODEL VERSIONS
-- Tracks which ML model produced which scores.
-- Lets you compare model versions over time.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS model_versions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT,                   -- "gbm_v1", "gbm_v2_with_nlp"
    features        TEXT,                   -- JSON list of feature names used
    trained_at      TEXT DEFAULT (datetime('now')),
    notes           TEXT                    -- free text — what changed
);


-- ─────────────────────────────────────────────
-- EDGE SCORES
-- ML model outputs per market.
-- Versioned so you can compare models.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS edge_scores (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    market_ticker       TEXT REFERENCES markets(ticker),
    model_version_id    INTEGER REFERENCES model_versions(id),
    model_prob          REAL,               -- model's predicted probability
    implied_prob        REAL,               -- market's implied probability at scoring time
    edge                REAL,               -- model_prob - implied_prob
    scored_at           TEXT DEFAULT (datetime('now'))
);


-- ─────────────────────────────────────────────
-- ALERTS
-- Log of every edge signal fired.
-- Tracks whether you acted on it.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS alerts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    market_ticker   TEXT REFERENCES markets(ticker),
    edge            REAL,                   -- edge score that triggered alert
    model_prob      REAL,
    implied_prob    REAL,
    triggered_by    TEXT,                   -- "ml_model", "news_signal", "manual"
    acted_on        INTEGER DEFAULT 0,      -- 1 = you traded on it, 0 = observed only
    outcome         TEXT,                   -- "correct", "incorrect", NULL = pending
    fired_at        TEXT DEFAULT (datetime('now'))
);


-- ─────────────────────────────────────────────
-- SEEN STORIES
-- Persistent dedup store for news scraper.
-- Survives pod restarts — no reprocessing.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS seen_stories (
    id          TEXT PRIMARY KEY,           -- article URL or API ID
    source_id   INTEGER REFERENCES sources(id),
    seen_at     TEXT DEFAULT (datetime('now'))
);


-- ─────────────────────────────────────────────
-- INDEXES
-- Critical for ML query performance.
-- ─────────────────────────────────────────────

-- Markets — most queried columns
CREATE INDEX IF NOT EXISTS idx_markets_event          ON markets(event_ticker);
CREATE INDEX IF NOT EXISTS idx_markets_mve            ON markets(mve_collection_ticker);
CREATE INDEX IF NOT EXISTS idx_markets_type           ON markets(market_type);
CREATE INDEX IF NOT EXISTS idx_markets_resolved       ON markets(resolved_yes);
CREATE INDEX IF NOT EXISTS idx_markets_close          ON markets(close_time);
CREATE INDEX IF NOT EXISTS idx_markets_volume         ON markets(volume_fp);
CREATE INDEX IF NOT EXISTS idx_markets_implied        ON markets(implied_prob);
CREATE INDEX IF NOT EXISTS idx_markets_is_mve         ON markets(is_mve);

-- Edge scores — queried heavily during analysis
CREATE INDEX IF NOT EXISTS idx_edges_market           ON edge_scores(market_ticker);
CREATE INDEX IF NOT EXISTS idx_edges_edge             ON edge_scores(edge);
CREATE INDEX IF NOT EXISTS idx_edges_model            ON edge_scores(model_version_id);
CREATE INDEX IF NOT EXISTS idx_edges_scored_at        ON edge_scores(scored_at);

-- News
CREATE INDEX IF NOT EXISTS idx_news_source            ON news_articles(source_id);
CREATE INDEX IF NOT EXISTS idx_news_published         ON news_articles(published_at);
CREATE INDEX IF NOT EXISTS idx_links_market           ON news_market_links(market_ticker);
CREATE INDEX IF NOT EXISTS idx_links_article          ON news_market_links(article_id);

-- Alerts
CREATE INDEX IF NOT EXISTS idx_alerts_market          ON alerts(market_ticker);
CREATE INDEX IF NOT EXISTS idx_alerts_fired           ON alerts(fired_at);
CREATE INDEX IF NOT EXISTS idx_alerts_acted           ON alerts(acted_on);

"""

# ── Setup ──────────────────────────────────────────────────────────────────────

def setup(db_path):
    print(f"Setting up database: {db_path}")

    existed = os.path.exists(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")     # better concurrent read performance
    conn.execute("PRAGMA foreign_keys=ON")      # enforce referential integrity
    conn.execute("PRAGMA synchronous=NORMAL")   # balance safety vs speed

    conn.executescript(SCHEMA)
    conn.commit()

    # Summary
    tables = conn.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' ORDER BY name
    """).fetchall()

    indexes = conn.execute("""
        SELECT name FROM sqlite_master
        WHERE type='index' ORDER BY name
    """).fetchall()

    sources = conn.execute("SELECT id, name, type FROM sources").fetchall()

    conn.close()

    print()
    print("=" * 50)
    print("DATABASE READY")
    print("=" * 50)
    print(f"Path:     {os.path.abspath(db_path)}")
    print(f"Status:   {'existing' if existed else 'created fresh'}")
    print(f"Tables:   {len(tables)}")
    for t in tables:
        print(f"  → {t[0]}")
    print(f"Indexes:  {len(indexes)}")
    print(f"Sources seeded: {len(sources)}")
    for s in sources:
        print(f"  [{s[0]}] {s[1]} ({s[2]})")
    print("=" * 50)
    print()
    print("Next step: run kalshi_scraper.py to populate markets table")


if __name__ == "__main__":
    setup(DB_PATH)
