import json
import sqlite3
import logging
from datetime import datetime, timedelta, timezone

import config

logger = logging.getLogger("btc_signal_bot")

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS channels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id INTEGER NOT NULL UNIQUE, name TEXT NOT NULL,
    parser_type TEXT NOT NULL, description TEXT,
    message_count INTEGER DEFAULT 0, last_message_at TEXT,
    is_active BOOLEAN DEFAULT 1, added_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS btc_price (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL UNIQUE, price REAL NOT NULL,
    volume REAL, source TEXT DEFAULT 'binance_kline'
);
CREATE TABLE IF NOT EXISTS raw_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id INTEGER NOT NULL, channel_name TEXT NOT NULL,
    message_id INTEGER NOT NULL, timestamp TEXT NOT NULL, text TEXT,
    has_text BOOLEAN DEFAULT 1, from_username TEXT,
    reply_to_topic_id INTEGER, is_parsed BOOLEAN DEFAULT NULL,
    parse_error TEXT, UNIQUE(channel_id, message_id)
);
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id INTEGER NOT NULL, channel_name TEXT NOT NULL,
    message_id INTEGER NOT NULL, message_text TEXT,
    timestamp TEXT NOT NULL, indicator_value REAL,
    signal_color TEXT, signal_direction TEXT, timeframe TEXT,
    btc_price_from_channel REAL, btc_price_binance REAL,
    extra_data TEXT, UNIQUE(channel_id, message_id)
);
CREATE TABLE IF NOT EXISTS signal_price_context (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id INTEGER NOT NULL UNIQUE, channel_name TEXT NOT NULL,
    signal_timestamp TEXT NOT NULL, price_at_signal REAL,
    price_5m_before REAL, price_15m_before REAL, price_1h_before REAL,
    price_5m_after REAL, price_15m_after REAL, price_1h_after REAL,
    price_4h_after REAL, price_24h_after REAL,
    change_5m_pct REAL, change_15m_pct REAL, change_1h_pct REAL,
    change_4h_pct REAL, change_24h_pct REAL,
    filled_mask INTEGER DEFAULT 0,
    FOREIGN KEY (signal_id) REFERENCES signals(id)
);
CREATE TABLE IF NOT EXISTS sync_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_name TEXT NOT NULL, phase TEXT NOT NULL,
    total_messages INTEGER DEFAULT 0, parsed_ok INTEGER DEFAULT 0,
    parsed_fail INTEGER DEFAULT 0, skipped_media INTEGER DEFAULT 0,
    skipped_filter INTEGER DEFAULT 0, earliest_message TEXT,
    latest_message TEXT, started_at TEXT, completed_at TEXT, notes TEXT
);
CREATE INDEX IF NOT EXISTS idx_price_ts ON btc_price(timestamp);
CREATE INDEX IF NOT EXISTS idx_sig_ts ON signals(timestamp);
CREATE INDEX IF NOT EXISTS idx_sig_ch ON signals(channel_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_sig_dir ON signals(signal_direction, timestamp);
CREATE INDEX IF NOT EXISTS idx_ctx_mask ON signal_price_context(filled_mask);
CREATE INDEX IF NOT EXISTS idx_ctx_ts ON signal_price_context(signal_timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_ch ON raw_messages(channel_id, message_id);
CREATE INDEX IF NOT EXISTS idx_raw_parsed ON raw_messages(is_parsed, channel_name);
"""


def init_database() -> sqlite3.Connection:
    """Create tables and indexes, return connection."""
    conn = sqlite3.connect('btc_signals.db')
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA_SQL)
    return conn


def build_price_index() -> dict:
    """Build {minute_key: price} dict for O(1) lookup. ~15 MB for 90 days."""
    rows = config.db.execute(
        "SELECT timestamp, price FROM btc_price ORDER BY timestamp"
    ).fetchall()
    idx = {}
    for row in rows:
        idx[row["timestamp"][:16]] = row["price"]
    logger.info(f"Price index: {len(idx)} points in RAM")
    return idx


def get_price_fast(target: datetime, tolerance_minutes: int = 2):
    """O(1) price lookup from global price_index."""
    for offset in range(tolerance_minutes + 1):
        deltas = ([timedelta(0)] if offset == 0
                  else [timedelta(minutes=offset), timedelta(minutes=-offset)])
        for delta in deltas:
            key = (target + delta).strftime("%Y-%m-%dT%H:%M")
            if key in config.price_index:
                return config.price_index[key]
    return None


def get_closest_price_sql(target_time: datetime, tolerance_minutes: int = 2):
    """SQL fallback for live mode single lookups."""
    ts = target_time.strftime("%Y-%m-%dT%H:%M:%S")
    lo = (target_time - timedelta(minutes=tolerance_minutes)).strftime("%Y-%m-%dT%H:%M:%S")
    hi = (target_time + timedelta(minutes=tolerance_minutes)).strftime("%Y-%m-%dT%H:%M:%S")
    row = config.db.execute("""
        SELECT price FROM btc_price WHERE timestamp BETWEEN ? AND ?
        ORDER BY ABS(julianday(timestamp) - julianday(?)) LIMIT 1
    """, (lo, hi, ts)).fetchone()
    return row["price"] if row else None


def save_signals_batch(batch: list):
    """Insert a batch of parsed signals into the signals table."""
    for sig in batch:
        p = sig["parsed"]
        try:
            config.db.execute("""
                INSERT OR IGNORE INTO signals
                (channel_id, channel_name, message_id, message_text, timestamp,
                 indicator_value, signal_color, signal_direction, timeframe,
                 btc_price_from_channel, btc_price_binance, extra_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (sig["channel_id"], sig["channel_name"],
                  sig["message_id"], sig["message_text"][:2000],
                  sig["timestamp"],
                  p.get("value"), p.get("color"), p.get("direction"),
                  p.get("timeframe"), p.get("btc_price"), sig["btc_price_binance"],
                  json.dumps(p.get("extra", {}), ensure_ascii=False)))
        except Exception as e:
            logger.error(f"save_signal: {e}")
    config.db.commit()


async def resolve_channel_ids(channel_config: dict) -> dict:
    """Resolve @username -> numeric chat_id, register in channels table."""
    resolved = {}
    for key, cfg in channel_config.items():
        try:
            if key.lstrip('-').isdigit():
                chat_id = int(key)
            else:
                chat = await config.userbot.get_chat(key)
                chat_id = chat.id
                logger.info(f"Resolved {key} -> {chat_id}")
            resolved[chat_id] = cfg
            config.db.execute("""
                INSERT OR IGNORE INTO channels (channel_id, name, parser_type)
                VALUES (?, ?, ?)
            """, (chat_id, cfg["name"], cfg["parser"]))
        except Exception as e:
            logger.error(f"Cannot resolve {key}: {e}")
    config.db.commit()
    return resolved


def create_live_price_context(signal_id, channel_name, signal_time, price_at):
    """Create initial price context for a live signal (before-prices only)."""
    if not price_at:
        return
    p5b = get_price_fast(signal_time - timedelta(minutes=5))
    p15b = get_price_fast(signal_time - timedelta(minutes=15))
    p1hb = get_price_fast(signal_time - timedelta(hours=1))
    config.db.execute("""
        INSERT OR IGNORE INTO signal_price_context
        (signal_id, channel_name, signal_timestamp, price_at_signal,
         price_5m_before, price_15m_before, price_1h_before, filled_mask)
        VALUES (?,?,?,?,?,?,?,0)
    """, (signal_id, channel_name, signal_time.strftime("%Y-%m-%dT%H:%M:%S"),
          price_at, p5b, p15b, p1hb))
    config.db.commit()
