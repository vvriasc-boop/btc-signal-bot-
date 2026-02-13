"""
Database operations for orderbook analysis:
insert parsed signals, fill price context, cleanup.
"""
import json
import sqlite3
import logging
from datetime import datetime, timedelta, timezone

from tools.orderbook_config import (
    ALL_TITLES, channel_id_for, infer_side, FEE_RATE,
)
from tools.orderbook_parsers import parse_message

logger = logging.getLogger("orderbook.db")

DB_PATH = None  # Set by caller


def get_db(db_path: str) -> sqlite3.Connection:
    """Open DB with WAL + busy_timeout."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def build_price_index(conn) -> dict:
    """Build {minute_key: price} dict for O(1) lookup."""
    rows = conn.execute("SELECT timestamp, price FROM btc_price ORDER BY timestamp").fetchall()
    idx = {}
    for ts, price in rows:
        idx[ts[:16]] = price
    logger.info(f"Price index: {len(idx)} points")
    return idx


def get_price(price_index: dict, target: datetime, tolerance: int = 2):
    """O(1) price lookup with tolerance."""
    for offset in range(tolerance + 1):
        deltas = ([timedelta(0)] if offset == 0
                  else [timedelta(minutes=offset), timedelta(minutes=-offset)])
        for delta in deltas:
            key = (target + delta).strftime("%Y-%m-%dT%H:%M")
            if key in price_index:
                return price_index[key]
    return None


def _pct_change(base, target):
    if base and target and base > 0:
        return round(((target - base) / base) * 100, 4)
    return None


# ---- Cleanup + Insert ----

def cleanup_orderbook_data(conn, channel_names: list):
    """Delete old signals + price_context for orderbook channels."""
    if not channel_names:
        return
    ph = ",".join("?" * len(channel_names))
    conn.execute(f"""
        DELETE FROM signal_price_context
        WHERE signal_id IN (SELECT id FROM signals WHERE channel_name IN ({ph}))
    """, channel_names)
    conn.execute(f"DELETE FROM signals WHERE channel_name IN ({ph})", channel_names)
    conn.commit()
    logger.info(f"Cleaned up {len(channel_names)} channels")


def parse_and_insert(conn, price_index: dict) -> dict:
    """Parse raw_messages for orderbook channels -> signals table.
    Returns {channel_name: {"parsed": N, "failed": N, "total": N}}.
    """
    stats = {}
    for title in ALL_TITLES:
        rows = conn.execute(
            "SELECT message_id, timestamp, text FROM raw_messages "
            "WHERE channel_name = ? AND text IS NOT NULL ORDER BY timestamp",
            (title,),
        ).fetchall()

        if not rows:
            stats[title] = {"total": 0, "parsed": 0, "failed": 0}
            continue

        ch_id = channel_id_for(title)
        default_side = infer_side(title)
        parsed_count = 0
        failed_count = 0

        for msg_id, ts_str, text in rows:
            result = parse_message(title, text)
            if result is None:
                failed_count += 1
                continue

            side = result.get("side") or default_side
            direction = "bullish" if side == "bid" else "bearish"
            quantity = result.get("quantity")
            btc_price_ch = result.get("btc_price")

            ts_dt = datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
            btc_binance = get_price(price_index, ts_dt)

            extra = {
                "side": side,
                "market": result.get("market"),
                "pct": result.get("pct"),
                "duration_min": result.get("duration_min"),
                "quantity": quantity,
            }

            conn.execute("""
                INSERT OR IGNORE INTO signals
                (channel_id, channel_name, message_id, message_text, timestamp,
                 indicator_value, signal_color, signal_direction, timeframe,
                 btc_price_from_channel, btc_price_binance, extra_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (ch_id, title, msg_id, text[:2000], ts_str,
                  quantity, None, direction, None,
                  btc_price_ch, btc_binance,
                  json.dumps(extra, ensure_ascii=False)))
            parsed_count += 1

        conn.commit()
        stats[title] = {"total": len(rows), "parsed": parsed_count, "failed": failed_count}
        if parsed_count > 0:
            logger.info(f"  {title}: {parsed_count}/{len(rows)} parsed "
                        f"({failed_count} failed)")

    # Register synthetic channels
    for title in ALL_TITLES:
        conn.execute(
            "INSERT OR REPLACE INTO channels (channel_id, name, parser_type) "
            "VALUES (?, ?, ?)",
            (channel_id_for(title), title, "orderbook"),
        )
    conn.commit()

    return stats


def fill_price_context(conn, channel_names: list, price_index: dict) -> int:
    """Fill signal_price_context for orderbook signals."""
    ph = ",".join("?" * len(channel_names))
    rows = conn.execute(f"""
        SELECT s.id, s.timestamp, s.btc_price_binance, s.btc_price_from_channel,
               s.channel_name
        FROM signals s
        LEFT JOIN signal_price_context ctx ON ctx.signal_id = s.id
        WHERE s.channel_name IN ({ph}) AND ctx.id IS NULL
        ORDER BY s.timestamp
    """, channel_names).fetchall()

    filled = 0
    for sig_id, ts_str, bp_bin, bp_ch, ch_name in rows:
        st = datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
        price_at = bp_bin or bp_ch
        if not price_at:
            price_at = get_price(price_index, st)
        if not price_at:
            continue

        p5b = get_price(price_index, st - timedelta(minutes=5))
        p15b = get_price(price_index, st - timedelta(minutes=15))
        p1hb = get_price(price_index, st - timedelta(hours=1))
        p5 = get_price(price_index, st + timedelta(minutes=5))
        p15 = get_price(price_index, st + timedelta(minutes=15))
        p1h = get_price(price_index, st + timedelta(hours=1))
        p4h = get_price(price_index, st + timedelta(hours=4))
        p24h = get_price(price_index, st + timedelta(hours=24))

        mask = 0
        if p5:   mask |= 1
        if p15:  mask |= 2
        if p1h:  mask |= 4
        if p4h:  mask |= 8
        if p24h: mask |= 16

        conn.execute("""
            INSERT OR IGNORE INTO signal_price_context (
                signal_id, channel_name, signal_timestamp, price_at_signal,
                price_5m_before, price_15m_before, price_1h_before,
                price_5m_after, price_15m_after, price_1h_after,
                price_4h_after, price_24h_after,
                change_5m_pct, change_15m_pct, change_1h_pct,
                change_4h_pct, change_24h_pct, filled_mask
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (sig_id, ch_name, ts_str, price_at,
              p5b, p15b, p1hb, p5, p15, p1h, p4h, p24h,
              _pct_change(price_at, p5), _pct_change(price_at, p15),
              _pct_change(price_at, p1h), _pct_change(price_at, p4h),
              _pct_change(price_at, p24h), mask))
        filled += 1

    conn.commit()
    logger.info(f"Filled price context: {filled} signals")
    return filled
