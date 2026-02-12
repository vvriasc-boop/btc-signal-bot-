import os
import json
import asyncio
import logging
from datetime import datetime, timedelta, timezone

import config
from database.db import get_price_fast, get_closest_price_sql, create_live_price_context
from services.binance import fetch_btc_price
from services.parsers import parse_message, validate_parsed, is_from_author
from utils.telegram import send_admin_message

logger = logging.getLogger("btc_signal_bot")


# ---- Live signal handler ----

async def on_new_signal(client, message):
    """Pyrogram handler for new messages from monitored channels."""
    if not message.text:
        return
    ch_config = config.RESOLVED_CHANNELS.get(message.chat.id)
    if not ch_config:
        return
    if not _passes_live_filter(message, ch_config):
        return

    ts_str = message.date.strftime("%Y-%m-%dT%H:%M:%S")
    _save_raw_live(message, ch_config, ts_str)

    parsed = parse_message(ch_config["parser"], message.text)
    if parsed is None:
        _mark_unparsed(message, ch_config, ts_str)
        return

    valid, reason = validate_parsed(ch_config["parser"], parsed)
    if not valid:
        config.db.execute(
            "UPDATE raw_messages SET is_parsed=0, parse_error=? "
            "WHERE channel_id=? AND message_id=?",
            (f"validation: {reason}", message.chat.id, message.id))
        config.db.commit()
        return

    btc_price = (get_price_fast(message.date) or
                 get_closest_price_sql(message.date) or
                 await fetch_btc_price())
    _save_live_signal(message, ch_config, ts_str, parsed, btc_price)
    logger.info(f"[{ch_config['name']}] LIVE: val={parsed.get('value')}, dir={parsed.get('direction')}")


def _passes_live_filter(message, ch_config) -> bool:
    """Check if live message passes author/topic filter."""
    if ch_config.get("filter_author"):
        if not is_from_author(message, ch_config["filter_author"]):
            return False
    if ch_config.get("topic_id") is not None:
        tid = ch_config["topic_id"]
        if tid > 0:
            if getattr(message, 'reply_to_top_message_id', None) != tid:
                return False
        elif "BTCUSDT" not in message.text.upper():
            return False
    return True


def _save_raw_live(message, ch_config, ts_str):
    """Save raw message to DB for a live message."""
    from_username = (message.from_user.username if message.from_user else
                     message.sender_chat.username if message.sender_chat else None)
    topic_id = getattr(message, 'reply_to_top_message_id', None)
    config.db.execute("""
        INSERT OR IGNORE INTO raw_messages
        (channel_id, channel_name, message_id, timestamp, text, has_text,
         from_username, reply_to_topic_id)
        VALUES (?,?,?,?,?,1,?,?)
    """, (message.chat.id, ch_config["name"], message.id, ts_str,
          message.text[:2000], from_username, topic_id))


def _mark_unparsed(message, ch_config, ts_str):
    """Mark message as unparsed and log to unrecognized file."""
    config.db.execute(
        "UPDATE raw_messages SET is_parsed=0, parse_error='no_match' "
        "WHERE channel_id=? AND message_id=?",
        (message.chat.id, message.id))
    config.db.commit()
    os.makedirs(config.UNRECOGNIZED_DIR, exist_ok=True)
    with open(os.path.join(config.UNRECOGNIZED_DIR, "live_unrecognized.jsonl"), 'a',
              encoding='utf-8') as f:
        json.dump({"channel": ch_config["name"], "message_id": message.id,
                   "timestamp": ts_str, "text": message.text[:500]}, f,
                  ensure_ascii=False)
        f.write('\n')


def _save_live_signal(message, ch_config, ts_str, parsed, btc_price):
    """Save parsed signal to signals table and create price context."""
    try:
        cursor = config.db.execute("""
            INSERT OR IGNORE INTO signals
            (channel_id, channel_name, message_id, message_text, timestamp,
             indicator_value, signal_color, signal_direction, timeframe,
             btc_price_from_channel, btc_price_binance, extra_data)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (message.chat.id, ch_config["name"], message.id, message.text[:2000], ts_str,
              parsed.get("value"), parsed.get("color"), parsed.get("direction"),
              parsed.get("timeframe"), parsed.get("btc_price"), btc_price,
              json.dumps(parsed.get("extra", {}), ensure_ascii=False)))
        config.db.commit()
        config.db.execute(
            "UPDATE raw_messages SET is_parsed=1 WHERE channel_id=? AND message_id=?",
            (message.chat.id, message.id))
        config.db.commit()
        if cursor.rowcount > 0:
            create_live_price_context(cursor.lastrowid, ch_config["name"],
                                      message.date, btc_price)
    except Exception as e:
        logger.error(f"Live save: {e}")


# ---- Background loops ----

async def price_ticker_loop():
    """Fetch current BTC price every 60 seconds."""
    while True:
        try:
            price = await fetch_btc_price()
            if price:
                ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:00")
                config.db.execute(
                    "INSERT OR IGNORE INTO btc_price (timestamp, price, source) "
                    "VALUES (?,?,'ticker')", (ts, price))
                config.db.commit()
                config.price_index[ts[:16]] = price
        except Exception as e:
            logger.error(f"Ticker: {e}")
        await asyncio.sleep(60)


async def fill_delayed_prices_loop():
    """Fill after-prices for signals every 5 minutes."""
    while True:
        try:
            _fill_delayed_batch()
        except Exception as e:
            logger.error(f"Fill delayed: {e}")
        await asyncio.sleep(300)


def _fill_delayed_batch():
    """Process one batch of unfilled price contexts."""
    now = datetime.now(timezone.utc)
    rows = config.db.execute("""
        SELECT id, signal_id, signal_timestamp, price_at_signal, filled_mask
        FROM signal_price_context WHERE filled_mask < 31
        ORDER BY signal_timestamp DESC LIMIT 200
    """).fetchall()

    for row in rows:
        sig_t = datetime.fromisoformat(row["signal_timestamp"]).replace(tzinfo=timezone.utc)
        p_at = row["price_at_signal"]
        mask = row["filled_mask"]
        if not p_at or p_at <= 0:
            continue
        new_mask = mask
        updates = {}
        for mins, fld, pct_fld, bit in [
            (5, "price_5m_after", "change_5m_pct", config.MASK_5M),
            (15, "price_15m_after", "change_15m_pct", config.MASK_15M),
            (60, "price_1h_after", "change_1h_pct", config.MASK_1H),
            (240, "price_4h_after", "change_4h_pct", config.MASK_4H),
            (1440, "price_24h_after", "change_24h_pct", config.MASK_24H),
        ]:
            if not (mask & bit) and now >= sig_t + timedelta(minutes=mins + 1):
                p = (get_price_fast(sig_t + timedelta(minutes=mins)) or
                     get_closest_price_sql(sig_t + timedelta(minutes=mins)))
                if p:
                    updates[fld] = p
                    updates[pct_fld] = round(((p - p_at) / p_at) * 100, 4)
                    new_mask |= bit
        if new_mask != mask:
            cols = ", ".join(f"{k}=?" for k in updates)
            config.db.execute(
                f"UPDATE signal_price_context SET {cols}, filled_mask=? WHERE id=?",
                list(updates.values()) + [new_mask, row["id"]])
    config.db.commit()


async def healthcheck_loop():
    """Check channel health every hour."""
    while True:
        await asyncio.sleep(3600)
        try:
            issues = []
            now = datetime.now(timezone.utc)
            price = await fetch_btc_price()
            if not price:
                issues.append("\u26a0\ufe0f Binance not responding")
            for cid, cfg in config.RESOLVED_CHANNELS.items():
                row = config.db.execute(
                    "SELECT MAX(timestamp) as t FROM signals WHERE channel_id=?", (cid,)
                ).fetchone()
                if row and row["t"]:
                    hrs = (now - datetime.fromisoformat(row["t"]).replace(
                        tzinfo=timezone.utc)).total_seconds() / 3600
                    if hrs > 48:
                        issues.append(f"\u26a0\ufe0f {cfg['name']}: silent {int(hrs)}h")
            if issues:
                await send_admin_message(
                    "\U0001f514 Healthcheck:\n" + "\n".join(issues))
        except Exception as e:
            logger.error(f"Healthcheck: {e}")
