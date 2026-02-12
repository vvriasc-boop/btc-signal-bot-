import os
import json
import asyncio
import logging
from datetime import datetime, timedelta, timezone

from pyrogram.errors import FloodWait

import config
from database.db import (
    build_price_index, get_price_fast, save_signals_batch,
)
from services.binance import fetch_btc_price_history
from services.parsers import parse_message, validate_parsed
from utils.helpers import fmt_madrid, pct_change
from utils.telegram import send_admin_message

logger = logging.getLogger("btc_signal_bot")


# ---- Phase 0: Price backbone ----

async def phase_0_load_prices():
    """Download BTC price history. Default 90 days, incremental after."""
    logger.info("=" * 60)
    logger.info("PHASE 0: BTC price backbone")
    logger.info("=" * 60)

    existing = config.db.execute(
        "SELECT COUNT(*) as cnt, MIN(timestamp) as earliest FROM btc_price"
    ).fetchone()

    if existing["cnt"] > 10000:
        last = config.db.execute("SELECT MAX(timestamp) as ts FROM btc_price").fetchone()
        start = datetime.fromisoformat(last["ts"]).replace(tzinfo=timezone.utc)
        logger.info(f"Have {existing['cnt']} points, loading from {start}")
    else:
        start = datetime.now(timezone.utc) - timedelta(days=90)
        logger.info(f"First run, loading from {start.isoformat()}")

    klines = await fetch_btc_price_history(start, datetime.now(timezone.utc))
    total_before = config.db.execute("SELECT COUNT(*) as cnt FROM btc_price").fetchone()["cnt"]

    for kline in klines:
        ts = datetime.fromtimestamp(kline[0] / 1000, tz=timezone.utc)
        ts_str = ts.strftime("%Y-%m-%dT%H:%M:%S")
        try:
            config.db.execute(
                "INSERT OR IGNORE INTO btc_price (timestamp, price, volume, source) "
                "VALUES (?, ?, ?, 'binance_kline')",
                (ts_str, float(kline[4]), float(kline[5]))
            )
        except Exception:
            pass
    config.db.commit()

    total = config.db.execute("SELECT COUNT(*) as cnt FROM btc_price").fetchone()["cnt"]
    inserted = total - total_before
    logger.info(f"PHASE 0: +{inserted} new, total {total} ({total // 1440} days)")

    config.price_index = build_price_index()
    return total


async def phase_0_extend(earliest_signal: datetime):
    """Extend price history if signals are older than loaded prices."""
    min_row = config.db.execute("SELECT MIN(timestamp) as ts FROM btc_price").fetchone()
    if not min_row or not min_row["ts"]:
        return
    min_price_date = datetime.fromisoformat(min_row["ts"]).replace(tzinfo=timezone.utc)
    if earliest_signal.tzinfo is None:
        earliest_signal = earliest_signal.replace(tzinfo=timezone.utc)
    if earliest_signal >= min_price_date:
        return

    logger.info(f"Extending prices: signal {earliest_signal} older than {min_price_date}")
    start = earliest_signal - timedelta(days=1)
    klines = await fetch_btc_price_history(start, min_price_date)
    for kline in klines:
        ts = datetime.fromtimestamp(kline[0] / 1000, tz=timezone.utc)
        ts_str = ts.strftime("%Y-%m-%dT%H:%M:%S")
        config.db.execute(
            "INSERT OR IGNORE INTO btc_price (timestamp, price, volume, source) "
            "VALUES (?, ?, ?, 'binance_kline')",
            (ts_str, float(kline[4]), float(kline[5]))
        )
    config.db.commit()
    logger.info(f"Extended with {len(klines)} points")
    config.price_index = build_price_index()


# ---- Phases 1-9: Per-channel parsing ----

async def download_and_save_raw(chat_id: int, channel_name: str) -> int:
    """Download ALL messages from channel -> raw_messages (not in RAM)."""
    count = 0
    offset_id = 0
    consecutive_errors = 0

    while True:
        try:
            batch = []
            async for msg in config.userbot.get_chat_history(
                chat_id, limit=100, offset_id=offset_id
            ):
                batch.append(msg)
            if not batch:
                break
            _save_raw_batch(batch, chat_id, channel_name)
            count += len(batch)
            offset_id = batch[-1].id
            consecutive_errors = 0
            if count % 500 == 0:
                logger.info(f"  Downloaded {count} messages...")
            await asyncio.sleep(0.5)
        except FloodWait as e:
            logger.warning(f"FloodWait {e.value}s at {count} messages, waiting...")
            await asyncio.sleep(e.value + 2)
            continue
        except Exception as e:
            consecutive_errors += 1
            logger.error(f"download error at {count} (attempt {consecutive_errors}): {e}")
            if consecutive_errors >= 5:
                logger.error(f"download: {consecutive_errors} errors, stopping {channel_name}")
                break
            await asyncio.sleep(5)
            continue

    logger.info(f"  Total downloaded: {count}")
    return count


def _save_raw_batch(batch, chat_id, channel_name):
    """Save a batch of Pyrogram messages to raw_messages."""
    for msg in batch:
        ts_str = msg.date.strftime("%Y-%m-%dT%H:%M:%S")
        from_username = (msg.from_user.username if msg.from_user else
                         msg.sender_chat.username if msg.sender_chat else None)
        topic_id = getattr(msg, 'reply_to_top_message_id', None)
        config.db.execute("""
            INSERT OR IGNORE INTO raw_messages
            (channel_id, channel_name, message_id, timestamp, text, has_text,
             from_username, reply_to_topic_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (chat_id, channel_name, msg.id, ts_str,
              msg.text[:2000] if msg.text else None,
              1 if msg.text else 0, from_username, topic_id))
    config.db.commit()


def _should_filter_row(row, channel_config) -> bool:
    """Returns True if row should be skipped by author/topic filter."""
    if channel_config.get("filter_author"):
        stored_user = row["from_username"]
        if not stored_user or stored_user.lower() != channel_config["filter_author"].lower():
            return True
    if channel_config.get("topic_id") is not None:
        tid = channel_config["topic_id"]
        if tid > 0 and row["reply_to_topic_id"] != tid:
            return True
        if tid == 0 and "BTCUSDT" not in row["text"].upper():
            return True
    return False


def parse_raw_messages(chat_id, channel_name, parser_type, channel_config, unrec_file):
    """Read raw_messages from DB, parse, save results. Returns stats dict."""
    rows = config.db.execute("""
        SELECT id, message_id, timestamp, text, from_username, reply_to_topic_id
        FROM raw_messages WHERE channel_id = ? AND has_text = 1 ORDER BY timestamp
    """, (chat_id,)).fetchall()

    stats = _init_parse_stats(chat_id, len(rows))
    signals_batch = []

    with open(unrec_file, 'a', encoding='utf-8') as unrec_fh:
        for row in rows:
            msg_time = datetime.fromisoformat(row["timestamp"]).replace(tzinfo=timezone.utc)
            if stats["earliest"] is None or msg_time < stats["earliest"]:
                stats["earliest"] = msg_time
            if stats["latest"] is None or msg_time > stats["latest"]:
                stats["latest"] = msg_time

            if _should_filter_row(row, channel_config):
                stats["skipped_filter"] += 1
                continue

            result = _parse_single_row(
                row, chat_id, channel_name, parser_type, msg_time, stats, unrec_fh
            )
            if result:
                signals_batch.append(result)
                if len(signals_batch) >= 100:
                    save_signals_batch(signals_batch)
                    signals_batch = []

    if signals_batch:
        save_signals_batch(signals_batch)
    config.db.commit()
    return stats


def _init_parse_stats(chat_id, text_count):
    """Initialize stats dict for parse_raw_messages."""
    total_raw = config.db.execute(
        "SELECT COUNT(*) as c FROM raw_messages WHERE channel_id=?", (chat_id,)
    ).fetchone()["c"]
    return {
        "total_raw": total_raw, "text_messages": text_count,
        "parsed_ok": 0, "parsed_fail": 0, "skipped_filter": 0,
        "validation_fail": 0, "errors": [],
        "earliest": None, "latest": None, "fail_examples": [],
    }


def _parse_single_row(row, chat_id, channel_name, parser_type, msg_time, stats, unrec_fh):
    """Parse one raw message row. Returns signal dict or None."""
    text = row["text"]
    parsed = parse_message(parser_type, text)

    if parsed is None:
        stats["parsed_fail"] += 1
        _log_parse_failure(row, channel_name, unrec_fh, stats, "parser_returned_none")
        config.db.execute(
            "UPDATE raw_messages SET is_parsed=0, parse_error='no_match' WHERE id=?",
            (row["id"],))
        return None

    valid, reason = validate_parsed(parser_type, parsed)
    if not valid:
        stats["validation_fail"] += 1
        _log_parse_failure(row, channel_name, unrec_fh, stats, f"validation: {reason}")
        config.db.execute(
            "UPDATE raw_messages SET is_parsed=0, parse_error=? WHERE id=?",
            (f"validation: {reason}", row["id"]))
        return None

    stats["parsed_ok"] += 1
    config.db.execute("UPDATE raw_messages SET is_parsed=1 WHERE id=?", (row["id"],))
    return {
        "channel_id": chat_id, "channel_name": channel_name,
        "message_id": row["message_id"], "message_text": text,
        "timestamp": row["timestamp"], "parsed": parsed,
        "btc_price_binance": get_price_fast(msg_time),
    }


def _log_parse_failure(row, channel_name, unrec_fh, stats, reason):
    """Log a parse failure to JSONL file and stats."""
    entry = {
        "channel": channel_name, "message_id": row["message_id"],
        "timestamp": row["timestamp"], "text": row["text"][:500], "reason": reason,
    }
    json.dump(entry, unrec_fh, ensure_ascii=False)
    unrec_fh.write('\n')
    if len(stats["fail_examples"]) < 5:
        stats["fail_examples"].append(entry)


def generate_channel_report(num, name, stats, unrec_file) -> str:
    """Generate human-readable report for a channel."""
    ok, fail = stats["parsed_ok"], stats["parsed_fail"]
    val_fail, text_msgs = stats["validation_fail"], stats["text_messages"]
    filtered, total_raw = stats["skipped_filter"], stats["total_raw"]
    parseable = text_msgs - filtered
    rate = (ok / max(parseable, 1)) * 100
    e_str = fmt_madrid(stats['earliest'].strftime("%Y-%m-%dT%H:%M:%S")) if stats['earliest'] else "N/A"
    l_str = fmt_madrid(stats['latest'].strftime("%Y-%m-%dT%H:%M:%S")) if stats['latest'] else "N/A"

    report = (
        f"\U0001f4ca REPORT \u2014 Channel {num}: {name}\n{'=' * 40}\n\n"
        f"\U0001f4e5 Downloaded:           {stats.get('downloaded', total_raw)}\n"
        f"\U0001f4dd With text:            {text_msgs}\n"
        f"\U0001f5bc No text (media):      {total_raw - text_msgs}\n"
        f"\U0001f507 Filtered:             {filtered}\n"
        f"\u2705 Parsed:               {ok} ({rate:.1f}%)\n"
        f"\u274c Unrecognized:         {fail}\n"
        f"\u26a0\ufe0f Validation fail:      {val_fail}\n\n"
        f"\U0001f4c5 Period: {e_str} \u2014 {l_str}\n"
    )
    if stats["fail_examples"]:
        report += "\n\u274c Unrecognized examples:\n"
        for i, ex in enumerate(stats["fail_examples"][:3], 1):
            report += f"  {i}. [{ex['timestamp'][:16]}] {ex['text'][:80].replace(chr(10), ' ')}...\n"
        if fail > 3:
            report += f"  ... and {fail - 3} more (see {unrec_file})\n"
    if stats["errors"]:
        report += f"\n\U0001f6a8 Errors: {'; '.join(stats['errors'])}\n"
    if rate < 80:
        report += f"\n\U0001f534 LOW % ({rate:.0f}%) -> check parser!"
    elif rate < 95:
        report += "\n\U0001f7e1 OK, but some unrecognized."
    else:
        report += "\n\U0001f7e2 Excellent result!"
    return report


async def phase_channel(channel_num, chat_id, channel_config):
    """Phase N: Download -> raw_messages -> parse -> report."""
    name = channel_config["name"]
    parser_type = channel_config["parser"]

    logger.info("=" * 60)
    logger.info(f"PHASE {channel_num}: '{name}' (chat_id={chat_id})")
    logger.info("=" * 60)

    completed = config.db.execute(
        "SELECT COUNT(*) as cnt FROM sync_log WHERE channel_name=? AND phase='complete'",
        (name,)).fetchone()["cnt"]
    if completed > 0:
        existing = config.db.execute(
            "SELECT COUNT(*) as cnt FROM signals WHERE channel_name=?", (name,)
        ).fetchone()["cnt"]
        logger.info(f"'{name}': already done ({existing} signals), skipping")
        return {"status": "skipped", "existing": existing}

    await send_admin_message(f"\u23f3 PHASE {channel_num}: Downloading '{name}'...")
    downloaded = await download_and_save_raw(chat_id, name)

    os.makedirs(config.UNRECOGNIZED_DIR, exist_ok=True)
    unrec_file = os.path.join(config.UNRECOGNIZED_DIR, f"channel_{channel_num}_{name}.jsonl")
    if os.path.exists(unrec_file):
        os.remove(unrec_file)

    stats = parse_raw_messages(chat_id, name, parser_type, channel_config, unrec_file)
    stats["downloaded"] = downloaded
    if stats["earliest"]:
        await phase_0_extend(stats["earliest"])

    report = generate_channel_report(channel_num, name, stats, unrec_file)
    _save_report_and_log(channel_num, name, stats, chat_id, unrec_file, report)
    await send_admin_message(report)
    logger.info(report)
    stats["status"] = "ok"
    return stats


def _save_report_and_log(num, name, stats, chat_id, unrec_file, report):
    """Save report file, sync_log, and update channels table."""
    report_file = os.path.join(config.UNRECOGNIZED_DIR, f"channel_{num}_{name}_REPORT.txt")
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    config.db.execute("""
        INSERT INTO sync_log (channel_name, phase, total_messages, parsed_ok, parsed_fail,
            skipped_media, skipped_filter, earliest_message, latest_message,
            started_at, completed_at, notes)
        VALUES (?, 'complete', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (name, stats["total_raw"], stats["parsed_ok"],
          stats["parsed_fail"] + stats["validation_fail"],
          stats["total_raw"] - stats["text_messages"],
          stats["skipped_filter"],
          stats["earliest"].strftime("%Y-%m-%dT%H:%M:%S") if stats["earliest"] else None,
          stats["latest"].strftime("%Y-%m-%dT%H:%M:%S") if stats["latest"] else None,
          now_str, now_str, "; ".join(stats["errors"]) or None))

    config.db.execute(
        "UPDATE channels SET message_count=?, last_message_at=? WHERE channel_id=?",
        (stats["parsed_ok"],
         stats["latest"].strftime("%Y-%m-%dT%H:%M:%S") if stats["latest"] else None,
         chat_id))
    config.db.commit()


# ---- Phase 10: Price context ----

async def phase_10_fill_price_context():
    """Fill price_at/before/after for each signal via price_index."""
    config.price_index = build_price_index()

    signals = config.db.execute("""
        SELECT s.id, s.timestamp, s.btc_price_binance, s.btc_price_from_channel, s.channel_name
        FROM signals s LEFT JOIN signal_price_context ctx ON ctx.signal_id = s.id
        WHERE ctx.id IS NULL ORDER BY s.timestamp
    """).fetchall()

    logger.info(f"PHASE 10: context for {len(signals)} signals...")

    filled = 0
    for i, sig in enumerate(signals):
        if _fill_one_signal_context(sig):
            filled += 1
        if (i + 1) % 1000 == 0:
            config.db.commit()
            logger.info(f"  Context: {i + 1}/{len(signals)}...")
            await send_admin_message(f"\u23f3 Phase 10: {i + 1}/{len(signals)}...")

    config.db.commit()
    logger.info(f"PHASE 10: {filled}/{len(signals)} signals")


def _fill_one_signal_context(sig) -> bool:
    """Compute and insert price context for one signal. Returns True on success."""
    st = datetime.fromisoformat(sig["timestamp"]).replace(tzinfo=timezone.utc)
    price_at = sig["btc_price_binance"] or sig["btc_price_from_channel"]
    if not price_at:
        price_at = get_price_fast(st)
    if not price_at:
        return False

    p5b = get_price_fast(st - timedelta(minutes=5))
    p15b = get_price_fast(st - timedelta(minutes=15))
    p1hb = get_price_fast(st - timedelta(hours=1))
    p5 = get_price_fast(st + timedelta(minutes=5))
    p15 = get_price_fast(st + timedelta(minutes=15))
    p1h = get_price_fast(st + timedelta(hours=1))
    p4h = get_price_fast(st + timedelta(hours=4))
    p24h = get_price_fast(st + timedelta(hours=24))

    mask = 0
    if p5:  mask |= config.MASK_5M
    if p15: mask |= config.MASK_15M
    if p1h: mask |= config.MASK_1H
    if p4h: mask |= config.MASK_4H
    if p24h: mask |= config.MASK_24H

    try:
        config.db.execute("""
            INSERT OR IGNORE INTO signal_price_context (
                signal_id, channel_name, signal_timestamp, price_at_signal,
                price_5m_before, price_15m_before, price_1h_before,
                price_5m_after, price_15m_after, price_1h_after, price_4h_after, price_24h_after,
                change_5m_pct, change_15m_pct, change_1h_pct, change_4h_pct, change_24h_pct,
                filled_mask
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (sig["id"], sig["channel_name"], sig["timestamp"], price_at,
              p5b, p15b, p1hb, p5, p15, p1h, p4h, p24h,
              pct_change(price_at, p5), pct_change(price_at, p15),
              pct_change(price_at, p1h), pct_change(price_at, p4h),
              pct_change(price_at, p24h), mask))
        return True
    except Exception as e:
        logger.error(f"Context sig {sig['id']}: {e}")
        return False


# ---- Reparse ----

async def reparse_channel(channel_name: str) -> str:
    """Re-parse unparsed raw_messages for a channel. Returns status text."""
    rows = config.db.execute("""
        SELECT id, channel_id, message_id, timestamp, text, from_username, reply_to_topic_id
        FROM raw_messages
        WHERE channel_name=? AND (is_parsed = 0 OR is_parsed IS NULL) AND text IS NOT NULL
    """, (channel_name,)).fetchall()

    ch_config = next(
        (c for c in config.RESOLVED_CHANNELS.values() if c["name"] == channel_name), None
    )
    if not ch_config:
        return f"\u274c {channel_name} not found"

    reparsed = still_fail = skipped = 0
    for row in rows:
        if _should_filter_row(row, ch_config):
            skipped += 1
            continue
        parsed = parse_message(ch_config["parser"], row["text"])
        if not parsed:
            still_fail += 1
            continue
        valid, reason = validate_parsed(ch_config["parser"], parsed)
        if not valid:
            still_fail += 1
            continue
        _save_reparsed_signal(row, channel_name, ch_config, parsed)
        reparsed += 1

    config.db.commit()
    return (
        f"\u2705 Reparsed: {reparsed}/{len(rows)} "
        f"({still_fail} unrecognized, {skipped} filtered)"
    )


def _save_reparsed_signal(row, channel_name, ch_config, parsed):
    """Insert one reparsed signal into the DB."""
    btc_price = get_price_fast(
        datetime.fromisoformat(row["timestamp"]).replace(tzinfo=timezone.utc)
    )
    config.db.execute("""
        INSERT OR IGNORE INTO signals
        (channel_id, channel_name, message_id, message_text, timestamp,
         indicator_value, signal_color, signal_direction, timeframe,
         btc_price_from_channel, btc_price_binance, extra_data)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, (row["channel_id"], channel_name, row["message_id"], row["text"][:2000],
          row["timestamp"], parsed.get("value"), parsed.get("color"),
          parsed.get("direction"), parsed.get("timeframe"),
          parsed.get("btc_price"), btc_price,
          json.dumps(parsed.get("extra", {}), ensure_ascii=False)))
    config.db.execute(
        "UPDATE raw_messages SET is_parsed=1, parse_error=NULL WHERE id=?", (row["id"],)
    )
