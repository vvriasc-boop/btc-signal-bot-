#!/usr/bin/env python3
"""
Re-download AltSwing and DiamondMarks channels, then parse.
Uses Pyrogram userbot directly without starting the full bot.
"""
import asyncio
import os
import sys
import json
import re
import sqlite3
from datetime import datetime, timezone

from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.errors import FloodWait

load_dotenv()

userbot = Client(
    "session",
    api_id=int(os.getenv("API_ID", "0")),
    api_hash=os.getenv("API_HASH", ""),
    phone_number=os.getenv("PHONE", "")
)

CHANNELS = {
    "AltSwing": {
        "chat_id": int(os.getenv("CHANNEL_1", "0")),
        "parser": "altswing",
    },
    "DiamondMarks": {
        "chat_id": int(os.getenv("CHANNEL_2", "0")),
        "parser": "diamond_marks",
    },
}


# ═══ Parsers (from main.py) ═══

def parse_altswing(text):
    if 'AltSwing' not in text:
        return None
    m = re.search(r'Avg\.\s*([\d.]+)%', text)
    if not m:
        return None
    return {"value": float(m.group(1)), "color": None, "direction": None,
            "timeframe": None, "btc_price": None, "extra": {}}


def parse_diamond_marks(text):
    if 'Diamond Marks' not in text:
        return None
    tf = re.search(r'Total\s+(\d+[mhHМ])', text)
    price = re.search(r'BTC/USDT:\s*\$?([\d,]+\.?\d*)', text)
    g, o, r_ = text.count('\U0001f7e9'), text.count('\U0001f7e7'), text.count('\U0001f7e5')
    direction = "bullish" if g > r_ else ("bearish" if r_ > g else "neutral")
    colors = {"green": g, "orange": o, "red": r_}
    dominant = max(colors, key=colors.get) if any(colors.values()) else None
    return {"value": None, "color": dominant, "direction": direction,
            "timeframe": tf.group(1).lower() if tf else None,
            "btc_price": float(price.group(1).replace(',', '')) if price else None,
            "extra": {"green_count": g, "orange_count": o, "red_count": r_,
                      "has_fire": '\U0001f525' in text}}


PARSERS = {"altswing": parse_altswing, "diamond_marks": parse_diamond_marks}


async def download_channel(chat_id: int, channel_name: str, db: sqlite3.Connection) -> int:
    count = 0
    offset_id = 0
    consecutive_errors = 0

    while True:
        try:
            batch = []
            async for msg in userbot.get_chat_history(chat_id, limit=100, offset_id=offset_id):
                batch.append(msg)

            if not batch:
                break

            for msg in batch:
                ts_str = msg.date.strftime("%Y-%m-%dT%H:%M:%S")
                from_username = (msg.from_user.username if msg.from_user else
                                 msg.sender_chat.username if msg.sender_chat else None)
                topic_id = getattr(msg, 'reply_to_top_message_id', None)
                db.execute("""
                    INSERT OR IGNORE INTO raw_messages
                    (channel_id, channel_name, message_id, timestamp, text, has_text,
                     from_username, reply_to_topic_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (chat_id, channel_name, msg.id, ts_str,
                      msg.text[:2000] if msg.text else None,
                      1 if msg.text else 0,
                      from_username, topic_id))
            db.commit()

            count += len(batch)
            offset_id = batch[-1].id
            consecutive_errors = 0

            if count % 500 == 0:
                print(f"  Downloaded {count} messages...")

            await asyncio.sleep(0.5)

        except FloodWait as e:
            print(f"  FloodWait {e.value}s at message {count}, waiting...")
            await asyncio.sleep(e.value + 2)
            continue
        except Exception as e:
            consecutive_errors += 1
            print(f"  Error at {count} (attempt {consecutive_errors}): {e}")
            if consecutive_errors >= 5:
                print(f"  Too many errors, stopping {channel_name}")
                break
            await asyncio.sleep(5)
            continue

    return count


def parse_channel(chat_id: int, channel_name: str, parser_type: str, db: sqlite3.Connection):
    rows = db.execute("""
        SELECT id, message_id, timestamp, text
        FROM raw_messages
        WHERE channel_id = ? AND has_text = 1
        ORDER BY timestamp
    """, (chat_id,)).fetchall()

    parser_func = PARSERS[parser_type]
    parsed_ok = parsed_fail = 0
    unrec_file = os.path.join("unrecognized", f"channel_{channel_name}.jsonl")
    os.makedirs("unrecognized", exist_ok=True)
    if os.path.exists(unrec_file):
        os.remove(unrec_file)
    unrec_fh = open(unrec_file, 'a', encoding='utf-8')

    for row in rows:
        text = row["text"]
        parsed = parser_func(text)
        if parsed is None:
            parsed_fail += 1
            json.dump({"channel": channel_name, "msg_id": row["message_id"],
                        "ts": row["timestamp"], "text": text[:200]},
                      unrec_fh, ensure_ascii=False)
            unrec_fh.write('\n')
            db.execute("UPDATE raw_messages SET is_parsed=0, parse_error='no_match' WHERE id=?", (row["id"],))
            continue

        parsed_ok += 1
        db.execute("""INSERT OR IGNORE INTO signals
            (channel_id, channel_name, message_id, message_text, timestamp,
             indicator_value, signal_color, signal_direction, timeframe,
             btc_price_from_channel, btc_price_binance, extra_data)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (chat_id, channel_name, row["message_id"], text[:2000],
             row["timestamp"], parsed.get("value"), parsed.get("color"),
             parsed.get("direction"), parsed.get("timeframe"),
             parsed.get("btc_price"), None,
             json.dumps(parsed.get("extra", {}), ensure_ascii=False)))
        db.execute("UPDATE raw_messages SET is_parsed=1, parse_error=NULL WHERE id=?", (row["id"],))

    unrec_fh.close()
    db.commit()
    return len(rows), parsed_ok, parsed_fail


async def main():
    db = sqlite3.connect('btc_signals.db')
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=5000")
    db.row_factory = sqlite3.Row

    await userbot.start()
    print("Pyrogram connected\n")

    for name, cfg in CHANNELS.items():
        chat_id = cfg["chat_id"]
        parser_type = cfg["parser"]

        print(f"{'=' * 50}")
        print(f"Channel: {name} (chat_id={chat_id})")
        print(f"{'=' * 50}")

        # Verify access
        try:
            chat = await userbot.get_chat(chat_id)
            title = chat.title or chat.username or "N/A"
            print(f"  Resolved: {title} (id={chat.id})")
        except Exception as e:
            print(f"  FAILED to resolve: {e}")
            print(f"  Skipping {name}")
            continue

        # Download
        print(f"  Downloading messages...")
        downloaded = await download_channel(chat_id, name, db)
        print(f"  Downloaded: {downloaded}")

        # Count text messages
        text_count = db.execute(
            "SELECT COUNT(*) as c FROM raw_messages WHERE channel_id=? AND has_text=1",
            (chat_id,)
        ).fetchone()["c"]
        print(f"  Text messages: {text_count}")

        if text_count == 0:
            print(f"  No text messages to parse!")
            # Show sample raw messages
            samples = db.execute(
                "SELECT message_id, timestamp, has_text, text FROM raw_messages WHERE channel_id=? LIMIT 5",
                (chat_id,)
            ).fetchall()
            if samples:
                print(f"  Sample raw messages:")
                for s in samples:
                    print(f"    msg_id={s['message_id']} has_text={s['has_text']} text={repr((s['text'] or '')[:80])}")
            else:
                print(f"  raw_messages table is EMPTY for this channel!")
            continue

        # Parse
        print(f"  Parsing...")
        total, ok, fail = parse_channel(chat_id, name, parser_type, db)
        pct = (ok / max(total, 1)) * 100
        print(f"  Result: {ok}/{total} parsed ({pct:.1f}%), {fail} unrecognized")

        # Show sample unrecognized
        if fail > 0:
            print(f"  Sample unrecognized:")
            samples = db.execute(
                "SELECT text FROM raw_messages WHERE channel_id=? AND is_parsed=0 LIMIT 3",
                (chat_id,)
            ).fetchall()
            for s in samples:
                print(f"    {repr((s['text'] or '')[:100])}")

        # Write sync_log
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        db.execute("""
            INSERT INTO sync_log (channel_name, phase, total_messages, parsed_ok, parsed_fail,
                started_at, completed_at)
            VALUES (?, 'complete', ?, ?, ?, ?, ?)
        """, (name, downloaded, ok, fail, now_str, now_str))
        db.commit()

        print()

    await userbot.stop()
    db.close()
    print("Done.")


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    asyncio.run(main())
