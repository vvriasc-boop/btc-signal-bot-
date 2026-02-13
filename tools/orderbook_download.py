"""
Download messages from 18 orderbook channels via Pyrogram.

Usage:
    python3 -m tools.orderbook_download
"""
import os
import asyncio
import sqlite3
import logging

from dotenv import load_dotenv

from tools.orderbook_config import ALL_TITLES, channel_id_for

load_dotenv()
logger = logging.getLogger("orderbook.download")

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "btc_signals.db")
SESSION_NAME = os.path.join(os.path.dirname(__file__), "..", "session")


# ---- Channel resolution ----

async def resolve_channels(client) -> dict:
    """Match ALL_TITLES to chat_ids via get_dialogs(). Returns {title: chat_id}."""
    logger.info("Loading dialogs to resolve channel IDs...")
    title_lower = {t.lower(): t for t in ALL_TITLES}
    resolved = {}

    async for dialog in client.get_dialogs():
        chat = dialog.chat
        name = chat.title or chat.first_name or chat.username or ""
        nl = name.lower().strip()
        if nl in title_lower:
            orig = title_lower.pop(nl)
            resolved[orig] = chat.id
            logger.info(f"  Resolved: {orig} -> {chat.id}")

    if title_lower:
        for remaining in title_lower.values():
            logger.warning(f"  NOT FOUND: {remaining}")

    logger.info(f"Resolved {len(resolved)}/{len(ALL_TITLES)} channels")
    return resolved


# ---- Download one channel ----

async def download_channel(client, chat_id: int, title: str, conn) -> int:
    """Download all messages from a channel into raw_messages. Returns count."""
    from pyrogram.errors import FloodWait

    # Check existing count
    existing = conn.execute(
        "SELECT COUNT(*) FROM raw_messages WHERE channel_name = ?", (title,)
    ).fetchone()[0]
    if existing > 0:
        logger.info(f"  {title}: already has {existing} messages, skipping download")
        return existing

    ch_name = title
    count = 0
    offset_id = 0
    consecutive_errors = 0

    while True:
        try:
            batch = []
            async for msg in client.get_chat_history(
                chat_id, limit=100, offset_id=offset_id
            ):
                batch.append(msg)

            if not batch:
                break

            _save_raw_batch(batch, chat_id, ch_name, conn)
            count += len(batch)
            offset_id = batch[-1].id
            consecutive_errors = 0
            await asyncio.sleep(0.5)

        except FloodWait as e:
            logger.warning(f"  FloodWait {e.value}s at {count} messages, waiting...")
            await asyncio.sleep(e.value + 2)
            continue
        except Exception as e:
            consecutive_errors += 1
            logger.error(f"  download error at {count} (attempt {consecutive_errors}): {e}")
            if consecutive_errors >= 5:
                logger.error(f"  {ch_name}: {consecutive_errors} errors, stopping")
                break
            await asyncio.sleep(5)
            continue

    logger.info(f"  {ch_name}: downloaded {count} messages")
    return count


def _save_raw_batch(batch, chat_id: int, channel_name: str, conn):
    """Save batch of Pyrogram messages to raw_messages."""
    for msg in batch:
        ts_str = msg.date.strftime("%Y-%m-%dT%H:%M:%S")
        from_username = (msg.from_user.username if msg.from_user else
                         msg.sender_chat.username if msg.sender_chat else None)
        conn.execute("""
            INSERT OR IGNORE INTO raw_messages
            (channel_id, channel_name, message_id, timestamp, text, has_text,
             from_username)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (chat_id, channel_name, msg.id, ts_str,
              msg.text[:2000] if msg.text else None,
              1 if msg.text else 0, from_username))
    conn.commit()


# ---- Inspect samples ----

def inspect_samples(conn, channel_name: str, n: int = 10):
    """Print N sample raw messages for a channel."""
    rows = conn.execute(
        "SELECT message_id, timestamp, text FROM raw_messages "
        "WHERE channel_name = ? AND text IS NOT NULL ORDER BY timestamp DESC LIMIT ?",
        (channel_name, n),
    ).fetchall()
    print(f"\n=== {channel_name} ({len(rows)} samples) ===")
    for mid, ts, text in rows:
        print(f"  [{ts}] msg_id={mid}")
        print(f"    {text[:200]}")
        print()


# ---- Main entry ----

async def main_download():
    """Standalone: start Pyrogram, resolve channels, download all."""
    from pyrogram import Client

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    api_id = int(os.getenv("API_ID", "0"))
    api_hash = os.getenv("API_HASH", "")
    phone = os.getenv("PHONE", "")

    if not api_id or not api_hash:
        logger.error("API_ID / API_HASH not set in .env")
        return

    client = Client(SESSION_NAME, api_id=api_id, api_hash=api_hash,
                    phone_number=phone)

    try:
        await client.start()
    except Exception as e:
        err = str(e).lower()
        if "database is locked" in err or "locked" in err:
            print("\nSession is locked — stop the bot first:")
            print("  kill $(pgrep -f 'python3 main.py')")
            print("  sleep 2 && python3 -m tools.orderbook_download")
            return
        raise

    logger.info("Pyrogram connected")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")

    try:
        resolved = await resolve_channels(client)

        # Register channels in channels table
        for title, chat_id in resolved.items():
            conn.execute(
                "INSERT OR IGNORE INTO channels (channel_id, name, parser_type) "
                "VALUES (?, ?, ?)",
                (chat_id, title, "orderbook"),
            )
        conn.commit()

        # Download all resolved channels
        stats = {}
        for title, chat_id in resolved.items():
            logger.info(f"Downloading {title} (chat_id={chat_id})...")
            count = await download_channel(client, chat_id, title, conn)
            stats[title] = count

        # Print summary
        print("\n" + "=" * 50)
        print("DOWNLOAD SUMMARY")
        print("=" * 50)
        for title in ALL_TITLES:
            count = stats.get(title, 0)
            status = "OK" if count > 0 else "MISSING"
            print(f"  {title:30s} {count:>8,} msgs  [{status}]")

        # Inspect samples from special channels
        for title in ["Dyor signal", "Long Bid F", "Short Ask F", "SHORT ONLY"]:
            if title in resolved:
                inspect_samples(conn, title, n=5)

    finally:
        conn.close()
        await client.stop()
        logger.info("Done")


if __name__ == "__main__":
    asyncio.run(main_download())
