import os
import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

import config
from handlers.commands import is_admin
from handlers.keyboards import main_keyboard, back_keyboard
from services.binance import fetch_btc_price
from services.csv_export import export_csv
from services.phases import reparse_channel
from utils.helpers import fmt_madrid

logger = logging.getLogger("btc_signal_bot")


# ---- Callback handlers (exact match) ----

async def handle_channels_status(query, context):
    rows = config.db.execute("""
        SELECT c.name, c.message_count,
               (SELECT COUNT(*) FROM signals s WHERE s.channel_name = c.name) as sig_count,
               (SELECT MAX(s.timestamp) FROM signals s WHERE s.channel_name = c.name) as last_sig
        FROM channels c WHERE c.is_active = 1 ORDER BY c.name
    """).fetchall()
    if not rows:
        await query.edit_message_text("\u274c No active channels")
        return
    text = "\U0001f4cb Channels & status:\n\n"
    for r in rows:
        last = fmt_madrid(r["last_sig"]) if r["last_sig"] else "\u2014"
        text += f"\u2022 {r['name']}: {r['sig_count']} signals | Last: {last}\n"
    await query.edit_message_text(text, reply_markup=back_keyboard())


async def handle_recent_signals(query, context):
    rows = config.db.execute("""
        SELECT timestamp, channel_name, indicator_value, signal_color,
               signal_direction, btc_price_binance
        FROM signals ORDER BY timestamp DESC LIMIT 10
    """).fetchall()
    if not rows:
        await query.edit_message_text("\u274c No signals")
        return
    text = "\U0001f4ca Last 10 signals:\n\n"
    for r in rows:
        ts = fmt_madrid(r["timestamp"])
        val = f"{r['indicator_value']}" if r["indicator_value"] is not None else "\u2014"
        d = r["signal_direction"] or "\u2014"
        btc = f"${r['btc_price_binance']:,.0f}" if r["btc_price_binance"] else "\u2014"
        text += f"{ts} | {r['channel_name']}\n  val={val} dir={d} BTC={btc}\n"
    await query.edit_message_text(text, reply_markup=back_keyboard())


async def handle_btc_price(query, context):
    price = await fetch_btc_price()
    row = config.db.execute("""
        SELECT MIN(price) as lo, MAX(price) as hi
        FROM btc_price WHERE timestamp >= datetime('now', '-1 day')
    """).fetchone()
    lo = f"${row['lo']:,.0f}" if row and row["lo"] else "\u2014"
    hi = f"${row['hi']:,.0f}" if row and row["hi"] else "\u2014"
    cur = f"${price:,.2f}" if price else "\u274c Unavailable"
    text = f"\U0001f4b0 BTC Price:\n\nCurrent: {cur}\nMin 24h: {lo}\nMax 24h: {hi}\n"
    await query.edit_message_text(text, reply_markup=back_keyboard())


async def handle_by_channel_menu(query, context):
    rows = config.db.execute(
        "SELECT DISTINCT channel_name FROM signals ORDER BY channel_name"
    ).fetchall()
    if not rows:
        await query.edit_message_text("\u274c No signals")
        return
    keyboard = [[InlineKeyboardButton(
        r["channel_name"], callback_data=f"ch_signals_{r['channel_name']}"
    )] for r in rows]
    keyboard.append([InlineKeyboardButton("\u25c0 Back", callback_data="back_main")])
    await query.edit_message_text(
        "\U0001f50d Choose channel:", reply_markup=InlineKeyboardMarkup(keyboard))


async def handle_summary(query, context):
    rows = config.db.execute("""
        SELECT s.channel_name,
               COUNT(*) as total,
               SUM(CASE WHEN ctx.filled_mask = 31 THEN 1 ELSE 0 END) as full_ctx,
               COUNT(ctx.id) as has_ctx
        FROM signals s
        LEFT JOIN signal_price_context ctx ON ctx.signal_id = s.id
        GROUP BY s.channel_name ORDER BY s.channel_name
    """).fetchall()
    if not rows:
        await query.edit_message_text("\u274c No data")
        return
    text = "\U0001f4c8 Summary:\n\n"
    total_all = 0
    for r in rows:
        pct = (r["full_ctx"] / max(r["total"], 1)) * 100
        text += f"\u2022 {r['channel_name']}: {r['total']} sig. | context: {pct:.0f}%\n"
        total_all += r["total"]
    text += f"\n\U0001f4ca Total: {total_all} signals"
    await query.edit_message_text(text, reply_markup=back_keyboard())


async def handle_export_csv(query, context):
    filepath = export_csv()
    await query.edit_message_text(f"\U0001f4e5 Export ready: {filepath}")
    try:
        with open(filepath, 'rb') as f:
            await context.bot.send_document(
                chat_id=query.message.chat_id,
                document=f,
                filename=os.path.basename(filepath))
    except Exception as e:
        logger.error(f"CSV send error: {e}")


async def handle_reparse_menu(query, context):
    rows = config.db.execute(
        "SELECT DISTINCT channel_name FROM raw_messages ORDER BY channel_name"
    ).fetchall()
    if not rows:
        await query.edit_message_text("\u274c No raw_messages")
        return
    keyboard = [[InlineKeyboardButton(
        r["channel_name"], callback_data=f"reparse_{r['channel_name']}"
    )] for r in rows]
    keyboard.append([InlineKeyboardButton("\u25c0 Back", callback_data="back_main")])
    await query.edit_message_text(
        "\U0001f504 Choose channel for reparse:",
        reply_markup=InlineKeyboardMarkup(keyboard))


async def handle_system_status(query, context):
    channels_cnt = config.db.execute(
        "SELECT COUNT(*) as c FROM channels WHERE is_active=1").fetchone()["c"]
    signals_cnt = config.db.execute("SELECT COUNT(*) as c FROM signals").fetchone()["c"]
    prices_cnt = config.db.execute("SELECT COUNT(*) as c FROM btc_price").fetchone()["c"]
    raw_cnt = config.db.execute("SELECT COUNT(*) as c FROM raw_messages").fetchone()["c"]
    unfilled = config.db.execute(
        "SELECT COUNT(*) as c FROM signal_price_context WHERE filled_mask < 31"
    ).fetchone()["c"]
    text = (
        f"\u2699\ufe0f System status:\n\n"
        f"\U0001f4e1 Channels: {channels_cnt}\n"
        f"\U0001f4ca Signals: {signals_cnt}\n"
        f"\U0001f4b0 Price points: {prices_cnt}\n"
        f"\U0001f4e8 Raw messages: {raw_cnt}\n"
        f"\u23f3 Unfilled contexts: {unfilled}\n"
    )
    await query.edit_message_text(text, reply_markup=back_keyboard())


async def handle_back_main(query, context):
    await query.edit_message_text(
        "\U0001f4ca BTC Signal Aggregator", reply_markup=main_keyboard())


# ---- Prefix callback handlers ----

async def handle_channel_signals(query, context, channel_name):
    rows = config.db.execute("""
        SELECT timestamp, indicator_value, signal_color, signal_direction,
               btc_price_binance, timeframe
        FROM signals WHERE channel_name = ? ORDER BY timestamp DESC LIMIT 10
    """, (channel_name,)).fetchall()
    if not rows:
        await query.edit_message_text(f"\u274c No signals for {channel_name}")
        return
    text = f"\U0001f50d {channel_name} \u2014 last 10:\n\n"
    for r in rows:
        ts = fmt_madrid(r["timestamp"])
        val = f"{r['indicator_value']}" if r["indicator_value"] is not None else "\u2014"
        d = r["signal_direction"] or "\u2014"
        btc = f"${r['btc_price_binance']:,.0f}" if r["btc_price_binance"] else "\u2014"
        tf = r["timeframe"] or ""
        text += f"{ts} | val={val} dir={d} {tf} BTC={btc}\n"
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("\u25c0 Channels", callback_data="by_channel")],
        [InlineKeyboardButton("\u25c0 Main", callback_data="back_main")],
    ])
    await query.edit_message_text(text, reply_markup=keyboard)


async def handle_reparse_channel(query, context, channel_name):
    await query.edit_message_text(f"\u23f3 Reparsing {channel_name}...")
    result = await reparse_channel(channel_name)
    await query.edit_message_text(result, reply_markup=back_keyboard())


# ---- Router ----

CALLBACK_ROUTES = {
    "channels_status": handle_channels_status,
    "recent_signals":  handle_recent_signals,
    "btc_price":       handle_btc_price,
    "by_channel":      handle_by_channel_menu,
    "summary":         handle_summary,
    "export_csv":      handle_export_csv,
    "reparse":         handle_reparse_menu,
    "system_status":   handle_system_status,
    "back_main":       handle_back_main,
}

PREFIX_ROUTES = [
    ("ch_signals_", handle_channel_signals),
    ("reparse_",    handle_reparse_channel),
]


async def button_handler(update, context):
    """Main callback query router. Uses dict-based routing."""
    if not is_admin(update):
        await update.callback_query.answer("Access denied.", show_alert=True)
        return
    query = update.callback_query
    await query.answer()
    data = query.data

    handler = CALLBACK_ROUTES.get(data)
    if handler:
        await handler(query, context)
        return

    for prefix, handler in PREFIX_ROUTES:
        if data.startswith(prefix):
            await handler(query, context, data[len(prefix):])
            return
