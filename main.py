import sys
import asyncio
import logging
import signal as sig_mod

# Make "import main" work when running as __main__
sys.modules["main"] = sys.modules[__name__]

import config
from database.db import init_database, resolve_channel_ids
from services.phases import phase_0_load_prices, phase_channel, phase_10_fill_price_context
from services.live import (
    on_new_signal, price_ticker_loop, fill_delayed_prices_loop, healthcheck_loop,
)
from handlers.commands import cmd_start
from handlers.callbacks import button_handler
from utils.telegram import send_admin_message

from pyrogram.handlers import MessageHandler
from pyrogram import filters
from telegram.ext import Application, CommandHandler, CallbackQueryHandler

logger = logging.getLogger("btc_signal_bot")


async def main():
    config.db = init_database()
    await config.init_http()
    await config.userbot.start()
    logger.info("Pyrogram connected")

    channel_config = config.build_channel_config()
    config.RESOLVED_CHANNELS = await resolve_channel_ids(channel_config)
    logger.info(f"Channels: {len(config.RESOLVED_CHANNELS)}")

    if not config.RESOLVED_CHANNELS:
        logger.error("No channels resolved! Check .env")
        await config.close_http()
        await config.userbot.stop()
        config.db.close()
        return

    # ---- Initial sync ----
    price_count = await phase_0_load_prices()
    if price_count == 0:
        await send_admin_message("\U0001f6a8 Binance not returning prices!")
        await config.close_http()
        await config.userbot.stop()
        config.db.close()
        return
    await send_admin_message(f"\u2705 Phase 0: {price_count} BTC price points")

    for i, (chat_id, ch_config) in enumerate(config.RESOLVED_CHANNELS.items(), 1):
        await phase_channel(i, chat_id, ch_config)
        await asyncio.sleep(3)

    await phase_10_fill_price_context()
    await send_admin_message("\u2705 All phases done. LIVE MODE.")

    # ---- Live mode ----
    config.userbot.add_handler(
        MessageHandler(on_new_signal, filters.chat(list(config.RESOLVED_CHANNELS.keys())))
    )

    bg_tasks = [
        asyncio.create_task(price_ticker_loop()),
        asyncio.create_task(fill_delayed_prices_loop()),
        asyncio.create_task(healthcheck_loop()),
    ]

    bot_app = Application.builder().token(config.BOT_TOKEN).build()
    bot_app.add_handler(CommandHandler("start", cmd_start))
    bot_app.add_handler(CallbackQueryHandler(button_handler))
    await bot_app.initialize()
    await bot_app.start()
    await bot_app.updater.start_polling()

    logger.info("=== LIVE MODE ACTIVE ===")

    # ---- Graceful shutdown ----
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for s in (sig_mod.SIGINT, sig_mod.SIGTERM):
        loop.add_signal_handler(s, stop.set)
    await stop.wait()

    logger.info("Shutting down...")
    for t in bg_tasks:
        t.cancel()
    await asyncio.gather(*bg_tasks, return_exceptions=True)

    await bot_app.updater.stop()
    await bot_app.stop()
    await bot_app.shutdown()
    await config.userbot.stop()
    await config.close_http()
    config.db.close()
    logger.info("=== STOP ===")


if __name__ == "__main__":
    asyncio.run(main())
