import asyncio
import logging

import config
from utils.helpers import split_text

logger = logging.getLogger("btc_signal_bot")


async def send_admin_message(text: str):
    """Send message to admin via Pyrogram. Works at any stage."""
    if not config.ADMIN_USER_ID:
        logger.warning("ADMIN_USER_ID not set!")
        return
    try:
        for chunk in split_text(text, 4000):
            await config.userbot.send_message(config.ADMIN_USER_ID, chunk)
            await asyncio.sleep(0.5)
    except Exception as e:
        logger.error(f"Admin msg error: {e}")
