import config
from handlers.keyboards import main_keyboard


def is_admin(update) -> bool:
    """Check if the user is the admin."""
    user = update.effective_user
    return user and user.id == config.ADMIN_USER_ID


async def cmd_start(update, context):
    """Handle /start command."""
    if not is_admin(update):
        await update.message.reply_text("Access denied.")
        return
    await update.message.reply_text(
        "\U0001f4ca BTC Signal Aggregator",
        reply_markup=main_keyboard()
    )
