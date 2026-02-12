from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def main_keyboard() -> InlineKeyboardMarkup:
    """Build the main menu inline keyboard."""
    keyboard = [
        [InlineKeyboardButton("\U0001f4cb Channels & status", callback_data="channels_status")],
        [InlineKeyboardButton("\U0001f4ca Recent signals", callback_data="recent_signals")],
        [InlineKeyboardButton("\U0001f4b0 BTC Price", callback_data="btc_price")],
        [InlineKeyboardButton("\U0001f50d By channel", callback_data="by_channel")],
        [InlineKeyboardButton("\U0001f4c8 Summary", callback_data="summary")],
        [InlineKeyboardButton("\U0001f4e5 Export CSV", callback_data="export_csv")],
        [InlineKeyboardButton("\U0001f504 Reparse channel", callback_data="reparse")],
        [InlineKeyboardButton("\u2699\ufe0f System status", callback_data="system_status")],
    ]
    return InlineKeyboardMarkup(keyboard)


def back_keyboard(callback_data: str = "back_main") -> InlineKeyboardMarkup:
    """Single 'Back' button keyboard."""
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("\u25c0 Back", callback_data=callback_data)]]
    )
