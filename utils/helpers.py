from datetime import datetime, timezone

from config import MADRID


def split_text(text: str, max_length: int = 4096) -> list[str]:
    """Split text into chunks respecting Telegram message limit."""
    if len(text) <= max_length:
        return [text]
    chunks = []
    for i in range(0, len(text), max_length):
        chunks.append(text[i:i + max_length])
    return chunks


def fmt_madrid(iso_str: str) -> str:
    """UTC ISO string -> Madrid timezone display string."""
    dt = datetime.fromisoformat(iso_str).replace(tzinfo=timezone.utc)
    return dt.astimezone(MADRID).strftime('%d.%m %H:%M')


def fmt_number(n, prefix: str = "$", decimals: int = 0) -> str:
    """Format number with prefix and commas. Returns '---' for None."""
    if n is None:
        return "\u2014"
    if decimals == 0:
        return f"{prefix}{n:,.0f}"
    return f"{prefix}{n:,.{decimals}f}"


def pct_change(base, target):
    """Percentage change from base to target. Safe division."""
    if base and target and base > 0:
        return round(((target - base) / base) * 100, 4)
    return None
