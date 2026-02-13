"""
Parsers for orderbook channel messages.

Standard format: "A BTC/USDT-S A at 1.0%, q: 50000.0 $, d: 1 min - P..."
Special channels: Dyor signal, Long Bid F, Short Ask F, SHORT ONLY
"""
import re
import logging

logger = logging.getLogger("orderbook.parsers")

# ---- Standard orderbook format ----
# "A BTC/USDT-S A at 1.0%, q: 50000.0 $, d: 1 min - P..."
# "B BTC/USDT-F B at 0.7%, q: 5000000.0 $, d: 1 min..."
RE_STANDARD = re.compile(
    r"([AB])\s+BTC/USDT-([SF])\s+[AB]\s+at\s+([\d.]+)%"
    r".*?q:\s*([\d,.]+)\s*\$"
    r".*?d:\s*(\d+)\s*min",
    re.DOTALL,
)

RE_PRICE = re.compile(r"[Pp](?:rice)?[\s:=]*\$?([\d,]+\.?\d*)")


def parse_standard(text: str) -> dict | None:
    """Parse standard orderbook message (14 pair channels)."""
    if not text:
        return None
    m = RE_STANDARD.search(text)
    if not m:
        return None
    side_ch = m.group(1)   # A or B
    market_ch = m.group(2)  # S or F
    pct = float(m.group(3))
    qty_str = m.group(4).replace(",", "")
    quantity = float(qty_str)
    duration = int(m.group(5))

    side = "ask" if side_ch == "A" else "bid"
    market = "spot" if market_ch == "S" else "futures"

    btc_price = None
    m_price = RE_PRICE.search(text)
    if m_price:
        btc_price = float(m_price.group(1).replace(",", ""))
        if btc_price < 1000:
            btc_price = None

    return {
        "side": side,
        "market": market,
        "pct": pct,
        "quantity": quantity,
        "duration_min": duration,
        "btc_price": btc_price,
    }


# ---- Special channel parsers ----
# These channels may have different formats. Stubs parse what we can,
# return None if format is unrecognized.

# Dyor signal — may have "Long"/"Short" direction + BTC price
RE_DYOR = re.compile(
    r"(Long|Short|Buy|Sell)"
    r".*?BTC.*?(\d[\d,]*\.?\d*)",
    re.IGNORECASE | re.DOTALL,
)


def parse_dyor_signal(text: str) -> dict | None:
    """Parse Dyor signal messages."""
    if not text:
        return None
    # Try standard format first (Dyor might repost orderbook-style)
    std = parse_standard(text)
    if std:
        return std
    m = RE_DYOR.search(text)
    if not m:
        return None
    direction = m.group(1).lower()
    side = "bid" if direction in ("long", "buy") else "ask"
    btc_price = float(m.group(2).replace(",", ""))
    if btc_price < 1000:
        btc_price = None
    return {
        "side": side,
        "market": "unknown",
        "pct": None,
        "quantity": None,
        "duration_min": None,
        "btc_price": btc_price,
    }


# Long Bid F / Short Ask F / SHORT ONLY
# Likely contain direction + price info
RE_DIRECTION_PRICE = re.compile(
    r"(Long|Short|Bid|Ask|Buy|Sell)"
    r".*?(\d[\d,]*\.?\d*)\s*\$?",
    re.IGNORECASE | re.DOTALL,
)

RE_QTY = re.compile(r"q(?:ty)?[\s:=]*([\d,.]+)\s*\$", re.IGNORECASE)


def parse_directional(text: str, default_side: str) -> dict | None:
    """Parse directional channel messages (Long Bid F, Short Ask F, SHORT ONLY)."""
    if not text:
        return None
    # Try standard format first
    std = parse_standard(text)
    if std:
        return std

    side = default_side
    btc_price = None
    quantity = None

    # Try to extract price
    m_price = RE_PRICE.search(text)
    if m_price:
        btc_price = float(m_price.group(1).replace(",", ""))
        if btc_price < 1000:
            btc_price = None

    # Try to extract quantity
    m_qty = RE_QTY.search(text)
    if m_qty:
        quantity = float(m_qty.group(1).replace(",", ""))

    # Try to extract pct
    m_pct = re.search(r"at\s+([\d.]+)%", text)
    pct = float(m_pct.group(1)) if m_pct else None

    # Try to extract duration
    m_dur = re.search(r"d:\s*(\d+)\s*min", text)
    duration = int(m_dur.group(1)) if m_dur else None

    if btc_price is None and quantity is None and pct is None:
        return None

    return {
        "side": side,
        "market": "futures" if "F" in text[:50] else "unknown",
        "pct": pct,
        "quantity": quantity,
        "duration_min": duration,
        "btc_price": btc_price,
    }


def parse_long_bid_f(text: str) -> dict | None:
    return parse_directional(text, default_side="bid")


def parse_short_ask_f(text: str) -> dict | None:
    return parse_directional(text, default_side="ask")


def parse_short_only(text: str) -> dict | None:
    return parse_directional(text, default_side="ask")


# ---- Dispatcher ----

PARSER_MAP = {
    "Dyor signal": parse_dyor_signal,
    "Long Bid F": parse_long_bid_f,
    "Short Ask F": parse_short_ask_f,
    "SHORT ONLY": parse_short_only,
}


def parse_message(channel_title: str, text: str) -> dict | None:
    """Dispatch to correct parser based on channel title."""
    if channel_title in PARSER_MAP:
        return PARSER_MAP[channel_title](text)
    return parse_standard(text)
