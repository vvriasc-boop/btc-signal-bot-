import re
import logging

from config import VALIDATION_RULES

logger = logging.getLogger("btc_signal_bot")


# ---- Helpers ----

def _detect_color(text, emoji_map):
    """Return first matching color name from emoji_map."""
    return next((name for emoji, name in emoji_map if emoji in text), None)


def _detect_dyor_type(text):
    """Detect DyorAlerts signal type, direction, and level."""
    tl = text.lower()
    if '\u0434\u0438\u0441\u0431\u0430\u043b\u0430\u043d\u0441 \u043f\u043e\u043a\u0443\u043f\u0430\u0442\u0435\u043b\u044f' in tl:
        return "buyer_disbalance", "bullish", None
    if '\u0434\u0438\u0441\u0431\u0430\u043b\u0430\u043d\u0441 \u043f\u0440\u043e\u0434\u0430\u0432\u0446\u0430' in tl:
        return "seller_disbalance", "bearish", None
    if '\u043b\u043e\u043d\u0433\u043e\u0432\u044b\u0439 \u043f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442' in tl:
        lm = re.search(r'(\d+)\s*\u0443\u0440\u043e\u0432\u0435\u043d\u044c', text)
        return "long_priority", "bullish", int(lm.group(1)) if lm else None
    if '\u0448\u043e\u0440\u0442\u043e\u0432\u044b\u0439' in tl:
        return "short_signal", "bearish", None
    if '\u0441\u0438\u0433\u043d\u0430\u043b \u043b\u043e\u043d\u0433' in tl:
        return "long_signal", "bullish", None
    if '\u0441\u0438\u0433\u043d\u0430\u043b \u0448\u043e\u0440\u0442' in tl:
        return "short_signal", "bearish", None
    if '\u0431\u0430\u043b\u0430\u043d\u0441' in tl:
        return "balance", "neutral", None
    return "unknown", None, None


def _parse_money(pattern, section):
    """Parse money value like $1.5M or $200K from section."""
    m = re.search(pattern, section)
    if not m:
        return None
    nm = re.match(r'([\d.]+)\s*([Mm\u041cKk\u041a]?)', m.group(1))
    if not nm:
        return None
    val = float(nm.group(1))
    s = nm.group(2).upper()
    if s in ('M', '\u041c'):
        val *= 1_000_000
    elif s in ('K', '\u041a'):
        val *= 1_000
    return val


# ---- Color maps ----

_ALTSWING_COLORS = [
    ('\U0001f7e9', 'green'), ('\U0001f7e7', 'orange'),
    ('\U0001f7e5', 'red'), ('\U0001f7e6', 'blue'), ('\u2b1c', 'white'),
]

_SELLS_COLORS = [
    ('\U0001f7e9', 'green'), ('\U0001f7e6', 'blue'),
    ('\U0001f7e5', 'red'), ('\U0001f7e7', 'orange'),
]

_SCALP_COLORS = [
    ('\U0001f7e9', 'green'), ('\U0001f7e7', 'orange'),
    ('\U0001f7e5', 'red'), ('\U0001f7e6', 'blue'), ('\U0001f7ea', 'purple'),
]


# ---- 9 Parsers ----

def parse_altswing(text):
    m = re.search(r'Avg\.\s*(-?[\d.]+)%', text)
    if not m:
        return None
    return {"value": float(m.group(1)), "color": _detect_color(text, _ALTSWING_COLORS),
            "direction": None, "timeframe": None, "btc_price": None, "extra": {}}


def parse_diamond_marks(text):
    if 'Total' not in text or 'BTC/USDT:' not in text:
        return None
    tf = re.search(r'Total\s+(\d+[mhHM\u041c])', text)
    price = re.search(r'BTC/USDT:\s*\$?([\d,]+\.?\d*)', text)
    g = text.count('\U0001f7e9')
    o = text.count('\U0001f7e7')
    r_ = text.count('\U0001f7e5')
    y = text.count('\U0001f7e8')
    direction = "bullish" if g > r_ else ("bearish" if r_ > g else "neutral")
    colors = {"green": g, "orange": o, "red": r_, "yellow": y}
    dominant = max(colors, key=colors.get) if any(colors.values()) else None
    return {
        "value": None, "color": dominant, "direction": direction,
        "timeframe": tf.group(1).lower() if tf else None,
        "btc_price": float(price.group(1).replace(',', '')) if price else None,
        "extra": {"green_count": g, "orange_count": o, "red_count": r_,
                  "yellow_count": y, "has_fire": '\U0001f525' in text},
    }


def parse_sells_power(text):
    m = re.search(r'(-?[\d.]+)\s*%', text)
    if not m:
        return None
    return {"value": float(m.group(1)), "color": _detect_color(text, _SELLS_COLORS),
            "direction": None, "timeframe": None, "btc_price": None, "extra": {}}


def parse_altspi(text):
    avg = re.search(r'(?:Market\s+Av\.|Avg\.)\s*(-?[\d.]+)%', text)
    if not avg:
        return None

    def cnt(e):
        m = re.search(e + r'\ufe0f?\s*(\d+)', text)
        return int(m.group(1)) if m else 0

    return {
        "value": float(avg.group(1)), "color": None, "direction": None,
        "timeframe": None, "btc_price": None,
        "extra": {"red": cnt('\U0001f7e5'), "orange": cnt('\U0001f7e7'),
                  "white": cnt('\u26aa'), "blue": cnt('\U0001f7e6'),
                  "green": cnt('\U0001f7e9')},
    }


def parse_scalp17(text):
    if '\u26a1' not in text:
        return None
    m = re.search(r'Avg\.\s*(-?[\d.]+)%', text)
    if not m:
        return None
    return {"value": float(m.group(1)), "color": _detect_color(text, _SCALP_COLORS),
            "direction": None, "timeframe": None, "btc_price": None, "extra": {}}


def parse_index_btc(text):
    if 'INDEX' not in text or 'Bitcoin' not in text:
        return None
    tf = re.search(r'INDEX\s+(\d+\s*(?:min|m|h))', text, re.IGNORECASE)
    price = re.search(r'Bitcoin\s+([\d.]+)', text)
    prefix = text[:text.index('INDEX')]
    g = prefix.count('\U0001f7e9')
    r_ = prefix.count('\U0001f7e5')
    direction = "bullish" if g > r_ else ("bearish" if r_ > g else "neutral")
    return {
        "value": None,
        "color": "green" if g > r_ else ("red" if r_ > g else None),
        "direction": direction,
        "timeframe": tf.group(1).lower() if tf else None,
        "btc_price": float(price.group(1)) if price else None,
        "extra": {"green_count": g, "red_count": r_},
    }


def parse_dmi_smf(text):
    if 'SMF' not in text:
        return None
    m = re.search(r'SMF\s*(?:BTC\s*)?(-?[\d.]+)', text)
    if not m:
        return None
    is_btc = bool(re.search(r'SMF\s+BTC\s', text))
    color = "orange" if '\U0001f536' in text else ("blue" if '\U0001f537' in text else None)
    direction = "bullish" if color == "orange" else ("bearish" if color == "blue" else None)
    return {"value": float(m.group(1)), "color": color, "direction": direction,
            "timeframe": "15m", "btc_price": None, "extra": {"is_btc_specific": is_btc}}


def parse_dyor_alerts(text):
    if 'BTC/USDT-SPOT:' not in text:
        return None
    sig_type, direction, level = _detect_dyor_type(text)
    pm = re.search(r'BTC/USDT-SPOT:\s*([\d.]+)', text)
    btc_price = float(pm.group(1)) if pm else None
    green_dots = text.count('\U0001f7e2')
    green_hearts = text.count('\U0001f49a')
    green_squares = text.count('\U0001f7e9')

    bp = text.split('Binance:')[-1].split('Total')[0] if 'Binance:' in text else ""
    lp = text.split('Total liquidations:')[-1] if 'Total liquidations:' in text else ""
    money_pat = r'Long:\s*\$([\d.]+\s*[Mm\u041cKk\u041a]?)'
    short_pat = r'Short:\s*\$([\d.]+\s*[Mm\u041cKk\u041a]?)'
    b_l, b_s = _parse_money(money_pat, bp), _parse_money(short_pat, bp)
    l_l, l_s = _parse_money(money_pat, lp), _parse_money(short_pat, lp)

    ratio = round(b_l / b_s, 2) if b_l and b_s and b_s > 0 else None
    value = ratio if ratio is not None else level
    bullish_ind = green_dots + green_hearts + green_squares
    color = "green" if bullish_ind > 0 else ("red" if '\U0001f7e5' in text else None)
    return {
        "value": value, "color": color, "direction": direction,
        "timeframe": None, "btc_price": btc_price,
        "extra": {"signal_type": sig_type, "green_dots": green_dots,
                  "green_hearts": green_hearts, "level": level,
                  "binance_long": b_l, "binance_short": b_s,
                  "liq_long": l_l, "liq_short": l_s, "long_short_ratio": ratio},
    }


def parse_rsi_btc(text):
    if 'BTCUSDT' not in text:
        return None
    tm = re.search(r'(RSI_OVERSOLD|RSI_OVERBOUGHT)', text)
    if not tm:
        return None
    sig = tm.group(1)
    direction = "bullish" if sig == "RSI_OVERSOLD" else "bearish"
    pm = re.search(r'\$\s*([\d,]+)', text)
    btc_price = float(pm.group(1).replace(',', '')) if pm else None
    rsi = {}
    for m in re.finditer(r'(\d+[mhd]):\s*([\d.]+)', text):
        rsi[m.group(1)] = float(m.group(2))
    trig = re.search(r'(\d+[mhd]):\s*[\d.]+\s*(?:\U0001f7e2|\U0001f534)\u2b05\ufe0f', text)
    triggered_tf = trig.group(1) if trig else None
    return {
        "value": rsi.get(triggered_tf),
        "color": "green" if sig == "RSI_OVERSOLD" else "red",
        "direction": direction, "timeframe": triggered_tf, "btc_price": btc_price,
        "extra": {"signal_type": sig, "triggered_tf": triggered_tf,
                  "rsi_5m": rsi.get("5m"), "rsi_15m": rsi.get("15m"),
                  "rsi_1h": rsi.get("1h"), "rsi_4h": rsi.get("4h"), "rsi_1d": rsi.get("1d")},
    }


# ---- Dispatcher ----

PARSERS = {
    "altswing": parse_altswing, "diamond_marks": parse_diamond_marks,
    "sells_power": parse_sells_power, "altspi": parse_altspi,
    "scalp17": parse_scalp17, "index_btc": parse_index_btc,
    "dmi_smf": parse_dmi_smf, "dyor_alerts": parse_dyor_alerts,
    "rsi_btc": parse_rsi_btc,
}


def parse_message(parser_type, text):
    """Dispatch to the correct parser. Returns parsed dict or None."""
    func = PARSERS.get(parser_type)
    if func:
        try:
            return func(text)
        except Exception as e:
            logger.error(f"Parser {parser_type}: {e}")
    return None


def validate_parsed(parser_type: str, parsed: dict) -> tuple[bool, str]:
    """Validate parsed result against rules. Returns (ok, reason)."""
    rules = VALIDATION_RULES.get(parser_type, {})
    value = parsed.get("value")
    if value is not None and "value_min" in rules:
        if value < rules["value_min"] or value > rules["value_max"]:
            return False, f"value {value} out of [{rules['value_min']},{rules['value_max']}]"
    btc = parsed.get("btc_price")
    if btc is not None and (btc < 1000 or btc > 500000):
        return False, f"btc_price {btc} suspicious"
    return True, "ok"


def is_from_author(message, expected_username):
    """Check if Pyrogram message is from expected username."""
    u = None
    if message.from_user:
        u = message.from_user.username
    elif message.sender_chat:
        u = message.sender_chat.username
    return (u or "").lower() == expected_username.lower() if u else False
