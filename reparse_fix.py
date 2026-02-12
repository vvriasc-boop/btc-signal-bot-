#!/usr/bin/env python3
"""
Test fixed parsers on real data, then reparse all 7 channels from raw_messages.
Run ONCE after fixing parsers in main.py.
"""
import re
import json
import sqlite3
import os
from datetime import datetime, timezone

# ═══ PARSERS (copied from fixed main.py) ═══

def parse_sells_power(text):
    m = re.search(r'(-?[\d.]+)\s*%', text)
    if not m:
        return None
    val = float(m.group(1))
    color = next((n for e, n in [('\U0001f7e9', 'green'), ('\U0001f7e6', 'blue'),
                                  ('\U0001f7e5', 'red'), ('\U0001f7e7', 'orange')]
                  if e in text), None)
    return {"value": val, "color": color, "direction": None,
            "timeframe": None, "btc_price": None, "extra": {}}


def parse_altspi(text):
    avg = re.search(r'(?:Market\s+Av\.|Avg\.)\s*(-?[\d.]+)%', text)
    if not avg:
        return None
    def cnt(e):
        m = re.search(e + r'\ufe0f?\s*(\d+)', text)
        return int(m.group(1)) if m else 0
    return {"value": float(avg.group(1)), "color": None, "direction": None,
            "timeframe": None, "btc_price": None,
            "extra": {"red": cnt('\U0001f7e5'), "orange": cnt('\U0001f7e7'),
                      "white": cnt('\u26aa'), "blue": cnt('\U0001f7e6'),
                      "green": cnt('\U0001f7e9')}}


def parse_scalp17(text):
    if '\u26a1' not in text:
        return None
    m = re.search(r'Avg\.\s*(-?[\d.]+)%', text)
    if not m:
        return None
    color = next((n for e, n in [('\U0001f7e9', 'green'), ('\U0001f7e7', 'orange'),
                                  ('\U0001f7e5', 'red'), ('\U0001f7e6', 'blue'),
                                  ('\U0001f7ea', 'purple')] if e in text), None)
    return {"value": float(m.group(1)), "color": color, "direction": None,
            "timeframe": None, "btc_price": None, "extra": {}}


def parse_index_btc(text):
    if 'INDEX' not in text or 'Bitcoin' not in text:
        return None
    tf = re.search(r'INDEX\s+(\d+\s*(?:min|m|h))', text, re.IGNORECASE)
    price = re.search(r'Bitcoin\s+([\d.]+)', text)
    idx_pos = text.index('INDEX')
    prefix = text[:idx_pos]
    g = prefix.count('\U0001f7e9')
    r_ = prefix.count('\U0001f7e5')
    direction = "bullish" if g > r_ else ("bearish" if r_ > g else "neutral")
    return {"value": None, "color": "green" if g > r_ else ("red" if r_ > g else None),
            "direction": direction, "timeframe": tf.group(1).lower() if tf else None,
            "btc_price": float(price.group(1)) if price else None,
            "extra": {"green_count": g, "red_count": r_}}


def parse_dyor_alerts(text):
    if 'BTC/USDT-SPOT:' not in text:
        return None
    sig_type, direction = "unknown", None
    level = None
    text_lower = text.lower()
    if '\u0434\u0438\u0441\u0431\u0430\u043b\u0430\u043d\u0441 \u043f\u043e\u043a\u0443\u043f\u0430\u0442\u0435\u043b\u044f' in text_lower:
        sig_type, direction = "buyer_disbalance", "bullish"
    elif '\u0434\u0438\u0441\u0431\u0430\u043b\u0430\u043d\u0441 \u043f\u0440\u043e\u0434\u0430\u0432\u0446\u0430' in text_lower:
        sig_type, direction = "seller_disbalance", "bearish"
    elif '\u043b\u043e\u043d\u0433\u043e\u0432\u044b\u0439 \u043f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442' in text_lower:
        sig_type, direction = "long_priority", "bullish"
        lm = re.search(r'(\d+)\s*\u0443\u0440\u043e\u0432\u0435\u043d\u044c', text)
        if lm:
            level = int(lm.group(1))
    elif '\u0448\u043e\u0440\u0442\u043e\u0432\u044b\u0439' in text_lower:
        sig_type, direction = "short_signal", "bearish"
    elif '\u0441\u0438\u0433\u043d\u0430\u043b \u043b\u043e\u043d\u0433' in text_lower:
        sig_type, direction = "long_signal", "bullish"
    elif '\u0441\u0438\u0433\u043d\u0430\u043b \u0448\u043e\u0440\u0442' in text_lower:
        sig_type, direction = "short_signal", "bearish"
    elif '\u0431\u0430\u043b\u0430\u043d\u0441' in text_lower:
        sig_type, direction = "balance", "neutral"

    pm = re.search(r'BTC/USDT-SPOT:\s*([\d.]+)', text)
    btc_price = float(pm.group(1)) if pm else None
    green_dots = text.count('\U0001f7e2')
    green_hearts = text.count('\U0001f49a')
    green_squares = text.count('\U0001f7e9')

    def parse_money(pattern, section):
        m = re.search(pattern, section)
        if not m:
            return None
        nm = re.match(r'([\d.]+)\s*([MmМkKк]?)', m.group(1))
        if not nm:
            return None
        val = float(nm.group(1))
        s = nm.group(2).upper()
        if s in ('M', '\u041c'):
            val *= 1_000_000
        elif s in ('K', '\u041a'):
            val *= 1_000
        return val

    bp = text.split('Binance:')[-1].split('Total')[0] if 'Binance:' in text else ""
    lp = text.split('Total liquidations:')[-1] if 'Total liquidations:' in text else ""
    b_l = parse_money(r'Long:\s*\$([\d.]+\s*[MmМkKк]?)', bp)
    b_s = parse_money(r'Short:\s*\$([\d.]+\s*[MmМkKк]?)', bp)
    l_l = parse_money(r'Long:\s*\$([\d.]+\s*[MmМkKк]?)', lp)
    l_s = parse_money(r'Short:\s*\$([\d.]+\s*[MmМkKк]?)', lp)
    ratio = round(b_l / b_s, 2) if b_l and b_s and b_s > 0 else None
    value = ratio if ratio is not None else level
    bullish_indicators = green_dots + green_hearts + green_squares
    color = ("green" if bullish_indicators > 0 else
             ("red" if '\U0001f7e5' in text else None))
    return {"value": value, "color": color,
            "direction": direction, "timeframe": None, "btc_price": btc_price,
            "extra": {"signal_type": sig_type, "green_dots": green_dots,
                      "green_hearts": green_hearts, "level": level,
                      "binance_long": b_l, "binance_short": b_s,
                      "liq_long": l_l, "liq_short": l_s, "long_short_ratio": ratio}}


PARSERS = {
    "sells_power": parse_sells_power,
    "altspi": parse_altspi,
    "scalp17": parse_scalp17,
    "index_btc": parse_index_btc,
    "dyor_alerts": parse_dyor_alerts,
}

VALIDATION_RULES = {
    "sells_power":  {"value_min": -300, "value_max": 300},
    "scalp17":      {"value_min": -200, "value_max": 200},
    "altspi":       {"value_min": -100, "value_max": 200},
    "index_btc":    {},
    "dyor_alerts":  {"value_min": 0, "value_max": 1000},
}


def validate_parsed(parser_type, parsed):
    rules = VALIDATION_RULES.get(parser_type, {})
    value = parsed.get("value")
    if value is not None and "value_min" in rules:
        if value < rules["value_min"] or value > rules["value_max"]:
            return False, f"value {value} out of [{rules['value_min']},{rules['value_max']}]"
    btc = parsed.get("btc_price")
    if btc is not None and (btc < 1000 or btc > 500000):
        return False, f"btc_price {btc} suspicious"
    return True, "ok"


# ═══════════════════════════════════════════════════
# STEP 1: TESTS (10 real examples per parser)
# ═══════════════════════════════════════════════════

def run_tests():
    print("=" * 60)
    print("TESTING PARSERS ON REAL DATA")
    print("=" * 60)
    errors = 0

    # --- SellsPowerIndex ---
    print("\n--- SellsPowerIndex ---")
    tests_sp = [
        ("\u26aa\ufe0f 55%", 55.0, None),
        ("\u26aa\ufe0f 49%", 49.0, None),
        ("\u26aa\ufe0f 50%", 50.0, None),
        ("\U0001f7e9 -28%", -28.0, "green"),
        ("\U0001f7e5 12%", 12.0, "red"),
        ("\U0001f7e7 0%", 0.0, "orange"),
        ("\U0001f7e6 -150%", -150.0, "blue"),
        ("\u26aa\ufe0f 100%", 100.0, None),
        ("\U0001f7e9 3.5%", 3.5, "green"),
        ("\u26aa\ufe0f -0.5%", -0.5, None),
    ]
    for text, exp_val, exp_color in tests_sp:
        r = parse_sells_power(text)
        try:
            assert r is not None, f"returned None for '{text}'"
            assert r["value"] == exp_val, f"value={r['value']} expected {exp_val} for '{text}'"
            assert r["color"] == exp_color, f"color={r['color']} expected {exp_color} for '{text}'"
            print(f"  OK: '{text}' -> val={r['value']}, color={r['color']}")
        except AssertionError as e:
            print(f"  FAIL: {e}")
            errors += 1

    # --- AltSPI ---
    print("\n--- AltSPI ---")
    tests_as = [
        ("\U0001f7e5 21 \U0001f7e7 22 \u26aa\ufe0f 56 \U0001f7e6 1 \U0001f7e9 0\nMarket Av. 94.8%",
         94.8, {"red": 21, "orange": 22, "white": 56, "blue": 1, "green": 0}),
        ("\U0001f7e5 16 \U0001f7e7 19 \u26aa\ufe0f 64 \U0001f7e6 1 \U0001f7e9 0\nMarket Av. 88.6%",
         88.6, {"red": 16, "orange": 19, "white": 64, "blue": 1, "green": 0}),
        ("\U0001f7e5 19 \U0001f7e7 23 \u26aa\ufe0f 57 \U0001f7e6 1 \U0001f7e9 0\nMarket Av. 93.8%",
         93.8, {"red": 19, "orange": 23, "white": 57, "blue": 1, "green": 0}),
        ("\U0001f7e5 5 \U0001f7e7 10 \u26aa\ufe0f 70 \U0001f7e6 10 \U0001f7e9 5\nAvg. 47.3%",
         47.3, {"red": 5, "orange": 10, "white": 70, "blue": 10, "green": 5}),
        ("\U0001f7e5 0 \U0001f7e7 0 \u26aa\ufe0f 99 \U0001f7e6 1 \U0001f7e9 0\nMarket Av. 50.0%",
         50.0, {"red": 0, "orange": 0, "white": 99, "blue": 1, "green": 0}),
    ]
    for text, exp_val, exp_extra in tests_as:
        r = parse_altspi(text)
        try:
            assert r is not None, f"returned None"
            assert r["value"] == exp_val, f"value={r['value']} expected {exp_val}"
            for k in ("red", "orange", "white", "blue", "green"):
                assert r["extra"][k] == exp_extra[k], f"extra[{k}]={r['extra'][k]} expected {exp_extra[k]}"
            print(f"  OK: val={r['value']}, red={r['extra']['red']}, white={r['extra']['white']}")
        except AssertionError as e:
            print(f"  FAIL: {e}")
            errors += 1

    # --- Scalp17 ---
    print("\n--- Scalp17 ---")
    tests_sc = [
        ("\u26a1\ufe0fAvg. 70.2%", 70.2, None),
        ("\u26a1\ufe0fAvg. -6.5%", -6.5, None),
        ("\u26a1\ufe0fAvg. 0.1%", 0.1, None),
        ("\u26a1\ufe0fAvg. -28.7%", -28.7, None),
        ("\u26a1\ufe0f\U0001f7e7Avg. 64.3%", 64.3, "orange"),
        ("\u26a1\ufe0f\U0001f7e9Avg. 85.0%", 85.0, "green"),
        ("\u26a1\ufe0f\U0001f7e5Avg. 3.0%", 3.0, "red"),
        ("\u26a1\ufe0f\U0001f7e6Avg. 12.5%", 12.5, "blue"),
        ("\u26a1\ufe0f\U0001f7eaAvg. 50.0%", 50.0, "purple"),
        ("\u26a1\ufe0fAvg. -41.7%", -41.7, None),
    ]
    for text, exp_val, exp_color in tests_sc:
        r = parse_scalp17(text)
        try:
            assert r is not None, f"returned None for '{text}'"
            assert abs(r["value"] - exp_val) < 0.01, f"value={r['value']} expected {exp_val}"
            assert r["color"] == exp_color, f"color={r['color']} expected {exp_color}"
            print(f"  OK: val={r['value']}, color={r['color']}")
        except AssertionError as e:
            print(f"  FAIL: {e}")
            errors += 1

    # --- Scalp17: must reject non-signal messages ---
    r = parse_scalp17("Текущие настройки канала: <=5% и >=60%")
    assert r is None, "Should reject config message without ⚡"
    print("  OK: rejects config messages")

    # --- Index ---
    print("\n--- Index ---")
    tests_ix = [
        ("\U0001f7e5INDEX 15min\n\n\U0001f7e1Bitcoin 116633.02", "bearish", "red", "15min", 116633.02),
        ("\U0001f7e5INDEX 15min\n\n\U0001f7e1Bitcoin 117499.63", "bearish", "red", "15min", 117499.63),
        ("\U0001f7e5\U0001f7e5\U0001f7e5INDEX 1h\n\n\U0001f7e1Bitcoin 99000.0", "bearish", "red", "1h", 99000.0),
        ("\U0001f7e9\U0001f7e9INDEX 15min\n\n\U0001f7e1Bitcoin 105000.0", "bullish", "green", "15min", 105000.0),
        ("\U0001f7e9INDEX 30m\n\n\U0001f7e1Bitcoin 110000.5", "bullish", "green", "30m", 110000.5),
    ]
    for text, exp_dir, exp_color, exp_tf, exp_price in tests_ix:
        r = parse_index_btc(text)
        try:
            assert r is not None, f"returned None for '{text[:40]}'"
            assert r["direction"] == exp_dir, f"direction={r['direction']} expected {exp_dir}"
            assert r["color"] == exp_color, f"color={r['color']} expected {exp_color}"
            assert r["timeframe"] == exp_tf, f"timeframe={r['timeframe']} expected {exp_tf}"
            assert r["btc_price"] == exp_price, f"btc_price={r['btc_price']} expected {exp_price}"
            print(f"  OK: dir={r['direction']}, tf={r['timeframe']}, btc=${r['btc_price']}")
        except AssertionError as e:
            print(f"  FAIL: {e}")
            errors += 1

    # Index: must reject ETH/SOL messages
    r = parse_index_btc("\U0001f7e5\U0001f7e5INDEX 30m\n\n\u26aaETH 4324.57")
    assert r is None, "Should reject ETH-only messages"
    r = parse_index_btc("\U0001f7ea\U0001f7eaINDEX 30m\n\n\U0001f7e3SOL 219.09")
    assert r is None, "Should reject SOL-only messages"
    print("  OK: rejects ETH/SOL-only messages")

    # --- DyorAlerts ---
    print("\n--- DyorAlerts ---")
    tests_da = [
        ("\U0001f7e2\U0001f7e2\U0001f7e2\U0001f7e2 \u0414\u0438\u0441\u0431\u0430\u043b\u0430\u043d\u0441 \u043f\u043e\u043a\u0443\u043f\u0430\u0442\u0435\u043b\u044f\n\nBTC/USDT-SPOT: 65247.4\n\nBinance:\n   Long: $1.15M\n   Short: $428.036k\nTotal liquidations:\n   Long: $5.02M\n   Short: $838.890k",
         "buyer_disbalance", "bullish", 65247.4),
        ("\U0001f49a\U0001f49a\U0001f49a\U0001f49a\u0412\u043d\u0438\u043c\u0430\u043d\u0438\u0435, \u041b\u043e\u043d\u0433\u043e\u0432\u044b\u0439 \u043f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442 4 \u0443\u0440\u043e\u0432\u0435\u043d\u044c (\u041c\u0430\u043a\u0441\u0438\u043c\u0430\u043b\u044c\u043d\u044b\u0439)\n\nBTC/USDT-SPOT: 65812.87\n\nBinance:\n   Long: $3.74M\n   Short: $708.139k\nTotal liquidations:\n   Long: $10.19M\n   Short: $1.66M",
         "long_priority", "bullish", 65812.87),
        ("\U0001f49a\u0412\u043d\u0438\u043c\u0430\u043d\u0438\u0435, \u041b\u043e\u043d\u0433\u043e\u0432\u044b\u0439 \u043f\u0440\u0438\u043e\u0440\u0438\u0442\u0435\u0442 1 \u0443\u0440\u043e\u0432\u0435\u043d\u044c\n\nBTC/USDT-SPOT: 65812.87\n\nBinance:\n   Long: $3.74M\n   Short: $708.139k\nTotal liquidations:\n   Long: $10.19M\n   Short: $1.66M",
         "long_priority", "bullish", 65812.87),
        ("\U0001f7e1 \u041b\u043e\u043a\u0430\u043b\u044c\u043d\u044b\u0435 \u043c\u0435\u0442\u0440\u0438\u043a\u0438 \u043f\u0440\u0438\u0448\u043b\u0438 \u0432 \u0411\u0430\u043b\u0430\u043d\u0441\n\nBTC/USDT-SPOT: 67814.01\n\nBinance:\n   Long: $1.89M\n   Short: $801.430k\nTotal liquidations:\n   Long: $6.94M\n   Short: $1.78M",
         "balance", "neutral", 67814.01),
        ("\U0001f7e9\U0001f7e9\U0001f7e9\U0001f7e9 \u0441\u0438\u0433\u043d\u0430\u043b \u043b\u043e\u043d\u0433, \u0441\u0438\u043b\u0430 \u0441\u0438\u0433\u043d\u0430\u043b\u0430 GOLD \u0412\u043d\u0438\u043c\u0430\u043d\u0438\u0435, \u0437\u043e\u043d\u0430 \u043d\u0430\u0431\u043e\u0440\u0430.\n\nBTC/USDT-SPOT: 66171.56\n\nBinance:\n   Long: $6.60M\n   Short: $701.335k\nTotal liquidations:\n   Long: $18.62M\n   Short: $2.39M",
         "long_signal", "bullish", 66171.56),
        ("\U0001f7e5\u0441\u0438\u0433\u043d\u0430\u043b \u0428\u043e\u0440\u0442, \u0441\u0438\u043b\u0430 \u0441\u0438\u0433\u043d\u0430\u043b\u0430 \u041d\u0435 \u043e\u043f\u0440\u0435\u0434\u0435\u043b\u0435\u043d\u0430.\n\nBTC/USDT-SPOT: 69895.01\n\nBinance:\n   Long: $193.369k\n   Short: $247.630k\nTotal liquidations:\n   Long: $419.952k\n   Short: $610.529k",
         "short_signal", "bearish", 69895.01),
        ("\U0001f53b\U0001f53b\U0001f53b\u0412\u043d\u0438\u043c\u0430\u043d\u0438\u0435, \u0428\u043e\u0440\u0442\u043e\u0432\u044b\u0439 \u0432\u0441\u043f\u043b\u0435\u0441\u043a\n\nBTC/USDT-SPOT: 70944\n\nBinance:\n   Long: $190.704k\n   Short: $94.547k\nTotal liquidations:\n   Long: $345.169k\n   Short: $121.248k",
         "short_signal", "bearish", 70944.0),
    ]
    for text, exp_type, exp_dir, exp_price in tests_da:
        r = parse_dyor_alerts(text)
        try:
            assert r is not None, f"returned None for {exp_type}"
            assert r["extra"]["signal_type"] == exp_type, f"type={r['extra']['signal_type']} expected {exp_type}"
            assert r["direction"] == exp_dir, f"dir={r['direction']} expected {exp_dir}"
            assert r["btc_price"] == exp_price, f"price={r['btc_price']} expected {exp_price}"
            print(f"  OK: type={exp_type}, dir={r['direction']}, btc=${r['btc_price']}")
        except AssertionError as e:
            print(f"  FAIL: {e}")
            errors += 1

    print(f"\n{'=' * 60}")
    if errors:
        print(f"TESTS: {errors} FAILURES")
    else:
        print("ALL TESTS PASSED")
    print(f"{'=' * 60}")
    return errors == 0


# ═══════════════════════════════════════════════════
# STEP 2: REPARSE FROM raw_messages
# ═══════════════════════════════════════════════════

CHANNELS_TO_REPARSE = {
    "SellsPowerIndex": {"parser": "sells_power", "filter_author": None, "topic_id": None},
    "AltSPI":          {"parser": "altspi", "filter_author": None, "topic_id": None},
    "Scalp17":         {"parser": "scalp17", "filter_author": None, "topic_id": None},
    "Index":           {"parser": "index_btc", "filter_author": None, "topic_id": None},
    "DyorAlerts":      {"parser": "dyor_alerts", "filter_author": "dyor_alerts_EtH_2_O_bot", "topic_id": None},
    "AltSwing":        {"parser": "altswing", "filter_author": None, "topic_id": None},
    "DiamondMarks":    {"parser": "diamond_marks", "filter_author": None, "topic_id": None},
}


def reparse_all():
    db = sqlite3.connect('btc_signals.db')
    db.row_factory = sqlite3.Row

    print("\n" + "=" * 60)
    print("BEFORE REPARSE: CURRENT STATS")
    print("=" * 60)

    before_stats = {}
    for name in CHANNELS_TO_REPARSE:
        raw = db.execute("SELECT COUNT(*) as c FROM raw_messages WHERE channel_name=?", (name,)).fetchone()["c"]
        text_msgs = db.execute("SELECT COUNT(*) as c FROM raw_messages WHERE channel_name=? AND has_text=1", (name,)).fetchone()["c"]
        signals = db.execute("SELECT COUNT(*) as c FROM signals WHERE channel_name=?", (name,)).fetchone()["c"]
        before_stats[name] = {"raw": raw, "text": text_msgs, "signals": signals}
        print(f"  {name:>20}: raw={raw:>6}, text={text_msgs:>6}, signals={signals:>6}")

    print("\n" + "=" * 60)
    print("REPARSING...")
    print("=" * 60)

    os.makedirs("unrecognized", exist_ok=True)
    after_stats = {}

    for name, config in CHANNELS_TO_REPARSE.items():
        parser_type = config["parser"]
        parser_func = PARSERS.get(parser_type)

        if parser_func is None:
            print(f"\n  {name}: SKIPPED (parser '{parser_type}' not in this script)")
            after_stats[name] = {"parsed_ok": 0, "parsed_fail": 0, "skipped_filter": 0, "validation_fail": 0}
            continue

        # Get raw messages
        rows = db.execute("""
            SELECT id, channel_id, message_id, timestamp, text, from_username, reply_to_topic_id
            FROM raw_messages WHERE channel_name=? AND has_text=1 ORDER BY timestamp
        """, (name,)).fetchall()

        if not rows:
            print(f"\n  {name}: 0 text messages (nothing to reparse)")
            after_stats[name] = {"parsed_ok": 0, "parsed_fail": 0, "skipped_filter": 0, "validation_fail": 0}
            continue

        # Reset is_parsed for this channel
        db.execute("UPDATE raw_messages SET is_parsed=NULL, parse_error=NULL WHERE channel_name=?", (name,))
        # Delete old signals for this channel
        db.execute("DELETE FROM signals WHERE channel_name=?", (name,))
        # Delete old price context
        db.execute("DELETE FROM signal_price_context WHERE channel_name=?", (name,))
        db.commit()

        # Reparse
        parsed_ok = parsed_fail = skipped_filter = validation_fail = 0
        unrec_file = os.path.join("unrecognized", f"reparse_{name}.jsonl")
        if os.path.exists(unrec_file):
            os.remove(unrec_file)
        unrec_fh = open(unrec_file, 'a', encoding='utf-8')

        for row in rows:
            text = row["text"]

            # Filter by author
            if config["filter_author"]:
                stored_user = row["from_username"]
                if not stored_user or stored_user.lower() != config["filter_author"].lower():
                    skipped_filter += 1
                    continue

            # Parse
            parsed = parser_func(text)
            if parsed is None:
                parsed_fail += 1
                json.dump({"channel": name, "msg_id": row["message_id"],
                           "ts": row["timestamp"], "text": text[:200]},
                          unrec_fh, ensure_ascii=False)
                unrec_fh.write('\n')
                db.execute("UPDATE raw_messages SET is_parsed=0, parse_error='no_match' WHERE id=?", (row["id"],))
                continue

            # Validate
            valid, reason = validate_parsed(parser_type, parsed)
            if not valid:
                validation_fail += 1
                db.execute("UPDATE raw_messages SET is_parsed=0, parse_error=? WHERE id=?",
                           (f"validation: {reason}", row["id"]))
                continue

            # Save signal
            parsed_ok += 1
            db.execute("""INSERT OR IGNORE INTO signals
                (channel_id, channel_name, message_id, message_text, timestamp,
                 indicator_value, signal_color, signal_direction, timeframe,
                 btc_price_from_channel, btc_price_binance, extra_data)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (row["channel_id"], name, row["message_id"], text[:2000],
                 row["timestamp"], parsed.get("value"), parsed.get("color"),
                 parsed.get("direction"), parsed.get("timeframe"),
                 parsed.get("btc_price"), None,
                 json.dumps(parsed.get("extra", {}), ensure_ascii=False)))
            db.execute("UPDATE raw_messages SET is_parsed=1, parse_error=NULL WHERE id=?", (row["id"],))

        unrec_fh.close()
        db.commit()

        total_attempted = len(rows) - skipped_filter
        pct = (parsed_ok / max(total_attempted, 1)) * 100
        after_stats[name] = {
            "parsed_ok": parsed_ok, "parsed_fail": parsed_fail,
            "skipped_filter": skipped_filter, "validation_fail": validation_fail,
        }
        print(f"\n  {name}: {parsed_ok}/{total_attempted} parsed ({pct:.1f}%)"
              f"  | fail={parsed_fail} | val_fail={validation_fail} | filtered={skipped_filter}")

    # Final summary
    print("\n" + "=" * 60)
    print("BEFORE -> AFTER COMPARISON")
    print("=" * 60)
    print(f"  {'Channel':>20} | {'Before':>8} | {'After':>8} | {'Filtered':>8} | {'Fail':>8} | {'ValFail':>8}")
    print(f"  {'-'*20}-+-{'-'*8}-+-{'-'*8}-+-{'-'*8}-+-{'-'*8}-+-{'-'*8}")
    for name in CHANNELS_TO_REPARSE:
        b = before_stats[name]["signals"]
        a = after_stats[name]["parsed_ok"]
        f_ = after_stats[name]["parsed_fail"]
        vf = after_stats[name]["validation_fail"]
        sf = after_stats[name]["skipped_filter"]
        print(f"  {name:>20} | {b:>8} | {a:>8} | {sf:>8} | {f_:>8} | {vf:>8}")

    # Total signals now
    total = db.execute("SELECT COUNT(*) as c FROM signals").fetchone()["c"]
    print(f"\n  Total signals in DB: {total}")

    db.close()


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    tests_ok = run_tests()
    if tests_ok:
        reparse_all()
    else:
        print("\nTests failed! Fix issues before reparsing.")
