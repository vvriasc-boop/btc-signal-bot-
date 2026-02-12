#!/usr/bin/env python3
"""Reparse AltSwing and DiamondMarks after download."""
import re, json, sqlite3, os
from datetime import datetime, timezone

def parse_altswing(text):
    m = re.search(r'Avg\.\s*(-?[\d.]+)%', text)
    if not m:
        return None
    color = next((n for e, n in [('\U0001f7e9', 'green'), ('\U0001f7e7', 'orange'),
                                  ('\U0001f7e5', 'red'), ('\U0001f7e6', 'blue'),
                                  ('\u2b1c', 'white')] if e in text), None)
    return {'value': float(m.group(1)), 'color': color, 'direction': None,
            'timeframe': None, 'btc_price': None, 'extra': {}}

def parse_diamond_marks(text):
    if 'Total' not in text or 'BTC/USDT:' not in text:
        return None
    tf = re.search(r'Total\s+(\d+[mhHМ])', text)
    price = re.search(r'BTC/USDT:\s*\$?([\d,]+\.?\d*)', text)
    g, o, r_ = text.count('\U0001f7e9'), text.count('\U0001f7e7'), text.count('\U0001f7e5')
    y = text.count('\U0001f7e8')
    direction = 'bullish' if g > r_ else ('bearish' if r_ > g else 'neutral')
    colors = {'green': g, 'orange': o, 'red': r_, 'yellow': y}
    dominant = max(colors, key=colors.get) if any(colors.values()) else None
    return {'value': None, 'color': dominant, 'direction': direction,
            'timeframe': tf.group(1).lower() if tf else None,
            'btc_price': float(price.group(1).replace(',', '')) if price else None,
            'extra': {'green_count': g, 'orange_count': o, 'red_count': r_,
                      'yellow_count': y, 'has_fire': '\U0001f525' in text}}


# === TESTS ===
print('=== TESTS ===')
errors = 0

# AltSwing
for text, exp_val, exp_color in [
    ('Avg. 60.1%', 60.1, None),
    ('Avg. 58.6%', 58.6, None),
    ('\U0001f7e7Avg. 67.3%', 67.3, 'orange'),
    ('\u2b1c\ufe0fAvg. 18.4%', 18.4, 'white'),
    ('Avg. 22%', 22.0, None),
    ('Avg. 39.9%', 39.9, None),
    ('\U0001f7e9Avg. 85.0%', 85.0, 'green'),
    ('\U0001f7e5Avg. 5.0%', 5.0, 'red'),
    ('Avg. 50.4%', 50.4, None),
    ('Avg. 0.1%', 0.1, None),
]:
    r = parse_altswing(text)
    if r is None or r['value'] != exp_val or r['color'] != exp_color:
        print(f'  FAIL AltSwing: {text} -> {r}')
        errors += 1
    else:
        print(f'  OK AltSwing: val={r["value"]}, color={r["color"]}')

assert parse_altswing('-1002351387526') is None, "Should reject channel ID"
print('  OK AltSwing: rejects non-signal')

# DiamondMarks
for text, exp_dir, exp_tf in [
    ('\U0001f525\U0001f7e9\U0001f525 Total 15m\nBTCUSDT : 114140.72\n\nBTC/USDT: $114,141', 'bullish', '15m'),
    ('\U0001f525\U0001f7e9\U0001f7e9\U0001f525Total 30m \nBTCUSDT : 112696.08\nBTC/USDT: $112,746', 'bullish', '30m'),
    ('\U0001f7e8 Total 5m\n\nBTC/USDT: $65,458', 'neutral', '5m'),
    ('\U0001f525\U0001f7e5\U0001f525Total 15m\nBTC/USDT: $68,023', 'bearish', '15m'),
    ('\U0001f525\U0001f7e9\U0001f7e9\U0001f7e9\U0001f7e9\U0001f525Total 1H\nBTC/USDT: $107,929', 'bullish', '1h'),
    ('\U0001f525\U0001f7e9\U0001f525 Total 15m\n\nBTC/USDT: $65,595', 'bullish', '15m'),
    ('\U0001f525\U0001f7e9I\U0001f7e9I\U0001f7e9I\U0001f7e9\U0001f525Total 2H\nBTC/USDT: $67,451', 'bullish', '2h'),
    ('\U0001f525\U0001f7e9\U0001f7e9\U0001f525Total 30m \nBTC/USDT: $66,984', 'bullish', '30m'),
    ('\U0001f525\U0001f7e9\U0001f7e9\U0001f7e9\U0001f525Total 1H\nBTC/USDT: $67,458', 'bullish', '1h'),
    ('\U0001f525\U0001f7e5\U0001f525Total 15m\nBTC/USDT: $68,023', 'bearish', '15m'),
]:
    r = parse_diamond_marks(text)
    if r is None or r['direction'] != exp_dir or r['timeframe'] != exp_tf:
        print(f'  FAIL DM: {text[:40]} -> {r}')
        errors += 1
    else:
        print(f'  OK DM: dir={r["direction"]}, tf={r["timeframe"]}, btc=${r["btc_price"]}')

if errors:
    print(f'\n{errors} FAILURES')
    exit(1)
print('\nALL TESTS PASSED\n')

# === REPARSE ===
os.chdir(os.path.dirname(os.path.abspath(__file__)))
db = sqlite3.connect('btc_signals.db')
db.row_factory = sqlite3.Row
os.makedirs('unrecognized', exist_ok=True)

for name, parser_func in [('AltSwing', parse_altswing), ('DiamondMarks', parse_diamond_marks)]:
    rows = db.execute(
        'SELECT id, channel_id, message_id, timestamp, text FROM raw_messages '
        'WHERE channel_name=? AND has_text=1 ORDER BY timestamp', (name,)
    ).fetchall()
    if not rows:
        print(f'{name}: 0 text messages')
        continue

    db.execute('UPDATE raw_messages SET is_parsed=NULL, parse_error=NULL WHERE channel_name=?', (name,))
    db.execute('DELETE FROM signals WHERE channel_name=?', (name,))
    db.execute('DELETE FROM signal_price_context WHERE channel_name=?', (name,))

    ok = fail = 0
    unrec = os.path.join('unrecognized', f'reparse_{name}.jsonl')
    if os.path.exists(unrec):
        os.remove(unrec)
    fh = open(unrec, 'a', encoding='utf-8')

    for row in rows:
        parsed = parser_func(row['text'])
        if parsed is None:
            fail += 1
            json.dump({'channel': name, 'msg_id': row['message_id'],
                       'ts': row['timestamp'], 'text': row['text'][:200]},
                      fh, ensure_ascii=False)
            fh.write('\n')
            db.execute('UPDATE raw_messages SET is_parsed=0, parse_error=? WHERE id=?',
                       ('no_match', row['id']))
            continue
        ok += 1
        db.execute(
            'INSERT OR IGNORE INTO signals '
            '(channel_id, channel_name, message_id, message_text, timestamp, '
            'indicator_value, signal_color, signal_direction, timeframe, '
            'btc_price_from_channel, btc_price_binance, extra_data) '
            'VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
            (row['channel_id'], name, row['message_id'], row['text'][:2000],
             row['timestamp'], parsed.get('value'), parsed.get('color'),
             parsed.get('direction'), parsed.get('timeframe'),
             parsed.get('btc_price'), None,
             json.dumps(parsed.get('extra', {}), ensure_ascii=False)))
        db.execute('UPDATE raw_messages SET is_parsed=1, parse_error=NULL WHERE id=?', (row['id'],))

    fh.close()
    db.commit()
    pct = (ok / max(len(rows), 1)) * 100
    print(f'{name}: {ok}/{len(rows)} parsed ({pct:.1f}%) | fail={fail}')

    if fail > 0:
        samples = db.execute(
            'SELECT text FROM raw_messages WHERE channel_name=? AND is_parsed=0 LIMIT 3',
            (name,)
        ).fetchall()
        for s in samples:
            print(f'  FAIL: {repr(s["text"][:100])}')

# Update sync_log
now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
for name in ['AltSwing', 'DiamondMarks']:
    raw = db.execute('SELECT COUNT(*) as c FROM raw_messages WHERE channel_name=?', (name,)).fetchone()['c']
    ok_cnt = db.execute('SELECT COUNT(*) as c FROM signals WHERE channel_name=?', (name,)).fetchone()['c']
    fail_cnt = db.execute('SELECT COUNT(*) as c FROM raw_messages WHERE channel_name=? AND is_parsed=0', (name,)).fetchone()['c']
    db.execute('DELETE FROM sync_log WHERE channel_name=?', (name,))
    db.execute(
        'INSERT INTO sync_log (channel_name, phase, total_messages, parsed_ok, parsed_fail, started_at, completed_at) '
        'VALUES (?, ?, ?, ?, ?, ?, ?)', (name, 'complete', raw, ok_cnt, fail_cnt, now, now))
db.commit()

# Final stats
print('\n' + '=' * 60)
print('FULL SIGNAL STATS (ALL 9 CHANNELS)')
print('=' * 60)
print(f'{"Channel":>20} | {"Signals":>8} | {"Raw text":>8} | {"Rate":>8}')
print(f'{"-"*20}-+-{"-"*8}-+-{"-"*8}-+-{"-"*8}')
for name in ['AltSwing','DiamondMarks','SellsPowerIndex','AltSPI','Scalp17',
             'Index','DyorAlerts','DMI_SMF','RSI_BTC']:
    raw = db.execute('SELECT COUNT(*) as c FROM raw_messages WHERE channel_name=? AND has_text=1',
                     (name,)).fetchone()['c']
    sig = db.execute('SELECT COUNT(*) as c FROM signals WHERE channel_name=?',
                     (name,)).fetchone()['c']
    pct = f'{sig/max(raw,1)*100:.1f}%' if raw > 0 else 'N/A'
    print(f'{name:>20} | {sig:>8} | {raw:>8} | {pct:>8}')
total = db.execute('SELECT COUNT(*) as c FROM signals').fetchone()['c']
print(f'\nTotal signals in DB: {total}')
db.close()
