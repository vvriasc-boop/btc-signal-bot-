import csv
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import config


MAX_SIGNALS = 5
START_DATE = "2025-07-01T00:00:00"


def _round5(iso_str):
    """Round ISO timestamp to nearest 5-minute boundary."""
    dt = datetime.fromisoformat(iso_str).replace(tzinfo=timezone.utc)
    m = dt.minute - dt.minute % 5
    return dt.replace(minute=m, second=0, microsecond=0)


def _build_price_map():
    """Build {rounded_5min_ts: price} from btc_price table."""
    rows = config.db.execute(
        "SELECT timestamp, price FROM btc_price WHERE timestamp >= ? ORDER BY timestamp",
        (START_DATE,)
    ).fetchall()
    price_map = {}
    for r in rows:
        key = _round5(r["timestamp"])
        if key not in price_map:
            price_map[key] = r["price"]
    return price_map


def _build_signal_buckets():
    """Build {rounded_5min_ts: [signals]} from signals table."""
    rows = config.db.execute("""
        SELECT timestamp, channel_name, indicator_value, signal_color, signal_direction
        FROM signals WHERE timestamp >= ? ORDER BY timestamp
    """, (START_DATE,)).fetchall()
    buckets = defaultdict(list)
    for r in rows:
        buckets[_round5(r["timestamp"])].append(r)
    return buckets


def _build_timeline(price_map, signal_buckets):
    """Generate continuous 5-minute timeline."""
    all_keys = sorted(set(list(price_map.keys()) + list(signal_buckets.keys())))
    if not all_keys:
        return []
    start = all_keys[0]
    end = max(all_keys[-1], datetime.now(timezone.utc).replace(second=0, microsecond=0))
    end = end.replace(minute=end.minute - end.minute % 5)
    timeline = []
    current = start
    while current <= end:
        timeline.append(current)
        current += timedelta(minutes=5)
    return timeline


def export_csv() -> str:
    """Export 5-min BTC price stream with overlaid signals to CSV."""
    price_map = _build_price_map()
    signal_buckets = _build_signal_buckets()
    timeline = _build_timeline(price_map, signal_buckets)
    if not timeline:
        return ""

    filepath = f"export_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}.csv"
    header = ["timestamp", "btc_price", "signal_count"]
    for i in range(1, MAX_SIGNALS + 1):
        header.extend([f"signal_{i}_channel", f"signal_{i}_value",
                       f"signal_{i}_color", f"signal_{i}_direction"])

    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for ts in timeline:
            _write_row(writer, ts, price_map.get(ts), signal_buckets.get(ts, []))

    return filepath


def _write_row(writer, ts, price, sigs):
    """Write one CSV row for a 5-minute window."""
    count = min(len(sigs), MAX_SIGNALS)
    row = [
        ts.strftime("%Y-%m-%d %H:%M"),
        f"{price:.2f}" if price is not None else "",
        count,
    ]
    for i in range(MAX_SIGNALS):
        if i < len(sigs):
            s = sigs[i]
            val = s["indicator_value"]
            row.extend([
                s["channel_name"],
                f"{val:.1f}" if val is not None else "",
                s["signal_color"] or "",
                s["signal_direction"] or "",
            ])
        else:
            row.extend(["", "", "", ""])
    writer.writerow(row)
