"""Signal latency decay: how performance degrades with delayed entry."""
import numpy as np
import pandas as pd

DELAY_MINUTES = [0, 1, 3, 5, 10]
HOLD_MINUTES = 60


def run(df_signals, df_prices, df_context, fee_rate=0.001):
    fee_pct = fee_rate * 2 * 100
    price_ts, prices = _prepare_prices(df_prices)
    directional = df_signals[
        df_signals["derived_direction"].isin(["bullish", "bearish"])
    ].copy()
    if len(directional) == 0:
        return {}

    sig_ts = _to_unix(directional["timestamp"])
    sig_dirs = directional["derived_direction"].map(
        {"bullish": 1.0, "bearish": -1.0}
    ).values
    ch_names = directional["channel_name"].values

    delays_result = {}
    per_channel = {}

    for delay in DELAY_MINUTES:
        rets = _compute_delayed_returns(
            sig_ts, sig_dirs, price_ts, prices, delay, HOLD_MINUTES, fee_pct
        )
        valid = np.isfinite(rets)
        if valid.sum() == 0:
            continue
        delays_result[delay] = _summarize(rets[valid])

        for ch in np.unique(ch_names):
            ch_mask = (ch_names == ch) & valid
            if ch_mask.sum() < 5:
                continue
            per_channel.setdefault(ch, {})
            per_channel[ch][delay] = _summarize(rets[ch_mask])

    decay_rate = _estimate_decay_rate(delays_result)
    half_life = _estimate_half_life(delays_result)

    return {
        "delays": delays_result,
        "per_channel_decay": per_channel,
        "decay_rate_pct_per_minute": decay_rate,
        "half_life_minutes": half_life,
    }


def _prepare_prices(df_prices):
    ts = _to_unix(df_prices["timestamp"])
    prices = df_prices["price"].values.astype(np.float64)
    order = np.argsort(ts)
    return ts[order], prices[order]


def _to_unix(series):
    return (series.astype(np.int64) // 10**9).values.astype(np.int64)


def _compute_delayed_returns(sig_ts, sig_dirs, price_ts, prices, delay, hold, fee_pct):
    """Compute returns with delayed entry (vectorized)."""
    m = len(prices)
    entry_ts = sig_ts + delay * 60
    exit_ts = sig_ts + (delay + hold) * 60

    entry_idx = np.searchsorted(price_ts, entry_ts, side="left")
    exit_idx = np.searchsorted(price_ts, exit_ts, side="left")
    entry_idx = np.clip(entry_idx, 0, m - 1)
    exit_idx = np.clip(exit_idx, 0, m - 1)

    entry_prices = prices[entry_idx]
    exit_prices = prices[exit_idx]

    valid = entry_prices > 0
    raw_pct = np.where(
        valid, (exit_prices - entry_prices) / entry_prices * 100, np.nan
    )
    return raw_pct * sig_dirs - fee_pct


def _summarize(rets):
    return {
        "avg_return_net": round(float(np.mean(rets)), 4),
        "win_rate_pct": round(float((rets > 0).mean() * 100), 1),
        "sharpe": round(
            float(np.mean(rets) / np.std(rets) * np.sqrt(8760))
            if np.std(rets) > 0 else 0, 4
        ),
        "trades": int(len(rets)),
    }


def _estimate_decay_rate(delays_result):
    """Linear regression of avg return vs delay."""
    points = [(d, v["avg_return_net"]) for d, v in delays_result.items()
              if v.get("avg_return_net") is not None]
    if len(points) < 2:
        return 0.0
    x = np.array([p[0] for p in points], dtype=float)
    y = np.array([p[1] for p in points], dtype=float)
    if np.std(x) == 0:
        return 0.0
    slope = np.polyfit(x, y, 1)[0]
    return round(float(slope), 6)


def _estimate_half_life(delays_result):
    """Estimate minutes until signal loses 50% of its value."""
    if 0 not in delays_result:
        return None
    base = delays_result[0].get("avg_return_net", 0)
    if base <= 0:
        return None
    target = base * 0.5
    for delay in sorted(delays_result.keys()):
        ret = delays_result[delay].get("avg_return_net", 0)
        if ret <= target:
            return float(delay)
    return None
