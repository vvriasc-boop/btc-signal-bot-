"""Win/loss streak analysis and serial correlation testing."""
import numpy as np
import pandas as pd
from scipy import stats as scipy_stats


def run(df_signals, df_prices, df_context, fee_rate=0.001):
    fee_pct = fee_rate * 2 * 100
    merged = _merge_directional(df_signals, df_context, fee_pct)
    if len(merged) == 0:
        return {}

    overall_outcomes = merged["outcome"].values
    result = {
        "overall": _streak_stats(overall_outcomes),
        "per_channel": {},
        "after_streak": _after_streak_analysis(overall_outcomes),
    }

    for ch, grp in merged.groupby("channel_name"):
        if len(grp) < 10:
            continue
        outcomes = grp["outcome"].values
        ch_stats = _streak_stats(outcomes)
        z, p = _runs_test(outcomes)
        ch_stats["runs_test_z"] = round(z, 3)
        ch_stats["runs_test_p"] = round(p, 4)
        ch_stats["serial_correlation"] = bool(p < 0.05)
        result["per_channel"][ch] = ch_stats

    return result


def _merge_directional(df_signals, df_context, fee_pct):
    merged = df_signals.merge(
        df_context, left_on="id", right_on="signal_id", how="inner",
        suffixes=("", "_ctx"),
    )
    mask = (
        merged["derived_direction"].isin(["bullish", "bearish"])
        & merged["change_1h_pct"].notna()
        & ((merged["filled_mask"].astype(int) & 4) > 0)
    )
    df = merged[mask].sort_values("timestamp").copy()
    sign = df["derived_direction"].map({"bullish": 1.0, "bearish": -1.0})
    net = df["change_1h_pct"] * sign - fee_pct
    df["outcome"] = np.where(net > 0, 1, 0)  # 1=win, 0=loss
    return df.reset_index(drop=True)


def _streak_stats(outcomes):
    """Compute streak statistics from binary outcome array."""
    if len(outcomes) == 0:
        return {}
    streaks = _find_streaks(outcomes)
    win_streaks = [l for t, l in streaks if t == 1]
    loss_streaks = [l for t, l in streaks if t == 0]
    dist = {}
    for _, l in streaks:
        dist[l] = dist.get(l, 0) + 1

    return {
        "max_win_streak": max(win_streaks) if win_streaks else 0,
        "max_loss_streak": max(loss_streaks) if loss_streaks else 0,
        "avg_win_streak": round(np.mean(win_streaks), 2) if win_streaks else 0,
        "avg_loss_streak": round(np.mean(loss_streaks), 2) if loss_streaks else 0,
        "streak_distribution": dict(sorted(dist.items())),
    }


def _find_streaks(outcomes):
    """Find consecutive streaks. Returns list of (type, length)."""
    if len(outcomes) == 0:
        return []
    streaks = []
    current_type = outcomes[0]
    current_len = 1
    for i in range(1, len(outcomes)):
        if outcomes[i] == current_type:
            current_len += 1
        else:
            streaks.append((current_type, current_len))
            current_type = outcomes[i]
            current_len = 1
    streaks.append((current_type, current_len))
    return streaks


def _runs_test(outcomes):
    """Wald-Wolfowitz runs test for randomness."""
    n = len(outcomes)
    if n < 20:
        return 0.0, 1.0
    n1 = int(outcomes.sum())
    n0 = n - n1
    if n1 == 0 or n0 == 0:
        return 0.0, 1.0

    runs = 1 + int(np.sum(outcomes[1:] != outcomes[:-1]))
    expected = 1 + 2 * n0 * n1 / n
    denom = n * n
    var_num = 2 * n0 * n1 * (2 * n0 * n1 - n)
    var_denom = denom * (n - 1)
    if var_denom == 0:
        return 0.0, 1.0
    variance = var_num / var_denom
    if variance <= 0:
        return 0.0, 1.0

    z = (runs - expected) / np.sqrt(variance)
    p = 2 * (1 - scipy_stats.norm.cdf(abs(z)))
    return float(z), float(p)


def _after_streak_analysis(outcomes):
    """Win rate after streaks of N wins/losses."""
    result = {}
    for streak_len in [3, 5]:
        for streak_type, label in [(1, "wins"), (0, "losses")]:
            next_outcomes = []
            count = 0
            for i in range(len(outcomes)):
                if outcomes[i] == streak_type:
                    count += 1
                else:
                    count = 0
                if count == streak_len and i + 1 < len(outcomes):
                    next_outcomes.append(outcomes[i + 1])
            key = f"after_{streak_len}_{label}"
            if next_outcomes:
                arr = np.array(next_outcomes)
                result[key] = {
                    "count": len(arr),
                    "next_win_rate_pct": round(float(arr.mean() * 100), 1),
                }
            else:
                result[key] = {"count": 0, "next_win_rate_pct": None}
    return result
