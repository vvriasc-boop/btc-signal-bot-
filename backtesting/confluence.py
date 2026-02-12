"""Multi-channel signal coincidence analysis."""
import numpy as np
import pandas as pd
from itertools import combinations

COINCIDENCE_WINDOW_MINUTES = 30


def run(df_signals, df_prices, df_context, fee_rate=0.001):
    fee_pct = fee_rate * 2 * 100
    merged = _prepare(df_signals, df_context, fee_pct)
    if len(merged) == 0:
        return {}

    groups = _find_coincidence_groups(merged)
    isolated = merged[~merged.index.isin(
        [idx for g in groups for idx in g["indices"]]
    )]

    by_count = _stats_by_channel_count(groups)
    pair_matrix = _pair_coincidence(groups)
    single_ret = float(isolated["net_return"].mean()) if len(isolated) > 0 else 0
    multi_rets = [g["avg_return"] for g in groups if len(g["channels"]) >= 2]
    multi_ret = float(np.mean(multi_rets)) if multi_rets else 0

    return {
        "window_minutes": COINCIDENCE_WINDOW_MINUTES,
        "total_groups": len(groups),
        "isolated_signals": len(isolated),
        "stats_by_channel_count": by_count,
        "pair_coincidence": pair_matrix,
        "single_vs_multi": {
            "single_avg_return_1h": round(single_ret, 4),
            "multi_avg_return_1h": round(multi_ret, 4),
            "improvement_pct": round(multi_ret - single_ret, 4),
        },
    }


def _prepare(df_signals, df_context, fee_pct):
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
    df["net_return"] = df["change_1h_pct"] * sign - fee_pct
    return df.reset_index(drop=True)


def _find_coincidence_groups(df):
    """Group signals within COINCIDENCE_WINDOW_MINUTES of each other."""
    groups = []
    used = set()
    ts = df["timestamp"].values
    channels = df["channel_name"].values
    returns = df["net_return"].values
    dirs = df["derived_direction"].values
    window = np.timedelta64(COINCIDENCE_WINDOW_MINUTES, "m")

    for i in range(len(df)):
        if i in used:
            continue
        group_indices = [i]
        group_channels = {channels[i]}
        for j in range(i + 1, len(df)):
            if ts[j] - ts[i] > window:
                break
            if j not in used and channels[j] not in group_channels:
                group_indices.append(j)
                group_channels.add(channels[j])
                used.add(j)

        if len(group_channels) >= 2:
            used.update(group_indices)
            bull = sum(1 for idx in group_indices if dirs[idx] == "bullish")
            bear = sum(1 for idx in group_indices if dirs[idx] == "bearish")
            consensus = "bullish" if bull > bear else ("bearish" if bear > bull else "mixed")
            groups.append({
                "indices": group_indices,
                "channels": sorted(group_channels),
                "n_channels": len(group_channels),
                "consensus": consensus,
                "avg_return": float(np.mean([returns[idx] for idx in group_indices])),
            })
    return groups


def _stats_by_channel_count(groups):
    result = {}
    for n in [2, 3, 4]:
        label = f"{n}+" if n == 4 else str(n)
        matching = [g for g in groups if (g["n_channels"] >= n if n == 4
                                          else g["n_channels"] == n)]
        if not matching:
            continue
        rets = [g["avg_return"] for g in matching]
        result[label] = {
            "count": len(matching),
            "avg_return_1h_net": round(float(np.mean(rets)), 4),
            "win_rate_pct": round(float(sum(1 for r in rets if r > 0) / len(rets) * 100), 1),
        }
    return result


def _pair_coincidence(groups):
    result = {}
    for g in groups:
        for a, b in combinations(g["channels"], 2):
            key = f"{a}_{b}"
            if key not in result:
                result[key] = {"count": 0, "returns": []}
            result[key]["count"] += 1
            result[key]["returns"].append(g["avg_return"])

    for key in result:
        rets = result[key]["returns"]
        result[key] = {
            "count": result[key]["count"],
            "avg_return_1h": round(float(np.mean(rets)), 4),
            "win_rate_pct": round(float(sum(1 for r in rets if r > 0) / len(rets) * 100), 1),
        }
    return result
