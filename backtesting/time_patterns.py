"""Time-of-day and session analysis for signal performance."""
import numpy as np
import pandas as pd

SESSIONS = {
    "Asia":   (0, 8),
    "Europe": (8, 14),
    "US":     (14, 21),
    "Off":    (21, 24),
}

DAYS = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}


def run(df_signals, df_prices, df_context, fee_rate=0.001):
    fee_pct = fee_rate * 2 * 100
    merged = _merge_directional(df_signals, df_context, fee_pct)
    if len(merged) == 0:
        return {}

    by_hour = _analyze_by_group(merged, "hour")
    by_session = _analyze_by_group(merged, "session")
    by_dow = _analyze_by_group(merged, "dow")

    best_hour = max(by_hour, key=lambda h: by_hour[h].get("avg_return_1h_net", -999))
    worst_hour = min(by_hour, key=lambda h: by_hour[h].get("avg_return_1h_net", 999))
    best_session = max(by_session, key=lambda s: by_session[s].get("avg_return_1h_net", -999))
    worst_session = min(by_session, key=lambda s: by_session[s].get("avg_return_1h_net", 999))

    return {
        "by_hour": by_hour,
        "by_session": by_session,
        "by_day_of_week": by_dow,
        "best_hour": best_hour,
        "worst_hour": worst_hour,
        "best_session": best_session,
        "worst_session": worst_session,
    }


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
    df = merged[mask].copy()
    sign = df["derived_direction"].map({"bullish": 1.0, "bearish": -1.0})
    df["net_return"] = df["change_1h_pct"] * sign - fee_pct
    df["hour"] = df["timestamp"].dt.hour
    df["dow"] = df["timestamp"].dt.dayofweek
    df["session"] = df["hour"].apply(_assign_session)
    return df


def _assign_session(hour):
    for name, (start, end) in SESSIONS.items():
        if start <= hour < end:
            return name
    return "Off"


def _analyze_by_group(df, group_col):
    """Compute stats grouped by a column."""
    result = {}
    for key, grp in df.groupby(group_col):
        rets = grp["net_return"].values
        label = DAYS.get(key, key) if group_col == "dow" else key
        if group_col == "dow":
            label = f"{key}_{DAYS[key]}"
        result[str(label)] = _group_stats(rets)
    return result


def _group_stats(rets):
    """Stats for a time group."""
    if len(rets) == 0:
        return {"signal_count": 0}
    return {
        "signal_count": len(rets),
        "avg_return_1h_net": round(float(np.mean(rets)), 4),
        "win_rate_1h_pct": round(float((rets > 0).mean() * 100), 1),
        "total_return_pct": round(float(np.sum(rets)), 2),
        "sharpe_1h": round(
            float(np.mean(rets) / np.std(rets) * np.sqrt(8760)) if np.std(rets) > 0 else 0, 3
        ),
    }
