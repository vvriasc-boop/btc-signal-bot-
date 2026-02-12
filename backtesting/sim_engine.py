"""Portfolio simulation engine: streak-filtered signals + position sizing."""
import numpy as np
import pandas as pd

# ---- Strategy config ----
STRATEGIES = {
    "DMI_SMF":    {"n_wins": 4, "m_losses": 1},
    "DyorAlerts": {"n_wins": 2, "m_losses": 1},
    "Scalp17":    {"n_wins": 5, "m_losses": 1},
}

HORIZONS = {
    "5m":  ("change_5m_pct",  1,  105120),
    "15m": ("change_15m_pct", 2,  35040),
    "1h":  ("change_1h_pct",  4,  8760),
    "4h":  ("change_4h_pct",  8,  2190),
}

POSITION_SIZES = [1, 2, 5, 10]       # % of capital
INITIAL_CAPITAL = 10_000.0
FEE_PCT = 0.2                        # 0.1% per side, round-trip
OOS_CUTOFF = "2025-11-05"


def _load_merged(df_signals, df_context, after_cutoff):
    """Merge signals + context for strategy channels, filter by OOS cutoff."""
    cutoff = pd.Timestamp(OOS_CUTOFF, tz="UTC")
    channels = list(STRATEGIES.keys())
    op = ">=" if after_cutoff else "<"
    mask = (df_signals["channel_name"].isin(channels)
            & (df_signals["timestamp"] >= cutoff if after_cutoff
               else df_signals["timestamp"] < cutoff))
    sigs = df_signals[mask].copy()
    merged = sigs.merge(
        df_context, left_on="id", right_on="signal_id",
        how="inner", suffixes=("", "_ctx"),
    )
    merged = merged[
        merged["derived_direction"].isin(["bullish", "bearish"])
    ].sort_values("timestamp").reset_index(drop=True)
    merged["dir_sign"] = merged["derived_direction"].map(
        {"bullish": 1.0, "bearish": -1.0}
    )
    return merged


def load_oos_data(df_signals, df_context):
    return _load_merged(df_signals, df_context, after_cutoff=True)


def load_preseed_data(df_signals, df_context):
    return _load_merged(df_signals, df_context, after_cutoff=False)


def _preseed_streak_state(df_is, horizon_col, mask_bit):
    """Run streak filter on IS data to get warm streak counters per channel."""
    states = {}
    for ch, cfg in STRATEGIES.items():
        ch_data = df_is[df_is["channel_name"] == ch].copy()
        valid = (
            ch_data[horizon_col].notna()
            & ((ch_data["filled_mask"].astype(int) & mask_bit) > 0)
        )
        ch_data = ch_data[valid].reset_index(drop=True)
        if len(ch_data) == 0:
            states[ch] = {"win_streak": 0, "loss_streak": 0, "active": False}
            continue

        rets = ch_data[horizon_col].values * ch_data["dir_sign"].values - FEE_PCT
        outcomes = (rets > 0).astype(int)

        win_streak, loss_streak, active = 0, 0, False
        n, m = cfg["n_wins"], cfg["m_losses"]
        for i in range(len(outcomes)):
            if not active and win_streak >= n:
                active, loss_streak = True, 0
            if active:
                if outcomes[i] == 0:
                    loss_streak += 1
                    if loss_streak >= m:
                        active, loss_streak, win_streak = False, 0, 0
                else:
                    loss_streak = 0
            win_streak = win_streak + 1 if outcomes[i] == 1 else 0

        states[ch] = {
            "win_streak": win_streak,
            "loss_streak": loss_streak,
            "active": active,
        }
    return states


def simulate(df_oos, df_is, horizon_name, size_pct):
    """Run portfolio simulation for one horizon + one position size.

    Returns dict with equity_curve (list of dicts) and trade_log (list of dicts).
    """
    col, mask_bit, _ = HORIZONS[horizon_name]

    # Filter valid signals for this horizon
    valid = (
        df_oos[col].notna()
        & ((df_oos["filled_mask"].astype(int) & mask_bit) > 0)
    )
    df = df_oos[valid].sort_values("timestamp").reset_index(drop=True)

    if len(df) == 0:
        return {"equity_curve": [], "trade_log": [], "skipped": True}

    # Preseed streak states from IS data
    states = _preseed_streak_state(df_is, col, mask_bit)

    # Simulation
    capital = INITIAL_CAPITAL
    equity_curve = [{"timestamp": df["timestamp"].iloc[0], "capital": capital}]
    trade_log = []

    # Per-channel streak tracking (continue from IS state)
    ch_state = {}
    for ch, st in states.items():
        ch_state[ch] = {
            "win_streak": st["win_streak"],
            "loss_streak": st["loss_streak"],
            "active": st["active"],
        }

    for _, row in df.iterrows():
        ch = row["channel_name"]
        cfg = STRATEGIES[ch]
        n, m = cfg["n_wins"], cfg["m_losses"]
        st = ch_state[ch]

        net_ret_pct = row[col] * row["dir_sign"] - FEE_PCT
        outcome = 1 if net_ret_pct > 0 else 0

        if not st["active"] and st["win_streak"] >= n:
            st["active"], st["loss_streak"] = True, 0

        if st["active"]:
            # Execute trade
            position = capital * size_pct / 100.0
            pnl = position * net_ret_pct / 100.0
            capital += pnl

            trade_log.append({
                "timestamp": row["timestamp"],
                "channel": ch,
                "direction": row["derived_direction"],
                "net_return_pct": round(net_ret_pct, 4),
                "position_usd": round(position, 2),
                "pnl_usd": round(pnl, 2),
                "capital_after": round(capital, 2),
            })

            equity_curve.append({
                "timestamp": row["timestamp"],
                "capital": round(capital, 2),
            })

            # Update loss streak
            if outcome == 0:
                st["loss_streak"] += 1
                if st["loss_streak"] >= m:
                    st["active"] = False
                    st["loss_streak"] = 0
                    st["win_streak"] = 0
            else:
                st["loss_streak"] = 0

        # Always update win_streak
        st["win_streak"] = st["win_streak"] + 1 if outcome == 1 else 0

    return {
        "equity_curve": equity_curve,
        "trade_log": trade_log,
        "skipped": False,
    }


def run_all_simulations(df_signals, df_context):
    """Run simulations across all horizons × position sizes.

    Returns dict keyed by (horizon, size_pct) with simulation results.
    """
    df_oos = load_oos_data(df_signals, df_context)
    df_is = load_preseed_data(df_signals, df_context)

    results = {}
    for hz in HORIZONS:
        for sz in POSITION_SIZES:
            key = f"{hz}_size{sz}pct"
            results[key] = simulate(df_oos, df_is, hz, sz)
            results[key]["horizon"] = hz
            results[key]["size_pct"] = sz

    return results, df_oos, df_is
