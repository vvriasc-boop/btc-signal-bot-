"""Risk metrics: drawdowns, Sortino, Ulcer, Kelly, portfolio simulation."""
import numpy as np
import pandas as pd

HOLD_MINUTES = 60  # 1h default hold for portfolio simulation


def run(df_signals, df_prices, df_context, fee_rate=0.001):
    fee_pct = fee_rate * 2 * 100
    merged = _merge_directional(df_signals, df_context)
    if len(merged) == 0:
        return {"error": "no directional signals"}

    isolated = _isolated_stats(merged, fee_pct)
    portfolio = _portfolio_simulation(merged, fee_pct, HOLD_MINUTES)
    per_ch_dd = _per_channel_drawdown(merged, fee_pct)

    return {
        "isolated": isolated,
        "portfolio": portfolio,
        "per_channel_drawdown": per_ch_dd,
    }


def _merge_directional(df_signals, df_context):
    """Merge signals with context, keep only directional with 1h data."""
    merged = df_signals.merge(
        df_context, left_on="id", right_on="signal_id", how="inner",
        suffixes=("", "_ctx"),
    )
    mask = (
        merged["derived_direction"].isin(["bullish", "bearish"])
        & merged["change_1h_pct"].notna()
        & ((merged["filled_mask"].astype(int) & 4) > 0)
    )
    return merged[mask].sort_values("timestamp").reset_index(drop=True)


def _compute_returns(df, fee_pct):
    """Compute directional net returns for 1h horizon."""
    sign = df["derived_direction"].map({"bullish": 1.0, "bearish": -1.0}).values
    gross = df["change_1h_pct"].values * sign
    return gross - fee_pct


def _isolated_stats(merged, fee_pct):
    """Stats treating every signal independently."""
    net = _compute_returns(merged, fee_pct)
    equity = np.cumprod(1 + net / 100)
    dd, dd_dur = _max_drawdown(equity)
    return {
        "total_trades": len(net),
        "cumulative_return_net_pct": round(float((equity[-1] - 1) * 100), 2),
        "max_drawdown_pct": round(dd, 4),
        "max_drawdown_duration_trades": int(dd_dur),
        "sharpe_1h": _sharpe(net, 8760),
        "sortino_1h": _sortino(net, 8760),
        "ulcer_index": _ulcer_index(equity),
        "kelly_pct": _kelly(net),
    }


def _portfolio_simulation(merged, fee_pct, hold_minutes):
    """Simulate portfolio: max 1 position per channel at a time."""
    open_until = {}  # channel_name -> expiry timestamp
    trades = []
    skipped = 0

    for _, row in merged.iterrows():
        ch = row["channel_name"]
        ts = row["timestamp"]
        if ch in open_until and ts < open_until[ch]:
            skipped += 1
            continue
        open_until[ch] = ts + pd.Timedelta(minutes=hold_minutes)
        sign = 1.0 if row["derived_direction"] == "bullish" else -1.0
        net_ret = row["change_1h_pct"] * sign - fee_pct
        trades.append(net_ret)

    if not trades:
        return {"error": "no trades"}
    trades_arr = np.array(trades)
    equity = np.cumprod(1 + trades_arr / 100)
    dd, dd_dur = _max_drawdown(equity)
    cum_ret = (equity[-1] - 1) * 100
    ann_ret = ((equity[-1]) ** (365 * 24 / max(len(trades), 1)) - 1) * 100

    return {
        "total_trades": len(trades),
        "skipped_overlapping": skipped,
        "cumulative_return_net_pct": round(float(cum_ret), 2),
        "max_drawdown_pct": round(dd, 4),
        "max_drawdown_duration_trades": int(dd_dur),
        "calmar_ratio": round(float(ann_ret / abs(dd)), 3) if dd != 0 else 0,
        "sharpe_1h": _sharpe(trades_arr, 8760),
        "sortino_1h": _sortino(trades_arr, 8760),
        "ulcer_index": _ulcer_index(equity),
        "kelly_pct": _kelly(trades_arr),
    }


def _per_channel_drawdown(merged, fee_pct):
    """Max drawdown per channel."""
    result = {}
    for ch, grp in merged.groupby("channel_name"):
        net = _compute_returns(grp, fee_pct)
        if len(net) < 2:
            continue
        equity = np.cumprod(1 + net / 100)
        dd, _ = _max_drawdown(equity)
        result[ch] = {"trades": len(net), "max_drawdown_pct": round(dd, 4)}
    return result


def _max_drawdown(equity):
    """Max drawdown % and duration (in periods) from equity curve."""
    running_max = np.maximum.accumulate(equity)
    drawdowns = (equity - running_max) / running_max * 100
    max_dd = float(drawdowns.min())
    peak_idx = np.argmax(equity[:np.argmin(drawdowns) + 1])
    trough_idx = np.argmin(drawdowns)
    return max_dd, trough_idx - peak_idx


def _ulcer_index(equity):
    """Ulcer Index = sqrt(mean(drawdown^2))."""
    running_max = np.maximum.accumulate(equity)
    dd_pct = (equity - running_max) / running_max * 100
    return round(float(np.sqrt(np.mean(dd_pct ** 2))), 4)


def _kelly(returns):
    """Kelly criterion: W - (1-W)/R."""
    wins = returns[returns > 0]
    losses = returns[returns < 0]
    if len(wins) == 0 or len(losses) == 0:
        return 0.0
    w = len(wins) / len(returns)
    r = np.mean(wins) / abs(np.mean(losses))
    if r == 0:
        return 0.0
    kelly = w - (1 - w) / r
    return round(float(kelly * 100), 2)


def _sharpe(returns, ann_factor):
    if len(returns) < 2 or np.std(returns) == 0:
        return 0.0
    return round(float(np.mean(returns) / np.std(returns) * np.sqrt(ann_factor)), 4)


def _sortino(returns, ann_factor):
    if len(returns) < 2:
        return 0.0
    downside = returns[returns < 0]
    if len(downside) == 0:
        return 0.0
    dd = np.sqrt(np.mean(downside ** 2))
    if dd == 0:
        return 0.0
    return round(float(np.mean(returns) / dd * np.sqrt(ann_factor)), 4)
