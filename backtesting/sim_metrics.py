"""Portfolio simulation metrics: Sharpe, Sortino, drawdown, Kelly, etc."""
import numpy as np

from backtesting.sim_engine import HORIZONS, INITIAL_CAPITAL


def compute_metrics(sim_result):
    """Compute full metrics for a single simulation run.

    sim_result: dict from sim_engine.simulate() with equity_curve and trade_log.
    Returns dict of metrics.
    """
    if sim_result.get("skipped") or not sim_result["trade_log"]:
        return {"trades": 0, "skipped": True}

    trades = sim_result["trade_log"]
    rets = np.array([t["net_return_pct"] for t in trades])
    pnls = np.array([t["pnl_usd"] for t in trades])
    capitals = np.array([t["capital_after"] for t in trades])

    hz = sim_result["horizon"]
    _, _, ann_factor = HORIZONS[hz]

    equity = np.array([INITIAL_CAPITAL] + [t["capital_after"] for t in trades])
    dd_pct, dd_dur = _max_drawdown(equity)
    final = equity[-1]
    cum_ret = (final / INITIAL_CAPITAL - 1) * 100

    # Per-channel breakdown
    ch_trades = {}
    for t in trades:
        ch = t["channel"]
        ch_trades.setdefault(ch, []).append(t["net_return_pct"])

    per_channel = {}
    for ch, ch_rets in ch_trades.items():
        r = np.array(ch_rets)
        per_channel[ch] = {
            "trades": len(r),
            "win_rate_pct": _win_rate(r),
            "avg_return_pct": round(float(np.mean(r)), 4),
            "total_pnl_usd": round(float(sum(
                t["pnl_usd"] for t in trades if t["channel"] == ch
            )), 2),
        }

    return {
        "trades": len(rets),
        "wins": int((rets > 0).sum()),
        "losses": int((rets <= 0).sum()),
        "win_rate_pct": _win_rate(rets),
        "avg_return_pct": round(float(np.mean(rets)), 4),
        "median_return_pct": round(float(np.median(rets)), 4),
        "total_return_pct": round(float(np.sum(rets)), 2),
        "cumulative_return_pct": round(cum_ret, 2),
        "final_capital_usd": round(final, 2),
        "total_pnl_usd": round(final - INITIAL_CAPITAL, 2),
        "profit_factor": _profit_factor(pnls),
        "sharpe": _sharpe(rets, ann_factor),
        "sortino": _sortino(rets, ann_factor),
        "max_drawdown_pct": round(dd_pct, 2),
        "max_drawdown_duration": int(dd_dur),
        "max_drawdown_usd": round(_max_drawdown_usd(equity), 2),
        "calmar": _calmar(cum_ret, dd_pct, len(rets), ann_factor),
        "kelly_pct": _kelly(rets),
        "per_channel": per_channel,
    }


def compute_all_metrics(sim_results):
    """Compute metrics for all simulation runs.

    sim_results: dict from sim_engine.run_all_simulations().
    Returns dict keyed the same way with metrics added.
    """
    out = {}
    for key, sim in sim_results.items():
        m = compute_metrics(sim)
        m["horizon"] = sim.get("horizon", "?")
        m["size_pct"] = sim.get("size_pct", 0)
        out[key] = m
    return out


def find_best_combo(metrics_dict):
    """Find the best horizon × size combo by Sharpe ratio."""
    best_key, best_sharpe = None, -999
    for key, m in metrics_dict.items():
        if m.get("skipped"):
            continue
        sh = m.get("sharpe", -999)
        if sh > best_sharpe:
            best_sharpe = sh
            best_key = key
    return best_key


# ---- Internal metric functions ----

def _max_drawdown(equity):
    """Max drawdown % and duration (trades) from equity array."""
    if len(equity) < 2:
        return 0.0, 0
    running_max = np.maximum.accumulate(equity)
    drawdowns = (equity - running_max) / running_max * 100
    max_dd = float(drawdowns.min())
    trough_idx = int(np.argmin(drawdowns))
    peak_idx = int(np.argmax(equity[:trough_idx + 1])) if trough_idx > 0 else 0
    return max_dd, trough_idx - peak_idx


def _max_drawdown_usd(equity):
    """Max drawdown in absolute USD."""
    if len(equity) < 2:
        return 0.0
    running_max = np.maximum.accumulate(equity)
    return float((running_max - equity).max())


def _win_rate(returns):
    if len(returns) == 0:
        return 0.0
    return round(float((returns > 0).sum() / len(returns) * 100), 2)


def _profit_factor(pnls):
    gains = float(pnls[pnls > 0].sum())
    losses = abs(float(pnls[pnls < 0].sum()))
    if losses == 0:
        return 99.0 if gains > 0 else 0.0
    return round(gains / losses, 3)


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


def _calmar(cum_ret_pct, max_dd_pct, n_trades, ann_factor):
    if max_dd_pct == 0 or n_trades == 0:
        return 0.0
    ann_ret = cum_ret_pct * (ann_factor / max(n_trades, 1))
    return round(ann_ret / abs(max_dd_pct), 3)


def _kelly(returns):
    wins = returns[returns > 0]
    losses = returns[returns < 0]
    if len(wins) == 0 or len(losses) == 0:
        return 0.0
    w = len(wins) / len(returns)
    r = np.mean(wins) / abs(np.mean(losses))
    if r == 0:
        return 0.0
    return round(float((w - (1 - w) / r) * 100), 2)
