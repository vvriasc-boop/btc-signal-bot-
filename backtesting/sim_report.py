"""Portfolio simulation report: txt report, equity CSV, results JSON."""
import os
import json
import csv
from datetime import datetime, timezone

from backtesting.sim_engine import (
    STRATEGIES, HORIZONS, POSITION_SIZES, INITIAL_CAPITAL, FEE_PCT, OOS_CUTOFF,
)
from backtesting.sim_metrics import find_best_combo

OUTPUT_DIR = os.path.dirname(__file__)


def write_all(sim_results, metrics, output_dir=None):
    """Write portfolio_report.txt, equity_curve.csv, portfolio_results.json."""
    d = output_dir or OUTPUT_DIR
    _write_report(metrics, os.path.join(d, "portfolio_report.txt"))
    _write_equity_csv(sim_results, os.path.join(d, "equity_curve.csv"))
    _write_json(metrics, os.path.join(d, "portfolio_results.json"))


def _write_report(metrics, path):
    lines = _build_report(metrics)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def _write_equity_csv(sim_results, path):
    """Write equity curves for all combos into one CSV."""
    rows = []
    for key, sim in sim_results.items():
        if sim.get("skipped") or not sim.get("equity_curve"):
            continue
        hz = sim["horizon"]
        sz = sim["size_pct"]
        for pt in sim["equity_curve"]:
            rows.append({
                "timestamp": str(pt["timestamp"])[:19],
                "horizon": hz,
                "size_pct": sz,
                "capital": pt["capital"],
            })

    rows.sort(key=lambda r: (r["horizon"], r["size_pct"], r["timestamp"]))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["timestamp", "horizon", "size_pct", "capital"])
        w.writeheader()
        w.writerows(rows)


def _write_json(metrics, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, default=str, ensure_ascii=False)


def _build_report(metrics):
    L = [
        "=" * 70,
        "PORTFOLIO SIMULATION REPORT (Streak-Filtered)",
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"OOS period: {OOS_CUTOFF} onwards",
        f"Initial capital: ${INITIAL_CAPITAL:,.0f}",
        f"Fee: {FEE_PCT:.1f}% round-trip",
        "=" * 70,
        "",
        "Strategies:",
    ]
    for ch, cfg in STRATEGIES.items():
        L.append(f"  {ch:15s}  N={cfg['n_wins']} wins to enter, "
                 f"M={cfg['m_losses']} losses to stop")
    L += ["", f"Position sizes: {POSITION_SIZES}", f"Horizons: {list(HORIZONS.keys())}"]

    # Best combo
    best_key = find_best_combo(metrics)
    if best_key:
        bm = metrics[best_key]
        L += [
            "",
            "-" * 70,
            f"  BEST COMBO (by Sharpe): {best_key}",
            f"    Sharpe={bm['sharpe']:.4f}  "
            f"Return={bm['cumulative_return_pct']:+.2f}%  "
            f"DD={bm['max_drawdown_pct']:.2f}%  "
            f"Trades={bm['trades']}",
            "-" * 70,
        ]

    # Summary table by horizon
    for hz in HORIZONS:
        L += _section_horizon(metrics, hz)

    # Per-channel breakdown for 1h horizon
    L += _section_channel_breakdown(metrics)

    L += ["", "=" * 70, "END OF PORTFOLIO SIMULATION REPORT", "=" * 70]
    return L


def _section_horizon(metrics, hz):
    """One section per horizon with all position sizes."""
    L = ["", "=" * 60, f"  HORIZON: {hz}", "=" * 60, ""]
    fmt = "{:>6s}  {:>6s} {:>7s} {:>9s} {:>10s} {:>9s} {:>8s} {:>7s} {:>7s}"
    L.append(fmt.format(
        "Size%", "Trades", "WR%", "AvgRet%", "CumRet%",
        "Final$", "MaxDD%", "Sharpe", "Sortino",
    ))
    L.append("-" * 78)

    for sz in POSITION_SIZES:
        key = f"{hz}_size{sz}pct"
        m = metrics.get(key, {})
        if m.get("skipped") or m.get("trades", 0) == 0:
            L.append(f"  {sz}%: no trades")
            continue
        L.append(fmt.format(
            f"{sz}%",
            str(m["trades"]),
            f"{m['win_rate_pct']:.1f}",
            f"{m['avg_return_pct']:+.4f}",
            f"{m['cumulative_return_pct']:+.2f}",
            f"${m['final_capital_usd']:,.0f}",
            f"{m['max_drawdown_pct']:.2f}",
            f"{m['sharpe']:.4f}",
            f"{m['sortino']:.4f}",
        ))

    # Extra details for each size
    for sz in POSITION_SIZES:
        key = f"{hz}_size{sz}pct"
        m = metrics.get(key, {})
        if m.get("skipped") or m.get("trades", 0) == 0:
            continue
        L += [
            "",
            f"  [{hz} / {sz}% position]",
            f"    PnL: ${m['total_pnl_usd']:+,.2f}  |  "
            f"PF: {m['profit_factor']:.3f}  |  "
            f"Calmar: {m['calmar']:.3f}  |  "
            f"Kelly: {m['kelly_pct']:.2f}%",
            f"    Max DD USD: ${m['max_drawdown_usd']:,.2f}  |  "
            f"DD duration: {m['max_drawdown_duration']} trades",
            f"    W/L: {m['wins']}/{m['losses']}  |  "
            f"Median ret: {m['median_return_pct']:+.4f}%",
        ]
    return L


def _section_channel_breakdown(metrics):
    """Per-channel stats for the 1h horizon across sizes."""
    L = ["", "=" * 60, "  PER-CHANNEL BREAKDOWN (1h horizon)", "=" * 60]

    for sz in POSITION_SIZES:
        key = f"1h_size{sz}pct"
        m = metrics.get(key, {})
        if m.get("skipped") or not m.get("per_channel"):
            continue
        L += ["", f"  [Position size: {sz}%]"]
        fmt = "    {:<15s} {:>6s} {:>7s} {:>9s} {:>10s}"
        L.append(fmt.format("Channel", "Trades", "WR%", "AvgRet%", "PnL$"))
        L.append("    " + "-" * 52)
        for ch in sorted(m["per_channel"]):
            c = m["per_channel"][ch]
            L.append(fmt.format(
                ch[:15],
                str(c["trades"]),
                f"{c['win_rate_pct']:.1f}",
                f"{c['avg_return_pct']:+.4f}",
                f"${c['total_pnl_usd']:+,.2f}",
            ))
    return L
