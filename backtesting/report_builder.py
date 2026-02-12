"""Assemble all backtesting results into report.txt + results.json."""
import os
import json
from datetime import datetime, timezone


def run(results, output_dir):
    """Build report.txt and results.json from all module results."""
    report_text = _build_report(results)
    report_path = os.path.join(output_dir, "report.txt")
    json_path = os.path.join(output_dir, "results.json")

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_text)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str, ensure_ascii=False)

    print(f"Report saved: {report_path}")
    print(f"JSON saved:   {json_path}")
    return report_path, json_path


def _build_report(results):
    """Build human-readable report text."""
    meta = results.get("metadata", {})
    lines = [
        "=" * 70,
        "BTC SIGNAL BACKTESTING REPORT",
        f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"Data: {meta.get('data_start', '?')[:10]} to {meta.get('data_end', '?')[:10]}",
        f"Signals: {meta.get('total_signals', '?'):,}  |  "
        f"Prices: {meta.get('total_prices', '?'):,}",
        f"Fee: {meta.get('fee_rate', 0.001)*100:.1f}% per side "
        f"({meta.get('fee_rate', 0.001)*200:.1f}% round-trip)",
        f"IS/OOS: {meta.get('is_oos_ratio', 0.7)*100:.0f}%/"
        f"{(1-meta.get('is_oos_ratio', 0.7))*100:.0f}%  |  "
        f"Split: {str(meta.get('split_timestamp', ''))[:10]}",
        "=" * 70, "",
    ]

    lines += _section_channel_stats(results.get("channel_stats", {}))
    lines += _section_walk_forward(results.get("optimal_params", {}))
    lines += _section_risk(results.get("risk_metrics", {}))
    lines += _section_mfe_mae(results.get("mfe_mae", {}))
    lines += _section_time(results.get("time_patterns", {}))
    lines += _section_regimes(results.get("market_regimes", {}))
    lines += _section_confluence(results.get("confluence", {}))
    lines += _section_correlations(results.get("correlations", {}))
    lines += _section_latency(results.get("latency_decay", {}))
    lines += _section_sequences(results.get("sequences", {}))
    lines += _section_monte_carlo(results.get("monte_carlo", {}))
    lines += _section_conclusions(results)

    return "\n".join(lines)


def _header(title, n=1):
    return [f"\n{'=' * 60}", f"  {n}. {title}", "=" * 60, ""]


def _section_channel_stats(data):
    if not data:
        return []
    lines = _header("CHANNEL PERFORMANCE (1h horizon, net of fees)", 1)
    fmt = "{:<20s} {:>6s} {:>7s} {:>8s} {:>6s} {:>7s} {:>7s}"
    lines.append(fmt.format("Channel", "Trades", "WinR%", "AvgRet%", "PF", "Sharpe", "Sortino"))
    lines.append("-" * 65)
    ranked = []
    for ch, stats in sorted(data.items()):
        h = stats.get("horizons", {}).get("1h", {})
        if not h:
            continue
        ranked.append((ch, h))
    ranked.sort(key=lambda x: x[1].get("sharpe_net", 0), reverse=True)
    for ch, h in ranked:
        lines.append(fmt.format(
            ch[:20], str(h.get("trades", 0)),
            f"{h.get('win_rate_net_pct', 0):.1f}",
            f"{h.get('avg_return_net_pct', 0):+.3f}",
            f"{h.get('profit_factor_net', 0):.2f}",
            f"{h.get('sharpe_net', 0):.3f}",
            f"{h.get('sortino_net', 0):.3f}",
        ))
    return lines


def _section_walk_forward(data):
    if not data:
        return []
    lines = _header("WALK-FORWARD VALIDATION (TP/SL optimization)", 2)
    fmt = "{:<18s} {:>5s}/{:>5s} {:>5s} {:>8s} {:>8s} {:>8s} {:>8s} {:>4s}"
    lines.append(fmt.format("Channel", "TP%", "SL%", "Thr", "IS_Sh", "OOS_Sh", "IS_Ret", "OOS_Ret", "Fit"))
    lines.append("-" * 80)
    for ch, info in sorted(data.items()):
        if info.get("skipped"):
            lines.append(f"  {ch}: SKIPPED ({info.get('reason', '')})")
            continue
        p = info.get("best_params_is", {})
        flag = "OVER" if info.get("overfitted") else "OK"
        lines.append(fmt.format(
            ch[:18],
            f"{p.get('tp_pct', 0):.1f}", f"{p.get('sl_pct', 0):.1f}",
            f"{p.get('threshold', '-') or '-'}",
            f"{info.get('is_sharpe', 0):.3f}",
            f"{info.get('oos_sharpe', 0):.3f}",
            f"{info.get('is_total_return_pct', 0):+.1f}",
            f"{info.get('oos_total_return_pct', 0):+.1f}",
            flag,
        ))
    return lines


def _section_risk(data):
    if not data:
        return []
    lines = _header("RISK METRICS", 3)
    for label in ["isolated", "portfolio"]:
        sec = data.get(label, {})
        if not sec or "error" in sec:
            continue
        lines.append(f"  [{label.upper()}]")
        lines.append(f"    Trades:       {sec.get('total_trades', 0)}")
        if "skipped_overlapping" in sec:
            lines.append(f"    Skipped:      {sec['skipped_overlapping']}")
        lines.append(f"    Cumul. Ret:   {sec.get('cumulative_return_net_pct', 0):+.2f}%")
        lines.append(f"    Max Drawdown: {sec.get('max_drawdown_pct', 0):.2f}%")
        lines.append(f"    Sharpe:       {sec.get('sharpe_1h', 0):.4f}")
        lines.append(f"    Sortino:      {sec.get('sortino_1h', 0):.4f}")
        lines.append(f"    Ulcer Index:  {sec.get('ulcer_index', 0):.4f}")
        lines.append(f"    Kelly:        {sec.get('kelly_pct', 0):.2f}%")
        if "calmar_ratio" in sec:
            lines.append(f"    Calmar:       {sec['calmar_ratio']:.3f}")
        lines.append("")
    return lines


def _section_mfe_mae(data):
    if not data:
        return []
    lines = _header("MFE / MAE ANALYSIS (1h hold)", 4)
    per_ch = data.get("per_channel", {})
    for ch in sorted(per_ch):
        h = per_ch[ch].get("1h", {})
        if not h:
            continue
        lines.append(f"  {ch}:")
        lines.append(f"    MFE avg/med: {h.get('avg_mfe_pct', 0):+.3f}% / {h.get('median_mfe_pct', 0):+.3f}%")
        lines.append(f"    MAE avg/med: {h.get('avg_mae_pct', 0):.3f}% / {h.get('median_mae_pct', 0):.3f}%")
        lines.append(f"    MFE/MAE ratio: {h.get('mfe_mae_ratio', 0):.2f}")
        lines.append(f"    Suggested TP: {h.get('suggested_tp_pct', 0):.2f}%  SL: {h.get('suggested_sl_pct', 0):.2f}%")
    return lines


def _section_time(data):
    if not data:
        return []
    lines = _header("TIME PATTERNS", 5)
    lines.append(f"  Best hour (UTC):    {data.get('best_hour', '?')}")
    lines.append(f"  Worst hour (UTC):   {data.get('worst_hour', '?')}")
    lines.append(f"  Best session:       {data.get('best_session', '?')}")
    lines.append(f"  Worst session:      {data.get('worst_session', '?')}")
    by_session = data.get("by_session", {})
    for s, info in sorted(by_session.items()):
        lines.append(f"    {s:8s}: {info.get('signal_count', 0):5d} sig, "
                     f"ret={info.get('avg_return_1h_net', 0):+.4f}%, "
                     f"WR={info.get('win_rate_1h_pct', 0):.1f}%")
    return lines


def _section_regimes(data):
    if not data:
        return []
    lines = _header("MARKET REGIMES", 6)
    for label in ["volatility_regimes", "trend_regimes"]:
        regimes = data.get(label, {})
        lines.append(f"  [{label.replace('_', ' ').upper()}]")
        for r, info in sorted(regimes.items()):
            lines.append(f"    {r:12s}: {info.get('signal_count', 0):5d} sig, "
                         f"ret={info.get('avg_return_1h_net', 0):+.4f}%, "
                         f"WR={info.get('win_rate_1h_pct', 0):.1f}%")
        lines.append("")
    return lines


def _section_confluence(data):
    if not data:
        return []
    lines = _header("MULTI-CHANNEL CONFLUENCE", 7)
    sm = data.get("single_vs_multi", {})
    lines.append(f"  Window: {data.get('window_minutes', 30)} min")
    lines.append(f"  Groups found: {data.get('total_groups', 0)}")
    lines.append(f"  Single avg ret: {sm.get('single_avg_return_1h', 0):+.4f}%")
    lines.append(f"  Multi avg ret:  {sm.get('multi_avg_return_1h', 0):+.4f}%")
    lines.append(f"  Improvement:    {sm.get('improvement_pct', 0):+.4f}%")
    by_cnt = data.get("stats_by_channel_count", {})
    for n, info in sorted(by_cnt.items()):
        lines.append(f"    {n} channels: {info.get('count', 0)} groups, "
                     f"ret={info.get('avg_return_1h_net', 0):+.4f}%, "
                     f"WR={info.get('win_rate_pct', 0):.1f}%")
    return lines


def _section_correlations(data):
    if not data:
        return []
    lines = _header("INTER-CHANNEL CORRELATIONS", 8)
    lines.append(f"  Diversification score: {data.get('diversification_score', 0):.3f}")
    mc = data.get("most_correlated_pair", {})
    lc = data.get("least_correlated_pair", {})
    if mc:
        lines.append(f"  Most correlated:  {mc.get('pair', '')} ({mc.get('correlation', 0):.3f})")
    if lc:
        lines.append(f"  Least correlated: {lc.get('pair', '')} ({lc.get('correlation', 0):.3f})")
    return lines


def _section_latency(data):
    if not data:
        return []
    lines = _header("SIGNAL LATENCY DECAY", 9)
    delays = data.get("delays", {})
    for d in sorted(delays, key=lambda x: int(x)):
        info = delays[d]
        lines.append(f"  +{d:2d}m: ret={info.get('avg_return_net', 0):+.4f}%, "
                     f"WR={info.get('win_rate_pct', 0):.1f}%, "
                     f"Sharpe={info.get('sharpe', 0):.3f}")
    lines.append(f"  Decay rate: {data.get('decay_rate_pct_per_minute', 0):.6f} %/min")
    hl = data.get("half_life_minutes")
    lines.append(f"  Half-life:  {hl} min" if hl else "  Half-life:  N/A")
    return lines


def _section_sequences(data):
    if not data:
        return []
    lines = _header("WIN/LOSS SEQUENCES", 10)
    ov = data.get("overall", {})
    lines.append(f"  Max win streak:  {ov.get('max_win_streak', 0)}")
    lines.append(f"  Max loss streak: {ov.get('max_loss_streak', 0)}")
    after = data.get("after_streak", {})
    for key, info in sorted(after.items()):
        if info and info.get("count", 0) > 0:
            lines.append(f"  {key}: WR={info.get('next_win_rate_pct', 0):.1f}% (n={info['count']})")
    serial = [ch for ch, v in data.get("per_channel", {}).items()
              if v.get("serial_correlation")]
    if serial:
        lines.append(f"  Serial correlation detected: {', '.join(serial)}")
    return lines


def _section_monte_carlo(data):
    if not data:
        return []
    lines = _header("MONTE CARLO SIGNIFICANCE", 11)
    lines.append(f"  Verdict: {data.get('overall_verdict', '?')}")
    lines.append("")
    dr = data.get("direction_shuffle", {})
    fmt = "  {:<18s} {:>7s} {:>7s} {:>7s} {:>3s}"
    lines.append(fmt.format("Channel", "Sharpe", "p-val", "z-score", "Sig"))
    lines.append("  " + "-" * 50)
    for ch in sorted(dr):
        info = dr[ch]
        sig = "**" if info.get("significant_1pct") else ("*" if info.get("significant_5pct") else "")
        lines.append(fmt.format(
            ch[:18],
            f"{info.get('actual_sharpe', 0):.3f}",
            f"{info.get('p_value', 1):.3f}",
            f"{info.get('z_score', 0):.2f}",
            sig,
        ))
    return lines


def _section_conclusions(results):
    lines = _header("CONCLUSIONS", 12)

    cs = results.get("channel_stats", {})
    best_ch = None
    best_sharpe = -999
    for ch, stats in cs.items():
        h = stats.get("horizons", {}).get("1h", {})
        sh = h.get("sharpe_net", 0)
        if sh > best_sharpe:
            best_sharpe = sh
            best_ch = ch
    if best_ch:
        lines.append(f"  Top channel (1h Sharpe): {best_ch} ({best_sharpe:.4f})")

    mc = results.get("monte_carlo", {})
    sig_channels = [ch for ch, v in mc.get("direction_shuffle", {}).items()
                    if v.get("significant_5pct")]
    lines.append(f"  Statistically significant: {', '.join(sig_channels) or 'NONE'}")

    op = results.get("optimal_params", {})
    overfit = [ch for ch, v in op.items() if v.get("overfitted")]
    if overfit:
        lines.append(f"  OVERFITTED channels: {', '.join(overfit)}")

    conf = results.get("confluence", {}).get("single_vs_multi", {})
    imp = conf.get("improvement_pct", 0)
    if imp > 0:
        lines.append(f"  Multi-channel confluence: +{imp:.4f}% improvement")
    else:
        lines.append(f"  Multi-channel confluence: no improvement ({imp:+.4f}%)")

    lines.append("")
    lines.append("=" * 70)
    lines.append("END OF REPORT")
    lines.append("=" * 70)
    return lines
