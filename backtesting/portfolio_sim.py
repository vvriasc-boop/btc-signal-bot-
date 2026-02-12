"""Portfolio simulation entry point.

Usage:
    python3 -m backtesting.portfolio_sim
"""
import logging
import time

from backtesting.analyze import load_data, derive_directions
from backtesting.sim_engine import run_all_simulations
from backtesting.sim_metrics import compute_all_metrics
from backtesting.sim_report import write_all

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("portfolio_sim")


def main():
    t0 = time.time()

    logger.info("Loading data...")
    df_signals, df_prices, df_context = load_data()
    df_signals = derive_directions(df_signals)

    logger.info("Running simulations (4 horizons × 4 sizes)...")
    sim_results, _, _ = run_all_simulations(df_signals, df_context)

    logger.info("Computing metrics...")
    metrics = compute_all_metrics(sim_results)

    logger.info("Writing outputs...")
    write_all(sim_results, metrics)

    logger.info(f"Done in {time.time() - t0:.1f}s")
    logger.info("Files: portfolio_report.txt, equity_curve.csv, portfolio_results.json")


if __name__ == "__main__":
    main()
