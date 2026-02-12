import asyncio
import logging
from datetime import datetime, timezone

import config

logger = logging.getLogger("btc_signal_bot")


async def fetch_btc_price() -> float | None:
    """Current BTC/USDT price from Binance."""
    try:
        resp = await config.http_client.get(
            "https://api.binance.com/api/v3/ticker/price",
            params={"symbol": "BTCUSDT"}
        )
        resp.raise_for_status()
        return float(resp.json()["price"])
    except Exception as e:
        logger.error(f"Binance price error: {e}")
        return None


async def fetch_btc_price_history(start: datetime, end: datetime) -> list:
    """Download 1-min BTC klines for a period, in batches of 1000."""
    all_klines = []
    current_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    consecutive_errors = 0

    while current_ms < end_ms:
        try:
            resp = await config.http_client.get(
                "https://api.binance.com/api/v3/klines",
                params={"symbol": "BTCUSDT", "interval": "1m",
                        "startTime": current_ms, "limit": 1000}
            )
            resp.raise_for_status()
            data = resp.json()
            if not data:
                break
            all_klines.extend(data)
            current_ms = data[-1][0] + 60000
            consecutive_errors = 0
            await asyncio.sleep(0.3)
        except Exception as e:
            consecutive_errors += 1
            logger.error(f"Binance history (attempt {consecutive_errors}): {e}")
            if consecutive_errors >= 5:
                logger.error(f"Binance history: {consecutive_errors} errors, skipping chunk")
                current_ms += 1000 * 60000
                consecutive_errors = 0
            await asyncio.sleep(2)
            continue
    return all_klines
