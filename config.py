import os
import logging
from zoneinfo import ZoneInfo

import httpx
from dotenv import load_dotenv
from pyrogram import Client

load_dotenv()

# Logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("btc_signal_bot")

# Constants
MADRID = ZoneInfo("Europe/Madrid")
UNRECOGNIZED_DIR = "unrecognized"
MASK_5M, MASK_15M, MASK_1H, MASK_4H, MASK_24H = 1, 2, 4, 8, 16
MASK_ALL = 31
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

# Validation ranges
VALIDATION_RULES = {
    "altswing":      {"value_min": -100, "value_max": 100},
    "scalp17":       {"value_min": -200, "value_max": 200},
    "altspi":        {"value_min": -100, "value_max": 200},
    "sells_power":   {"value_min": -300, "value_max": 300},
    "dmi_smf":       {"value_min": -300, "value_max": 300},
    "rsi_btc":       {"value_min": 0, "value_max": 100},
    "diamond_marks": {},
    "index_btc":     {},
    "dyor_alerts":   {"value_min": 0, "value_max": 1000},
}

# Pyrogram userbot
userbot = Client(
    "session",
    api_id=int(os.getenv("API_ID", "0")),
    api_hash=os.getenv("API_HASH", ""),
    phone_number=os.getenv("PHONE", "")
)

# Mutable global state (set during startup)
http_client: httpx.AsyncClient | None = None
db = None  # sqlite3.Connection, set by init_database()
price_index: dict = {}
RESOLVED_CHANNELS: dict = {}


def build_channel_config() -> dict:
    """Build channel config from .env. Call AFTER load_dotenv()."""
    cfg = {}
    for env_key, name, parser in [
        ("CHANNEL_1", "AltSwing", "altswing"),
        ("CHANNEL_2", "DiamondMarks", "diamond_marks"),
        ("CHANNEL_3", "SellsPowerIndex", "sells_power"),
        ("CHANNEL_4", "AltSPI", "altspi"),
        ("CHANNEL_5", "Scalp17", "scalp17"),
        ("CHANNEL_6", "Index", "index_btc"),
        ("CHANNEL_7", "DMI_SMF", "dmi_smf"),
    ]:
        val = os.getenv(env_key)
        if val:
            cfg[val] = {"name": name, "parser": parser}

    imba = os.getenv("IMBA_GROUP_ID")
    if imba:
        cfg[imba] = {
            "name": "DyorAlerts", "parser": "dyor_alerts",
            "is_group": True, "filter_author": "dyor_alerts_EtH_2_O_bot",
        }

    bfs = os.getenv("BFS_GROUP_ID")
    if bfs:
        cfg[bfs] = {
            "name": "RSI_BTC", "parser": "rsi_btc",
            "is_group": True,
            "topic_id": int(os.getenv("BFS_BTC_TOPIC_ID", "0")),
        }

    return cfg


async def init_http():
    global http_client
    http_client = httpx.AsyncClient(
        timeout=10.0, headers={"User-Agent": "BTC-Signal-Bot/1.0"}
    )


async def close_http():
    global http_client
    if http_client:
        await http_client.aclose()
        http_client = None
