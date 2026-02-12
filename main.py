import os
import re
import json
import csv
import sqlite3
import asyncio
import logging
import signal as sig_mod
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import httpx
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler
from pyrogram.errors import FloodWait
from telegram.ext import Application, CommandHandler, CallbackQueryHandler
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

# ═══ ПОРЯДОК ИНИЦИАЛИЗАЦИИ ═══
# 1. load_dotenv() — ПЕРВЫМ ДЕЛОМ
# 2. Потом os.getenv() для конфигурации
# 3. Потом создание Client/Application

load_dotenv()

# Логирование
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Константы
MADRID = ZoneInfo("Europe/Madrid")
UNRECOGNIZED_DIR = "unrecognized"
MASK_5M, MASK_15M, MASK_1H, MASK_4H, MASK_24H = 1, 2, 4, 8, 16
MASK_ALL = 31
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

# ═══ Pyrogram userbot ═══
userbot = Client(
    "session",
    api_id=int(os.getenv("API_ID", "0")),
    api_hash=os.getenv("API_HASH", ""),
    phone_number=os.getenv("PHONE", "")
)

# ═══ HTTP клиент (один на всё) ═══
http_client: httpx.AsyncClient = None

# ═══ SQLite (глобальная переменная) ═══
db: sqlite3.Connection = None

# ═══ Price index (глобальный, строится после Phase 0) ═══
price_index: dict = {}

# ═══ Resolved channels (заполняется при старте) ═══
RESOLVED_CHANNELS: dict = {}


# ═══════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ КАНАЛОВ
# ═══════════════════════════════════════════════════════════════

def build_channel_config() -> dict:
    """Строит конфигурацию каналов из .env. Вызывать ПОСЛЕ load_dotenv()."""
    config = {}

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
            config[val] = {"name": name, "parser": parser}

    imba = os.getenv("IMBA_GROUP_ID")
    if imba:
        config[imba] = {"name": "DyorAlerts", "parser": "dyor_alerts",
                        "is_group": True, "filter_author": "dyor_alerts"}

    bfs = os.getenv("BFS_GROUP_ID")
    if bfs:
        config[bfs] = {"name": "RSI_BTC", "parser": "rsi_btc",
                       "is_group": True,
                       "topic_id": int(os.getenv("BFS_BTC_TOPIC_ID", "0"))}

    return config


# ═══════════════════════════════════════════════════════════════
# БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════════════

def init_database() -> sqlite3.Connection:
    conn = sqlite3.connect('btc_signals.db')
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL UNIQUE,
            name TEXT NOT NULL,
            parser_type TEXT NOT NULL,
            description TEXT,
            message_count INTEGER DEFAULT 0,
            last_message_at TEXT,
            is_active BOOLEAN DEFAULT 1,
            added_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS btc_price (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL UNIQUE,
            price REAL NOT NULL,
            volume REAL,
            source TEXT DEFAULT 'binance_kline'
        );

        CREATE TABLE IF NOT EXISTS raw_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            channel_name TEXT NOT NULL,
            message_id INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            text TEXT,
            has_text BOOLEAN DEFAULT 1,
            from_username TEXT,
            reply_to_topic_id INTEGER,
            is_parsed BOOLEAN DEFAULT NULL,
            parse_error TEXT,
            UNIQUE(channel_id, message_id)
        );

        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            channel_name TEXT NOT NULL,
            message_id INTEGER NOT NULL,
            message_text TEXT,
            timestamp TEXT NOT NULL,
            indicator_value REAL,
            signal_color TEXT,
            signal_direction TEXT,
            timeframe TEXT,
            btc_price_from_channel REAL,
            btc_price_binance REAL,
            extra_data TEXT,
            UNIQUE(channel_id, message_id)
        );

        CREATE TABLE IF NOT EXISTS signal_price_context (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id INTEGER NOT NULL UNIQUE,
            channel_name TEXT NOT NULL,
            signal_timestamp TEXT NOT NULL,
            price_at_signal REAL,
            price_5m_before REAL,
            price_15m_before REAL,
            price_1h_before REAL,
            price_5m_after REAL,
            price_15m_after REAL,
            price_1h_after REAL,
            price_4h_after REAL,
            price_24h_after REAL,
            change_5m_pct REAL,
            change_15m_pct REAL,
            change_1h_pct REAL,
            change_4h_pct REAL,
            change_24h_pct REAL,
            filled_mask INTEGER DEFAULT 0,
            FOREIGN KEY (signal_id) REFERENCES signals(id)
        );

        CREATE TABLE IF NOT EXISTS sync_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_name TEXT NOT NULL,
            phase TEXT NOT NULL,
            total_messages INTEGER DEFAULT 0,
            parsed_ok INTEGER DEFAULT 0,
            parsed_fail INTEGER DEFAULT 0,
            skipped_media INTEGER DEFAULT 0,
            skipped_filter INTEGER DEFAULT 0,
            earliest_message TEXT,
            latest_message TEXT,
            started_at TEXT,
            completed_at TEXT,
            notes TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_price_ts ON btc_price(timestamp);
        CREATE INDEX IF NOT EXISTS idx_sig_ts ON signals(timestamp);
        CREATE INDEX IF NOT EXISTS idx_sig_ch ON signals(channel_id, timestamp);
        CREATE INDEX IF NOT EXISTS idx_sig_dir ON signals(signal_direction, timestamp);
        CREATE INDEX IF NOT EXISTS idx_ctx_mask ON signal_price_context(filled_mask);
        CREATE INDEX IF NOT EXISTS idx_ctx_ts ON signal_price_context(signal_timestamp);
        CREATE INDEX IF NOT EXISTS idx_raw_ch ON raw_messages(channel_id, message_id);
        CREATE INDEX IF NOT EXISTS idx_raw_parsed ON raw_messages(is_parsed, channel_name);
    """)
    return conn


# ═══════════════════════════════════════════════════════════════
# HTTP КЛИЕНТ
# ═══════════════════════════════════════════════════════════════

async def init_http():
    global http_client
    http_client = httpx.AsyncClient(timeout=10.0, headers={"User-Agent": "BTC-Signal-Bot/1.0"})


async def close_http():
    global http_client
    if http_client:
        await http_client.aclose()
        http_client = None


# ═══════════════════════════════════════════════════════════════
# ADMIN MESSAGE — через Pyrogram (работает ДО запуска бота!)
# ═══════════════════════════════════════════════════════════════

async def send_admin_message(text: str):
    """Отправляет через Pyrogram. Работает на любом этапе."""
    if not ADMIN_USER_ID:
        logger.warning("ADMIN_USER_ID не задан!")
        return
    try:
        for i in range(0, len(text), 4000):
            await userbot.send_message(ADMIN_USER_ID, text[i:i + 4000])
            await asyncio.sleep(0.5)
    except Exception as e:
        logger.error(f"Admin msg error: {e}")


# ═══════════════════════════════════════════════════════════════
# TIMEZONE DISPLAY — Madrid (только для вывода в Telegram)
# ═══════════════════════════════════════════════════════════════

def fmt_madrid(iso_str: str) -> str:
    """UTC ISO-строка -> строка в Madrid timezone."""
    dt = datetime.fromisoformat(iso_str).replace(tzinfo=timezone.utc)
    return dt.astimezone(MADRID).strftime('%d.%m %H:%M')


# ═══════════════════════════════════════════════════════════════
# RESOLVE CHANNELS
# ═══════════════════════════════════════════════════════════════

async def resolve_channel_ids(channel_config: dict) -> dict:
    """Резолвит @username -> числовой chat_id."""
    resolved = {}
    for key, config in channel_config.items():
        try:
            if key.lstrip('-').isdigit():
                chat_id = int(key)
            else:
                chat = await userbot.get_chat(key)
                chat_id = chat.id
                logger.info(f"Resolved {key} -> {chat_id}")
            resolved[chat_id] = config

            db.execute("""
                INSERT OR IGNORE INTO channels (channel_id, name, parser_type)
                VALUES (?, ?, ?)
            """, (chat_id, config["name"], config["parser"]))
        except Exception as e:
            logger.error(f"Не удалось резолвить {key}: {e}")
    db.commit()
    return resolved


# ═══════════════════════════════════════════════════════════════
# BINANCE API
# ═══════════════════════════════════════════════════════════════

async def fetch_btc_price() -> float | None:
    """Текущая цена BTC/USDT."""
    try:
        resp = await http_client.get(
            "https://api.binance.com/api/v3/ticker/price",
            params={"symbol": "BTCUSDT"}
        )
        resp.raise_for_status()
        return float(resp.json()["price"])
    except Exception as e:
        logger.error(f"Binance price error: {e}")
        return None


async def fetch_btc_price_history(start: datetime, end: datetime) -> list:
    """Скачать 1-мин свечи BTC за период, пакетами по 1000."""
    all_klines = []
    current_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    consecutive_errors = 0

    while current_ms < end_ms:
        try:
            resp = await http_client.get(
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
                logger.error(f"Binance history: {consecutive_errors} consecutive errors, skipping chunk")
                current_ms += 1000 * 60000
                consecutive_errors = 0
            await asyncio.sleep(2)
            continue
    return all_klines


# ═══════════════════════════════════════════════════════════════
# PRICE INDEX (O(1) LOOKUP)
# ═══════════════════════════════════════════════════════════════

def build_price_index() -> dict:
    """Строит {minute_key: price} для O(1) поиска. ~15 МБ на 90 дней."""
    rows = db.execute("SELECT timestamp, price FROM btc_price ORDER BY timestamp").fetchall()
    idx = {}
    for row in rows:
        idx[row["timestamp"][:16]] = row["price"]  # "2026-02-12T14:30"
    logger.info(f"Price index: {len(idx)} точек в RAM")
    return idx


def get_price_fast(target: datetime, tolerance_minutes: int = 2) -> float | None:
    """O(1) поиск цены из глобального price_index."""
    for offset in range(tolerance_minutes + 1):
        for delta in ([timedelta(0)] if offset == 0 else
                      [timedelta(minutes=offset), timedelta(minutes=-offset)]):
            key = (target + delta).strftime("%Y-%m-%dT%H:%M")
            if key in price_index:
                return price_index[key]
    return None


def get_closest_price_sql(target_time: datetime, tolerance_minutes: int = 2) -> float | None:
    """SQL-поиск для единичных запросов (live mode fallback)."""
    ts = target_time.strftime("%Y-%m-%dT%H:%M:%S")
    lo = (target_time - timedelta(minutes=tolerance_minutes)).strftime("%Y-%m-%dT%H:%M:%S")
    hi = (target_time + timedelta(minutes=tolerance_minutes)).strftime("%Y-%m-%dT%H:%M:%S")
    row = db.execute("""
        SELECT price FROM btc_price WHERE timestamp BETWEEN ? AND ?
        ORDER BY ABS(julianday(timestamp) - julianday(?)) LIMIT 1
    """, (lo, hi, ts)).fetchone()
    return row["price"] if row else None


# ═══════════════════════════════════════════════════════════════
# ВАЛИДАЦИЯ
# ═══════════════════════════════════════════════════════════════

VALIDATION_RULES = {
    "altswing":     {"value_min": 0, "value_max": 100},
    "scalp17":      {"value_min": 0, "value_max": 100},
    "altspi":       {"value_min": 0, "value_max": 100},
    "sells_power":  {"value_min": -300, "value_max": 300},
    "dmi_smf":      {"value_min": -300, "value_max": 300},
    "rsi_btc":      {"value_min": 0, "value_max": 100},
    "diamond_marks": {},
    "index_btc":    {},
    "dyor_alerts":  {"value_min": 0, "value_max": 1000},
}


def validate_parsed(parser_type: str, parsed: dict) -> tuple[bool, str]:
    rules = VALIDATION_RULES.get(parser_type, {})
    value = parsed.get("value")
    if value is not None and "value_min" in rules:
        if value < rules["value_min"] or value > rules["value_max"]:
            return False, f"value {value} out of [{rules['value_min']},{rules['value_max']}]"
    btc = parsed.get("btc_price")
    if btc is not None and (btc < 1000 or btc > 500000):
        return False, f"btc_price {btc} suspicious"
    return True, "ok"


# ═══════════════════════════════════════════════════════════════
# ПАРСЕРЫ (9 каналов)
# ═══════════════════════════════════════════════════════════════

def parse_altswing(text):
    if 'AltSwing' not in text:
        return None
    m = re.search(r'Avg\.\s*([\d.]+)%', text)
    if not m:
        return None
    return {"value": float(m.group(1)), "color": None, "direction": None,
            "timeframe": None, "btc_price": None, "extra": {}}


def parse_diamond_marks(text):
    if 'Diamond Marks' not in text:
        return None
    tf = re.search(r'Total\s+(\d+[mhHМ])', text)
    price = re.search(r'BTC/USDT:\s*\$?([\d,]+\.?\d*)', text)
    g, o, r_ = text.count('\U0001f7e9'), text.count('\U0001f7e7'), text.count('\U0001f7e5')
    direction = "bullish" if g > r_ else ("bearish" if r_ > g else "neutral")
    colors = {"green": g, "orange": o, "red": r_}
    dominant = max(colors, key=colors.get) if any(colors.values()) else None
    return {"value": None, "color": dominant, "direction": direction,
            "timeframe": tf.group(1).lower() if tf else None,
            "btc_price": float(price.group(1).replace(',', '')) if price else None,
            "extra": {"green_count": g, "orange_count": o, "red_count": r_,
                      "has_fire": '\U0001f525' in text}}


def parse_sells_power(text):
    if 'Sells Power Index' not in text:
        return None
    m = re.search(r'(-?[\d.]+)%', text)
    if not m:
        return None
    color = "green" if '\U0001f7e9' in text else ("blue" if '\U0001f7e6' in text else None)
    return {"value": float(m.group(1)), "color": color, "direction": None,
            "timeframe": None, "btc_price": None, "extra": {}}


def parse_altspi(text):
    if 'AltSPI' not in text:
        return None
    avg = re.search(r'Avg\.\s*([\d.]+)%', text)
    if not avg:
        return None

    def cnt(e):
        m = re.search(e + r'\s*(\d+)', text)
        return int(m.group(1)) if m else 0

    return {"value": float(avg.group(1)), "color": None, "direction": None,
            "timeframe": None, "btc_price": None,
            "extra": {"red": cnt('\U0001f7e5'), "orange": cnt('\U0001f7e7'),
                      "white": cnt('\u26aa'), "blue": cnt('\U0001f7e6'),
                      "green": cnt('\U0001f7e9')}}


def parse_scalp17(text):
    if 'Scalp17' not in text:
        return None
    m = re.search(r'Avg\.\s*([\d.]+)%', text)
    if not m:
        return None
    color = next((n for e, n in [('\U0001f7e9', 'green'), ('\U0001f7e7', 'orange'),
                                  ('\U0001f7e5', 'red')] if e in text), None)
    return {"value": float(m.group(1)), "color": color, "direction": None,
            "timeframe": None, "btc_price": None, "extra": {}}


def parse_index_btc(text):
    if 'Index' not in text or 'Bitcoin' not in text:
        return None
    tf = re.search(r'INDEX\s+(\d+[mhH\u043c\u0438\u043dMIN]+)', text)
    price = re.search(r'Bitcoin\s+([\d.]+)', text)
    g, r_ = text.count('\U0001f7e9'), text.count('\U0001f7e5')
    direction = "bullish" if g > r_ else ("bearish" if r_ > g else "neutral")
    return {"value": None, "color": "green" if g > r_ else ("red" if r_ > g else None),
            "direction": direction, "timeframe": tf.group(1).lower() if tf else None,
            "btc_price": float(price.group(1)) if price else None,
            "extra": {"green_count": g, "red_count": r_}}


def parse_dmi_smf(text):
    if 'SMF' not in text:
        return None
    m = re.search(r'SMF\s*(?:BTC\s*)?(-?[\d.]+)', text)
    if not m:
        return None
    is_btc = bool(re.search(r'SMF\s+BTC\s', text))
    color = "orange" if '\U0001f536' in text else ("blue" if '\U0001f537' in text else None)
    direction = "bullish" if color == "orange" else ("bearish" if color == "blue" else None)
    return {"value": float(m.group(1)), "color": color, "direction": direction,
            "timeframe": "15m", "btc_price": None, "extra": {"is_btc_specific": is_btc}}


def parse_dyor_alerts(text):
    if 'BTC/USDT-SPOT:' not in text:
        return None
    if 'Дисбаланс' not in text and 'Disbalance' not in text:
        return None
    if 'покупателя' in text.lower():
        sig_type, direction = "buyer_disbalance", "bullish"
    elif 'продавца' in text.lower():
        sig_type, direction = "seller_disbalance", "bearish"
    else:
        sig_type, direction = "unknown", None
    pm = re.search(r'BTC/USDT-SPOT:\s*([\d.]+)', text)
    btc_price = float(pm.group(1)) if pm else None
    green_dots = text.count('\U0001f7e2')

    def parse_money(pattern, section):
        m = re.search(pattern, section)
        if not m:
            return None
        nm = re.match(r'([\d.]+)\s*([MmМkKк]?)', m.group(1))
        if not nm:
            return None
        val = float(nm.group(1))
        s = nm.group(2).upper()
        if s in ('M', '\u041c'):
            val *= 1_000_000
        elif s in ('K', '\u041a'):
            val *= 1_000
        return val

    bp = text.split('Binance:')[-1].split('Total')[0] if 'Binance:' in text else ""
    lp = text.split('Total liquidations:')[-1] if 'Total liquidations:' in text else ""
    b_l = parse_money(r'Long:\s*\$([\d.]+\s*[MmМkKк]?)', bp)
    b_s = parse_money(r'Short:\s*\$([\d.]+\s*[MmМkKк]?)', bp)
    l_l = parse_money(r'Long:\s*\$([\d.]+\s*[MmМkKк]?)', lp)
    l_s = parse_money(r'Short:\s*\$([\d.]+\s*[MmМkKк]?)', lp)
    ratio = round(b_l / b_s, 2) if b_l and b_s and b_s > 0 else None
    return {"value": ratio, "color": "green" if green_dots > 0 else None,
            "direction": direction, "timeframe": None, "btc_price": btc_price,
            "extra": {"signal_type": sig_type, "green_dots": green_dots,
                      "binance_long": b_l, "binance_short": b_s,
                      "liq_long": l_l, "liq_short": l_s, "long_short_ratio": ratio}}


def parse_rsi_btc(text):
    if 'BTCUSDT' not in text:
        return None
    tm = re.search(r'(RSI_OVERSOLD|RSI_OVERBOUGHT)', text)
    if not tm:
        return None
    sig = tm.group(1)
    direction = "bullish" if sig == "RSI_OVERSOLD" else "bearish"
    pm = re.search(r'\$\s*([\d,]+)', text)
    btc_price = float(pm.group(1).replace(',', '')) if pm else None
    rsi = {}
    for m in re.finditer(r'(\d+[mhd]):\s*([\d.]+)', text):
        rsi[m.group(1)] = float(m.group(2))
    trig = re.search(r'(\d+[mhd]):\s*[\d.]+\s*(?:\U0001f7e2|\U0001f534)\u2b05\ufe0f', text)
    triggered_tf = trig.group(1) if trig else None
    return {"value": rsi.get(triggered_tf), "color": "green" if sig == "RSI_OVERSOLD" else "red",
            "direction": direction, "timeframe": triggered_tf, "btc_price": btc_price,
            "extra": {"signal_type": sig, "triggered_tf": triggered_tf,
                      "rsi_5m": rsi.get("5m"), "rsi_15m": rsi.get("15m"),
                      "rsi_1h": rsi.get("1h"), "rsi_4h": rsi.get("4h"), "rsi_1d": rsi.get("1d")}}


# ═══ Диспетчер + фильтр автора ═══

PARSERS = {
    "altswing": parse_altswing, "diamond_marks": parse_diamond_marks,
    "sells_power": parse_sells_power, "altspi": parse_altspi,
    "scalp17": parse_scalp17, "index_btc": parse_index_btc,
    "dmi_smf": parse_dmi_smf, "dyor_alerts": parse_dyor_alerts,
    "rsi_btc": parse_rsi_btc,
}


def parse_message(parser_type, text):
    func = PARSERS.get(parser_type)
    if func:
        try:
            return func(text)
        except Exception as e:
            logger.error(f"Parser {parser_type}: {e}")
    return None


def is_from_author(message, expected_username):
    u = None
    if message.from_user:
        u = message.from_user.username
    elif message.sender_chat:
        u = message.sender_chat.username
    return (u or "").lower() == expected_username.lower() if u else False


# ═══════════════════════════════════════════════════════════════
# ФАЗА 0: ХРЕБЕТ ЦЕН
# ═══════════════════════════════════════════════════════════════

async def phase_0_load_prices():
    """Скачать историю цен BTC. Дефолт 90 дней, догрузка если нужно."""
    global price_index

    logger.info("=" * 60)
    logger.info("ФАЗА 0: Хребет цен BTC")
    logger.info("=" * 60)

    existing = db.execute(
        "SELECT COUNT(*) as cnt, MIN(timestamp) as earliest FROM btc_price"
    ).fetchone()

    if existing["cnt"] > 10000:
        last = db.execute("SELECT MAX(timestamp) as ts FROM btc_price").fetchone()
        start = datetime.fromisoformat(last["ts"]).replace(tzinfo=timezone.utc)
        logger.info(f"Уже есть {existing['cnt']} точек, догружаю с {start}")
    else:
        start = datetime.now(timezone.utc) - timedelta(days=90)
        logger.info(f"Первый запуск, загружаю с {start.isoformat()}")

    klines = await fetch_btc_price_history(start, datetime.now(timezone.utc))

    total_before = db.execute("SELECT COUNT(*) as cnt FROM btc_price").fetchone()["cnt"]
    for kline in klines:
        ts = datetime.fromtimestamp(kline[0] / 1000, tz=timezone.utc)
        ts_str = ts.strftime("%Y-%m-%dT%H:%M:%S")
        try:
            db.execute(
                "INSERT OR IGNORE INTO btc_price (timestamp, price, volume, source) "
                "VALUES (?, ?, ?, 'binance_kline')",
                (ts_str, float(kline[4]), float(kline[5]))
            )
        except Exception:
            pass
    db.commit()

    total = db.execute("SELECT COUNT(*) as cnt FROM btc_price").fetchone()["cnt"]
    inserted = total - total_before
    logger.info(f"ФАЗА 0: +{inserted} новых, всего {total} ({total // 1440} дней)")

    # Построить price_index для использования во ВСЕХ последующих фазах
    price_index = build_price_index()

    return total


async def phase_0_extend(earliest_signal: datetime):
    """Догрузить цены если сигналы оказались старше загруженных."""
    global price_index

    min_row = db.execute("SELECT MIN(timestamp) as ts FROM btc_price").fetchone()
    if not min_row or not min_row["ts"]:
        return

    min_price_date = datetime.fromisoformat(min_row["ts"]).replace(tzinfo=timezone.utc)
    # Привести earliest_signal к aware если нужно
    if earliest_signal.tzinfo is None:
        earliest_signal = earliest_signal.replace(tzinfo=timezone.utc)

    if earliest_signal < min_price_date:
        logger.info(f"Догружаю цены: сигнал {earliest_signal} старше цен {min_price_date}")
        start = earliest_signal - timedelta(days=1)
        klines = await fetch_btc_price_history(start, min_price_date)
        for kline in klines:
            ts = datetime.fromtimestamp(kline[0] / 1000, tz=timezone.utc)
            ts_str = ts.strftime("%Y-%m-%dT%H:%M:%S")
            db.execute(
                "INSERT OR IGNORE INTO btc_price (timestamp, price, volume, source) "
                "VALUES (?, ?, ?, 'binance_kline')",
                (ts_str, float(kline[4]), float(kline[5]))
            )
        db.commit()
        logger.info(f"Догружено {len(klines)} точек")

        # ПЕРЕСТРОИТЬ price_index после догрузки
        price_index = build_price_index()


# ═══════════════════════════════════════════════════════════════
# ФАЗЫ 1-9: ПОКАНАЛЬНЫЙ ПАРСИНГ
# ═══════════════════════════════════════════════════════════════

async def download_and_save_raw(chat_id: int, channel_name: str) -> int:
    """
    Скачивает ВСЕ сообщения из канала -> СРАЗУ в raw_messages.
    НЕ накапливает в RAM. Возвращает кол-во скачанных.
    """
    count = 0
    offset_id = 0
    consecutive_errors = 0

    while True:
        try:
            batch = []
            async for msg in userbot.get_chat_history(chat_id, limit=100, offset_id=offset_id):
                batch.append(msg)

            if not batch:
                break

            # Сразу в БД — не держим в RAM
            for msg in batch:
                ts_str = msg.date.strftime("%Y-%m-%dT%H:%M:%S")
                from_username = (msg.from_user.username if msg.from_user else
                                 msg.sender_chat.username if msg.sender_chat else None)
                topic_id = getattr(msg, 'reply_to_top_message_id', None)
                db.execute("""
                    INSERT OR IGNORE INTO raw_messages
                    (channel_id, channel_name, message_id, timestamp, text, has_text,
                     from_username, reply_to_topic_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (chat_id, channel_name, msg.id, ts_str,
                      msg.text[:2000] if msg.text else None,
                      1 if msg.text else 0,
                      from_username, topic_id))
            db.commit()

            count += len(batch)
            offset_id = batch[-1].id
            consecutive_errors = 0

            if count % 500 == 0:
                logger.info(f"  Скачано {count} сообщений...")

            await asyncio.sleep(0.5)

        except FloodWait as e:
            logger.warning(f"FloodWait {e.value}с на {count}-м сообщении, жду...")
            await asyncio.sleep(e.value + 2)
            continue
        except Exception as e:
            consecutive_errors += 1
            logger.error(f"download error at {count} (attempt {consecutive_errors}): {e}")
            if consecutive_errors >= 5:
                logger.error(f"download: {consecutive_errors} consecutive errors, stopping channel {channel_name}")
                break
            await asyncio.sleep(5)
            continue

    logger.info(f"  Итого скачано: {count}")
    return count


def parse_raw_messages(chat_id: int, channel_name: str, parser_type: str,
                       config: dict, unrec_file: str) -> dict:
    """
    Читает raw_messages из БД, парсит, сохраняет результаты.
    Возвращает stats.
    """
    rows = db.execute("""
        SELECT id, message_id, timestamp, text, from_username, reply_to_topic_id
        FROM raw_messages
        WHERE channel_id = ? AND has_text = 1
        ORDER BY timestamp
    """, (chat_id,)).fetchall()

    stats = {
        "total_raw": db.execute(
            "SELECT COUNT(*) as c FROM raw_messages WHERE channel_id=?", (chat_id,)
        ).fetchone()["c"],
        "text_messages": len(rows),
        "parsed_ok": 0,
        "parsed_fail": 0,
        "skipped_filter": 0,
        "validation_fail": 0,
        "errors": [],
        "earliest": None,
        "latest": None,
        "fail_examples": [],
    }

    signals_batch = []
    unrec_fh = open(unrec_file, 'a', encoding='utf-8')

    for row in rows:
        msg_time = datetime.fromisoformat(row["timestamp"]).replace(tzinfo=timezone.utc)
        text = row["text"]

        # Трек дат
        if stats["earliest"] is None or msg_time < stats["earliest"]:
            stats["earliest"] = msg_time
        if stats["latest"] is None or msg_time > stats["latest"]:
            stats["latest"] = msg_time

        # ── Фильтрация для групп ──

        if config.get("filter_author"):
            stored_user = row["from_username"]
            if not stored_user or stored_user.lower() != config["filter_author"].lower():
                stats["skipped_filter"] += 1
                continue

        if config.get("topic_id") is not None:
            tid = config["topic_id"]
            if tid > 0:
                if row["reply_to_topic_id"] != tid:
                    stats["skipped_filter"] += 1
                    continue
            elif tid == 0:
                if "BTCUSDT" not in text.upper():
                    stats["skipped_filter"] += 1
                    continue

        # ── Парсинг ──
        parsed = parse_message(parser_type, text)

        if parsed is None:
            stats["parsed_fail"] += 1
            fail_entry = {
                "channel": channel_name, "message_id": row["message_id"],
                "timestamp": row["timestamp"], "text": text[:500],
                "reason": "parser_returned_none"
            }
            json.dump(fail_entry, unrec_fh, ensure_ascii=False)
            unrec_fh.write('\n')
            if len(stats["fail_examples"]) < 5:
                stats["fail_examples"].append(fail_entry)
            db.execute(
                "UPDATE raw_messages SET is_parsed=0, parse_error='no_match' WHERE id=?",
                (row["id"],)
            )
            continue

        # ── Валидация ──
        valid, reason = validate_parsed(parser_type, parsed)
        if not valid:
            stats["validation_fail"] += 1
            fail_entry = {
                "channel": channel_name, "message_id": row["message_id"],
                "timestamp": row["timestamp"], "text": text[:500],
                "reason": f"validation: {reason}"
            }
            json.dump(fail_entry, unrec_fh, ensure_ascii=False)
            unrec_fh.write('\n')
            db.execute(
                "UPDATE raw_messages SET is_parsed=0, parse_error=? WHERE id=?",
                (f"validation: {reason}", row["id"])
            )
            continue

        # ── Успех ──
        stats["parsed_ok"] += 1

        # Цена из price_index (O(1), НЕ SQL!)
        btc_price_binance = get_price_fast(msg_time)

        signals_batch.append({
            "channel_id": chat_id, "channel_name": channel_name,
            "message_id": row["message_id"], "message_text": text,
            "timestamp": row["timestamp"], "parsed": parsed,
            "btc_price_binance": btc_price_binance,
        })

        db.execute("UPDATE raw_messages SET is_parsed=1 WHERE id=?", (row["id"],))

        if len(signals_batch) >= 100:
            save_signals_batch(signals_batch)
            signals_batch = []

    if signals_batch:
        save_signals_batch(signals_batch)

    unrec_fh.close()
    db.commit()
    return stats


def save_signals_batch(batch: list):
    for sig in batch:
        p = sig["parsed"]
        try:
            db.execute("""
                INSERT OR IGNORE INTO signals
                (channel_id, channel_name, message_id, message_text, timestamp,
                 indicator_value, signal_color, signal_direction, timeframe,
                 btc_price_from_channel, btc_price_binance, extra_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (sig["channel_id"], sig["channel_name"],
                  sig["message_id"], sig["message_text"][:2000],
                  sig["timestamp"],
                  p.get("value"), p.get("color"), p.get("direction"),
                  p.get("timeframe"), p.get("btc_price"), sig["btc_price_binance"],
                  json.dumps(p.get("extra", {}), ensure_ascii=False)))
        except Exception as e:
            logger.error(f"save_signal: {e}")
    db.commit()


def generate_channel_report(num: int, name: str, stats: dict, unrec_file: str) -> str:
    ok = stats["parsed_ok"]
    fail = stats["parsed_fail"]
    val_fail = stats["validation_fail"]
    text_msgs = stats["text_messages"]
    filtered = stats["skipped_filter"]
    total_raw = stats["total_raw"]
    media = total_raw - text_msgs

    parseable = text_msgs - filtered
    success_rate = (ok / max(parseable, 1)) * 100

    earliest_str = fmt_madrid(stats['earliest'].strftime("%Y-%m-%dT%H:%M:%S")) if stats['earliest'] else "N/A"
    latest_str = fmt_madrid(stats['latest'].strftime("%Y-%m-%dT%H:%M:%S")) if stats['latest'] else "N/A"

    report = (
        f"\U0001f4ca ОТЧЁТ — Канал {num}: {name}\n"
        f"{'=' * 40}\n\n"
        f"\U0001f4e5 Скачано:              {stats.get('downloaded', total_raw)}\n"
        f"\U0001f4dd С текстом:            {text_msgs}\n"
        f"\U0001f5bc Без текста (медиа):   {media}\n"
        f"\U0001f507 Отфильтровано:        {filtered}\n"
        f"\u2705 Распарсено:           {ok} ({success_rate:.1f}%)\n"
        f"\u274c Не распознано:         {fail}\n"
        f"\u26a0\ufe0f Не прошли валидацию:  {val_fail}\n\n"
        f"\U0001f4c5 Период: {earliest_str} — {latest_str}\n"
    )

    if stats["fail_examples"]:
        report += "\n\u274c Примеры нераспознанных:\n"
        for i, ex in enumerate(stats["fail_examples"][:3], 1):
            preview = ex["text"][:80].replace('\n', ' ')
            report += f"  {i}. [{ex['timestamp'][:16]}] {preview}...\n"
        if fail > 3:
            report += f"  ... и ещё {fail - 3} (см. {unrec_file})\n"

    if stats["errors"]:
        report += f"\n\U0001f6a8 Ошибки: {'; '.join(stats['errors'])}\n"

    if success_rate < 80:
        report += f"\n\U0001f534 НИЗКИЙ % ({success_rate:.0f}%) -> проверь парсер!"
    elif success_rate < 95:
        report += "\n\U0001f7e1 Неплохо, но есть нераспознанные."
    else:
        report += "\n\U0001f7e2 Отличный результат!"

    return report


async def phase_channel(channel_num: int, chat_id: int, config: dict):
    """Фаза N: Скачать -> raw_messages -> парсить -> отчёт."""
    name = config["name"]
    parser_type = config["parser"]

    logger.info("=" * 60)
    logger.info(f"ФАЗА {channel_num}: '{name}' (chat_id={chat_id})")
    logger.info("=" * 60)

    # Проверка: канал уже ПОЛНОСТЬЮ обработан?
    completed = db.execute(
        "SELECT COUNT(*) as cnt FROM sync_log WHERE channel_name=? AND phase='complete'",
        (name,)
    ).fetchone()["cnt"]
    if completed > 0:
        existing = db.execute(
            "SELECT COUNT(*) as cnt FROM signals WHERE channel_name=?", (name,)
        ).fetchone()["cnt"]
        logger.info(f"'{name}': уже завершён ({existing} сигналов), пропускаю")
        return {"status": "skipped", "existing": existing}

    # ── Шаг 1: Скачать -> raw_messages ──
    await send_admin_message(f"\u23f3 ФАЗА {channel_num}: Скачиваю '{name}'...")
    downloaded = await download_and_save_raw(chat_id, name)

    # ── Шаг 2: Парсить из raw_messages ──
    os.makedirs(UNRECOGNIZED_DIR, exist_ok=True)
    unrec_file = os.path.join(UNRECOGNIZED_DIR, f"channel_{channel_num}_{name}.jsonl")
    if os.path.exists(unrec_file):
        os.remove(unrec_file)

    stats = parse_raw_messages(chat_id, name, parser_type, config, unrec_file)
    stats["downloaded"] = downloaded

    # ── Шаг 3: Догрузить цены если нужно ──
    if stats["earliest"]:
        await phase_0_extend(stats["earliest"])

    # ── Шаг 4: Отчёт ──
    report = generate_channel_report(channel_num, name, stats, unrec_file)

    report_file = os.path.join(UNRECOGNIZED_DIR, f"channel_{channel_num}_{name}_REPORT.txt")
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report)

    # sync_log
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    db.execute("""
        INSERT INTO sync_log (channel_name, phase, total_messages, parsed_ok, parsed_fail,
            skipped_media, skipped_filter, earliest_message, latest_message,
            started_at, completed_at, notes)
        VALUES (?, 'complete', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (name, stats["total_raw"], stats["parsed_ok"],
          stats["parsed_fail"] + stats["validation_fail"],
          stats["total_raw"] - stats["text_messages"],
          stats["skipped_filter"],
          stats["earliest"].strftime("%Y-%m-%dT%H:%M:%S") if stats["earliest"] else None,
          stats["latest"].strftime("%Y-%m-%dT%H:%M:%S") if stats["latest"] else None,
          now_str, now_str,
          "; ".join(stats["errors"]) or None))

    # Обновить channels
    db.execute("UPDATE channels SET message_count=?, last_message_at=? WHERE channel_id=?",
               (stats["parsed_ok"],
                stats["latest"].strftime("%Y-%m-%dT%H:%M:%S") if stats["latest"] else None,
                chat_id))
    db.commit()

    await send_admin_message(report)
    logger.info(report)
    stats["status"] = "ok"
    return stats


# ═══════════════════════════════════════════════════════════════
# ФАЗА 10: ЗАПОЛНЕНИЕ ЦЕНОВОГО КОНТЕКСТА
# ═══════════════════════════════════════════════════════════════

async def phase_10_fill_price_context():
    """Для каждого сигнала — цены до/после через price_index."""
    global price_index

    # Перестроить индекс (могли быть догрузки в Phase 1-9)
    price_index = build_price_index()

    signals = db.execute("""
        SELECT s.id, s.timestamp, s.btc_price_binance, s.btc_price_from_channel, s.channel_name
        FROM signals s LEFT JOIN signal_price_context ctx ON ctx.signal_id = s.id
        WHERE ctx.id IS NULL ORDER BY s.timestamp
    """).fetchall()

    logger.info(f"ФАЗА 10: контекст для {len(signals)} сигналов...")

    def pct(base, target):
        return round(((target - base) / base) * 100, 4) if base and target and base > 0 else None

    filled = 0
    for i, sig in enumerate(signals):
        st = datetime.fromisoformat(sig["timestamp"]).replace(tzinfo=timezone.utc)

        price_at = sig["btc_price_binance"] or sig["btc_price_from_channel"]
        if not price_at:
            price_at = get_price_fast(st)
        if not price_at:
            continue

        p5b = get_price_fast(st - timedelta(minutes=5))
        p15b = get_price_fast(st - timedelta(minutes=15))
        p1hb = get_price_fast(st - timedelta(hours=1))
        p5 = get_price_fast(st + timedelta(minutes=5))
        p15 = get_price_fast(st + timedelta(minutes=15))
        p1h = get_price_fast(st + timedelta(hours=1))
        p4h = get_price_fast(st + timedelta(hours=4))
        p24h = get_price_fast(st + timedelta(hours=24))

        mask = 0
        if p5:
            mask |= MASK_5M
        if p15:
            mask |= MASK_15M
        if p1h:
            mask |= MASK_1H
        if p4h:
            mask |= MASK_4H
        if p24h:
            mask |= MASK_24H

        try:
            db.execute("""
                INSERT OR IGNORE INTO signal_price_context (
                    signal_id, channel_name, signal_timestamp, price_at_signal,
                    price_5m_before, price_15m_before, price_1h_before,
                    price_5m_after, price_15m_after, price_1h_after, price_4h_after, price_24h_after,
                    change_5m_pct, change_15m_pct, change_1h_pct, change_4h_pct, change_24h_pct,
                    filled_mask
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (sig["id"], sig["channel_name"], sig["timestamp"], price_at,
                  p5b, p15b, p1hb, p5, p15, p1h, p4h, p24h,
                  pct(price_at, p5), pct(price_at, p15), pct(price_at, p1h),
                  pct(price_at, p4h), pct(price_at, p24h), mask))
            filled += 1
        except Exception as e:
            logger.error(f"Context sig {sig['id']}: {e}")

        if (i + 1) % 1000 == 0:
            db.commit()
            logger.info(f"  Контекст: {i + 1}/{len(signals)}...")
            await send_admin_message(f"\u23f3 Phase 10: {i + 1}/{len(signals)}...")

    db.commit()
    logger.info(f"ФАЗА 10: {filled}/{len(signals)} сигналов")


# ═══════════════════════════════════════════════════════════════
# LIVE MODE (ФАЗА 11)
# ═══════════════════════════════════════════════════════════════

async def on_new_signal(client, message):
    if not message.text:
        return
    config = RESOLVED_CHANNELS.get(message.chat.id)
    if not config:
        return

    # Фильтрация
    if config.get("filter_author") and not is_from_author(message, config["filter_author"]):
        return
    if config.get("topic_id") is not None:
        tid = config["topic_id"]
        if tid > 0:
            if getattr(message, 'reply_to_top_message_id', None) != tid:
                return
        elif "BTCUSDT" not in message.text.upper():
            return

    ts_str = message.date.strftime("%Y-%m-%dT%H:%M:%S")
    from_username = (message.from_user.username if message.from_user else
                     message.sender_chat.username if message.sender_chat else None)
    topic_id = getattr(message, 'reply_to_top_message_id', None)

    # raw_messages
    db.execute("""INSERT OR IGNORE INTO raw_messages
        (channel_id, channel_name, message_id, timestamp, text, has_text, from_username, reply_to_topic_id)
        VALUES (?,?,?,?,?,1,?,?)""",
        (message.chat.id, config["name"], message.id, ts_str, message.text[:2000],
         from_username, topic_id))

    parsed = parse_message(config["parser"], message.text)
    if parsed is None:
        db.execute(
            "UPDATE raw_messages SET is_parsed=0, parse_error='no_match' "
            "WHERE channel_id=? AND message_id=?",
            (message.chat.id, message.id)
        )
        db.commit()
        os.makedirs(UNRECOGNIZED_DIR, exist_ok=True)
        with open(os.path.join(UNRECOGNIZED_DIR, "live_unrecognized.jsonl"), 'a',
                  encoding='utf-8') as f:
            json.dump({"channel": config["name"], "message_id": message.id,
                       "timestamp": ts_str, "text": message.text[:500]}, f,
                      ensure_ascii=False)
            f.write('\n')
        return

    valid, reason = validate_parsed(config["parser"], parsed)
    if not valid:
        db.execute(
            "UPDATE raw_messages SET is_parsed=0, parse_error=? "
            "WHERE channel_id=? AND message_id=?",
            (f"validation: {reason}", message.chat.id, message.id)
        )
        db.commit()
        return

    btc_price = (get_price_fast(message.date) or
                 get_closest_price_sql(message.date) or
                 await fetch_btc_price())

    try:
        cursor = db.execute("""
            INSERT OR IGNORE INTO signals
            (channel_id, channel_name, message_id, message_text, timestamp,
             indicator_value, signal_color, signal_direction, timeframe,
             btc_price_from_channel, btc_price_binance, extra_data)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (message.chat.id, config["name"], message.id, message.text[:2000], ts_str,
              parsed.get("value"), parsed.get("color"), parsed.get("direction"),
              parsed.get("timeframe"), parsed.get("btc_price"), btc_price,
              json.dumps(parsed.get("extra", {}), ensure_ascii=False)))
        db.commit()
        db.execute(
            "UPDATE raw_messages SET is_parsed=1 WHERE channel_id=? AND message_id=?",
            (message.chat.id, message.id)
        )
        db.commit()
        if cursor.rowcount > 0:
            create_live_price_context(cursor.lastrowid, config["name"], message.date, btc_price)
    except Exception as e:
        logger.error(f"Live save: {e}")

    logger.info(f"[{config['name']}] LIVE: val={parsed.get('value')}, dir={parsed.get('direction')}")


def create_live_price_context(signal_id, channel_name, signal_time, price_at):
    if not price_at:
        return
    p5b = get_price_fast(signal_time - timedelta(minutes=5))
    p15b = get_price_fast(signal_time - timedelta(minutes=15))
    p1hb = get_price_fast(signal_time - timedelta(hours=1))
    db.execute("""INSERT OR IGNORE INTO signal_price_context
        (signal_id, channel_name, signal_timestamp, price_at_signal,
         price_5m_before, price_15m_before, price_1h_before, filled_mask)
        VALUES (?,?,?,?,?,?,?,0)
    """, (signal_id, channel_name, signal_time.strftime("%Y-%m-%dT%H:%M:%S"),
          price_at, p5b, p15b, p1hb))
    db.commit()


# ═══════════════════════════════════════════════════════════════
# ФОНОВЫЕ ЗАДАЧИ
# ═══════════════════════════════════════════════════════════════

async def price_ticker_loop():
    while True:
        try:
            price = await fetch_btc_price()
            if price:
                ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:00")
                db.execute(
                    "INSERT OR IGNORE INTO btc_price (timestamp, price, source) "
                    "VALUES (?,?,'ticker')",
                    (ts, price)
                )
                db.commit()
                # Обновить price_index
                price_index[ts[:16]] = price
        except Exception as e:
            logger.error(f"Ticker: {e}")
        await asyncio.sleep(60)


async def fill_delayed_prices_loop():
    while True:
        try:
            now = datetime.now(timezone.utc)
            rows = db.execute("""
                SELECT id, signal_id, signal_timestamp, price_at_signal, filled_mask
                FROM signal_price_context WHERE filled_mask < 31
                ORDER BY signal_timestamp DESC LIMIT 200
            """).fetchall()
            for row in rows:
                sig_t = datetime.fromisoformat(row["signal_timestamp"]).replace(
                    tzinfo=timezone.utc
                )
                p_at = row["price_at_signal"]
                mask = row["filled_mask"]
                if not p_at:
                    continue
                new_mask = mask
                updates = {}
                for mins, fld, pct_fld, bit in [
                    (5, "price_5m_after", "change_5m_pct", MASK_5M),
                    (15, "price_15m_after", "change_15m_pct", MASK_15M),
                    (60, "price_1h_after", "change_1h_pct", MASK_1H),
                    (240, "price_4h_after", "change_4h_pct", MASK_4H),
                    (1440, "price_24h_after", "change_24h_pct", MASK_24H),
                ]:
                    if not (mask & bit) and now >= sig_t + timedelta(minutes=mins + 1):
                        p = (get_price_fast(sig_t + timedelta(minutes=mins)) or
                             get_closest_price_sql(sig_t + timedelta(minutes=mins)))
                        if p:
                            updates[fld] = p
                            updates[pct_fld] = round(((p - p_at) / p_at) * 100, 4)
                            new_mask |= bit
                if new_mask != mask:
                    cols = ", ".join(f"{k}=?" for k in updates)
                    db.execute(
                        f"UPDATE signal_price_context SET {cols}, filled_mask=? WHERE id=?",
                        list(updates.values()) + [new_mask, row["id"]]
                    )
            db.commit()
        except Exception as e:
            logger.error(f"Fill delayed: {e}")
        await asyncio.sleep(300)


async def healthcheck_loop():
    while True:
        await asyncio.sleep(3600)
        try:
            issues = []
            now = datetime.now(timezone.utc)
            price = await fetch_btc_price()
            if not price:
                issues.append("\u26a0\ufe0f Binance не отвечает")
            for cid, cfg in RESOLVED_CHANNELS.items():
                row = db.execute(
                    "SELECT MAX(timestamp) as t FROM signals WHERE channel_id=?", (cid,)
                ).fetchone()
                if row and row["t"]:
                    hrs = (now - datetime.fromisoformat(row["t"]).replace(
                        tzinfo=timezone.utc
                    )).total_seconds() / 3600
                    if hrs > 48:
                        issues.append(f"\u26a0\ufe0f {cfg['name']}: молчит {int(hrs)}ч")
            if issues:
                await send_admin_message(
                    "\U0001f514 Healthcheck:\n" + "\n".join(issues)
                )
        except Exception as e:
            logger.error(f"Healthcheck: {e}")


# ═══════════════════════════════════════════════════════════════
# TELEGRAM-БОТ (УПРАВЛЕНИЕ) — Кнопки
# ═══════════════════════════════════════════════════════════════

def is_admin(update) -> bool:
    """Проверяет что пользователь — админ."""
    user = update.effective_user
    return user and user.id == ADMIN_USER_ID


async def cmd_start(update, context):
    if not is_admin(update):
        await update.message.reply_text("Access denied.")
        return
    keyboard = [
        [InlineKeyboardButton("\U0001f4cb Каналы и статус", callback_data="channels_status")],
        [InlineKeyboardButton("\U0001f4ca Последние сигналы", callback_data="recent_signals")],
        [InlineKeyboardButton("\U0001f4b0 Цена BTC", callback_data="btc_price")],
        [InlineKeyboardButton("\U0001f50d По каналу", callback_data="by_channel")],
        [InlineKeyboardButton("\U0001f4c8 Сводная", callback_data="summary")],
        [InlineKeyboardButton("\U0001f4e5 Экспорт CSV", callback_data="export_csv")],
        [InlineKeyboardButton("\U0001f504 Перепарсить канал", callback_data="reparse")],
        [InlineKeyboardButton("\u2699\ufe0f Статус системы", callback_data="system_status")],
    ]
    await update.message.reply_text(
        "\U0001f4ca BTC Signal Aggregator",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def button_handler(update, context):
    if not is_admin(update):
        await update.callback_query.answer("Access denied.", show_alert=True)
        return
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "channels_status":
        await handle_channels_status(query)
    elif data == "recent_signals":
        await handle_recent_signals(query)
    elif data == "btc_price":
        await handle_btc_price(query)
    elif data == "by_channel":
        await handle_by_channel_menu(query)
    elif data.startswith("ch_signals_"):
        channel_name = data[len("ch_signals_"):]
        await handle_channel_signals(query, channel_name)
    elif data == "summary":
        await handle_summary(query)
    elif data == "export_csv":
        await handle_export_csv(query, context)
    elif data == "reparse":
        await handle_reparse_menu(query)
    elif data.startswith("reparse_"):
        channel_name = data[len("reparse_"):]
        await handle_reparse_channel(query, channel_name)
    elif data == "system_status":
        await handle_system_status(query)
    elif data == "back_main":
        keyboard = [
            [InlineKeyboardButton("\U0001f4cb Каналы и статус", callback_data="channels_status")],
            [InlineKeyboardButton("\U0001f4ca Последние сигналы", callback_data="recent_signals")],
            [InlineKeyboardButton("\U0001f4b0 Цена BTC", callback_data="btc_price")],
            [InlineKeyboardButton("\U0001f50d По каналу", callback_data="by_channel")],
            [InlineKeyboardButton("\U0001f4c8 Сводная", callback_data="summary")],
            [InlineKeyboardButton("\U0001f4e5 Экспорт CSV", callback_data="export_csv")],
            [InlineKeyboardButton("\U0001f504 Перепарсить канал", callback_data="reparse")],
            [InlineKeyboardButton("\u2699\ufe0f Статус системы", callback_data="system_status")],
        ]
        await query.edit_message_text(
            "\U0001f4ca BTC Signal Aggregator",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )


async def handle_channels_status(query):
    rows = db.execute("""
        SELECT c.name, c.message_count,
               (SELECT COUNT(*) FROM signals s WHERE s.channel_name = c.name) as sig_count,
               (SELECT MAX(s.timestamp) FROM signals s WHERE s.channel_name = c.name) as last_sig
        FROM channels c WHERE c.is_active = 1 ORDER BY c.name
    """).fetchall()
    if not rows:
        await query.edit_message_text("\u274c Нет активных каналов")
        return
    text = "\U0001f4cb Каналы и статус:\n\n"
    for r in rows:
        last = fmt_madrid(r["last_sig"]) if r["last_sig"] else "—"
        text += f"\u2022 {r['name']}: {r['sig_count']} сигналов | Послед.: {last}\n"
    text += "\n"
    keyboard = [[InlineKeyboardButton("\u25c0 Назад", callback_data="back_main")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


async def handle_recent_signals(query):
    rows = db.execute("""
        SELECT timestamp, channel_name, indicator_value, signal_color,
               signal_direction, btc_price_binance
        FROM signals ORDER BY timestamp DESC LIMIT 10
    """).fetchall()
    if not rows:
        await query.edit_message_text("\u274c Нет сигналов")
        return
    text = "\U0001f4ca Последние 10 сигналов:\n\n"
    for r in rows:
        ts = fmt_madrid(r["timestamp"])
        val = f"{r['indicator_value']}" if r["indicator_value"] is not None else "—"
        d = r["signal_direction"] or "—"
        btc = f"${r['btc_price_binance']:,.0f}" if r["btc_price_binance"] else "—"
        text += f"{ts} | {r['channel_name']}\n  val={val} dir={d} BTC={btc}\n"
    keyboard = [[InlineKeyboardButton("\u25c0 Назад", callback_data="back_main")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


async def handle_btc_price(query):
    price = await fetch_btc_price()
    row = db.execute("""
        SELECT MIN(price) as lo, MAX(price) as hi
        FROM btc_price
        WHERE timestamp >= datetime('now', '-1 day')
    """).fetchone()
    lo = f"${row['lo']:,.0f}" if row and row["lo"] else "—"
    hi = f"${row['hi']:,.0f}" if row and row["hi"] else "—"
    cur = f"${price:,.2f}" if price else "\u274c Недоступна"
    text = (
        f"\U0001f4b0 Цена BTC:\n\n"
        f"Текущая: {cur}\n"
        f"Мин. 24ч: {lo}\n"
        f"Макс. 24ч: {hi}\n"
    )
    keyboard = [[InlineKeyboardButton("\u25c0 Назад", callback_data="back_main")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


async def handle_by_channel_menu(query):
    rows = db.execute("SELECT DISTINCT channel_name FROM signals ORDER BY channel_name").fetchall()
    if not rows:
        await query.edit_message_text("\u274c Нет сигналов")
        return
    keyboard = []
    for r in rows:
        keyboard.append([InlineKeyboardButton(
            r["channel_name"], callback_data=f"ch_signals_{r['channel_name']}"
        )])
    keyboard.append([InlineKeyboardButton("\u25c0 Назад", callback_data="back_main")])
    await query.edit_message_text(
        "\U0001f50d Выбери канал:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def handle_channel_signals(query, channel_name):
    rows = db.execute("""
        SELECT timestamp, indicator_value, signal_color, signal_direction,
               btc_price_binance, timeframe
        FROM signals WHERE channel_name = ? ORDER BY timestamp DESC LIMIT 10
    """, (channel_name,)).fetchall()
    if not rows:
        await query.edit_message_text(f"\u274c Нет сигналов для {channel_name}")
        return
    text = f"\U0001f50d {channel_name} — последние 10:\n\n"
    for r in rows:
        ts = fmt_madrid(r["timestamp"])
        val = f"{r['indicator_value']}" if r["indicator_value"] is not None else "—"
        d = r["signal_direction"] or "—"
        btc = f"${r['btc_price_binance']:,.0f}" if r["btc_price_binance"] else "—"
        tf = r["timeframe"] or ""
        text += f"{ts} | val={val} dir={d} {tf} BTC={btc}\n"
    keyboard = [
        [InlineKeyboardButton("\u25c0 К каналам", callback_data="by_channel")],
        [InlineKeyboardButton("\u25c0 Главная", callback_data="back_main")],
    ]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


async def handle_summary(query):
    rows = db.execute("""
        SELECT s.channel_name,
               COUNT(*) as total,
               SUM(CASE WHEN ctx.filled_mask = 31 THEN 1 ELSE 0 END) as full_ctx,
               COUNT(ctx.id) as has_ctx
        FROM signals s
        LEFT JOIN signal_price_context ctx ON ctx.signal_id = s.id
        GROUP BY s.channel_name ORDER BY s.channel_name
    """).fetchall()
    if not rows:
        await query.edit_message_text("\u274c Нет данных")
        return
    text = "\U0001f4c8 Сводная:\n\n"
    total_all = 0
    for r in rows:
        pct = (r["full_ctx"] / max(r["total"], 1)) * 100
        text += f"\u2022 {r['channel_name']}: {r['total']} сиг. | контекст: {pct:.0f}%\n"
        total_all += r["total"]
    text += f"\n\U0001f4ca Всего: {total_all} сигналов"
    keyboard = [[InlineKeyboardButton("\u25c0 Назад", callback_data="back_main")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


def export_csv() -> str:
    rows = db.execute("""
        SELECT s.timestamp, s.channel_name, s.indicator_value, s.signal_color,
               s.signal_direction, s.timeframe, s.btc_price_binance, s.extra_data,
               ctx.price_at_signal, ctx.change_5m_pct, ctx.change_15m_pct,
               ctx.change_1h_pct, ctx.change_4h_pct, ctx.change_24h_pct
        FROM signals s LEFT JOIN signal_price_context ctx ON ctx.signal_id = s.id
        ORDER BY s.timestamp
    """).fetchall()

    filepath = f"export_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}.csv"

    def v(x):
        return x if x is not None else ""

    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp", "channel", "value", "color", "direction", "timeframe",
            "btc_price", "change_5m", "change_15m", "change_1h", "change_4h",
            "change_24h", "extra"
        ])
        for r in rows:
            writer.writerow([
                r["timestamp"], r["channel_name"], v(r["indicator_value"]),
                v(r["signal_color"]), v(r["signal_direction"]),
                v(r["timeframe"]), v(r["btc_price_binance"]),
                v(r["change_5m_pct"]), v(r["change_15m_pct"]),
                v(r["change_1h_pct"]), v(r["change_4h_pct"]),
                v(r["change_24h_pct"]), v(r["extra_data"])
            ])
    return filepath


async def handle_export_csv(query, context):
    filepath = export_csv()
    await query.edit_message_text(f"\U0001f4e5 Экспорт готов: {filepath}")
    try:
        with open(filepath, 'rb') as f:
            await context.bot.send_document(
                chat_id=query.message.chat_id,
                document=f,
                filename=os.path.basename(filepath)
            )
    except Exception as e:
        logger.error(f"CSV send error: {e}")


async def handle_reparse_menu(query):
    rows = db.execute("SELECT DISTINCT channel_name FROM raw_messages ORDER BY channel_name").fetchall()
    if not rows:
        await query.edit_message_text("\u274c Нет raw_messages")
        return
    keyboard = []
    for r in rows:
        keyboard.append([InlineKeyboardButton(
            r["channel_name"], callback_data=f"reparse_{r['channel_name']}"
        )])
    keyboard.append([InlineKeyboardButton("\u25c0 Назад", callback_data="back_main")])
    await query.edit_message_text(
        "\U0001f504 Выбери канал для перепарсинга:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def reparse_channel(channel_name: str) -> str:
    rows = db.execute("""
        SELECT id, channel_id, message_id, timestamp, text, from_username, reply_to_topic_id
        FROM raw_messages
        WHERE channel_name=? AND (is_parsed = 0 OR is_parsed IS NULL) AND text IS NOT NULL
    """, (channel_name,)).fetchall()
    config = next((c for c in RESOLVED_CHANNELS.values() if c["name"] == channel_name), None)
    if not config:
        return f"\u274c {channel_name} не найден"
    reparsed = still_fail = skipped = 0
    for row in rows:
        # Фильтрация
        if config.get("filter_author"):
            if not row["from_username"] or row["from_username"].lower() != config["filter_author"].lower():
                skipped += 1
                continue
        if config.get("topic_id") is not None:
            tid = config["topic_id"]
            if tid > 0 and row["reply_to_topic_id"] != tid:
                skipped += 1
                continue
            elif tid == 0 and "BTCUSDT" not in row["text"].upper():
                skipped += 1
                continue
        parsed = parse_message(config["parser"], row["text"])
        if not parsed:
            still_fail += 1
            continue
        valid, reason = validate_parsed(config["parser"], parsed)
        if not valid:
            still_fail += 1
            continue
        btc_price = get_price_fast(
            datetime.fromisoformat(row["timestamp"]).replace(tzinfo=timezone.utc)
        )
        db.execute("""INSERT OR IGNORE INTO signals
            (channel_id, channel_name, message_id, message_text, timestamp,
             indicator_value, signal_color, signal_direction, timeframe,
             btc_price_from_channel, btc_price_binance, extra_data)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (row["channel_id"], channel_name, row["message_id"], row["text"][:2000],
             row["timestamp"], parsed.get("value"), parsed.get("color"),
             parsed.get("direction"), parsed.get("timeframe"),
             parsed.get("btc_price"), btc_price,
             json.dumps(parsed.get("extra", {}), ensure_ascii=False)))
        db.execute("UPDATE raw_messages SET is_parsed=1, parse_error=NULL WHERE id=?", (row["id"],))
        reparsed += 1
    db.commit()
    return (
        f"\u2705 Перепарсено: {reparsed}/{len(rows)} "
        f"({still_fail} не распознаны, {skipped} отфильтровано)"
    )


async def handle_reparse_channel(query, channel_name):
    await query.edit_message_text(f"\u23f3 Перепарсинг {channel_name}...")
    result = await reparse_channel(channel_name)
    keyboard = [[InlineKeyboardButton("\u25c0 Назад", callback_data="back_main")]]
    await query.edit_message_text(result, reply_markup=InlineKeyboardMarkup(keyboard))


async def handle_system_status(query):
    channels_cnt = db.execute("SELECT COUNT(*) as c FROM channels WHERE is_active=1").fetchone()["c"]
    signals_cnt = db.execute("SELECT COUNT(*) as c FROM signals").fetchone()["c"]
    prices_cnt = db.execute("SELECT COUNT(*) as c FROM btc_price").fetchone()["c"]
    raw_cnt = db.execute("SELECT COUNT(*) as c FROM raw_messages").fetchone()["c"]
    unfilled = db.execute(
        "SELECT COUNT(*) as c FROM signal_price_context WHERE filled_mask < 31"
    ).fetchone()["c"]
    text = (
        f"\u2699\ufe0f Статус системы:\n\n"
        f"\U0001f4e1 Каналов: {channels_cnt}\n"
        f"\U0001f4ca Сигналов: {signals_cnt}\n"
        f"\U0001f4b0 Ценовых точек: {prices_cnt}\n"
        f"\U0001f4e8 Raw messages: {raw_cnt}\n"
        f"\u23f3 Незаполн. контекстов: {unfilled}\n"
    )
    keyboard = [[InlineKeyboardButton("\u25c0 Назад", callback_data="back_main")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


# ═══════════════════════════════════════════════════════════════
# ЗАПУСК — GRACEFUL SHUTDOWN
# ═══════════════════════════════════════════════════════════════

async def main():
    global db, RESOLVED_CHANNELS, price_index

    db = init_database()
    await init_http()
    await userbot.start()
    logger.info("Pyrogram подключён")

    channel_config = build_channel_config()
    RESOLVED_CHANNELS = await resolve_channel_ids(channel_config)
    logger.info(f"Каналов: {len(RESOLVED_CHANNELS)}")

    if not RESOLVED_CHANNELS:
        logger.error("Ни один канал не резолвлен! Проверь .env")
        await close_http()
        await userbot.stop()
        db.close()
        return

    # ═══ INITIAL SYNC ═══

    price_count = await phase_0_load_prices()
    if price_count == 0:
        await send_admin_message("\U0001f6a8 Binance не отдаёт цены!")
        await close_http()
        await userbot.stop()
        db.close()
        return
    await send_admin_message(f"\u2705 Фаза 0: {price_count} ценовых точек BTC")

    for i, (chat_id, config) in enumerate(RESOLVED_CHANNELS.items(), 1):
        await phase_channel(i, chat_id, config)
        await asyncio.sleep(3)

    await phase_10_fill_price_context()
    await send_admin_message("\u2705 Все фазы завершены. LIVE MODE.")

    # ═══ LIVE MODE ═══

    userbot.add_handler(
        MessageHandler(on_new_signal, filters.chat(list(RESOLVED_CHANNELS.keys())))
    )

    # Фоновые задачи — сохраняем ссылки для graceful shutdown
    bg_tasks = [
        asyncio.create_task(price_ticker_loop()),
        asyncio.create_task(fill_delayed_prices_loop()),
        asyncio.create_task(healthcheck_loop()),
    ]

    bot_app = Application.builder().token(BOT_TOKEN).build()
    bot_app.add_handler(CommandHandler("start", cmd_start))
    bot_app.add_handler(CallbackQueryHandler(button_handler))
    await bot_app.initialize()
    await bot_app.start()
    await bot_app.updater.start_polling()

    logger.info("=== LIVE MODE АКТИВЕН ===")

    # ═══ GRACEFUL SHUTDOWN ═══
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for s in (sig_mod.SIGINT, sig_mod.SIGTERM):
        loop.add_signal_handler(s, stop.set)

    await stop.wait()

    logger.info("Останавливаюсь...")

    # 1. Отменить фоновые задачи
    for t in bg_tasks:
        t.cancel()
    await asyncio.gather(*bg_tasks, return_exceptions=True)

    # 2. Остановить бот
    await bot_app.updater.stop()
    await bot_app.stop()
    await bot_app.shutdown()

    # 3. Остановить userbot
    await userbot.stop()

    # 4. Закрыть HTTP и БД
    await close_http()
    db.close()

    logger.info("=== СТОП ===")


if __name__ == "__main__":
    asyncio.run(main())
