# BTC Signal Aggregator Bot

Telegram-бот, который слушает 9 каналов с BTC-индикаторами, парсит сигналы, синхронизирует их с минутными ценами BTC с Binance и хранит всё в SQLite. Управление через Telegram-бота с инлайн-кнопками. Экспорт в CSV — непрерывный 5-минутный ценовой поток с наложенными сигналами.

## Структура файлов

```
btc-signal-bot/
├── main.py                  — точка входа, запуск и graceful shutdown (~100 строк)
├── config.py                — .env переменные, константы, настройки каналов, глобальное состояние
├── .env                     — API ключи, ID каналов
├── requirements.txt         — pyrogram, tgcrypto, python-telegram-bot, httpx, python-dotenv
├── CLAUDE.md
├── database/
│   ├── __init__.py
│   └── db.py                — создание таблиц, SQL-функции (price_index, save, resolve)
├── services/
│   ├── __init__.py
│   ├── binance.py           — загрузка цен BTC (fetch_btc_price, fetch_btc_price_history)
│   ├── parsers.py           — 9 парсеров + диспетчер + валидация + is_from_author
│   ├── phases.py            — phase_0/1-9/10, download, parse_raw_messages, reparse
│   ├── live.py              — on_new_signal, price_ticker, fill_delayed_prices, healthcheck
│   └── csv_export.py        — export_csv (5-мин ценовой поток + сигналы)
├── handlers/
│   ├── __init__.py
│   ├── commands.py          — /start, is_admin
│   ├── callbacks.py         — CALLBACK_ROUTES словарь-роутер, 8 кнопок
│   └── keyboards.py         — main_keyboard, back_keyboard
├── utils/
│   ├── __init__.py
│   ├── helpers.py           — split_text, fmt_madrid, fmt_number, pct_change
│   └── telegram.py          — send_admin_message (Pyrogram)
├── backtesting/
│   ├── __init__.py
│   ├── analyze.py           — точка входа: загрузка данных, оркестровка 11 модулей
│   ├── channel_stats.py     — поканальная статистика (5m/15m/1h/4h/24h), Sharpe/Sortino/PF
│   ├── mfe_mae.py           — MFE/MAE анализ (numpy vectorized, чанками)
│   ├── risk_metrics.py      — drawdown, Kelly, Ulcer Index, portfolio simulation
│   ├── sequences.py         — серии побед/поражений, runs test, serial correlation
│   ├── time_patterns.py     — по часам/сессиям/дням недели
│   ├── market_regimes.py    — волатильность (terciles) и тренд (4h change)
│   ├── correlations.py      — межканальные корреляции, diversification score
│   ├── confluence.py        — совпадение сигналов в 30-мин окне
│   ├── latency_decay.py     — деградация при задержке входа (0/1/3/5/10 мин)
│   ├── optimal_params.py    — grid search TP/SL + walk-forward IS/OOS
│   ├── monte_carlo.py       — 1000 перестановок направлений + timestamp shuffle
│   └── report_builder.py    — report.txt + results.json
├── btc-signal-bot.service   — systemd unit
├── fix_peers.py             — одноразовый: прогрев peer-кэша Pyrogram
├── reparse_fix.py           — одноразовый: тесты парсеров + перепарсинг 5 каналов
├── reparse_2ch.py           — одноразовый: перепарсинг AltSwing + DiamondMarks
├── redownload_2ch.py        — одноразовый: повторная загрузка AltSwing + DiamondMarks
├── btc_signals.db           — SQLite база (WAL mode), ~130 МБ
├── session.session          — файл сессии Pyrogram
├── unrecognized/            — JSONL с нераспознанными сообщениями (по каналам)
└── bot.log                  — лог текущего запуска
```

### Модульная архитектура

- **config.py** — единый источник глобального состояния (`db`, `price_index`, `http_client`, `userbot`, `RESOLVED_CHANNELS`). Все модули импортируют `import config` и обращаются к `config.db`, `config.price_index` и т.д.
- **callbacks.py** — словарь `CALLBACK_ROUTES` для exact match и список `PREFIX_ROUTES` для prefix match (вместо if/elif цепочки). Все обработчики принимают `(query, context)`.
- **Граф зависимостей** (без циклов): `config` <- `utils/*` <- `database/db` <- `services/*` <- `handlers/*` <- `main`

## Таблицы БД

| Таблица | Назначение |
|---|---|
| `btc_price` | Минутные свечи BTC/USDT с Binance. ~760k строк (529 дней). Поля: `timestamp`, `price`, `volume`, `source` |
| `raw_messages` | Все скачанные сообщения из каналов. Поля: `channel_id`, `message_id`, `timestamp`, `text`, `from_username`, `is_parsed`, `parse_error` |
| `signals` | Распарсенные сигналы. Поля: `channel_name`, `timestamp`, `indicator_value`, `signal_color`, `signal_direction`, `timeframe`, `btc_price_from_channel`, `btc_price_binance`, `extra_data` (JSON) |
| `signal_price_context` | Цены до/после сигнала. Поля: `price_at_signal`, `price_5m/15m/1h_before`, `price_5m/15m/1h/4h/24h_after`, `change_*_pct`, `filled_mask` (битовая маска заполненности) |
| `channels` | Реестр каналов: `channel_id`, `name`, `parser_type`, `message_count` |
| `sync_log` | Лог синхронизации: `channel_name`, `phase`, `total_messages`, `parsed_ok`, `parsed_fail` |

`filled_mask` в `signal_price_context`: 1=5m, 2=15m, 4=1h, 8=4h, 16=24h. Полный = 31.

## 9 каналов и парсеры

| # | Канал | Парсер | Формат сообщений | value | Распознано |
|---|---|---|---|---|---|
| 1 | AltSwing | `parse_altswing` | `Avg. 60.1%` или `🟧Avg. 67.3%` | % (от -100 до 100) | 100.0% |
| 2 | DiamondMarks | `parse_diamond_marks` | `🔥🟩🔥 Total 15m\nBTC/USDT: $114,141` | null (color-based) | 99.9% |
| 3 | SellsPowerIndex | `parse_sells_power` | `⚪️ 55%` или `🟩 -28%` | % (от -300 до 300) | 99.9% |
| 4 | AltSPI | `parse_altspi` | `🟥 21 🟧 22 ⚪️ 56 🟦 1 🟩 0\nMarket Av. 94.8%` | % (от -100 до 200) | 99.8% |
| 5 | Scalp17 | `parse_scalp17` | `⚡️Avg. 70.2%` или `⚡️🟧Avg. 64.3%` | % (от -200 до 200) | 99.9% |
| 6 | Index | `parse_index_btc` | `🟥INDEX 15min\n🟡Bitcoin 116633.02` | null (direction-based) | 55.9%* |
| 7 | DMI_SMF | `parse_dmi_smf` | `🔷 SMF Long -33.69` | числовое | 84.1% |
| 8 | DyorAlerts | `parse_dyor_alerts` | `🟢🟢 Дисбаланс покупателя\nBTC/USDT-SPOT: 65247.4\nBinance: Long/Short...` | long/short ratio | 99.9%** |
| 9 | RSI_BTC | `parse_rsi_btc` | `RSI_OVERSOLD BTCUSDT $90,153\n5m: 28.5 15m: 33.1...` | RSI значение | 0.3%*** |

\* Index: 55.9% потому что ETH/SOL сообщения корректно отсекаются (только Bitcoin).
\*\* DyorAlerts: 99.9% от сообщений бота; остальные — сообщения участников группы (filter_author).
\*\*\* RSI_BTC: канал публикует VOLUME SPIKE для разных пар, RSI_OVERSOLD/OVERBOUGHT для BTC — редкость.

DyorAlerts распознаёт 7 типов сигналов: `buyer_disbalance`, `seller_disbalance`, `long_priority` (с уровнем 1-4), `short_signal`, `long_signal`, `balance`, `unknown`.

## Фазы работы

### Phase 0: Хребет цен
Загрузка 1-минутных свечей BTC/USDT с Binance API. При первом запуске — 90 дней. При повторных — догрузка с последней точки. Строит `price_index` — dict `{minute_key: price}` в RAM для O(1) поиска.

### Phases 1-9: Поканальный парсинг
Для каждого канала последовательно:
1. Скачать все сообщения через Pyrogram -> `raw_messages` (не в RAM, сразу в БД)
2. Парсить из `raw_messages` через соответствующий парсер -> `signals`
3. Догрузить цены если сигналы старше имеющихся цен (`phase_0_extend`)
4. Отчёт админу через Pyrogram

Пропуск канала если в `sync_log` есть запись с `phase='complete'`.

Для групповых каналов (DyorAlerts, RSI_BTC):
- `filter_author` — фильтрация по username отправителя
- `topic_id` — фильтрация по теме (для RSI_BTC: `topic_id=0` значит фильтр по "BTCUSDT" в тексте)

### Phase 10: Ценовой контекст
Для каждого сигнала заполняет `signal_price_context`: цены за 5m/15m/1h до и 5m/15m/1h/4h/24h после сигнала. Использует `price_index` для O(1) поиска.

### Phase 11: Live Mode
- Pyrogram handler на новые сообщения из всех каналов
- Парсинг в реальном времени -> `signals`
- Фоновые задачи:
  - `price_ticker_loop`: текущая цена BTC каждые 60 сек
  - `fill_delayed_prices_loop`: дозаполнение `signal_price_context` (5m, 15m, 1h, 4h, 24h после сигнала) каждые 5 мин
  - `healthcheck_loop`: проверка каналов каждый час
- Telegram-бот с 8 инлайн-кнопками (каналы, сигналы, цена, экспорт CSV и т.д.)
- Graceful shutdown через SIGINT/SIGTERM

## CSV экспорт

Непрерывный 5-минутный ценовой поток BTC с наложенными сигналами. Период: с 2025-07-01 по текущий момент (~65k строк).

```
timestamp,btc_price,signal_count,signal_1_channel,signal_1_value,signal_1_color,signal_1_direction,...signal_5_*
2025-07-01 00:00,107126.37,0,,,,,,,,,,,,,,,,,,,,
2025-07-01 00:55,107347.64,1,SellsPowerIndex,55.0,,,,,,,,,,,,,,,,,,
2025-07-01 02:55,107129.78,2,AltSPI,55.8,,,SellsPowerIndex,60.0,,,,,,,,,,,,,,
```

- Каждая строка = одна 5-минутная свеча
- `signal_count` = 0 если нет сигналов в этом окне
- До 5 сигналов на строку (`signal_1_*` ... `signal_5_*`)
- Timestamp сигнала округляется до ближайших 5 минут
- `btc_price` — 2 знака, `signal_value` — 1 знак

## Запуск

```bash
# Установка зависимостей
pip install -r requirements.txt

# Первый запуск: прогрев peer-кэша (нужно один раз после свежей сессии)
python3 fix_peers.py

# Запуск бота
python3 main.py

# Запуск в фоне
nohup python3 main.py > /dev/null 2>&1 &

# Через systemd
sudo cp btc-signal-bot.service /etc/systemd/system/
sudo systemctl enable btc-signal-bot
sudo systemctl start btc-signal-bot
```

### Перезапуск

```bash
# Найти PID
ps aux | grep 'python3 main.py' | grep -v grep

# Остановить
kill <PID>

# Подождать 2 сек, запустить заново
sleep 2 && cd /home/s.riashchikow/btc-signal-bot && nohup python3 main.py > /dev/null 2>&1 &
```

### Перепарсинг (без перескачивания)

Через Telegram-бота: кнопка "Перепарсить канал" — перепарсит сообщения с `is_parsed=0` или `NULL`.

Для полного перепарсинга: удалить записи из `sync_log` для канала, удалить `signals` и перезапустить бот.

## API ключи (.env)

```
API_ID=           # Telegram API ID (my.telegram.org)
API_HASH=         # Telegram API Hash
PHONE=            # Номер телефона для userbot (+XXXXXXXXXXX)
BOT_TOKEN=        # Токен Telegram-бота (@BotFather)
ADMIN_USER_ID=    # Telegram user ID администратора

CHANNEL_1=        # AltSwing (числовой ID)
CHANNEL_2=        # DiamondMarks
CHANNEL_3=        # SellsPowerIndex
CHANNEL_4=        # AltSPI
CHANNEL_5=        # Scalp17
CHANNEL_6=        # Index
CHANNEL_7=        # DMI_SMF

IMBA_GROUP_ID=    # DyorAlerts (группа, filter_author=dyor_alerts_EtH_2_O_bot)
BFS_GROUP_ID=     # RSI_BTC (группа/канал @username)
BFS_BTC_TOPIC_ID= # ID темы для RSI_BTC (0 = фильтр по BTCUSDT в тексте)
```

## Backtesting

Количественный бэктестинг 9 каналов. Только pandas/numpy/scipy, без ML.

```bash
python3 -m backtesting.analyze
# Выход: backtesting/report.txt + backtesting/results.json
# Время: ~14 сек
```

### Параметры
- **FEE_RATE**: 0.001 (0.1% per side, 0.2% round-trip) — применяется ко всем метрикам
- **IS/OOS**: 70%/30% по времени (split_timestamp), OVERFITTED если OOS_Sharpe < IS_Sharpe × 0.5
- **Direction derivation**: для каналов без `signal_direction` (AltSwing, Scalp17, SellsPowerIndex, AltSPI) направление выводится из `indicator_value` через пороги

### 11 модулей
Все модули имеют сигнатуру `run(df_signals, df_prices, df_context, fee_rate=0.001) -> dict`.

1. **channel_stats** — win rate, avg return, PF, Sharpe, Sortino по 5 горизонтам (gross и net)
2. **confluence** — группировка сигналов разных каналов в 30-мин окне, сравнение single vs multi
3. **optimal_params** — grid search TP∈[0.2,3.0]×SL∈[0.2,3.0]×threshold∈[30,80], walk-forward валидация. Numpy vectorized: 2D pct-matrix + precomputed SL first-hits
4. **time_patterns** — по часам UTC, сессиям (Asia/Europe/US/Off), дням недели
5. **risk_metrics** — isolated + portfolio (max 1 позиция на канал), equity curve, max drawdown, Calmar, Kelly
6. **sequences** — макс. серии, Wald-Wolfowitz runs test, условная WR после серий
7. **mfe_mae** — vectorized через `np.searchsorted` + 2D indexing, чанки 5000/2000 сигналов
8. **market_regimes** — rolling 24h vol terciles + 4h trend buckets, `pd.merge_asof`
9. **correlations** — temporal + return корреляция, diversification score
10. **latency_decay** — задержки [0,1,3,5,10] мин, линейная регрессия decay rate, half-life
11. **monte_carlo** — 1000 direction shuffles + timestamp shuffles, p-value, z-score

### Ключевые решения
- `_build_pct_matrix()` — сборка 2D матрицы (n_signals × 1440) сразу для всех сигналов канала, переиспользуется при переборе порогов
- `_search_tpsl()` — precompute всех `sl_first` для 15 SL значений, затем TP loop × SL lookup (30 boolean scans вместо 225)
- MFE/MAE чанками: CHUNK_SIZE=5000 для 60m, 2000 для 1440m (~30 MB/chunk)

## Lessons Learned

1. **Парсеры не должны полагаться на заголовки каналов.** 7 из 9 парсеров изначально проверяли наличие текста типа "AltSwing", "Scalp17", "Sells Power Index" в сообщениях — но реальные сообщения содержат только данные (эмодзи + числа), без заголовков. Каждый парсер вызывается только для своего канала, поэтому проверка заголовка избыточна.

2. **Case-sensitive сравнение строк в Python.** `'Index' in 'INDEX 15min'` возвращает `False`. Канал Index отправлял `INDEX` (caps), парсер искал `Index` — 0% распознавания.

3. **`filter_author` должен содержать точный username бота.** DyorAlerts фильтровал по `"dyor_alerts"`, но реальный бот — `"dyor_alerts_EtH_2_O_bot"`. Все 10743 сообщений молча пропускались.

4. **Validation ranges должны соответствовать реальным данным.** AltSPI может выдавать >100% (Market Av. 111.7%), Scalp17 — >100% и отрицательные значения. Узкие диапазоны [0, 100] отсекали легитимные сигналы.

5. **Pyrogram peer cache.** Свежая сессия не может резолвить числовые channel_id. Нужен прогрев через `get_dialogs()` — скрипт `fix_peers.py`.

6. **`cursor.lastrowid` ненадёжен с INSERT OR IGNORE.** При дубликате `lastrowid` возвращает stale значение от предыдущего INSERT. Использовать `cursor.rowcount > 0` для проверки вставки.

7. **Pyrogram `stop()` бросает RuntimeError при `asyncio.run()`.** Косметический баг — dispatcher пытается остановить задачи из другого event loop. Не влияет на работу, данные сохранены.

8. **Каналы могут возвращать 0 сообщений при первой попытке.** AltSwing и DiamondMarks вернули 0 при первом скачивании, но 10296 и 924 при повторном. Причина неизвестна — возможно, Telegram API rate limiting или кэширование.
