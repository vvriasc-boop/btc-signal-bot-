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
│   ├── report_builder.py    — report.txt + results.json
│   ├── deep_analysis.py     — streak strategy, contrarian signals, DMI_SMF deep dive
│   ├── portfolio_sim.py     — точка входа portfolio simulation
│   ├── sim_engine.py        — streak-фильтр + симуляция портфеля
│   ├── sim_metrics.py       — метрики портфеля (Sharpe, Sortino, DD, Kelly)
│   ├── sim_report.py        — portfolio_report.txt + equity_curve.csv + JSON
│   ├── dmi_range_test.py    — тест гипотезы: кластеры сигналов + узкий диапазон цены
│   ├── zone_test.py         — тест гипотезы: провалившиеся сигналы как S/R зоны
│   └── import_csv_signals.py — импорт CSV (Total alert + BTC low) + бэктестинг
├── tools/
│   ├── __init__.py
│   ├── orderbook_analysis.py   — точка входа: download + parse + H1/H2 анализ
│   ├── orderbook_config.py     — 7 bid/ask пар, 4 спецканала, константы
│   ├── orderbook_download.py   — скачивание 18 каналов через Pyrogram
│   ├── orderbook_parsers.py    — парсеры: стандартный + 4 спецканала
│   ├── orderbook_db.py         — insert signals, fill price_context
│   ├── orderbook_h1_imbalance.py — H1: bid/ask дисбаланс в 5-мин окнах
│   ├── orderbook_h2_levels.py  — H2: лимитки как S/R уровни
│   └── orderbook_report.py     — orderbook_report.txt + orderbook_results.json
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

DyorAlerts распознаёт 7 типов: `buyer_disbalance`, `seller_disbalance`, `long_priority` (1-4), `short_signal`, `long_signal`, `balance`, `unknown`.

## Фазы работы

**Phase 0**: Загрузка 1-мин свечей BTC/USDT с Binance → `price_index` dict `{minute_key: price}` для O(1) поиска.
**Phases 1-9**: Скачать → парсить → `signals`. Пропуск если `sync_log` phase='complete'. Для групп: `filter_author` + `topic_id`.
**Phase 10**: Заполнение `signal_price_context` (5m/15m/1h до, 5m/15m/1h/4h/24h после).
**Phase 11**: Live mode — Pyrogram handler + price_ticker (60s) + fill_delayed_prices (5m) + healthcheck (1h) + Telegram-бот (8 кнопок). Graceful shutdown.

## CSV экспорт

Непрерывный 5-минутный ценовой поток BTC с наложенными сигналами. Период: с 2025-07-01 (~65k строк). Формат: `timestamp,btc_price,signal_count,signal_1_channel,signal_1_value,signal_1_color,signal_1_direction,...signal_5_*`. До 5 сигналов на строку, timestamp округляется до 5 минут.

## Запуск

```bash
pip install -r requirements.txt
python3 fix_peers.py          # прогрев peer-кэша (один раз)
python3 main.py               # запуск бота
# или: nohup python3 main.py > /dev/null 2>&1 &
# или: sudo systemctl start btc-signal-bot
```

Перепарсинг: кнопка "Перепарсить канал" в Telegram-боте, или удалить `sync_log` + `signals` и перезапустить.

## .env

`API_ID`, `API_HASH`, `PHONE`, `BOT_TOKEN`, `ADMIN_USER_ID`, `CHANNEL_1`..`7`, `IMBA_GROUP_ID`, `BFS_GROUP_ID`, `BFS_BTC_TOPIC_ID`.

## Backtesting

Количественный бэктестинг 9 каналов. Только pandas/numpy/scipy, без ML.

```bash
python3 -m backtesting.analyze
# Выход: backtesting/report.txt + backtesting/results.json
# Время: ~14 сек (включая deep_analysis)
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

### Deep Analysis (deep_analysis.py)

Вызывается автоматически из `analyze.py`. 3 анализа: streak strategy (N побед → вход, M поражений → стоп), contrarian signals (инверсия WR<30% каналов), DMI_SMF deep dive (разбивка по фильтрам). Walk-forward + Monte Carlo.

```bash
python3 -m backtesting.deep_analysis  # → deep_report.txt + deep_results.json
```

### Portfolio Simulation (portfolio_sim.py)

Streak-фильтр + симуляция портфеля на OOS. 4 модуля: portfolio_sim (entry), sim_engine, sim_metrics, sim_report. Стратегии: DMI_SMF N=4/M=1, DyorAlerts N=2/M=1, Scalp17 N=5/M=1. Капитал $10K, позиции 1%/2%/5%/10%, горизонты 5m/15m/1h/4h.

```bash
python3 -m backtesting.portfolio_sim  # → portfolio_report.txt + equity_curve.csv + JSON
```

### Hypothesis Tests (standalone)

**dmi_range_test.py** — кластеры сигналов + узкий диапазон цены. **Результат: отвергнута.**
**zone_test.py** — провалившиеся сигналы как S/R зоны. **Результат: DyorAlerts слабый (N=31), Scalp17 no edge.**

```bash
python3 -m backtesting.dmi_range_test  # → dmi_range_report.txt + .json
python3 -m backtesting.zone_test       # → zone_report.txt + .json
```

### CSV Signal Import (import_csv_signals.py)

Standalone скрипт для импорта orderbook CSV-файлов и сравнения с 9 основными каналами:

```bash
python3 -m backtesting.import_csv_signals
# Выход: backtesting/csv_signals_report.txt + csv_signals_results.json
```

Импортирует `data/Total alert.csv` (6515 сигналов, bid/ask imbalance) и `data/BTC low.csv` (4283 сигнала, low/high liquidity). Sentinel channel_ids: TotalAlert=-100, BTCLow=-200.

### Orderbook Channel Analysis (tools/)

Анализ 18 каналов с orderbook-сигналами (7 bid/ask пар + 4 спецканала). 9 модулей, ≤500 строк каждый.

```bash
python3 -m tools.orderbook_analysis --download  # скачать + анализ
python3 -m tools.orderbook_analysis             # только анализ
# Выход: backtesting/orderbook_report.txt + orderbook_results.json
```

**18 каналов**: UltraLight Spot (B UL S / A UL S), Light Spot/Futures, Medium Spot/Futures (BID/ASK), Mega Spot/Futures (BID/ASK MEGA), Dyor signal, Long Bid F, Short Ask F, SHORT ONLY.

**Гипотеза 1 — Bid/Ask Imbalance**: подсчёт bid vs ask в 5-мин окнах, тест direct (дисбаланс→движение) vs inverse (спуфинг). Пороги [1,2,3,5], горизонты [5m,15m,1h,4h]. **Результат: NO EDGE** — все Sharpe глубоко отрицательные на OOS.

**Гипотеза 2 — Лимитки как S/R**: bid→support, ask→resistance. Zone widths [0.1,0.2,0.3]%, touch detection + breakout-return. Medium + Mega каналы. **Результат: NO EDGE** — OOS Sharpe отрицательные. Mega каналы пропущены (0% parsed — формат отличается от стандартного).

**Парсинг**: 14 стандартных каналов (`A/B BTC/USDT-S/F at X%, q: Y$, d: Z min`) — ~100% parsed. 4 спецканала (Short Ask F, SHORT ONLY, Long Bid F, MEGA) — 0% parsed (нестандартный формат). Dyor signal — 99.7%.

**Download**: ~30 мин на 18 каналов (~180K сообщений), FloodWait ~10-12s на батч. Бот должен быть остановлен (session lock).

## Lessons Learned

1. **Парсеры: не проверяй заголовки.** Каждый парсер вызывается только для своего канала, сообщения содержат только данные.
2. **Case-sensitive строки.** `'Index' in 'INDEX 15min'` = `False`. Всегда `.lower()`.
3. **`filter_author` = точный username.** `"dyor_alerts"` vs `"dyor_alerts_EtH_2_O_bot"` — все сообщения молча пропускались.
4. **Validation ranges = реальные данные.** AltSPI >100%, Scalp17 отрицательные. Не ставить [0, 100].
5. **Pyrogram peer cache.** Свежая сессия не резолвит channel_id. Прогрев: `fix_peers.py`.
6. **`cursor.lastrowid` + INSERT OR IGNORE.** При дубликате = stale. Проверять `cursor.rowcount > 0`.
7. **Каналы: 0 сообщений при первой попытке.** AltSwing/DiamondMarks: 0, потом тысячи. Повторять скачивание.
8. **FloodWait при массовом скачивании.** Pyrogram `get_chat_history` вызывает FloodWait ~10-12s на каждый батч 100 сообщений. 18 каналов × ~180K сообщений = ~30 мин. Бот должен быть остановлен (session file lock).
9. **Channels table: parser_type NOT NULL.** При INSERT в channels всегда указывать parser_type (`"orderbook"`, `"csv_import"` и т.д.), иначе IntegrityError.
