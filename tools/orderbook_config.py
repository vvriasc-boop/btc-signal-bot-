"""Channel definitions, pair mappings, and constants for orderbook analysis."""

# 7 bid/ask pairs: (bid_title, ask_title, pair_label)
PAIRS = [
    ("Fresh B UL S",         "Fresh A UL S",         "UltraLight_Spot"),
    ("Fresh BID light spot", "Fresh ASK light spot",  "Light_Spot"),
    ("Fresh BID light F",    "Fresh ASK light F",     "Light_Futures"),
    ("Fresh BID",            "Fresh ASK",             "Medium_Spot"),
    ("Fresh BID futures",    "Fresh ASK futures",     "Medium_Futures"),
    ("Fresh BID MEGA",       "Fresh ASK MEGA",        "Mega_Futures"),
    ("Fresh BID MEGA spot",  "Fresh ASK MEGA spot",   "Mega_Spot"),
]

SPECIAL_CHANNELS = ["Dyor signal", "Long Bid F", "Short Ask F", "SHORT ONLY"]

# All 18 channel titles
ALL_TITLES = []
for bid_t, ask_t, _ in PAIRS:
    ALL_TITLES.extend([bid_t, ask_t])
ALL_TITLES.extend(SPECIAL_CHANNELS)

# Synthetic channel_id: deterministic, no collision with real IDs or CSV import
SYNTHETIC_ID_BASE = -1000


def channel_id_for(title: str) -> int:
    """Deterministic synthetic channel_id for a title."""
    try:
        idx = ALL_TITLES.index(title)
    except ValueError:
        idx = abs(hash(title)) % 10000
    return SYNTHETIC_ID_BASE - idx


# Bid/Ask side inference from channel title
def infer_side(title: str) -> str:
    """Return 'bid' or 'ask' based on channel title."""
    tl = title.lower()
    if "short" in tl:
        return "ask"
    if "long" in tl:
        return "bid"
    for kw in ["bid", " b "]:
        if kw in tl:
            return "bid"
    for kw in ["ask", " a "]:
        if kw in tl:
            return "ask"
    return "unknown"


# ---- H1 Imbalance constants ----
H1_WINDOW_SEC = 300          # 5 minutes
H1_THRESHOLDS = [1, 2, 3, 5]
H1_HORIZONS = {"5m": 5, "15m": 15, "1h": 60, "4h": 240}

# ---- H2 Levels constants ----
H2_ZONE_WIDTHS = [0.1, 0.2, 0.3]       # %
H2_LIFETIME_H = 24
H2_MIN_STRENGTHS = [1, 2, 3]
H2_ENTRY_HORIZONS = {"15m": 15, "1h": 60, "4h": 240}
H2_ENTRY_SEP_SEC = 3600                  # min 1h between entries at same level
H2_BREAKOUT_PCT = 0.3                    # breakout = price passes > 0.3% past level

# H2: channel sets to test (lower frequency = potentially stronger levels)
H2_CHANNEL_SETS = {
    "Medium": ["Fresh BID", "Fresh ASK", "Fresh BID futures", "Fresh ASK futures"],
    "Mega": ["Fresh BID MEGA", "Fresh ASK MEGA", "Fresh BID MEGA spot",
             "Fresh ASK MEGA spot"],
    "Medium+Mega": ["Fresh BID", "Fresh ASK", "Fresh BID futures", "Fresh ASK futures",
                    "Fresh BID MEGA", "Fresh ASK MEGA", "Fresh BID MEGA spot",
                    "Fresh ASK MEGA spot"],
}

# ---- Common ----
FEE_RATE = 0.001            # 0.1% per side
FEE_PCT = FEE_RATE * 2 * 100  # 0.2% round-trip
IS_RATIO = 0.70
MIN_TRADES = 20
ANN_FACTOR = 8760
