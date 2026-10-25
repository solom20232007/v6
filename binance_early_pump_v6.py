# ============================================================
# BINANCE EARLY-PUMP SCANNER V6.0 EXPERIMENTAL
# ============================================================
# القاعدة: scanner_v4 (أدق في الـscore والـgates) + إصلاح أخطاء النسخة المنشورة.
# V4.4.1 (إصلاح عطل كامل):
#  [إصلاح] TypeError: Invalid value 'SL' for dtype 'float64' — كان بيوقف
#    الدورة كلها. السبب: أعمدة outcome/lane الفاضية بتتقرا float64 وبانداس
#    بيرفض الكتابة النصية فيها. دلوقتي بنقرأها object.
#  [إصلاح] save_track لو فشل لإشارة واحدة مبقاش يقتل الدورة (try/except).
#  [إصلاح] إشعار TP1/SL مبقاش بيتخطى لو سعر التيكر ناقص مؤقتًا.
#  [إصلاح] COMPRESSED check بيمشي على كل النتائج مش أول 30 بس.
# V4.4:
#  [إصلاح] NameError في compressed_gate (كان بيوقف دورات المسح فعليًا)
#  [إصلاح] TRACK_FILE كان بيتمسح بعد 4 أيام -> بيضيّع بيانات التعلم الذاتي
#  [إصلاح] cleanup كان بيحلل 170k+ timestamp في كل دورة (بطيء) -> مقارنة نصية + كل 6 ساعات
#  [إصلاح] التتبع كان بيمشي ويشتغل بس لو BTC آمن -> بقى بيتحدّث دايمًا
#  [إصلاح] سعر الدخول كان ممكن يبقى قديم (best من مسح سابق) -> سعر لحظي
#  [جديد ] تتبع النتيجة بمسار شموع 1m: أي واحد اتحقق الأول TP1 ولا SL (outcome / exit_pct)
#  [جديد ] lane لكل إشارة (STRICT/EXPLOSIVE/PERSISTENT/REVERSAL/SQUEEZE) للتعلّم الذاتي
#  [جديد ] إشعار تيليجرام لما إشارة تحقق TP1 أو SL
#  [جديد ] فلتر الحلال شغّال افتراضيًا (SHARIAH_FILTER=0 لإيقافه)
#  [جديد ] Funding fallback (Bybit) + تقليل حجم early_scan_diagnostics.csv
# ============================================================

import requests, time, math, csv, os, json, sys, logging, re, html
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from entry_quality_v6 import confirm_one_minute_entry

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scanner_v6.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("scanner")

# ---------- Binance APIs ----------
BASE_URLS = [
    "https://data-api.binance.vision",
    "https://api.binance.com",
]
FUTURES_BASE_URL = "https://fapi.binance.com"
STATE_FILE = "scanner_state_v6.json"
INTERVAL = "5m"
KLINES_LIMIT = 180
MAX_WORKERS = 16
MAX_PROCESS_PER_CYCLE = int(os.getenv('MAX_PROCESS_PER_CYCLE', '100'))

# ---------- Scan ----------
SCAN_EVERY_SECONDS = 300
MIN_24H_QUOTE_VOLUME = 2_000_000
MAX_24H_GAIN_PCT = 50.0
MIN_15M_GAIN = -3.0
MAX_15M_GAIN = 12.0
MIN_1H_GAIN = -12.0
MAX_1H_GAIN = 20.0

# ---------- BTC Market Guard ----------
BTC_CRASH_THRESHOLD_15M = -1.2
BTC_CRASH_THRESHOLD_1H = -2.5

# ---------- Wash Trading Detector ----------
WASH_MAX_PRICE_MOVE_FOR_BIG_VOL = 0.5
WASH_ACCEL_THRESHOLD = 5.0
WASH_VOL_1H_THRESHOLD = 6.0
WASH_VOL_15M_HARD_CAP = 18.0

# ---------- FINAL gate ----------
FINAL_MIN_15M_MOVE = 1.25
FINAL_MIN_5M_MOVE = 0.25
FINAL_MIN_VOLUME_15M = 4.0
FINAL_MIN_VOLUME_1H = 2.0
FINAL_MIN_ACCELERATION = 1.2
FINAL_MAX_ACCELERATION = 4.5
FINAL_MIN_24H_VOLUME = 2_000_000
FINAL_MAX_24H_GAIN = 22.0
FINAL_MAX_RSI = 78.0
FINAL_MIN_BUY_PRESSURE = 0.55
FINAL_MIN_GREEN_STREAK = 2
FINAL_MIN_TREND_CONTINUITY = 3

# ---------- V6 Fake-breakout filter ----------
FAKE_BREAKOUT_FILTER_ENABLED = os.getenv("FAKE_BREAKOUT_FILTER_ENABLED", "1") == "1"
FAKE_BREAKOUT_MIN_CLOSE_LOCATION = float(os.getenv("FAKE_BREAKOUT_MIN_CLOSE_LOCATION", "0.45"))
FAKE_BREAKOUT_MIN_UPPER_WICK = float(os.getenv("FAKE_BREAKOUT_MIN_UPPER_WICK", "0.50"))
FAKE_BREAKOUT_MAX_DISTANCE_PCT = float(os.getenv("FAKE_BREAKOUT_MAX_DISTANCE_PCT", "2.0"))

# ---------- EXPLOSIVE EARLY ----------
EXPLOSIVE_MIN_5M_MOVE = 1.0
EXPLOSIVE_MIN_15M_MOVE = 0.8
EXPLOSIVE_MIN_VOLUME_15M = 4.0
EXPLOSIVE_MAX_VOLUME_15M = 12.0
EXPLOSIVE_MIN_VOLUME_1H = 2.0
EXPLOSIVE_MIN_ACCELERATION = 1.5
EXPLOSIVE_MAX_ACCELERATION = 4.0
EXPLOSIVE_MIN_BUY_PRESSURE = 0.58
EXPLOSIVE_MIN_24H_VOLUME = 2_000_000
EXPLOSIVE_MAX_24H_GAIN = 25.0
EXPLOSIVE_MAX_1H_GAIN = 10.0
EXPLOSIVE_MAX_RSI = 76.0
EXPLOSIVE_MAX_UPPER_WICK = 0.40
EXPLOSIVE_MIN_BREAKOUT = -1.50
EXPLOSIVE_MIN_GREEN_STREAK = 3
EXPLOSIVE_MIN_TREND_CONTINUITY = 4

# ---------- Persistent lane ----------
PERSISTENT_FINAL_SCORE = 70
PERSISTENT_MIN_5M_MOVE = 0.70
PERSISTENT_MIN_15M_MOVE = 1.25
PERSISTENT_MIN_VOLUME_15M = 2.50
PERSISTENT_MIN_VOLUME_1H = 1.50
PERSISTENT_MIN_PERSISTENCE = 2.00
PERSISTENT_MIN_BUY_PRESSURE = 0.54
PERSISTENT_MAX_1H_GAIN = 18.0
PERSISTENT_MAX_24H_GAIN = 30.0
PERSISTENT_MAX_RSI = 80.0
PERSISTENT_MAX_UPPER_WICK = 0.45
PERSISTENT_MIN_BREAKOUT = -1.25
PERSISTENT_MIN_GREEN_STREAK = 2
PERSISTENT_MIN_TREND_CONTINUITY = 3

# ---------- Reversal lane ----------
REVERSAL_FINAL_SCORE = 72
REVERSAL_MIN_5M_MOVE = 0.50
REVERSAL_MIN_15M_MOVE = 1.25
REVERSAL_MIN_VOLUME_15M = 3.00
REVERSAL_MIN_VOLUME_1H = 1.70
REVERSAL_MIN_BUY_PRESSURE = 0.54
REVERSAL_MIN_24H_VOLUME = 2_000_000
REVERSAL_MAX_24H_GAIN = 22.0
REVERSAL_MAX_RSI = 78.0
REVERSAL_MAX_UPPER_WICK = 0.45
REVERSAL_MIN_BREAKOUT = -1.50
REVERSAL_MIN_GREEN_STREAK = 2
REVERSAL_MIN_TREND_CONTINUITY = 3

# ---------- Short Squeeze lane ----------
SQUEEZE_FINAL_SCORE = 70
SQUEEZE_MAX_FUNDING = -0.0005
SQUEEZE_MIN_15M_MOVE = 1.0
SQUEEZE_MIN_VOLUME_15M = 2.5
SQUEEZE_MIN_BUY_PRESSURE = 0.55
SQUEEZE_MAX_24H_GAIN = 20.0

STABLE_BASES = {
    "USDT", "USDC", "FDUSD", "TUSD", "USDP", "DAI",
    "USDE", "USD1", "RLUSD", "USDD", "EUR", "EURI", "USTC",
}
MEGA_CAP_BASES = {"BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE"}

# ---------- Risk model ----------
RISK_MIN_SL_PCT = 3.5
RISK_MAX_SL_PCT = 12.0
RISK_ATR_MULT_SL = 1.5
RISK_RR_TP1 = 1.5
RISK_RR_TP2 = 3.0
RISK_HIGH_ATR_THRESHOLD = 6.0
RISK_HIGH_ATR_TP1_RR = 1.2
RISK_HIGH_RSI_THRESHOLD = 76.0
RISK_HIGH_RSI_TP1_RR = 1.0
RISK_LOW_LIQ_TP1_RR = 1.0

# ---------- Scores ----------
WATCH_SCORE = 65
FINAL_SCORE = 80
EXCEPTIONAL_SCORE = 88
CONFIRMATIONS_REQUIRED = 2
CANDIDATE_EXPIRY_MINUTES = 45
FINAL_COOLDOWN_HOURS = 12
MIN_CANDIDATE_SCORE = 68

# ---------- Cross-Exchange ----------
BYBIT_BASE = "https://api.bybit.com"
OKX_BASE = "https://www.okx.com"
CROSS_CONFIRM_AGREE_PCT = 1.0
CROSS_CONFIRM_DISAGREE_PCT = -1.0
SUPPRESS_ON_DIVERGENCE = False
CROSS_CACHE_TTL_SECONDS = 300

# ---------- V6 Smart Money layer ----------
SMART_MONEY_ENABLED = os.getenv("SMART_MONEY_ENABLED", "1") == "1"
SMART_MONEY_TOP_N = int(os.getenv("SMART_MONEY_TOP_N", "30"))
SMART_MONEY_WEIGHT = float(os.getenv("SMART_MONEY_WEIGHT", "0.30"))
FAST_EXPLOSIVE_ENABLED = os.getenv("FAST_EXPLOSIVE_ENABLED", "1") == "1"
FAST_EXPLOSIVE_MIN_VOLUME_15M = 7.0
FAST_EXPLOSIVE_MIN_BUY_PRESSURE = 0.62
FAST_EXPLOSIVE_MIN_ACCELERATION = 2.0
ORDERBOOK_DEPTH_LIMIT = 20
SMART_DATA_CACHE_TTL = 240
smart_data_cache = {}

SCAN_DIAGNOSTICS_FILE = "early_scan_diagnostics_v6.csv"

# ---------- OKX Lead Watcher ----------
OKX_LEAD_MIN_1H_CHG = 2.5
OKX_LEAD_MIN_4H_CHG = 5.0
OKX_LEAD_MIN_PREMIUM = 1.0
OKX_LEAD_MAX_24H_GAIN = 20.0
OKX_LEAD_MIN_24H_VOLUME = 2_000_000
OKX_LEAD_COOLDOWN_HOURS = 4

# ---------- Pre-Pump Compressed ----------
COMPRESSED_MAX_BREAKOUT = -6.0
COMPRESSED_MIN_GAIN_4H = 0.25
COMPRESSED_MAX_GAIN_4H = 3.0
COMPRESSED_MIN_VOL_1H = 1.4
COMPRESSED_MIN_VOL_15M = 1.2
COMPRESSED_MAX_15M_GAIN = 2.5
COMPRESSED_MAX_24H_GAIN = 20.0
COMPRESSED_MAX_RSI = 55.0
COMPRESSED_COOLDOWN_HOURS = 4

# ---------- Tracking ----------
TRACK_MINUTES = [5, 15, 30, 60, 240, 1440]
TRACK_FILE = "early_pump_signal_results_v6.csv"
TRADE_TIMEOUT_MIN = 240      # لو لا TP1 ولا SL خلال 4 ساعات -> TIMEOUT (خروج بالسعر وقتها)
NOTIFY_OUTCOMES = True       # تيليجرام لما إشارة تحقق TP1 أو SL
COMPRESSED_TELEGRAM = False  # إنذار Compressed: تسجيل فقط (زي نسخة v3 الأصلية)
TRACK_RETENTION_DAYS = 365   # سجل النتائج = بيانات التعلم الذاتي، ماينفعش يتمسح بعد 4 أيام
EARLY_WATCH_LOG_FILE = "early_watch_log_v6.csv"
EARLY_WATCH_COOLDOWN_HOURS = 2

# ---------- CSV retention ----------
LOG_RETENTION_DAYS = 4
LOG_CLEANUP_INTERVAL_HOURS = 6

# ---------- Shariah ----------
SHARIAH_CACHE_FILE = "shariah_compliant_bases_v6.json"
SHARIAH_UPDATE_HOURS = 24
SHARIAH_SOURCE_URL = "https://cryptoummah.com/halal-crypto-list"
SHARIAH_SOURCE_API = "https://gethalalcrypto.com/api/datasets/halal-coins?status=halal"

# ---------- Telegram ----------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# ---------- Self-learning thresholds (من self_learn.py) ----------
# قائمة بيضاء: self_learn مسموح له يغيّر الثوابت دي بس (مهما كان محتوى الملف)
AUTO_TUNABLE = {
    "FINAL_SCORE", "PERSISTENT_FINAL_SCORE", "REVERSAL_FINAL_SCORE", "SQUEEZE_FINAL_SCORE",
    "FINAL_MIN_BUY_PRESSURE", "FINAL_MIN_VOLUME_15M", "FINAL_MIN_ACCELERATION",
    "EXPLOSIVE_MIN_BUY_PRESSURE", "EXPLOSIVE_MIN_ACCELERATION",
}
AUTO_LEARN_ENABLED = os.getenv("AUTO_LEARN", "1") != "0"


def load_auto_thresholds():
    """تحميل تعديلات العتبات التي يولدها self_learn.py (state/auto_thresholds.json)."""
    if not AUTO_LEARN_ENABLED:
        return
    path = os.path.join("state", "auto_thresholds.json")
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            overrides = json.load(f)
        g = globals()
        applied = []
        for key, val in overrides.items():
            if key not in AUTO_TUNABLE or key not in g:
                continue
            if not isinstance(val, (int, float)) or isinstance(val, bool) or not math.isfinite(val):
                continue
            old = g[key]
            g[key] = int(round(val)) if isinstance(old, int) else float(val)
            applied.append(f"{key}: {old} -> {g[key]}")
        if applied:
            log.info("Self-learning: " + " | ".join(applied))
    except Exception as e:
        log.warning(f"auto thresholds load failed: {e}")


PROGRAM_TAG = "🤖 [V6]"
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    log.warning("TELEGRAM_BOT_TOKEN أو TELEGRAM_CHAT_ID غير موجودين — سيعمل بدون إرسال.")

session = requests.Session()
session.headers.update({"User-Agent": "Binance-Early-Pump-Scanner-V6.0/1.0"})

# ---------- Global state ----------
candidates = {}
final_cooldown = {}
active_tracks = {}
early_watch_logged = {}
cross_cache = {}
okx_lead_alerted = {}
compressed_alerted = {}
diagnostics_last_logged = {}
funding_rates_cache = {}
current_tickers = {}
meta = {}
DIAGNOSTIC_COOLDOWN_MINUTES = 15
SHARIAH_DIAGNOSTIC_COOLDOWN_HOURS = 24


# ============================================================
# HTTP
# ============================================================

def get_json(path, params=None, retries=3):
    last_err = None
    for base in BASE_URLS:
        for attempt in range(retries):
            try:
                r = session.get(base + path, params=params, timeout=10)
                if r.status_code == 451:
                    last_err = requests.HTTPError(f"451 from {base}", response=r)
                    break
                if 500 <= r.status_code < 600:
                    time.sleep(1 + attempt)
                    continue
                if r.status_code in (418, 429):
                    wait = 2 ** attempt
                    log.warning(f"Rate limit {r.status_code} من {base} — انتظار {wait}s")
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                return r.json()
            except requests.RequestException as e:
                last_err = e
                time.sleep(1 + attempt)
                continue
    raise last_err if last_err else RuntimeError("no base url worked")


def telegram_send(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = session.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=15)
        r.raise_for_status()
        return True
    except Exception as e:
        log.error(f"Telegram error: {e}")
        return False


# ============================================================
# BTC Market Guard + Funding Rate
# ============================================================

def check_btc_market_health():
    try:
        klines = get_json("/api/v3/klines", {"symbol": "BTCUSDT", "interval": "15m", "limit": 8})
        if not klines or len(klines) < 5:
            return True, 0.0, 0.0
        closes = [float(x[4]) for x in klines]
        btc_15m = (closes[-1] / closes[-2] - 1.0) * 100.0
        btc_1h = (closes[-1] / closes[-5] - 1.0) * 100.0
        is_safe = not (btc_15m <= BTC_CRASH_THRESHOLD_15M or btc_1h <= BTC_CRASH_THRESHOLD_1H)
        return is_safe, btc_15m, btc_1h
    except Exception as e:
        log.debug(f"BTC health check failed: {e}")
        return True, 0.0, 0.0


def _add_funding(rates, sym, val):
    try:
        v = float(val)
    except (TypeError, ValueError):
        return
    rates[sym] = v
    if sym.startswith("1000") and len(sym) > 8:      # 1000PEPEUSDT -> PEPEUSDT
        rates.setdefault(sym[4:], v)


def fetch_funding_rates_bulk():
    """Binance futures أولًا (غالبًا 451 على سيرفرات GitHub الأمريكية) ثم Bybit كبديل."""
    global funding_rates_cache
    rates = {}
    try:
        r = session.get(f"{FUTURES_BASE_URL}/fapi/v1/premiumIndex", timeout=10)
        r.raise_for_status()
        for x in r.json():
            sym = x.get("symbol", "")
            if sym.endswith("USDT"):
                _add_funding(rates, sym, x.get("lastFundingRate", 0.0))
        src_name = "Binance"
    except Exception as e:
        log.warning(f"Funding (Binance) failed: {e}")
        src_name = None
    if not rates:
        try:
            r = session.get(f"{BYBIT_BASE}/v5/market/tickers", params={"category": "linear"}, timeout=10)
            r.raise_for_status()
            for x in (r.json().get("result", {}) or {}).get("list", []) or []:
                sym = x.get("symbol", "")
                if sym.endswith("USDT") and x.get("fundingRate") not in (None, ""):
                    _add_funding(rates, sym, x.get("fundingRate"))
            src_name = "Bybit"
        except Exception as e:
            log.warning(f"Funding (Bybit) failed: {e}")
    if rates:
        funding_rates_cache = rates
        log.info(f"Funding rates fetched from {src_name}: {len(rates)} symbols")
    else:
        log.warning("Funding rates unavailable — Short Squeeze lane معطّلة في هذه الدورة")
    return funding_rates_cache


# ============================================================
# Shariah list
# ============================================================

STABLECOIN_BASES = {
    "USDC", "FDUSD", "TUSD", "USDP", "DAI", "BUSD", "BFUSD",
    "USDE", "PYUSD", "EUR", "GBP", "AEUR", "USD1", "WBETH",
}
SHARIAH_COMPLIANT_BASES = {
    "BTC", "BNB", "ADA", "ETH", "XRP", "XLM", "ALGO", "AVAX",
    "LTC", "DOT", "MATIC", "XTZ", "SOL", "LINK", "ETC", "UNI",
    "ATOM", "FIL", "HNT", "ICP", "XMR", "NEAR", "THETA", "TON",
    "TRX", "SUI", "CTSI", "STRK", "W", "OGN", "MET",
}
# User-supplied approved base symbols; values are normalized and deduplicated.
SHARIAH_USER_APPROVED_BASES = {
    '1000CAT', 'AAVE', 'ACM', 'ADA', 'AGLD', 'ALGO', 'ALICE', 'ANKR',
    'APT', 'AR', 'ARB', 'ARPA', 'ASR', 'ATA', 'ATM', 'ATOM',
    'AVA', 'AVAX', 'AXS', 'BAND', 'BAT', 'BCH', 'BEAMX', 'BIO',
    'BNT', 'BTC', 'CELO', 'CELR', 'CFX', 'CHIP', 'CHR', 'CHZ',
    'CKB', 'COMP', 'COS', 'CRV', 'CTK', 'CTSI', 'CTXC', 'CVC',
    'DASH', 'DATA', 'DCR', 'DENT', 'DGB', 'DIA', 'DOGE', 'DOT',
    'DUSK', 'EGLD', 'ENJ', 'ENS', 'EOS', 'ERA', 'ERN', 'ETC',
    'ETH', 'EUR', 'FET', 'FIL', 'FIO', 'FIRO', 'FIS', 'FLOW',
    'FTM', 'FUN', 'GALA', 'GRT', 'GTC', 'HIVE', 'HOOK', 'HOT',
    'ICP', 'ICX', 'IMX', 'INJ', 'IOST', 'IOTA', 'IOTX', 'JUV',
    'KAIA', 'KAVA', 'KMD', 'LDO', 'LINA', 'LINK', 'LIT', 'LPT',
    'LRC', 'LSK', 'LTC', 'LTO', 'MAGIC', 'MANA', 'MANTA', 'MASK',
    'MBL', 'MDT', 'ME', 'MET', 'MINA', 'MTL', 'NEAR', 'NEO',
    'NFP', 'NIL', 'NULS', 'OCEAN', 'OG', 'OGN', 'ONE', 'ONG',
    'ONT', 'OP', 'ORDI', 'OXT', 'PAXG', 'PEPE', 'PHA', 'POND',
    'PSG', 'PUNSIX', 'QNT', 'QTUM', 'RENDER', 'RIF', 'RLC', 'ROSE',
    'RSR', 'RVN', 'S', 'SAND', 'SC', 'SEI', 'SFP', 'SKL',
    'SLP', 'SNX', 'SOL', 'STMX', 'STORJ', 'STPT', 'STRAX', 'STRK',
    'STX', 'SUI', 'SUSHI', 'SXP', 'TFUEL', 'THETA', 'TIA', 'TLM',
    'TRX', 'TWT', 'UTK', 'VET', 'VITE', 'VTHO', 'W', 'WAN',
    'WIF', 'XLM', 'XRP', 'XTZ', 'ZAKI', 'ZEC', 'ZEN', 'ZIL',
}
SHARIAH_FILTER_ENABLED = os.getenv("SHARIAH_FILTER", "1") != "0"


def _extract_symbols_from_halalcrypto(payload):
    rows = payload
    if isinstance(payload, dict):
        for key in ("data", "results", "assets", "coins", "rows"):
            if isinstance(payload.get(key), list):
                rows = payload[key]
                break
    if not isinstance(rows, list):
        return set()
    out = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status", row.get("verdict", ""))).strip().lower()
        if status and "halal" not in status:
            continue
        sym = row.get("symbol") or row.get("ticker") or row.get("base")
        if isinstance(sym, str):
            sym = sym.strip().upper()
            if sym and sym.isascii() and 1 <= len(sym) <= 20:
                out.add(sym)
    return out


def _extract_symbols_from_crypto_ummmah(text):
    out = set()
    clean = html.unescape(re.sub(r"<[^>]+>", " ", text))
    clean = re.sub(r"\s+", " ", clean)
    pat = re.compile(r"\b([A-Z][A-Z0-9]{1,14})\s*\|\s*[0-9]+(?:\.[0-9]+)?\s*\|\s*HALAL\b")
    for m in pat.finditer(clean):
        out.add(m.group(1).upper())
    pat2 = re.compile(r"\b([A-Z][A-Z0-9]{1,14})\s+Same category:.*?\|\s*Halal\b", re.S)
    for m in pat2.finditer(clean):
        out.add(m.group(1).upper())
    return out


def load_user_shariah_overrides():
    """Return deduplicated built-in approvals plus optional local JSON approvals."""
    approved = {str(x).strip().upper() for x in SHARIAH_USER_APPROVED_BASES if str(x).strip()}
    filename = "shariah_user_overrides.json"
    if not os.path.exists(filename):
        return approved
    try:
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
        vals = data.get("approved_symbols", []) if isinstance(data, dict) else []
        approved.update(str(x).strip().upper() for x in vals if isinstance(x, str) and x.strip())
    except Exception as e:
        log.warning(f"Shariah overrides load failed: {e}")
    return approved


def load_shariah_cache():
    if not os.path.exists(SHARIAH_CACHE_FILE):
        return set()
    try:
        with open(SHARIAH_CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(str(x).upper() for x in data.get("symbols", []) if isinstance(x, str))
    except Exception as e:
        log.warning(f"Shariah cache load failed: {e}")
        return set()


def save_shariah_cache(symbols, source):
    payload = {
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "symbols": sorted(symbols),
    }
    with open(SHARIAH_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def update_shariah_list(force=False):
    global SHARIAH_COMPLIANT_BASES
    cached = load_shariah_cache()
    now = datetime.now(timezone.utc)
    should_update = force or not os.path.exists(SHARIAH_CACHE_FILE)
    if not should_update:
        try:
            with open(SHARIAH_CACHE_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            ts = pd.to_datetime(d.get("updated_at_utc"), utc=True, errors="coerce")
            should_update = pd.isna(ts) or (now - ts.to_pydatetime()).total_seconds() >= SHARIAH_UPDATE_HOURS * 3600
        except Exception:
            should_update = True

    discovered = set()
    source = "cache/fallback"
    if should_update:
        try:
            r = session.get(SHARIAH_SOURCE_API, timeout=20)
            r.raise_for_status()
            discovered = _extract_symbols_from_halalcrypto(r.json())
            if discovered:
                source = SHARIAH_SOURCE_API
        except Exception as e:
            log.warning(f"Shariah API update failed: {e}")

        if not discovered:
            try:
                r = session.get(SHARIAH_SOURCE_URL, timeout=20)
                r.raise_for_status()
                discovered = _extract_symbols_from_crypto_ummmah(r.text)
                if discovered:
                    source = SHARIAH_SOURCE_URL
            except Exception as e:
                log.warning(f"Shariah web update failed: {e}")

        if discovered:
            save_shariah_cache(discovered, source)
            cached = discovered
            log.info(f"Shariah list updated: {len(discovered)} halal symbols from {source}")
        else:
            log.warning("Shariah update unavailable — using last cached/fallback list")

    if cached:
        SHARIAH_COMPLIANT_BASES = set(cached)
    SHARIAH_COMPLIANT_BASES.update(load_user_shariah_overrides())
    log.info(f"Shariah scan universe: {len(SHARIAH_COMPLIANT_BASES)} symbols (including user-approved overrides)")


def get_symbols():
    data = get_json("/api/v3/exchangeInfo")
    out = []
    shariah_exclusions = []
    now_iso = datetime.now(timezone.utc).isoformat()
    for s in data["symbols"]:
        if s.get("status") != "TRADING":
            continue
        if s.get("quoteAsset") != "USDT":
            continue
        if s.get("isSpotTradingAllowed") is False:
            continue
        sym = s["symbol"]
        if any(x in sym for x in ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")):
            continue
        base = sym[:-4]
        if base in STABLECOIN_BASES or base in MEGA_CAP_BASES:
            continue
        if SHARIAH_FILTER_ENABLED and base not in SHARIAH_COMPLIANT_BASES:
            shariah_exclusions.append({
                "logged_time_utc": now_iso, "symbol": sym,
                "stage": "SHARIAH_FILTER", "reason": "NOT_IN_COMPLIANT_LIST",
            })
            continue
        out.append(sym)
    append_scan_diagnostics(shariah_exclusions)
    return out


def get_tickers():
    data = get_json("/api/v3/ticker/24hr")
    out = {}
    for x in data:
        sym = x["symbol"]
        if not sym.endswith("USDT"):
            continue
        try:
            out[sym] = {
                "price": float(x["lastPrice"]),
                "change": float(x["priceChangePercent"]),
                "quote_volume": float(x["quoteVolume"]),
            }
        except Exception:
            pass
    return out


# ============================================================
# Math
# ============================================================

def pct(a, b):
    if b == 0:
        return 0.0
    return (a / b - 1) * 100.0


def mean(v):
    v = [x for x in v if math.isfinite(x)]
    return sum(v) / len(v) if v else 0.0


def ema(values, period):
    if len(values) < period:
        return None
    k = 2.0 / (period + 1)
    e = sum(values[:period]) / period
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e


def rsi(values, period=14):
    if len(values) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(values)):
        d = values[i] - values[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
    if al == 0:
        return 100.0
    rs = ag / al
    return 100.0 - (100.0 / (1.0 + rs))


def atr_pct(highs, lows, closes, period=14):
    if len(closes) < period + 1:
        return None
    trs = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs.append(tr)
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return (atr / closes[-1] * 100.0) if closes[-1] else None


def pct_slope(values):
    if len(values) < 2 or values[0] == 0:
        return 0.0
    return (values[-1] / values[0] - 1.0) * 100.0


# ============================================================
# Trend continuity + wash detector
# ============================================================

def trend_continuity_score(closes_5m, volumes_5m):
    if len(closes_5m) < 6:
        return 0, 0

    last_closes = closes_5m[-6:]
    last_vols = volumes_5m[-6:]

    changes = [pct(last_closes[i], last_closes[i - 1]) for i in range(1, len(last_closes))]
    green_streak = 0
    for c in reversed(changes):
        if c > 0:
            green_streak += 1
        else:
            break

    green_count = sum(1 for c in changes if c > 0)
    higher_lows = sum(
        1 for i in range(2, len(last_closes))
        if last_closes[i] > last_closes[i - 2]
    )

    vol_increasing = 0
    for i in range(1, len(last_vols)):
        if last_vols[i] >= last_vols[i - 1] * 0.85:
            vol_increasing += 1

    score = 0
    if green_count >= 4:
        score += 3
    elif green_count >= 3:
        score += 2
    if green_streak >= 3:
        score += 2
    if higher_lows >= 3:
        score += 2
    if vol_increasing >= 4:
        score += 1

    return min(score, 8), green_streak


def is_likely_wash_trading(x):
    v15 = x.get("volume_15m", 0)
    v1h = x.get("volume_1h", 0)
    accel = x.get("acceleration", 0)
    g5 = x.get("5m", 0)
    g15 = x.get("15m", 0)
    g1h = x.get("1h", 0)

    if v15 > WASH_VOL_15M_HARD_CAP:
        return True, "vol_15m شاذ (>18x)"

    if v15 > 10 and abs(g15) < WASH_MAX_PRICE_MOVE_FOR_BIG_VOL:
        return True, f"vol_15m={v15:.1f}x مع 15m={g15:+.2f}%"

    if accel > WASH_ACCEL_THRESHOLD and abs(g15) < 1.0:
        return True, f"accel={accel:.1f}x مع 15m={g15:+.2f}%"

    if v1h > WASH_VOL_1H_THRESHOLD and abs(g1h) < 1.0:
        return True, f"vol_1h={v1h:.1f}x مع 1h={g1h:+.2f}%"

    return False, ""


# ============================================================
# Risk model
# ============================================================

def compute_risk_levels(entry, atr_val, rsi_val, liquidity):
    if not entry or entry <= 0:
        return None
    atr_use = atr_val if (atr_val and atr_val > 0) else 4.0

    sl_pct_raw = atr_use * RISK_ATR_MULT_SL
    sl_pct = max(RISK_MIN_SL_PCT, min(sl_pct_raw, RISK_MAX_SL_PCT))

    tp1_rr = RISK_RR_TP1
    notes = []
    if atr_use > RISK_HIGH_ATR_THRESHOLD:
        tp1_rr = min(tp1_rr, RISK_HIGH_ATR_TP1_RR)
        notes.append(f"ATR مرتفع ({atr_use:.1f}%) → TP1 أقرب")
    if rsi_val is not None and rsi_val > RISK_HIGH_RSI_THRESHOLD:
        tp1_rr = min(tp1_rr, RISK_HIGH_RSI_TP1_RR)
        notes.append(f"RSI مرتفع ({rsi_val:.0f}) → TP1 أقرب")
    if liquidity == "LOW":
        tp1_rr = min(tp1_rr, RISK_LOW_LIQ_TP1_RR)
        notes.append("سيولة منخفضة → TP1 أقرب")

    tp2_rr = max(RISK_RR_TP2, tp1_rr + 0.5)

    sl_price = entry * (1 - sl_pct / 100.0)
    risk_per_unit = entry - sl_price
    tp1_price = entry + risk_per_unit * tp1_rr
    tp2_price = entry + risk_per_unit * tp2_rr

    return {
        "entry": entry,
        "sl": sl_price,
        "sl_pct": sl_pct,
        "tp1": tp1_price,
        "tp1_pct": pct(tp1_price, entry),
        "tp1_rr": tp1_rr,
        "tp2": tp2_price,
        "tp2_pct": pct(tp2_price, entry),
        "tp2_rr": tp2_rr,
        "atr_used": atr_use,
        "notes": notes,
    }
    

# ============================================================
# Analyze
# ============================================================

def analyze(symbol, ticker):
    try:
        klines = get_json(
            "/api/v3/klines",
            {"symbol": symbol, "interval": INTERVAL, "limit": KLINES_LIMIT},
        )
    except Exception as e:
        log.debug(f"analyze klines failed for {symbol}: {e}")
        return {"symbol": symbol, "_rejected": "KLINES_FETCH_ERROR"}

    if len(klines) < int(os.getenv('MIN_KLINES', '25')):
        return {"symbol": symbol, "_rejected": "INSUFFICIENT_KLINES"}
    new_listing_mode = len(klines) < 80

    k = klines[:-1]

    opens = [float(x[1]) for x in k]
    closes = [float(x[4]) for x in k]
    highs = [float(x[2]) for x in k]
    lows = [float(x[3]) for x in k]
    quote_volumes = [float(x[7]) for x in k]
    taker_buy = [float(x[10]) for x in k]

    price = closes[-1]
    if price <= 0:
        return {"symbol": symbol, "_rejected": "INVALID_PRICE"}

    gain_5m = pct(closes[-1], closes[-2])
    gain_15m = pct(closes[-1], closes[-4])
    gain_1h = pct(closes[-1], closes[-13])
    gain_4h = pct(closes[-1], closes[-49]) if len(closes) >= 49 else 0.0
    gain_24h = ticker["change"]
    qv24 = ticker["quote_volume"]

    if qv24 < MIN_24H_QUOTE_VOLUME:
        return {"symbol": symbol, "_rejected": "LOW_24H_QUOTE_VOLUME"}
    if gain_24h > MAX_24H_GAIN_PCT:
        return {"symbol": symbol, "_rejected": "24H_GAIN_ABOVE_MAX"}
    if gain_15m < MIN_15M_GAIN:
        return {"symbol": symbol, "_rejected": "15M_GAIN_BELOW_MIN"}
    if gain_15m > MAX_15M_GAIN:
        return {"symbol": symbol, "_rejected": "15M_GAIN_ABOVE_MAX"}
    if gain_1h < MIN_1H_GAIN:
        return {"symbol": symbol, "_rejected": "1H_GAIN_BELOW_MIN"}
    if gain_1h > MAX_1H_GAIN:
        return {"symbol": symbol, "_rejected": "1H_GAIN_ABOVE_MAX"}

    # Volume
    current_15m = sum(quote_volumes[-3:])
    historical_15m = [sum(quote_volumes[i - 3:i])
                      for i in range(3, len(quote_volumes) - 3 + 1, 3)]
    baseline_15m = sorted(historical_15m)[len(historical_15m)//2] if historical_15m else 0
    volume_ratio_15m = current_15m / baseline_15m if baseline_15m else 0

    current_1h = sum(quote_volumes[-12:])
    historical_1h = [sum(quote_volumes[i - 12:i])
                     for i in range(12, len(quote_volumes) - 12 + 1, 12)]
    baseline_1h = sorted(historical_1h)[len(historical_1h)//2] if historical_1h else 0
    volume_ratio_1h = current_1h / baseline_1h if baseline_1h else 0

    previous_15m = sum(quote_volumes[-6:-3])
    prev_prev_15m = sum(quote_volumes[-9:-6])
    acceleration = current_15m / previous_15m if previous_15m else 0
    prior_acceleration = previous_15m / prev_prev_15m if prev_prev_15m else 0.0

    # Breakout
    lookback = 48
    previous_high = max(highs[-(lookback + 1):-1])
    breakout_pct = pct(price, previous_high)

    # Candle
    candle_range = highs[-1] - lows[-1]
    close_location = (price - lows[-1]) / candle_range if candle_range else 0.5
    body_pct = pct(closes[-1], opens[-1]) if opens[-1] else 0.0
    upper_wick = highs[-1] - max(opens[-1], closes[-1])
    upper_wick_ratio = upper_wick / candle_range if candle_range else 0.0

    # Indicators
    ema9 = ema(closes, 9)
    ema21 = ema(closes, 21)
    ema50 = ema(closes, 50)
    rsi14 = rsi(closes, 14)
    atr14_pct = atr_pct(highs, lows, closes, 14)

    ema9_slope_15m = pct_slope(closes[-4:]) if len(closes) >= 4 else 0.0
    ema21_slope_1h = pct_slope(closes[-13:]) if len(closes) >= 13 else 0.0

    trend_bullish = ema9 is not None and ema21 is not None and price > ema9 > ema21
    trend_strong = trend_bullish and ema50 is not None and price > ema50

    # Buy pressure
    current_buy = sum(taker_buy[-3:])
    current_total = sum(quote_volumes[-3:])
    buy_pressure_15m = current_buy / current_total if current_total else 0.5

    prev_buy = sum(taker_buy[-6:-3])
    prev_total = sum(quote_volumes[-6:-3])
    prev_buy_pressure = prev_buy / prev_total if prev_total else 0.5
    buy_pressure_delta = buy_pressure_15m - prev_buy_pressure

    # Trend continuity
    trend_cont, green_streak = trend_continuity_score(closes, quote_volumes)

    # Wash trading
    is_wash, wash_reason = is_likely_wash_trading({
        "volume_15m": volume_ratio_15m,
        "volume_1h": volume_ratio_1h,
        "acceleration": acceleration,
        "5m": gain_5m,
        "15m": gain_15m,
        "1h": gain_1h,
    })

    # Funding rate (V4.3 — من cache)
    funding_rate = funding_rates_cache.get(symbol, 0.0)

    volume_persistence = min(volume_ratio_15m, volume_ratio_1h)
    recent_high_1h = max(highs[-13:-1])
    distance_from_1h_high = pct(price, recent_high_1h) if recent_high_1h else 0.0
    efficiency = abs(gain_15m) / max(volume_ratio_15m, 1.0)

    # ---------- SCORE ----------
    score = 0

    if volume_ratio_1h >= 6 and gain_15m >= 1.5:
        score += 16
    elif volume_ratio_1h >= 6 and gain_15m >= 0.5:
        score += 8
    elif volume_ratio_1h >= 4 and gain_15m >= 1.0:
        score += 12
    elif volume_ratio_1h >= 4:
        score += 5
    elif volume_ratio_1h >= 2.5:
        score += 7
    elif volume_ratio_1h >= 1.7:
        score += 4

    if volume_ratio_15m >= 10 and gain_15m >= 1.5:
        score += 10
    elif volume_ratio_15m >= 6 and gain_15m >= 1.0:
        score += 8
    elif volume_ratio_15m >= 3:
        score += 5
    elif volume_ratio_15m >= 2:
        score += 3

    if volume_persistence >= 3:
        score += 6
    elif volume_persistence >= 2:
        score += 4
    elif volume_persistence >= 1.5:
        score += 2

    if 1.3 <= acceleration <= 3.0 and prior_acceleration >= 1.2:
        score += 6
    elif 1.3 <= acceleration <= 3.0:
        score += 4
    elif acceleration > 4.5:
        score -= 5

    if 0.5 <= gain_15m <= 4:
        score += 12
    elif 0 <= gain_15m < 0.5:
        score += 8
    elif 4 < gain_15m <= 6:
        score += 6
    elif 6 < gain_15m <= 10:
        score += 1

    if 0.5 <= gain_1h <= 5:
        score += 8
    elif 0 <= gain_1h < 0.5:
        score += 5
    elif 5 < gain_1h <= 8:
        score += 4
    elif 8 < gain_1h <= 12:
        score += 1

    if 0.05 <= efficiency <= 1.25:
        score += 6
    elif efficiency <= 2:
        score += 2
    elif efficiency > 3:
        score -= 5

    if trend_strong:
        score += 7
    elif trend_bullish:
        score += 4
    elif ema9 is not None and ema21 is not None and ema9 < ema21:
        score -= 3

    if ema9_slope_15m > 0.3 and ema21_slope_1h > 0.5:
        score += 4
    elif ema9_slope_15m < -0.5 or ema21_slope_1h < -1.0:
        score -= 4

    if buy_pressure_15m >= 0.62 and buy_pressure_delta >= 0.02:
        score += 7
    elif buy_pressure_15m >= 0.56:
        score += 4
    elif buy_pressure_15m < 0.45:
        score -= 6

    score += trend_cont

    if -1.0 <= breakout_pct <= 1.5:
        score += 5
    elif 1.5 < breakout_pct <= 3:
        score += 2
    elif breakout_pct > 4:
        score -= 4
    elif breakout_pct < -3:
        score -= 2

    if upper_wick_ratio > 0.35 and body_pct < 0.5:
        score -= 5
    elif close_location >= 0.75 and body_pct > 0:
        score += 3

    if rsi14 is not None:
        if 50 <= rsi14 <= 68:
            score += 4
        elif 68 < rsi14 <= 76:
            score += 2
        elif rsi14 > 82:
            score -= 8
        elif rsi14 < 42:
            score -= 4

    if atr14_pct is not None:
        if 0.4 <= atr14_pct <= 5:
            score += 2
        elif atr14_pct > 8:
            score -= 3

    if 0 <= gain_24h <= 8:
        score += 4
    elif gain_24h < 0:
        score += 2
    elif gain_24h <= 14:
        score += 1
    elif gain_24h > 20:
        score -= 3

    if qv24 >= 20_000_000:
        liquidity = "HIGH"; score += 4
    elif qv24 >= 10_000_000:
        liquidity = "GOOD"; score += 3
    elif qv24 >= 5_000_000:
        liquidity = "MEDIUM"; score += 1
    else:
        liquidity = "LOW"

    if gain_15m > 7 or gain_1h > 10:
        score -= 8
    if gain_15m > 8.5 and gain_1h > 12:
        score -= 10

    if is_wash:
        score -= 25

    score = max(0, min(int(round(score)), 100))

    # ---------- SETUP ----------
    if is_wash:
        setup = "SUSPECT WASH"
    elif (volume_ratio_15m >= 5 and volume_ratio_1h >= 2 and 1.3 <= acceleration <= 4.0
          and 0.5 <= gain_15m <= 6 and gain_1h <= 8
          and buy_pressure_15m >= 0.54 and trend_cont >= 3
          and (rsi14 is None or rsi14 < 78)):
        setup = "SMART EARLY PUMP"
    elif (breakout_pct >= -1 and volume_ratio_1h >= 2
          and 1.3 <= acceleration <= 4.0 and gain_15m <= 6):
        setup = "BREAKOUT + VOLUME ACCELERATION"
    elif volume_ratio_1h >= 3 and volume_persistence >= 2:
        setup = "VOLUME EXPANSION"
    elif breakout_pct >= 0:
        setup = "BREAKOUT"
    else:
        setup = "EARLY MOMENTUM"

    explosive_probe = {
        "symbol": symbol, "5m": gain_5m, "15m": gain_15m, "1h": gain_1h,
        "24h": gain_24h, "volume_15m": volume_ratio_15m,
        "volume_1h": volume_ratio_1h, "acceleration": acceleration,
        "buy_pressure": buy_pressure_15m, "upper_wick_ratio": upper_wick_ratio,
        "breakout": breakout_pct, "rsi14": rsi14, "quote_volume": qv24,
        "green_streak": green_streak, "trend_continuity": trend_cont,
        "is_wash": is_wash, "funding_rate": funding_rate,
        "volume_persistence": volume_persistence,
    }
    if explosive_early_gate(explosive_probe):
        setup = "EXPLOSIVE EARLY"
    elif persistent_explosion_gate(explosive_probe):
        setup = "PERSISTENT EXPLOSION"
    elif squeeze_gate(explosive_probe):
        setup = "SHORT SQUEEZE"

    price_confirmation = (
        (gain_15m >= 1.0 and gain_5m >= 0.3)
        or (gain_1h >= 2.0 and gain_5m >= 0.2)
        or (breakout_pct >= 0.0 and close_location >= 0.90
            and volume_ratio_1h >= 4.0 and gain_15m >= 0.6)
    )
    continuation_quality = (
        buy_pressure_15m >= 0.52
        and (rsi14 is None or rsi14 < 80)
        and upper_wick_ratio < 0.50
        and not (gain_15m > 8 and gain_1h > 12)
        and not is_wash
    )

    risk = compute_risk_levels(price, atr14_pct, rsi14, liquidity)

    return {
        "symbol": symbol,
        "new_listing_mode": bool(new_listing_mode),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "price": price,
        "5m": gain_5m, "15m": gain_15m, "1h": gain_1h, "4h": gain_4h, "24h": gain_24h,
        "volume_1h": volume_ratio_1h,
        "volume_15m": volume_ratio_15m,
        "acceleration": acceleration,
        "prior_acceleration": prior_acceleration,
        "breakout": breakout_pct,
        "close_location": close_location,
        "score": score,
        "setup": setup,
        "quote_volume": qv24,
        "liquidity": liquidity,
        "ema9": ema9, "ema21": ema21, "ema50": ema50,
        "rsi14": rsi14,
        "atr_pct": atr14_pct,
        "buy_pressure": buy_pressure_15m,
        "buy_pressure_delta": buy_pressure_delta,
        "body_pct": body_pct,
        "upper_wick_ratio": upper_wick_ratio,
        "trend": "STRONG_UP" if trend_strong else ("UP" if trend_bullish else "NEUTRAL"),
        "distance_from_1h_high": distance_from_1h_high,
        "volume_persistence": volume_persistence,
        "efficiency": efficiency,
        "price_confirmation": price_confirmation,
        "continuation_quality": continuation_quality,
        "trend_continuity": trend_cont,
        "green_streak": green_streak,
        "is_wash": is_wash,
        "wash_reason": wash_reason,
        "funding_rate": funding_rate,
        "risk": risk,
    }


# ============================================================
# V6 Smart Money data
# ============================================================
def _cached_smart_data(symbol):
    cached = smart_data_cache.get(symbol)
    if cached and time.time() - cached.get("ts", 0) < SMART_DATA_CACHE_TTL:
        return cached
    data = {"ts": time.time(), "orderbook_imbalance": None,
            "oi_change_pct": None, "smart_money_score": 50.0}
    try:
        depth = get_json("/api/v3/depth", {"symbol": symbol, "limit": ORDERBOOK_DEPTH_LIMIT}, retries=1)
        bids = sum(float(row[0]) * float(row[1]) for row in depth.get("bids", [])[:ORDERBOOK_DEPTH_LIMIT])
        asks = sum(float(row[0]) * float(row[1]) for row in depth.get("asks", [])[:ORDERBOOK_DEPTH_LIMIT])
        if bids + asks > 0:
            data["orderbook_imbalance"] = (bids - asks) / (bids + asks)
    except Exception as exc:
        log.debug("V6 depth unavailable %s: %s", symbol, exc)
    try:
        # Binance Futures OI history is public; some spot symbols have no futures contract.
        response = session.get(FUTURES_BASE_URL + "/futures/data/openInterestHist",
                                   params={"symbol": symbol, "period": "15m", "limit": 2}, timeout=8)
        response.raise_for_status()
        rows = response.json()
        if isinstance(rows, list) and len(rows) >= 2:
            old = float(rows[-2].get("sumOpenInterestValue", 0) or 0)
            new = float(rows[-1].get("sumOpenInterestValue", 0) or 0)
            if old > 0:
                data["oi_change_pct"] = (new / old - 1.0) * 100.0
    except Exception as exc:
        log.debug("V6 open interest unavailable %s: %s", symbol, exc)
    smart = []
    # Bounded components avoid one extreme observation dominating the ranking.
    bp = float(current_smart_context.get(symbol, {}).get("buy_pressure", 0.5))
    vol = float(current_smart_context.get(symbol, {}).get("volume_15m", 1.0))
    accel = float(current_smart_context.get(symbol, {}).get("acceleration", 1.0))
    smart.append(max(0.0, min(100.0, (bp - 0.45) / 0.25 * 100.0)))
    smart.append(max(0.0, min(100.0, vol / 8.0 * 100.0)))
    smart.append(max(0.0, min(100.0, accel / 3.0 * 100.0)))
    if data["orderbook_imbalance"] is not None:
        smart.append(max(0.0, min(100.0, (data["orderbook_imbalance"] + 0.2) / 0.6 * 100.0)))
    if data["oi_change_pct"] is not None:
        smart.append(max(0.0, min(100.0, (data["oi_change_pct"] + 1.0) / 4.0 * 100.0)))
    data["smart_money_score"] = round(sum(smart) / len(smart), 2) if smart else 50.0
    smart_data_cache[symbol] = data
    return data


current_smart_context = {}

def enrich_smart_money(x):
    """Attach V6 market microstructure fields; missing futures/depth data stays neutral."""
    if not SMART_MONEY_ENABLED:
        x.update({"orderbook_imbalance": None, "oi_change_pct": None, "smart_money_score": 50.0})
        x["final_rank"] = float(x.get("score", 0))
        return x
    sym = x.get("symbol")
    current_smart_context[sym] = {
        "buy_pressure": x.get("buy_pressure", 0.5),
        "volume_15m": x.get("volume_15m", 1.0),
        "acceleration": x.get("acceleration", 1.0),
    }
    data = _cached_smart_data(sym)
    x.update({k: data.get(k) for k in ("orderbook_imbalance", "oi_change_pct")})
    components = [
        max(0.0, min(100.0, (float(x.get("buy_pressure", 0.5)) - 0.45) / 0.25 * 100.0)),
        max(0.0, min(100.0, float(x.get("volume_15m", 1.0)) / 8.0 * 100.0)),
        max(0.0, min(100.0, float(x.get("acceleration", 1.0)) / 3.0 * 100.0)),
    ]
    if x["orderbook_imbalance"] is not None:
        components.append(max(0.0, min(100.0, (float(x["orderbook_imbalance"]) + 0.2) / 0.6 * 100.0)))
    if x["oi_change_pct"] is not None:
        components.append(max(0.0, min(100.0, (float(x["oi_change_pct"]) + 1.0) / 4.0 * 100.0)))
    x["smart_money_score"] = round(sum(components) / len(components), 2)
    base = max(0.0, min(100.0, float(x.get("score", 0))))
    smart = float(x.get("smart_money_score", 50))
    x["final_rank"] = round((1.0 - SMART_MONEY_WEIGHT) * base + SMART_MONEY_WEIGHT * smart, 2)
    return x


def fast_explosive_eligible(x):
    return bool(
        FAST_EXPLOSIVE_ENABLED and not x.get("is_wash")
        and not x.get("new_listing_mode", False)
        and x.get("volume_15m", 0) >= FAST_EXPLOSIVE_MIN_VOLUME_15M
        and x.get("buy_pressure", 0) >= FAST_EXPLOSIVE_MIN_BUY_PRESSURE
        and x.get("acceleration", 0) >= FAST_EXPLOSIVE_MIN_ACCELERATION
        and x.get("score", 0) >= EXCEPTIONAL_SCORE
        and x.get("smart_money_score", 0) >= 65
        and (x.get("orderbook_imbalance") is None or x.get("orderbook_imbalance", 0) >= -0.05)
    )


# ============================================================
# Gates
# ============================================================

def persistent_explosion_gate(x):
    sym = x.get("symbol", "")
    base = sym[:-4] if sym.endswith("USDT") else sym
    if base in STABLE_BASES or x.get("is_wash"):
        return False
    rsi = x.get("rsi14")
    return bool(
        x.get("5m", -999) >= PERSISTENT_MIN_5M_MOVE
        and x.get("15m", -999) >= PERSISTENT_MIN_15M_MOVE
        and x.get("volume_15m", 0) >= PERSISTENT_MIN_VOLUME_15M
        and x.get("volume_1h", 0) >= PERSISTENT_MIN_VOLUME_1H
        and x.get("volume_persistence", 0) >= PERSISTENT_MIN_PERSISTENCE
        and x.get("buy_pressure", 0) >= PERSISTENT_MIN_BUY_PRESSURE
        and x.get("upper_wick_ratio", 1.0) < PERSISTENT_MAX_UPPER_WICK
        and x.get("breakout", -999) >= PERSISTENT_MIN_BREAKOUT
        and rsi is not None and rsi < PERSISTENT_MAX_RSI
        and x.get("1h", 999) <= PERSISTENT_MAX_1H_GAIN
        and x.get("24h", 999) <= PERSISTENT_MAX_24H_GAIN
        and x.get("green_streak", 0) >= PERSISTENT_MIN_GREEN_STREAK
        and x.get("trend_continuity", 0) >= PERSISTENT_MIN_TREND_CONTINUITY
        and x.get("quote_volume", 0) >= MIN_24H_QUOTE_VOLUME
    )


def reversal_explosion_gate(x):
    sym = x.get("symbol", "")
    base = sym[:-4] if sym.endswith("USDT") else sym
    if base in STABLE_BASES or x.get("is_wash"):
        return False
    rsi = x.get("rsi14")
    return bool(
        x.get("5m", -999) >= REVERSAL_MIN_5M_MOVE
        and x.get("15m", -999) >= REVERSAL_MIN_15M_MOVE
        and x.get("1h", 999) <= 1.0
        and x.get("volume_15m", 0) >= REVERSAL_MIN_VOLUME_15M
        and x.get("volume_1h", 0) >= REVERSAL_MIN_VOLUME_1H
        and x.get("buy_pressure", 0) >= REVERSAL_MIN_BUY_PRESSURE
        and x.get("upper_wick_ratio", 1.0) < REVERSAL_MAX_UPPER_WICK
        and x.get("breakout", -999) >= REVERSAL_MIN_BREAKOUT
        and rsi is not None and rsi < REVERSAL_MAX_RSI
        and x.get("24h", 999) <= REVERSAL_MAX_24H_GAIN
        and x.get("green_streak", 0) >= REVERSAL_MIN_GREEN_STREAK
        and x.get("trend_continuity", 0) >= REVERSAL_MIN_TREND_CONTINUITY
        and x.get("quote_volume", 0) >= REVERSAL_MIN_24H_VOLUME
    )


def squeeze_gate(x):
    sym = x.get("symbol", "")
    base = sym[:-4] if sym.endswith("USDT") else sym
    if base in STABLE_BASES or x.get("is_wash"):
        return False
    fr = x.get("funding_rate", 0)
    if fr > SQUEEZE_MAX_FUNDING:
        return False
    return bool(
        x.get("15m", -999) >= SQUEEZE_MIN_15M_MOVE
        and x.get("volume_15m", 0) >= SQUEEZE_MIN_VOLUME_15M
        and x.get("buy_pressure", 0) >= SQUEEZE_MIN_BUY_PRESSURE
        and x.get("24h", 999) <= SQUEEZE_MAX_24H_GAIN
        and x.get("quote_volume", 0) >= MIN_24H_QUOTE_VOLUME
    )


def explosive_early_gate(x):
    sym = x.get("symbol", "")
    base = sym[:-4] if sym.endswith("USDT") else sym
    if base in STABLE_BASES:
        return False
    if x.get("is_wash"):
        return False

    v15 = x.get("volume_15m", 0)
    accel = x.get("acceleration", 0)
    rsi = x.get("rsi14")

    if v15 > EXPLOSIVE_MAX_VOLUME_15M:
        return False
    if accel > EXPLOSIVE_MAX_ACCELERATION:
        return False
    if rsi is None or rsi >= EXPLOSIVE_MAX_RSI:
        return False

    price_impulse = (
        x.get("5m", -999) >= EXPLOSIVE_MIN_5M_MOVE
        and x.get("15m", -999) >= EXPLOSIVE_MIN_15M_MOVE
    )
    acceleration_ok = (
        v15 >= EXPLOSIVE_MIN_VOLUME_15M
        and x.get("volume_1h", 0) >= EXPLOSIVE_MIN_VOLUME_1H
        and accel >= EXPLOSIVE_MIN_ACCELERATION
    )
    structure_ok = (
        x.get("buy_pressure", 0) >= EXPLOSIVE_MIN_BUY_PRESSURE
        and x.get("upper_wick_ratio", 1.0) < EXPLOSIVE_MAX_UPPER_WICK
        and x.get("breakout", -999) >= EXPLOSIVE_MIN_BREAKOUT
    )
    early_ok = (
        x.get("24h", 999) <= EXPLOSIVE_MAX_24H_GAIN
        and x.get("1h", 999) <= EXPLOSIVE_MAX_1H_GAIN
        and x.get("15m", 999) <= 4.0
    )
    trend_ok = (
        x.get("green_streak", 0) >= EXPLOSIVE_MIN_GREEN_STREAK
        and x.get("trend_continuity", 0) >= EXPLOSIVE_MIN_TREND_CONTINUITY
    )
    liquidity_ok = x.get("quote_volume", 0) >= EXPLOSIVE_MIN_24H_VOLUME

    return bool(price_impulse and acceleration_ok and structure_ok
                and early_ok and trend_ok and liquidity_ok)


def strict_pump_gate(x):
    sym = x.get("symbol", "")
    base = sym[:-4] if sym.endswith("USDT") else sym
    if base in STABLE_BASES:
        return False
    if x.get("is_wash"):
        return False

    v15 = x.get("volume_15m", 0)
    accel = x.get("acceleration", 0)
    rsi = x.get("rsi14")

    if v15 > WASH_VOL_15M_HARD_CAP:
        return False
    if accel > FINAL_MAX_ACCELERATION:
        return False
    if rsi is None or rsi >= FINAL_MAX_RSI:
        return False
    if x.get("buy_pressure", 0) < FINAL_MIN_BUY_PRESSURE:
        return False
    if x.get("green_streak", 0) < FINAL_MIN_GREEN_STREAK:
        return False
    if x.get("trend_continuity", 0) < FINAL_MIN_TREND_CONTINUITY:
        return False

    price_ok = (
        x.get("15m", -999) >= FINAL_MIN_15M_MOVE
        and (x.get("5m", -999) >= FINAL_MIN_5M_MOVE
             or x.get("breakout", -999) >= 0.0)
    )
    volume_ok = (
        x.get("volume_1h", 0) >= FINAL_MIN_VOLUME_1H
        and x.get("volume_15m", 0) >= FINAL_MIN_VOLUME_15M
        and accel >= FINAL_MIN_ACCELERATION
    )
    structure_ok = (
        x.get("upper_wick_ratio", 1.0) < 0.45
        and x.get("breakout", -999) >= -0.75
    )
    not_late = (
        x.get("24h", 999) <= FINAL_MAX_24H_GAIN
        and x.get("15m", 999) <= 6.0
        and x.get("1h", 999) <= 10.0
    )
    liquidity_ok = x.get("quote_volume", 0) >= FINAL_MIN_24H_VOLUME

    return bool(price_ok and volume_ok and structure_ok and not_late and liquidity_ok)


def has_price_confirmation(x):
    if not x.get("price_confirmation") or not x.get("continuation_quality"):
        return False
    return bool(strict_pump_gate(x) or explosive_early_gate(x))


def final_lane(x):
    """يرجّع اسم المسار اللي أهّل الإشارة للـFINAL (أو None). مستخدم في التعلّم الذاتي."""
    score = x.get("score", 0)
    cq = x.get("continuation_quality")
    if score >= FINAL_SCORE and has_price_confirmation(x):
        return "EXPLOSIVE" if explosive_early_gate(x) else "STRICT"
    if score >= PERSISTENT_FINAL_SCORE and cq and persistent_explosion_gate(x):
        return "PERSISTENT"
    if score >= REVERSAL_FINAL_SCORE and cq and reversal_explosion_gate(x):
        return "REVERSAL"
    if score >= SQUEEZE_FINAL_SCORE and cq and squeeze_gate(x):
        return "SQUEEZE"
    return None


def fake_breakout_rejection(x):
    """Return a reason when a marginal breakout has a weak close and rejection wick.

    This is intentionally conservative: it only filters near-breakout candidates;
    it does not claim to identify every false breakout.
    """
    if not FAKE_BREAKOUT_FILTER_ENABLED:
        return None
    try:
        breakout = float(x.get("breakout", -999))
        close_location = float(x.get("close_location", 0.5))
        upper_wick = float(x.get("upper_wick_ratio", 0.0))
    except (TypeError, ValueError):
        return None
    if (0.0 <= breakout <= FAKE_BREAKOUT_MAX_DISTANCE_PCT
            and close_location < FAKE_BREAKOUT_MIN_CLOSE_LOCATION
            and upper_wick >= FAKE_BREAKOUT_MIN_UPPER_WICK):
        return (f"WEAK_BREAKOUT_CLOSE (close_location={close_location:.2f}, "
                f"upper_wick={upper_wick:.2f}, breakout={breakout:.2f}%)")
    return None


def final_candidate_eligible(x):
    if fake_breakout_rejection(x):
        return False
    return final_lane(x) is not None


# ============================================================
# Cross-exchange
# ============================================================

def okx_inst_id(sym):
    base = sym[:-4] if sym.endswith("USDT") else sym
    return f"{base}-USDT"


def get_bybit_klines_15m(symbol, limit=6):
    try:
        r = session.get(
            f"{BYBIT_BASE}/v5/market/kline",
            params={"category": "spot", "symbol": symbol, "interval": "15", "limit": limit},
            timeout=8,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("retCode") != 0:
            return None
        return data.get("result", {}).get("list", []) or None
    except Exception as e:
        log.debug(f"Bybit klines failed for {symbol}: {e}")
        return None


def get_okx_klines_15m(inst_id, limit=6):
    try:
        r = session.get(
            f"{OKX_BASE}/api/v5/market/candles",
            params={"instId": inst_id, "bar": "15m", "limit": limit},
            timeout=8,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("code") != "0":
            return None
        return data.get("data", []) or None
    except Exception:
        return None


def klines_1h_change_pct(rows, close_index):
    if not rows:
        return None
    back = min(4, len(rows) - 1)
    if back <= 0:
        return None
    try:
        latest = float(rows[0][close_index])
        older = float(rows[back][close_index])
        if older == 0:
            return None
        return (latest / older - 1.0) * 100.0
    except (ValueError, IndexError):
        return None


def cross_exchange_confirmation(symbol):
    now = time.time()
    cached = cross_cache.get(symbol)
    if cached and (now - cached[0]) < CROSS_CACHE_TTL_SECONDS:
        return cached[1], cached[2]

    notes = []
    agree = 0
    disagree = 0
    listed = False

    bybit = get_bybit_klines_15m(symbol)
    if bybit:
        listed = True
        chg = klines_1h_change_pct(bybit, close_index=4)
        if chg is not None:
            notes.append(f"Bybit 1h: {chg:+.2f}%")
            if chg >= CROSS_CONFIRM_AGREE_PCT:
                agree += 1
            elif chg <= CROSS_CONFIRM_DISAGREE_PCT:
                disagree += 1

    okx = get_okx_klines_15m(okx_inst_id(symbol))
    if okx:
        listed = True
        chg = klines_1h_change_pct(okx, close_index=4)
        if chg is not None:
            notes.append(f"OKX 1h: {chg:+.2f}%")
            if chg >= CROSS_CONFIRM_AGREE_PCT:
                agree += 1
            elif chg <= CROSS_CONFIRM_DISAGREE_PCT:
                disagree += 1

    if not listed:
        label = "BINANCE-EXCLUSIVE"
    elif agree > 0:
        label = "CONFIRMED"
    elif disagree > 0:
        label = "DIVERGENCE"
    else:
        label = "NEUTRAL"

    detail = " | ".join(notes) if notes else "لا توجد بيانات"
    cross_cache[symbol] = (now, label, detail)
    return label, detail


CROSS_LABEL_EMOJI = {
    "CONFIRMED": "✅",
    "DIVERGENCE": "⚠️",
    "NEUTRAL": "➖",
    "BINANCE-EXCLUSIVE": "🔒",
}


# ============================================================
# COMPRESSED lane
# ============================================================

def compressed_gate(x):
    sym = x.get("symbol", "")
    base = sym[:-4] if sym.endswith("USDT") else sym
    if base in STABLE_BASES or x.get("is_wash"):
        return False
    rsi = x.get("rsi14")
    return bool(
        x.get("breakout", 0) <= COMPRESSED_MAX_BREAKOUT
        and COMPRESSED_MIN_GAIN_4H <= x.get("4h", 999) <= COMPRESSED_MAX_GAIN_4H
        and x.get("volume_1h", 0) >= COMPRESSED_MIN_VOL_1H
        and x.get("volume_15m", 0) >= COMPRESSED_MIN_VOL_15M
        and x.get("15m", 999) <= COMPRESSED_MAX_15M_GAIN
        and x.get("24h", 999) <= COMPRESSED_MAX_24H_GAIN
        and (rsi is None or rsi <= COMPRESSED_MAX_RSI)
        and x.get("quote_volume", 0) >= MIN_24H_QUOTE_VOLUME
    )


def format_compressed(x):
    return f"""{PROGRAM_TAG} 🧨 PRE-PUMP COMPRESSED — إنذار مبكر

⚠️ إشارة تحليلية — ليست ضمانًا ولا أمر شراء. إنذار مبكر مش FINAL.

COIN: {x['symbol']}
الفكرة (باك-تيست 30 يوم): العملات اللي بتقفز بتكون أبعد تحت قمتها
من المعتاد (-11% vs -7%) مع انحراف 4h موجب بسيط وحجم بدأ يصعد.

Price: {x['price']:.10g}
4h:  {x.get('4h', 0):+.2f}%
15m: {x['15m']:+.2f}%
24h: {x['24h']:+.2f}%
Breakout (تحت قمة 48 شمعة): {x['breakout']:+.2f}%
Volume 1h: {x['volume_1h']:.2f}x | 15m: {x['volume_15m']:.2f}x
RSI14: {safe_fmt(x.get('rsi14'), '.1f')}
Buy pressure: {x.get('buy_pressure', 0):.2f}
Funding: {x.get('funding_rate', 0):.5f}
Liquidity: {x['liquidity']}
24h Volume: ${x['quote_volume']:,.0f}

📌 استنى تأكيد الحجم/السعر قبل أي قرار — دي أقرب لـ "ضع في المفضلة".
"""


def maybe_compressed_alert(x, now):
    sym = x["symbol"]
    last = compressed_alerted.get(sym)
    if last is not None and now - last < COMPRESSED_COOLDOWN_HOURS * 3600:
        return False
    compressed_alerted[sym] = now
    cutoff = now - 24 * 3600
    for s in [s for s, t in compressed_alerted.items() if t < cutoff]:
        compressed_alerted.pop(s, None)
    log_early_watch(x, now)
    if COMPRESSED_TELEGRAM:
        telegram_send(format_compressed(x))
    log.info(f"🧨 COMPRESSED alert: {sym} breakout={x['breakout']:+.2f}% 4h={x.get('4h', 0):+.2f}%")
    return True


# ============================================================
# OKX Lead Watcher
# ============================================================

WATCH_LOG_HEADER = [
    "logged_time_utc", "symbol", "setup", "score", "price",
    "gain_15m", "gain_1h", "gain_24h",
    "volume_1h", "volume_15m", "acceleration",
    "breakout", "liquidity", "quote_volume",
    "trend_continuity", "green_streak", "is_wash",
]


def log_okx_lead(sym, ticker, chg_1h, chg_4h, premium, okx_price):
    exists = os.path.exists(EARLY_WATCH_LOG_FILE)
    with open(EARLY_WATCH_LOG_FILE, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(WATCH_LOG_HEADER)
        w.writerow([
            datetime.now(timezone.utc).isoformat(), sym, "OKX LEAD", "", okx_price,
            "", "", ticker["change"],
            "", "", "",
            "", "", ticker["quote_volume"],
            "", "", "",
        ])


def okx_lead_scan(tickers, symbols):
    now = time.time()
    cutoff = now - 24 * 3600
    for s in [s for s, t in okx_lead_alerted.items() if t < cutoff]:
        okx_lead_alerted.pop(s, None)

    pre = [s for s in symbols
           if s in tickers
           and tickers[s]["quote_volume"] >= OKX_LEAD_MIN_24H_VOLUME
           and tickers[s]["change"] <= OKX_LEAD_MAX_24H_GAIN]

    alerts = []

    def _check(sym):
        last = okx_lead_alerted.get(sym)
        if last is not None and now - last < OKX_LEAD_COOLDOWN_HOURS * 3600:
            return None
        rows = get_okx_klines_15m(okx_inst_id(sym), limit=16)
        if not rows or len(rows) < 4:
            return None
        try:
            latest = float(rows[0][4])
            older_1h = float(rows[3][4])
            older_4h = float(rows[15][4]) if len(rows) >= 16 else None
        except (ValueError, IndexError):
            return None
        chg_1h = (latest / older_1h - 1.0) * 100.0 if older_1h else None
        chg_4h = (latest / older_4h - 1.0) * 100.0 if older_4h else None
        binance_price = tickers[sym]["price"]
        premium = (latest / binance_price - 1.0) * 100.0 if binance_price else 0.0
        hit = ((chg_1h is not None and chg_1h >= OKX_LEAD_MIN_1H_CHG)
               or (chg_4h is not None and chg_4h >= OKX_LEAD_MIN_4H_CHG))
        if hit and premium >= OKX_LEAD_MIN_PREMIUM:
            return (sym, chg_1h, chg_4h, premium, latest)
        return None

    with ThreadPoolExecutor(max_workers=10) as ex:
        for r in ex.map(_check, pre):
            if r:
                alerts.append(r)

    for sym, chg_1h, chg_4h, premium, okx_price in alerts:
        okx_lead_alerted[sym] = now
        t = tickers[sym]
        log_okx_lead(sym, t, chg_1h, chg_4h, premium, okx_price)
        c1 = f"{chg_1h:+.2f}%" if chg_1h is not None else "N/A"
        c4 = f"{chg_4h:+.2f}%" if chg_4h is not None else "N/A"
        msg = f"""{PROGRAM_TAG} ⚡ OKX LEAD ALERT

⚠️ إشارة تحليلية — ليست ضمانًا ولا أمر شراء.
الباك-تيست (أكبر 15 قفزة): OKX سبق Binance بمتوسط ~7.75 ساعة.

COIN: {sym}
OKX price: {okx_price:.10g} (أعلى من Binance بـ {premium:+.2f}%)
OKX 1h: {c1}
OKX 4h: {c4}
Binance 24h: {t['change']:+.2f}%
Binance 24h Volume: ${t['quote_volume']:,.0f}

📌 تنبيه lead مبكر — راقب تأكيد الحجم على Binance قبل أي قرار.
"""
        telegram_send(msg)
        log.info(f"⚡ OKX LEAD: {sym} 1h={c1} 4h={c4} premium={premium:+.2f}%")

    log.info(f"OKX lead scan: فحص {len(pre)} زوج → {len(alerts)} تنبيه")
    return alerts


def append_scan_diagnostics(rows):
    if not rows:
        return
    columns = [
        "logged_time_utc", "symbol", "stage", "reason", "score", "setup",
        "gain_5m", "gain_15m", "gain_1h", "gain_24h", "volume_15m",
        "volume_1h", "acceleration", "buy_pressure", "trend_continuity",
        "green_streak", "is_wash", "quote_volume",
    ]
    now = time.time()
    accepted = []
    for row in rows:
        stage = row.get("stage")
        # 70% من الملف كان LOW_24H_QUOTE_VOLUME بلا فايدة -> مش بنسجّله
        if stage in ("UNIVERSE_FILTER", "SHARIAH_FILTER", "TICKER_DATA"):
            continue
        if stage == "SCORE_FILTER" and (row.get("score") or 0) < WATCH_SCORE - 5:
            continue
        key = "|".join(str(row.get(k, "")) for k in ("symbol", "stage", "reason"))
        last = diagnostics_last_logged.get(key, 0)
        cooldown = (SHARIAH_DIAGNOSTIC_COOLDOWN_HOURS * 3600
                    if row.get("stage") == "SHARIAH_FILTER"
                    else DIAGNOSTIC_COOLDOWN_MINUTES * 60)
        if now - last < cooldown:
            continue
        accepted.append(row)
        diagnostics_last_logged[key] = now
    if not accepted:
        return
    exists = os.path.exists(SCAN_DIAGNOSTICS_FILE)
    with open(SCAN_DIAGNOSTICS_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        if not exists or os.path.getsize(SCAN_DIAGNOSTICS_FILE) == 0:
            writer.writeheader()
        for row in accepted:
            writer.writerow({key: row.get(key, "") for key in columns})


# ============================================================
# Formatting
# ============================================================

def safe_fmt(val, fmt=".2f", default=0):
    return format(val if val is not None else default, fmt)


def format_final(x, confirmations, cross_label=None, cross_detail=None):
    r = x.get("risk") or {}
    risk_section = ""
    if r:
        notes_txt = ("\n".join(f"  • {n}" for n in r.get("notes", []))
                     if r.get("notes") else "  • لا تعديلات")
        risk_section = f"""
━━━ 🎯 خطة التداول (R/R) ━━━
Entry:      {r['entry']:.10g}
Stop Loss:  {r['sl']:.10g}  ({-r['sl_pct']:+.2f}%)
TP1:        {r['tp1']:.10g}  ({r['tp1_pct']:+.2f}%)  R/R={r['tp1_rr']:.2f}
TP2:        {r['tp2']:.10g}  ({r['tp2_pct']:+.2f}%)  R/R={r['tp2_rr']:.2f}
ATR14 used: {r['atr_used']:.2f}%
تعديلات:
{notes_txt}

⚠️ بعد TP1 → انقل SL إلى Entry (Break-even)
⚠️ لا تخاطر بأكثر من 1-2% من رأس مالك في الصفقة
"""

    cross_section = ""
    if cross_label:
        emoji = CROSS_LABEL_EMOJI.get(cross_label, "")
        cross_section = f"\nCross-exchange: {emoji} {cross_label}\n{cross_detail}\n"

    wash_warn = ""
    if x.get("is_wash"):
        wash_warn = f"\n⚠️ تحذير: النمط يشبه Wash Trading — {x.get('wash_reason','')}\n"

    funding_info = ""
    fr = x.get("funding_rate", 0)
    if fr != 0:
        funding_info = f"\nFunding rate: {fr:+.5f}  ({'Short Squeeze محتمل' if fr < -0.0005 else 'طبيعي'})\n"

    return f"""{PROGRAM_TAG} 🔥 FINAL EARLY-PUMP CANDIDATE

⚠️ إشارة تحليلية — ليست ضمانًا ولا أمر شراء.

COIN: {x['symbol']}
SETUP: {x['setup']}
LANE: {x.get('lane', '-')}
EARLY SCORE: {x['score']}/100

Price: {x['price']:.10g}

5m:  {x['5m']:+.2f}%
15m: {x['15m']:+.2f}%
1h:  {x['1h']:+.2f}%
24h: {x['24h']:+.2f}%

Volume 1h: {x['volume_1h']:.2f}x
Volume 15m: {x['volume_15m']:.2f}x
Acceleration: {x['acceleration']:.2f}x
Volume persistence: {x.get('volume_persistence', 0):.2f}x

Trend: {x.get('trend','N/A')}
Trend Continuity: {x.get('trend_continuity',0)}/8  (Green streak: {x.get('green_streak',0)})
EMA9/EMA21: {safe_fmt(x.get('ema9'), '.8g')} / {safe_fmt(x.get('ema21'), '.8g')}
RSI14: {safe_fmt(x.get('rsi14'), '.1f')}
ATR14: {safe_fmt(x.get('atr_pct'), '.2f')}%
Buy pressure: {x.get('buy_pressure',0):.2f}
Buy pressure Δ: {x.get('buy_pressure_delta',0):+.3f}
Candle body: {x.get('body_pct',0):+.2f}%
Upper wick: {x.get('upper_wick_ratio',0)*100:.1f}%
{funding_info}
Breakout: {x['breakout']:+.2f}%
Distance from 1h high: {x.get('distance_from_1h_high',0):+.2f}%
Liquidity: {x['liquidity']}
24h Volume: ${x['quote_volume']:,.0f}
Confirmation scans: {confirmations}
{cross_section}{wash_warn}{risk_section}
📌 لماذا ظهرت؟
حجم غير طبيعي + استمرار حجم + حركة سعر متواصلة + اتجاه/ضغط شراء
+ فلتر ضد الـexhaustion وضد الـwash trading.

🚫 لا يوجد Target ثابت. سيتم قياس MFE/MAE فعليًا بعد الإشارة.
📊 تتبع النتيجة: 5m / 15m / 30m / 1h / 4h / 24h
"""


# ============================================================
# CSV retention
# ============================================================

def cleanup_old_csv_logs(force=False):
    """حذف الصفوف القديمة. مقارنة نصية على ISO-UTC (أسرع بكتير من pd.to_datetime لكل صف)."""
    from datetime import timedelta
    now_dt = datetime.now(timezone.utc)
    files = [
        (EARLY_WATCH_LOG_FILE, "logged_time_utc", LOG_RETENTION_DAYS),
        (TRACK_FILE, "final_time_utc", TRACK_RETENTION_DAYS),
        (SCAN_DIAGNOSTICS_FILE, "logged_time_utc", LOG_RETENTION_DAYS),
    ]

    for filename, time_col, days in files:
        if not os.path.exists(filename):
            continue
        cutoff_str = (now_dt - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S")
        tmp_name = filename + ".cleanup.tmp"
        try:
            kept = removed = 0
            with open(filename, "r", newline="", encoding="utf-8-sig") as src_f:
                reader = csv.reader(src_f)
                header = next(reader, None)
                if not header:
                    continue
                try:
                    time_idx = header.index(time_col)
                except ValueError:
                    log.warning(f"Log cleanup skipped {filename}: missing column {time_col}")
                    continue
                with open(tmp_name, "w", newline="", encoding="utf-8") as dst:
                    writer = csv.writer(dst)
                    writer.writerow(header)
                    for row in reader:
                        if not row:
                            continue
                        raw_ts = row[time_idx].strip() if len(row) > time_idx else ""
                        if len(raw_ts) >= 19 and raw_ts[:4].isdigit() and raw_ts[:19] < cutoff_str:
                            removed += 1
                            continue
                        writer.writerow(row)
                        kept += 1
            if removed:
                os.replace(tmp_name, filename)
                log.info(f"Log cleanup: حذف {removed} سجل قديم من {filename} (بقى {kept})")
            else:
                os.remove(tmp_name)
        except Exception as e:
            try:
                if os.path.exists(tmp_name):
                    os.remove(tmp_name)
            except OSError:
                pass
            log.warning(f"Log cleanup failed for {filename}: {e}")


def maybe_cleanup_logs(force=False):
    now = time.time()
    last = float(meta.get("last_cleanup_ts", 0) or 0)
    if force or now - last >= LOG_CLEANUP_INTERVAL_HOURS * 3600:
        cleanup_old_csv_logs()
        meta["last_cleanup_ts"] = now


TRACK_COLUMNS = [
    "final_time_utc", "symbol", "setup", "score",
    "entry_price", "sl", "sl_pct", "tp1", "tp1_pct", "tp2", "tp2_pct",
    "atr_used", "quote_volume", "liquidity",
    "gain_5m_at_alert", "gain_15m_at_alert",
    "gain_1h_at_alert", "gain_24h_at_alert",
    "peak_price", "lowest_price", "mfe_pct", "mae_pct",
    "result_5m", "result_15m", "result_30m",
    "result_1h", "result_4h", "result_24h", "status",
    "lane", "outcome", "exit_pct", "outcome_min",
]

# أعمدة نصية لازم تقرا object — لو فاضية بانداس بيحوّلها float64
# وبيرفض الكتابة النصية فيها (Invalid value 'SL' for dtype 'float64')
TRACK_STR_COLUMNS = ("final_time_utc", "symbol", "setup", "liquidity",
                     "status", "lane", "outcome")


def read_track_file():
    df = pd.read_csv(TRACK_FILE, low_memory=False)
    for c in TRACK_STR_COLUMNS:
        if c in df.columns:
            df[c] = df[c].astype(object)
    return df


def init_results_file():
    columns = TRACK_COLUMNS
    if not os.path.exists(TRACK_FILE) or os.path.getsize(TRACK_FILE) == 0:
        pd.DataFrame(columns=columns).to_csv(TRACK_FILE, index=False)
    else:
        try:
            df = pd.read_csv(TRACK_FILE)
            missing = [c for c in columns if c not in df.columns]
            if missing:
                # ترحيل بدون فقد أي صف: بنضيف الأعمدة الناقصة بس
                for c in missing:
                    df[c] = None
                extra = [c for c in df.columns if c not in columns]
                df = df[columns + extra]
                tmp = TRACK_FILE + ".migrate.tmp"
                df.to_csv(tmp, index=False)
                os.replace(tmp, TRACK_FILE)
                log.info(f"TRACK_FILE: اتضافت أعمدة {missing} (الصفوف القديمة محفوظة)")
        except Exception as e:
            log.warning(f"init_results_file migration failed: {e}")
            try:
                os.replace(TRACK_FILE, TRACK_FILE + ".bak")
                pd.DataFrame(columns=columns).to_csv(TRACK_FILE, index=False)
            except Exception as e2:
                log.warning(f"TRACK_FILE backup failed: {e2}")

    try:
        if os.path.exists(TRACK_FILE) and os.path.getsize(TRACK_FILE) > 0:
            df = pd.read_csv(TRACK_FILE)
            if {"final_time_utc", "symbol"}.issubset(df.columns):
                before = len(df)
                df = df.drop_duplicates(subset=["final_time_utc", "symbol"], keep="last")
                removed = before - len(df)
                if removed:
                    tmp = TRACK_FILE + ".dedupe.tmp"
                    df.to_csv(tmp, index=False)
                    os.replace(tmp, TRACK_FILE)
                    log.warning(f"TRACK_FILE: تم حذف {removed} صف مكرر.")
    except Exception as e:
        log.warning(f"TRACK_FILE duplicate cleanup skipped: {e}")

    if os.path.exists(EARLY_WATCH_LOG_FILE):
        try:
            with open(EARLY_WATCH_LOG_FILE, "r", encoding="utf-8-sig", newline="") as f:
                old_cols = next(csv.reader(f), [])
            if "logged_time_utc" not in old_cols:
                log.warning("EARLY_WATCH_LOG_FILE lacks logged_time_utc; kept untouched.")
        except Exception as e:
            log.warning(f"watch log schema check failed: {e}")


def safe_save_track(track):
    """save_track مع عزل الأعطال — عطل إشارة واحدة مبيموتش الدورة."""
    try:
        save_track(track)
    except Exception as e:
        log.error(f"save_track failed for {track.get('symbol')}: {e}")


def save_track(track):
    row = {
        "final_time_utc": track["final_time_utc"],
        "symbol": track["symbol"],
        "setup": track["setup"],
        "score": track["score"],
        "entry_price": track["entry_price"],
        "sl": track.get("sl"),
        "sl_pct": track.get("sl_pct"),
        "tp1": track.get("tp1"),
        "tp1_pct": track.get("tp1_pct"),
        "tp2": track.get("tp2"),
        "tp2_pct": track.get("tp2_pct"),
        "atr_used": track.get("atr_used"),
        "quote_volume": track["quote_volume"],
        "liquidity": track["liquidity"],
        "gain_5m_at_alert": track["gain_5m_at_alert"],
        "gain_15m_at_alert": track["gain_15m_at_alert"],
        "gain_1h_at_alert": track["gain_1h_at_alert"],
        "gain_24h_at_alert": track["gain_24h_at_alert"],
        "peak_price": track["peak_price"],
        "lowest_price": track["lowest_price"],
        "mfe_pct": track["mfe_pct"],
        "mae_pct": track["mae_pct"],
        "result_5m": track["results"].get(5),
        "result_15m": track["results"].get(15),
        "result_30m": track["results"].get(30),
        "result_1h": track["results"].get(60),
        "result_4h": track["results"].get(240),
        "result_24h": track["results"].get(1440),
        "status": track["status"],
        "lane": track.get("lane"),
        "outcome": track.get("outcome"),
        "exit_pct": track.get("exit_pct"),
        "outcome_min": track.get("outcome_min"),
    }
    columns = list(row.keys())
    try:
        if os.path.exists(TRACK_FILE) and os.path.getsize(TRACK_FILE) > 0:
            df = read_track_file()
            for col in columns:
                if col not in df.columns:
                    df[col] = ""
            extra = [c for c in df.columns if c not in columns]
            ordered = columns + extra
            mask = (df["symbol"].astype(str) == str(row["symbol"])) & (
                df["final_time_utc"].astype(str) == str(row["final_time_utc"]))
            matches = df.index[mask].tolist()
            if matches:
                idx = matches[-1]
                for col, value in row.items():
                    df.at[idx, col] = value
                if len(matches) > 1:
                    df = df.drop(index=matches[:-1])
            else:
                df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
            df = df.reindex(columns=ordered)
        else:
            df = pd.DataFrame([row], columns=columns)
        tmp = TRACK_FILE + ".tmp"
        df.to_csv(tmp, index=False)
        os.replace(tmp, TRACK_FILE)
    except Exception as e:
        log.exception(f"save_track upsert failed for {row['symbol']}: {e}")
        raise


def _first_minute_after(ts_sec):
    return (int(ts_sec // 60) + 1) * 60 * 1000


def walk_track_path(symbol, track, now_ms):
    """
    يمشي على شموع 1m من لحظة الإشارة ويحدد مين اتحقق الأول: SL ولا TP1 (أو TIMEOUT).
    - لو SL وTP1 في نفس الشمعة -> SL (افتراض متحفّظ).
    - بيحدّث peak/lowest من high/low الشموع (مش من سعر لحظي بس).
    """
    entry = track["entry_price"]
    sl, tp1 = track.get("sl"), track.get("tp1")
    t0_ms = _first_minute_after(track["start_ts"])
    start_ms = int(track.get("last_k_ms") or t0_ms)
    limit = 1000
    for _ in range(3):
        rows = get_json("/api/v3/klines",
                        {"symbol": symbol, "interval": "1m", "startTime": start_ms, "limit": limit},
                        retries=2)
        if not rows:
            return
        for r in rows:
            open_t, close_t = int(r[0]), int(r[6])
            if close_t >= now_ms - 2000:          # شمعة لسه مقفولتش
                return
            hi, lo, cl = float(r[2]), float(r[3]), float(r[4])
            track["peak_price"] = max(track["peak_price"], hi)
            track["lowest_price"] = min(track["lowest_price"], lo)
            minutes_in = (open_t - t0_ms) / 60000.0
            if track.get("outcome") is None and sl and tp1:
                if lo <= sl:
                    track["outcome"], track["exit_pct"] = "SL", pct(sl, entry)
                elif hi >= tp1:
                    track["outcome"], track["exit_pct"] = "TP1", pct(tp1, entry)
                elif minutes_in >= TRADE_TIMEOUT_MIN:
                    track["outcome"], track["exit_pct"] = "TIMEOUT", pct(cl, entry)
                if track.get("outcome") is not None:
                    track["outcome_min"] = round(minutes_in + 1, 1)
            track["last_k_ms"] = open_t + 60000
        if len(rows) < limit:
            return
        start_ms = int(track["last_k_ms"])


def notify_outcome(track):
    if not NOTIFY_OUTCOMES or track.get("notified"):
        return
    oc = track.get("outcome")
    if oc not in ("TP1", "SL", "TIMEOUT"):
        return
    icon = {"TP1": "✅", "SL": "❌", "TIMEOUT": "⏱️"}[oc]
    msg = (f"{PROGRAM_TAG} {icon} نتيجة إشارة {track['symbol']}: {oc}\n"
           f"Lane: {track.get('lane', '-')} | Setup: {track.get('setup', '-')}\n"
           f"الخروج: {track.get('exit_pct', 0):+.2f}% بعد {track.get('outcome_min', 0):.0f} دقيقة\n"
           f"أقصى ربح لحظي: {track.get('mfe_pct', 0):+.2f}% | أقصى نزول: {track.get('mae_pct', 0):+.2f}%")
    telegram_send(msg)
    track["notified"] = True


def update_tracking(tickers):
    now = time.time()
    now_ms = int(now * 1000)
    MAX_AGE_MIN = 1500
    for symbol, track in list(active_tracks.items()):
        elapsed_min = (now - track["start_ts"]) / 60
        entry = track["entry_price"]
        changed = False

        had_outcome = track.get("outcome") is not None
        try:
            walk_track_path(symbol, track, now_ms)
        except Exception as e:
            log.debug(f"walk_track_path failed for {symbol}: {e}")
        if track.get("outcome") is not None and not had_outcome:
            changed = True

        price = tickers.get(symbol, {}).get("price")
        if price is None or price <= 0:
            notify_outcome(track)
            if elapsed_min > MAX_AGE_MIN:
                if track.get("outcome") is None:
                    track["outcome"], track["exit_pct"] = "TIMEOUT", track.get("mae_pct", 0.0)
                track["status"] = "COMPLETED"
                safe_save_track(track)
                del active_tracks[symbol]
            elif changed:
                safe_save_track(track)
            continue

        track["peak_price"] = max(track["peak_price"], price)
        track["lowest_price"] = min(track["lowest_price"], price)
        track["mfe_pct"] = pct(track["peak_price"], entry)
        track["mae_pct"] = pct(track["lowest_price"], entry)

        for target in TRACK_MINUTES:
            if target not in track["results"] and elapsed_min >= target:
                track["results"][target] = pct(price, entry)
                changed = True

        if elapsed_min >= TRADE_TIMEOUT_MIN + 10 and track.get("outcome") is None:
            # ماقدرناش نجيب الشموع (خطأ شبكة) -> نقفل بالسعر الحالي
            track["outcome"], track["exit_pct"] = "TIMEOUT", pct(price, entry)
            track["outcome_min"] = round(elapsed_min, 1)
            changed = True

        if changed:
            track["status"] = "TRACKING"
            safe_save_track(track)
        notify_outcome(track)

        if elapsed_min >= 1440:
            track["status"] = "COMPLETED"
            safe_save_track(track)
            del active_tracks[symbol]


def log_early_watch(x, now):
    sym = x["symbol"]
    last = early_watch_logged.get(sym)
    if last is not None and now - last < EARLY_WATCH_COOLDOWN_HOURS * 3600:
        return
    early_watch_logged[sym] = now
    exists = os.path.exists(EARLY_WATCH_LOG_FILE)
    with open(EARLY_WATCH_LOG_FILE, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow([
                "logged_time_utc", "symbol", "setup", "score", "price",
                "gain_15m", "gain_1h", "gain_24h",
                "volume_1h", "volume_15m", "acceleration",
                "breakout", "liquidity", "quote_volume",
                "trend_continuity", "green_streak", "is_wash",
            ])
        w.writerow([
            datetime.now(timezone.utc).isoformat(), sym, x["setup"], x["score"], x["price"],
            x["15m"], x["1h"], x["24h"],
            x["volume_1h"], x["volume_15m"], x["acceleration"],
            x["breakout"], x["liquidity"], x["quote_volume"],
            x.get("trend_continuity", 0), x.get("green_streak", 0),
            int(x.get("is_wash", False)),
        ])


def start_final_tracking(x):
    r = x.get("risk") or {}
    active_tracks[x["symbol"]] = {
        "final_time_utc": datetime.now(timezone.utc).isoformat(),
        "symbol": x["symbol"],
        "setup": x["setup"],
        "score": x["score"],
        "entry_price": x["price"],
        "sl": r.get("sl"),
        "sl_pct": r.get("sl_pct"),
        "tp1": r.get("tp1"),
        "tp1_pct": r.get("tp1_pct"),
        "tp2": r.get("tp2"),
        "tp2_pct": r.get("tp2_pct"),
        "atr_used": r.get("atr_used"),
        "quote_volume": x["quote_volume"],
        "liquidity": x["liquidity"],
        "gain_5m_at_alert": x["5m"],
        "gain_15m_at_alert": x["15m"],
        "gain_1h_at_alert": x["1h"],
        "gain_24h_at_alert": x["24h"],
        "peak_price": x["price"],
        "lowest_price": x["price"],
        "mfe_pct": 0.0,
        "mae_pct": 0.0,
        "results": {},
        "start_ts": time.time(),
        "status": "TRACKING",
        "lane": x.get("lane"),
        "outcome": None,
        "exit_pct": None,
        "outcome_min": None,
        "last_k_ms": None,
        "notified": False,
    }
    safe_save_track(active_tracks[x["symbol"]])


# ============================================================
# Final signal
# ============================================================

def maybe_final_signal(x, now):
    sym = x["symbol"]

    fake_reason = fake_breakout_rejection(x)
    if fake_reason:
        log.info(f"[V6 FakeBreakout] {sym}: FINAL blocked ({fake_reason})")
        return False

    # Never overwrite an existing tracked trade: keep its MFE/MAE and outcome history intact.
    if sym in active_tracks:
        log.info(f"[V6 Tracking] {sym}: new alert suppressed; existing track is still retained")
        return False

    if sym in final_cooldown:
        if now - final_cooldown[sym] < FINAL_COOLDOWN_HOURS * 3600:
            return False

    if x["score"] < MIN_CANDIDATE_SCORE:
        return False

    c = candidates.get(sym)
    if not c or (now - c["last_ts"]) > CANDIDATE_EXPIRY_MINUTES * 60:
        candidates[sym] = {
            "count": 1,
            "first_ts": now,
            "last_ts": now,
            "best": x,
            "first": x,
        }
        return False

    c["count"] += 1
    c["last_ts"] = now
    if x["score"] > c["best"]["score"]:
        c["best"] = x

    fast_lane = fast_explosive_eligible(x)
    if c["count"] < CONFIRMATIONS_REQUIRED and not fast_lane:
        return False

    if not final_candidate_eligible(x) and not fast_lane:
        return False

    first = c["first"]
    second_confirmed = (
        x.get("continuation_quality", False)
        and x["5m"] > -0.3
        and x["15m"] >= first["15m"] - 1.0
        and x["volume_1h"] >= first["volume_1h"] * 0.70
    )

    if not second_confirmed and not fast_lane:
        return False

    # نستخدم مسح الآن (مش best القديم) + سعر لحظي، عشان الدخول/SL/TP يطابقوا اللي يقدر المستخدم ينفذه
    final_x = dict(x)
    final_x["lane"] = final_lane(x)
    final_x["score"] = max(x["score"], c["best"]["score"])
    live = (current_tickers.get(sym) or {}).get("price")
    if live and live > 0:
        final_x["price"] = live
        final_x["risk"] = compute_risk_levels(live, final_x.get("atr_pct"),
                                              final_x.get("rsi14"), final_x["liquidity"])

    cross_label, cross_detail = cross_exchange_confirmation(sym)

    if SUPPRESS_ON_DIVERGENCE and cross_label == "DIVERGENCE":
        log.info(f"[Cross] {sym}: DIVERGENCE — تم منع FINAL. {cross_detail}")
        final_cooldown[sym] = now
        candidates.pop(sym, None)
        return False

    # Entry-quality gate: only use completed 1m candles, never the live/forming candle.
    try:
        one_minute_rows = get_json(
            "/api/v3/klines", {"symbol": sym, "interval": "1m", "limit": 3}, retries=2
        )
        entry_ok, entry_reason, entry_metrics = confirm_one_minute_entry(
            one_minute_rows or [], float(x.get("price") or final_x.get("price") or 0)
        )
    except Exception as exc:
        entry_ok, entry_reason, entry_metrics = False, "1M_CONFIRMATION_FETCH_ERROR", {}
        log.warning(f"[EntryQuality] {sym}: 1m confirmation fetch failed: {exc}")
    if not entry_ok:
        log.info(f"[EntryQuality] {sym}: FINAL blocked ({entry_reason}) metrics={entry_metrics}")
        return False

    log.info(f"[EntryQuality] {sym}: 1m confirmation passed metrics={entry_metrics}")
    final_cooldown[sym] = now
    candidates.pop(sym, None)

    msg = format_final(final_x, c["count"], cross_label, cross_detail)
    telegram_send(msg)
    log.info(f"\n{'!'*70}\n{msg}\n{'!'*70}")

    start_final_tracking(final_x)
    return True


def cleanup_expired_candidates(now):
    for sym, c in list(candidates.items()):
        if now - c["last_ts"] > CANDIDATE_EXPIRY_MINUTES * 60:
            del candidates[sym]


# ============================================================
# State
# ============================================================

def save_state():
    state = {
        "candidates": candidates,
        "final_cooldown": final_cooldown,
        "active_tracks": active_tracks,
        "early_watch_logged": early_watch_logged,
        "okx_lead_alerted": okx_lead_alerted,
        "compressed_alerted": compressed_alerted,
        "diagnostics_last_logged": diagnostics_last_logged,
        "meta": meta,
    }
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, default=str)
    except Exception as e:
        log.error(f"save_state failed: {e}")


def load_state():
    if not os.path.exists(STATE_FILE):
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception as e:
        log.error(f"load_state failed: {e}")
        return

    for sym, cand in state.get("candidates", {}).items():
        if isinstance(cand, dict) and "first" in cand and "best" in cand:
            candidates[sym] = cand
    final_cooldown.update(state.get("final_cooldown", {}))
    early_watch_logged.update(state.get("early_watch_logged", {}))
    okx_lead_alerted.update(state.get("okx_lead_alerted", {}))
    compressed_alerted.update(state.get("compressed_alerted", {}))
    diagnostics_last_logged.update(state.get("diagnostics_last_logged", {}))
    meta.update(state.get("meta", {}) or {})

    for sym, tr in state.get("active_tracks", {}).items():
        tr["results"] = {int(k): v for k, v in tr.get("results", {}).items()}
        active_tracks[sym] = tr


# ============================================================
# Cycle
# ============================================================

def run_cycle(symbols):
    cycle_start = time.time()

    # التتبع الأول: لازم يشتغل حتى لو السوق منهار (عشان نسجّل SL/TP صح)
    tickers = get_tickers()
    current_tickers.clear()
    current_tickers.update(tickers)
    update_tracking(tickers)

    btc_safe, btc_15m, btc_1h = check_btc_market_health()
    if not btc_safe:
        log.warning(f"⚠️ BTC crash: 15m={btc_15m:+.2f}% 1h={btc_1h:+.2f}% — توقف عن إشارات جديدة")
        return max(10, SCAN_EVERY_SECONDS - (time.time() - cycle_start))

    fetch_funding_rates_bulk()
    now_dt = datetime.now(timezone.utc).isoformat()
    diagnostics = []

    universe = []
    for symbol in symbols:
        ticker = tickers.get(symbol)
        if ticker is None:
            diagnostics.append({"logged_time_utc": now_dt, "symbol": symbol,
                                "stage": "TICKER_DATA", "reason": "SYMBOL_MISSING_FROM_TICKERS"})
        elif ticker["quote_volume"] < MIN_24H_QUOTE_VOLUME:
            diagnostics.append({"logged_time_utc": now_dt, "symbol": symbol,
                                "stage": "UNIVERSE_FILTER", "reason": "LOW_24H_QUOTE_VOLUME",
                                "quote_volume": ticker["quote_volume"], "gain_24h": ticker["change"]})
        elif ticker["change"] > MAX_24H_GAIN_PCT:
            diagnostics.append({"logged_time_utc": now_dt, "symbol": symbol,
                                "stage": "UNIVERSE_FILTER", "reason": "24H_GAIN_ABOVE_MAX",
                                "quote_volume": ticker["quote_volume"], "gain_24h": ticker["change"]})
        else:
            universe.append(symbol)

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(analyze, symbol, tickers[symbol]): symbol for symbol in universe}
        for fut in as_completed(futures):
            symbol = futures[fut]
            try:
                result = fut.result()
                if not result:
                    diagnostics.append({"logged_time_utc": now_dt, "symbol": symbol,
                                        "stage": "ANALYZE", "reason": "NO_RESULT_OR_EARLY_FILTER"})
                elif result.get("_rejected"):
                    diagnostics.append({"logged_time_utc": now_dt, "symbol": symbol,
                                        "stage": "ANALYZE_FILTER", "reason": result["_rejected"]})
                elif result["score"] < WATCH_SCORE:
                    diagnostics.append({
                        "logged_time_utc": now_dt, "symbol": symbol, "stage": "SCORE_FILTER",
                        "reason": f"SCORE_BELOW_WATCH_{WATCH_SCORE}", "score": result["score"],
                        "setup": result["setup"], "gain_5m": result["5m"],
                        "gain_15m": result["15m"], "gain_1h": result["1h"],
                        "gain_24h": result["24h"], "volume_15m": result["volume_15m"],
                        "volume_1h": result["volume_1h"], "acceleration": result["acceleration"],
                        "buy_pressure": result["buy_pressure"],
                        "trend_continuity": result.get("trend_continuity", 0),
                        "green_streak": result.get("green_streak", 0),
                        "is_wash": int(bool(result.get("is_wash"))),
                        "quote_volume": result["quote_volume"],
                    })
                    if compressed_gate(result):
                        maybe_compressed_alert(result, time.time())
                else:
                    results.append(result)
                    diagnostics.append({
                        "logged_time_utc": now_dt, "symbol": symbol, "stage": "WATCH_CANDIDATE",
                        "reason": "SCORE_PASSED_WATCH", "score": result["score"],
                        "setup": result["setup"], "gain_5m": result["5m"],
                        "gain_15m": result["15m"], "gain_1h": result["1h"],
                        "gain_24h": result["24h"], "volume_15m": result["volume_15m"],
                        "volume_1h": result["volume_1h"], "acceleration": result["acceleration"],
                        "buy_pressure": result["buy_pressure"],
                        "trend_continuity": result.get("trend_continuity", 0),
                        "green_streak": result.get("green_streak", 0),
                        "is_wash": int(bool(result.get("is_wash"))),
                        "quote_volume": result["quote_volume"],
                    })
            except Exception as e:
                log.debug(f"analyze error for {symbol}: {e}")
                diagnostics.append({"logged_time_utc": now_dt, "symbol": symbol,
                                    "stage": "ANALYZE_ERROR", "reason": type(e).__name__})

    append_scan_diagnostics(diagnostics)
    results.sort(key=lambda x: (x["score"], x["volume_1h"]), reverse=True)

    now = time.time()
    log.info("=" * 70)
    log.info(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    if not results:
        log.info(f"No watch candidates >= {WATCH_SCORE}")
    else:
        log.info(f"Watch candidates: {len(results)}")
        # Fetch expensive microstructure data only for the strongest candidates.
        for x in results[:min(MAX_PROCESS_PER_CYCLE, SMART_MONEY_TOP_N)]:
            try:
                enrich_smart_money(x)
            except Exception as exc:
                log.debug("V6 smart money enrichment failed for %s: %s", x.get("symbol"), exc)
                x.setdefault("smart_money_score", 50.0)
                x.setdefault("final_rank", float(x.get("score", 0)))
        results.sort(key=lambda x: (x.get("final_rank", x["score"]), x["volume_1h"]), reverse=True)
        for x in results[:MAX_PROCESS_PER_CYCLE]:
            log.info(
                f"{x['symbol']:12} Score={x['score']:3} "
                f"15m={x['15m']:+6.2f}% Vol1h={x['volume_1h']:5.2f}x "
                f"Vol15m={x['volume_15m']:5.2f}x Accel={x['acceleration']:4.2f}x "
                f"TrendC={x.get('trend_continuity',0)}/8 "
                f"Green={x.get('green_streak',0)} "
                f"Wash={'YES' if x.get('is_wash') else 'no'} "
                f"Fund={x.get('funding_rate',0):.5f} "
                f"Liq={x['liquidity']}"
            )
            log_early_watch(x, now)
            if final_candidate_eligible(x):
                maybe_final_signal(x, now)
            if compressed_gate(x):
                maybe_compressed_alert(x, now)

        # V4.4.1: فحص COMPRESSED لكل النتائج مش أول MAX_PROCESS_PER_CYCLE بس
        for x in results[MAX_PROCESS_PER_CYCLE:]:
            if compressed_gate(x):
                maybe_compressed_alert(x, now)

    okx_lead_scan(tickers, symbols)
    cleanup_expired_candidates(now)
    maybe_cleanup_logs()
    log.info(f"Active tracks: {len(active_tracks)} | Pending: {len(candidates)}")
    return max(10, SCAN_EVERY_SECONDS - (time.time() - cycle_start))


# ============================================================
# Main
# ============================================================

load_auto_thresholds()

ONCE = "--once" in sys.argv

init_results_file()
load_state()
update_shariah_list(force=False)
maybe_cleanup_logs()
last_log_cleanup_ts = time.time()

log.info("تحميل قائمة Binance...")
try:
    symbols = get_symbols()
except Exception as e:
    log.error(f"Cannot load symbols: {e}")
    sys.exit(1)

log.info(f"تم تحميل {len(symbols)} زوج USDT.")
log.info("Scanner V6.0 experimental started" + (" (single run)." if ONCE else "."))

if ONCE:
    try:
        run_cycle(symbols)
    except Exception as e:
        log.exception(f"Scanner error: {e}")
        save_state()
        sys.exit(1)
    save_state()
    sys.exit(0)

while True:
    try:
        sleep_time = run_cycle(symbols)
        save_state()
        if time.time() - last_log_cleanup_ts >= LOG_CLEANUP_INTERVAL_HOURS * 3600:
            update_shariah_list(force=False)
            last_log_cleanup_ts = time.time()
        log.info(f"Next scan in {sleep_time:.0f}s...")
        time.sleep(sleep_time)
    except KeyboardInterrupt:
        log.info("Scanner stopped.")
        save_state()
        break
    except Exception as e:
        log.exception(f"Scanner error: {e}")
        time.sleep(30)
