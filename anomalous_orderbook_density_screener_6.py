"""
Самостоятельный мультибиржевой скринер аномальных плотностей в стакане.

Биржи (8 шт., Фьючерсы + Спот):
- Binance (USDT Futures + Spot)
- BinanceAlpha (Binance Alpha CEX orderbooks)
- Bybit (Linear USDT + Spot)
- OKX (USDT-SWAP + Spot)
- Bitget (USDT-FUTURES + Spot)
- Gate (USDT Perpetual + Spot)
- KuCoin (USDT Futures + Spot)
- Aster (USDT Futures + Spot)

Ключевые возможности:
- Диапазон поиска по умолчанию: ±10% от спреда (10% вниз для BID / лонг, 10% вверх для ASK / шорт).
- Раздельные настраиваемые пороги: Спот от $250,000, Фьючерсы от $500,000.
- Мгновенные алерты сразу при обнаружении плотности (min_lifetime_sec = 0с, настраивается).
- Полный обход всех незаблокированных монет на всех 8 биржах каждые 3 минуты (глубокие стаканы)
  + WebSocket-потоки в реальном времени по топ-парам.
- Глобальный чёрный список (тяжи, стейблы, акции/RWA, ETF, металлы, нефть/газ, форекс + авто-API)
  с возможностью разблокировать любую монету (/allow) или следить только за нужными (/watch).
- Личный чёрный список (/ban, /banlist, /unban, кнопка «🚫 Заблокировать» под каждым алертом и в Mini App).
- Мгновенный скан стаканов по отдельной монете (/find COIN) и отправка в Discord Webhooks (/discord).
"""

import asyncio
import json
import logging
import os
import re
import statistics
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import aiohttp
import aiosqlite
import uvicorn
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

try:
    import asyncpg  # type: ignore
except ImportError:
    asyncpg = None

# ==================== ЛОГИРОВАНИЕ И ОКРУЖЕНИЕ ====================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("density_screener")

BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
WEBAPP_URL = (os.getenv("WEBAPP_URL") or "").strip()
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
SQLITE_PATH = os.getenv("SQLITE_PATH", "density_bot.db")
ENV_DISCORD_WEBHOOK = (os.getenv("DISCORD_WEBHOOK_URL") or "").strip()

ALL_EXCHANGES: List[str] = [
    "Binance",
    "BinanceAlpha",
    "Bybit",
    "OKX",
    "Bitget",
    "Gate",
    "KuCoin",
    "Aster",
]

FULL_SCAN_INTERVAL_SEC = 180.0   # Полный обход всех монет на всех 8 биржах каждые 3 минуты
ACTIVE_WALL_TTL_SEC = 210.0      # Сколько секунд стена держится в «Активных сейчас»
WALL_BITE_FIRST_PCT = 15.0       # На сколько % минимум должна уменьшиться стена для 1-го алерта «надкусана»
WALL_BITE_NEXT_PCT = 25.0        # На сколько % от прошлого остатка должна уменьшиться для повторного «надкусана»
WALL_BITE_MAX_DIST_PCT = 4.0     # Макс. дистанция до цены (или подход <= 1.5%), чтобы считать именно «надкусом»
WALL_BITE_MIN_GAP_SEC = 20.0     # Минимальная пауза между апдейтами «надкусана» по одной стене
WALL_BITE_MAX_ALERTS = 3         # Максимум апдейтов «надкусана» на одну и ту же стену
WALL_ABSENCE_FORGET_SEC = 300.0  # Сколько секунд стены не должно быть в стакане, чтобы забыть её

DEFAULT_CONFIG: Dict[str, Any] = {
    "enabled": True,
    "market_mode": "BOTH",           # BOTH | FUTURES | SPOT
    "min_wall_spot_usd": 250_000.0,
    "min_wall_futures_usd": 500_000.0,
    "max_distance_pct": 10.0,        # ±10% от спреда (10% лонг / 10% шорт)
    "min_anomaly_x": 4.0,            # Кратность стены к медианному уровню стакана
    "min_lifetime_sec": 0.0,         # 0с = алерт сразу при нахождении плотности
    "min_vol24h_usd": 200_000.0,
    "cooldown_sec": 180.0,
    "side_mode": "BOTH",             # BOTH | BID | ASK
    "exchanges": list(ALL_EXCHANGES),
    "watch_coins": [],               # Если задано — присылать только по этим тикерам
}

DEFAULT_DISCORD_CONFIG: Dict[str, Any] = {
    "enabled": True,
    "webhook_url": "",
}

VALID_DISCORD_PREFIXES = (
    "https://discord.com/api/webhooks/",
    "https://discordapp.com/api/webhooks/",
    "https://canary.discord.com/api/webhooks/",
    "https://ptb.discord.com/api/webhooks/",
)

QUICK_UNBLOCK_COINS: List[str] = [
    "BTC", "ETH", "SOL", "XRP", "DOGE", "SUI",
    "PEPE", "WIF", "AVAX", "LINK", "XAU", "TSLA",
]

# ==================== ГЛОБАЛЬНЫЙ ЧЁРНЫЙ СПИСОК МОНЕТ ====================

HEAVY_AND_STABLE_BLACKLIST: Set[str] = {
    # Тяжеловесы крипто-рынка
    "BTC", "XBT", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "TRX", "AVAX",
    "TON", "SHIB", "PEPE", "DOT", "LINK", "LTC", "BCH", "SUI", "XLM", "UNI",
    "NEAR", "APT", "ETC", "ICP", "HBAR", "ZEC", "PUMP", "AAVE", "ARB", "OP",
    "FIL", "ATOM", "POL", "MATIC", "TAO", "WLD", "ENA", "ONDO", "VET", "ALGO",
    "CRO", "MNT", "KAS", "RENDER", "RNDR", "FET", "INJ", "STX", "IMX", "SEI",
    "TIA", "JUP", "WIF", "BONK", "FLOKI", "TRUMP", "HYPE", "XMR", "OKB", "BGB",
    "CRV", "LDO", "MKR", "SKY", "PENDLE", "EIGEN", "ENS", "SAND", "MANA", "AXS",
    "GRT", "RUNE", "THETA", "FLOW", "NEO", "EOS", "IOTA", "XTZ", "QNT", "CAKE",
    "PYTH", "JTO", "STRK", "ZK", "ZRO", "PENGU", "VIRTUAL", "FARTCOIN", "PI",
    "IP", "WLFI", "ASTER", "ORDI", "SATS", "LUNC", "BTT", "XEC", "JASMY",
    "DASH", "BSV", "BERA", "KAITO", "BOME", "NOT", "DOGS", "GALA", "CHZ",
    "SNX", "COMP", "DYDX", "GMX", "CFX", "APE", "EGLD", "MINA", "KAVA",
    "SOLX", "BTCX", "ETHX", "XRPX", "BNBX", "ADAX", "SUIX", "DOGE2",
    "BTCDOM", "DEFI",
    # Обёрнутые и ликвид-стейкинг токены
    "WBTC", "WETH", "STETH", "WSTETH", "BNSOL", "CBETH", "WBETH", "MSOL",
    "JITOSOL", "EZETH", "WEETH", "CBBTC", "TBTC",
    # Стейблкоины, фиат и форекс
    "USDT", "USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDE", "USDS", "PYUSD",
    "EURC", "EURT", "EURI", "AEUR", "USD1", "RLUSD", "USDD", "USDP", "GUSD",
    "FRAX", "LUSD", "CRVUSD", "XUSD", "USDF", "USTC", "USDG", "USDY", "SUSDE",
    "EUR", "GBP", "TRY", "JPY", "AUD", "CHF", "CAD", "USD", "NZD", "CNH",
    "CNY", "HKD", "SGD", "KRW", "MXN", "ZAR", "BRL", "DXY", "U",
}

TRADFI_STATIC_BLACKLIST: Set[str] = {
    # Металлы
    "XAU", "XAG", "XPT", "XPD", "PAXG", "XAUT", "GOLD", "SILVER", "COPPER",
    "PLATINUM", "PALLADIUM", "XCU", "XAL", "XNI", "XZN", "SLV", "GLD", "IAU",
    "GDX", "GDXJ", "PPLT", "PALL", "CPER", "HG", "GC", "SI",
    # Нефть, газ и сырьё
    "USOIL", "UKOIL", "WTI", "BRENT", "CRUDE", "OIL", "CL", "BZ", "USO", "BNO",
    "NATGAS", "NGAS", "NG", "UNG", "BOIL", "KOLD", "GAS", "CORN", "WHEAT",
    "SOYB", "SUGAR", "COFFEE", "COCOA", "COTTON", "URANIUM", "URA", "XLE",
    # Индексы и ETF
    "SPY", "QQQ", "DIA", "IWM", "VOO", "VTI", "TQQQ", "SQQQ", "SOXL", "SOXS",
    "UPRO", "SPXU", "UVXY", "VIX", "TLT", "TMF", "HYG", "LQD", "EEM", "FXI",
    "KWEB", "ARKK", "XLF", "XLK", "XLV", "XLI", "XLP", "XLY", "XLU", "SMH",
    "US500", "SPX", "SPX500", "NAS100", "NDX", "USTEC", "US30", "DJI", "DJ30",
    "US2000", "RUT", "GER40", "DAX", "DE40", "UK100", "FTSE", "JP225", "NIKKEI",
    "HK50", "HSI", "FR40", "EU50", "CN50", "A50", "MAG7", "FAANG",
    # Популярные акции и токенизированные акции
    "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOG", "GOOGL", "META", "NFLX",
    "AMD", "INTC", "AVGO", "QCOM", "MU", "TSM", "ASML", "ARM", "ORCL", "CRM",
    "ADBE", "CSCO", "IBM", "NOW", "SNOW", "PLTR", "APP", "SMCI", "DELL", "HPE",
    "COIN", "MSTR", "HOOD", "MARA", "RIOT", "CLSK", "CRCL", "SQ", "XYZ", "PYPL",
    "V", "MA", "JPM", "BAC", "GS", "MS", "C", "WFC", "BLK", "SCHW", "BRK",
    "BRKA", "BRKB", "BABA", "JD", "PDD", "BIDU", "NIO", "XPEV", "LI", "BILI",
    "FUTU", "TCEHY", "DIS", "CMCSA", "SPOT", "ROKU", "RBLX", "UBER", "LYFT",
    "ABNB", "DASH", "SHOP", "SE", "MELI", "WMT", "COST", "TGT", "HD", "LOW",
    "NKE", "SBUX", "MCD", "KO", "PEP", "PG", "JNJ", "PFE", "MRNA", "LLY",
    "NVO", "UNH", "ABBV", "MRK", "BMY", "GILD", "AMGN", "XOM", "CVX", "OXY",
    "SLB", "COP", "BA", "LMT", "RTX", "GE", "CAT", "DE", "HON", "UPS", "FDX",
    "F", "GM", "TM", "RIVN", "LCID", "GME", "AMC", "DJT", "RDDT", "SNAP",
    "PINS", "ZM", "DOCU", "OKTA", "CRWD", "PANW", "ZS", "NET", "DDOG", "MDB",
    "TTD", "AFRM", "SOFI", "UPST", "NU", "SVV", "SNDK", "SNDKB", "WDC",
    "ANET", "VRT", "VST", "CEG", "OKLO", "SMR", "IONQ", "RGTI", "QBTS",
    "RKLB", "ASTS", "LUNR", "HIMS", "TEM", "CVNA", "CAVA", "CMG", "DUOL", "SPCX",
}

_TRADFI_KEYWORDS = (
    "STOCK", "EQUITY", "EQUITIES", "RWA", "TRADFI", "TRADIFI", "METAL",
    "GOLD", "SILVER", "OIL", "COMMODIT", "FOREX", "FX", "ETF", "INDEX",
    "INDICES", "SHARE", "ENERGY", "PRECIOUS",
)

# Динамические списки TradFi/акций из API бирж + объединённый Whitelist пользователей
_dynamic_tradfi_pairs: Set[Tuple[str, str]] = set()
_dynamic_tradfi_bases: Set[str] = set()
_global_allowed_bases: Set[str] = set()
_whitelist_version: int = 0


# ==================== МОДЕЛИ ДАННЫХ ====================

@dataclass
class DensityEvent:
    exchange: str
    market_type: str       # FUTURES | SPOT
    symbol: str
    side: str              # BID | ASK
    price: float           # Текущий спред (best_bid для BID, best_ask для ASK)
    wall_price: float
    wall_usd: float
    distance_pct: float
    anomaly_x: float
    eat_minutes: float
    volume_24h_usd: float
    timestamp_utc: str
    event_type: str = "NEW"          # NEW | BITTEN
    initial_wall_usd: float = 0.0    # Исходный (пиковый) объём стены до надкуса
    eaten_pct: float = 0.0           # Сколько % уже откусили


@dataclass
class _TrackedWall:
    """Состояние одной отслеживаемой плотности для режима «1 алерт + уведомление при надкусе»."""
    wall_price: float
    peak_usd: float
    last_usd: float
    last_alert_usd: float
    anomaly_x: float
    volume_24h_usd: float
    first_seen_ts: float
    last_seen_ts: float
    last_alert_ts: float = 0.0
    missing_since_ts: float = 0.0
    min_dist_pct: float = 100.0
    alerted_new: bool = False
    bitten_count: int = 0


# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        if val is None or val == "":
            return default
        return float(val)
    except (TypeError, ValueError):
        return default


def _has_tradfi_keyword(text: Any) -> bool:
    up = str(text or "").upper()
    return bool(up) and any(k in up for k in _TRADFI_KEYWORDS)


def extract_base_asset(symbol: str) -> str:
    """Извлекает базовый тикер (XBTUSDTM -> BTC, 1000PEPEUSDT -> PEPE, WIF-USDT-SWAP -> WIF)."""
    s = (symbol or "").upper().strip()
    if not s:
        return ""
    for sep in (
        "-USDT-SWAP", "-USDC-SWAP", "-USD-SWAP",
        "USDTM", "USDCM", "_USDT", "-USDT", "/USDT",
        "_USDC", "-USDC", "/USDC",
    ):
        if s.endswith(sep):
            s = s[: -len(sep)]
            break
    else:
        for suf in ("USDT", "USDC", "BUSD", "PERP"):
            if s.endswith(suf) and len(s) > len(suf):
                s = s[: -len(suf)]
                break
    s = re.sub(r"^(1000000|100000|10000|1000|100|10|1M|K)(?=[A-Z])", "", s)
    s = re.sub(r"[^A-Z0-9.]", "", s)
    if s == "XBT":
        return "BTC"
    return s


def _normalize_pair_key(symbol: str) -> str:
    s = (symbol or "").upper().strip()
    for sep in ("-USDT-SWAP", "_USDT", "-USDT", "/USDT", "USDTM"):
        if s.endswith(sep):
            s = s[: -len(sep)] + "USDT"
            break
    return s


def parse_coin_list(raw: Any) -> List[str]:
    """Парсит список монет из JSON/списка/строки через пробел или запятую."""
    items: List[str] = []
    if isinstance(raw, list):
        items = [str(x) for x in raw]
    elif isinstance(raw, str):
        txt = raw.strip()
        if txt.startswith("["):
            try:
                parsed = json.loads(txt)
                if isinstance(parsed, list):
                    items = [str(x) for x in parsed]
            except Exception:
                items = re.split(r"[\s,;]+", txt)
        else:
            items = re.split(r"[\s,;]+", txt)
    out: List[str] = []
    seen: Set[str] = set()
    for it in items:
        b = extract_base_asset(it)
        if b and b not in seen:
            seen.add(b)
            out.append(b)
    return out


def is_globally_blacklisted_symbol(symbol: str, exchange: str = "") -> bool:
    """Проверяет, попадает ли инструмент в глобальный ЧС (тяжи, стейблы, акции/RWA, металлы, нефть)."""
    raw_up = (symbol or "").upper().strip()
    if not raw_up:
        return True
    base = extract_base_asset(raw_up)
    if not base:
        return True
    clean = base.replace(".", "")

    pair_key = _normalize_pair_key(raw_up)
    if exchange and (exchange, pair_key) in _dynamic_tradfi_pairs:
        return True
    if ("", pair_key) in _dynamic_tradfi_pairs:
        return True

    if re.match(r"^R[A-Z]{2,6}(USDT|USDC|_USDT|-USDT)", raw_up) and (
        symbol.startswith("r") or base[1:] in TRADFI_STATIC_BLACKLIST
    ):
        return True

    if base in HEAVY_AND_STABLE_BLACKLIST or clean in HEAVY_AND_STABLE_BLACKLIST:
        return True
    if base in TRADFI_STATIC_BLACKLIST or clean in TRADFI_STATIC_BLACKLIST:
        return True
    if base in _dynamic_tradfi_bases or clean in _dynamic_tradfi_bases:
        return True

    if any(
        tok in clean
        for tok in ("XAU", "XAG", "XPT", "XPD", "OIL", "BRENT", "WTI", "GOLD", "SILVER", "NATGAS", "US500", "NAS100")
    ):
        return True

    for suf in ("ON", "X", "2L", "2S", "3L", "3S", "5L", "5S"):
        if len(clean) >= len(suf) + 2 and clean.endswith(suf):
            stem = clean[: -len(suf)]
            if (
                stem in TRADFI_STATIC_BLACKLIST
                or stem in HEAVY_AND_STABLE_BLACKLIST
                or stem in _dynamic_tradfi_bases
            ):
                return True

    return False


def is_symbol_blocked_for_scan(symbol: str, exchange: str = "") -> bool:
    """
    Проверка для сканеров: если хотя бы один пользователь разблокировал монету (/allow)
    или добавил её в /watch — сканеры НЕ пропускают её.
    """
    base = extract_base_asset(symbol)
    if base and base in _global_allowed_bases:
        return False
    return is_globally_blacklisted_symbol(symbol, exchange)


def is_symbol_blocked_for_user(
    symbol: str,
    exchange: str,
    user_bans: List[str],
    user_allowed: List[str],
    watch_coins: Optional[List[str]] = None,
) -> bool:
    """Персональная проверка блокировки для конкретного пользователя."""
    base = extract_base_asset(symbol)
    if not base:
        return True
    if base in user_bans:
        return True
    if watch_coins:
        return base not in watch_coins
    if base in user_allowed:
        return False
    return is_globally_blacklisted_symbol(symbol, exchange)


def is_valid_discord_webhook(url: str) -> bool:
    clean = (url or "").strip()
    return bool(clean) and clean.startswith(VALID_DISCORD_PREFIXES) and len(clean) > 45


def normalize_config(raw: Any) -> Dict[str, Any]:
    """Нормализует настройки пользователя в безопасные границы."""
    cfg = dict(DEFAULT_CONFIG)
    data: Dict[str, Any] = {}
    if isinstance(raw, dict):
        data = raw
    elif isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                data = parsed
        except Exception:
            data = {}

    cfg["enabled"] = bool(data.get("enabled", cfg["enabled"]))

    mm = str(data.get("market_mode") or cfg["market_mode"]).strip().upper()
    cfg["market_mode"] = mm if mm in ("BOTH", "FUTURES", "SPOT") else "BOTH"

    cfg["min_wall_spot_usd"] = round(
        max(5_000.0, _safe_float(data.get("min_wall_spot_usd"), cfg["min_wall_spot_usd"])), 2
    )
    cfg["min_wall_futures_usd"] = round(
        max(5_000.0, _safe_float(data.get("min_wall_futures_usd"), cfg["min_wall_futures_usd"])), 2
    )
    cfg["max_distance_pct"] = round(
        min(25.0, max(0.1, _safe_float(data.get("max_distance_pct"), cfg["max_distance_pct"]))), 2
    )
    cfg["min_anomaly_x"] = round(
        min(100.0, max(1.0, _safe_float(data.get("min_anomaly_x"), cfg["min_anomaly_x"]))), 2
    )
    cfg["min_lifetime_sec"] = round(
        min(600.0, max(0.0, _safe_float(data.get("min_lifetime_sec"), cfg["min_lifetime_sec"]))), 1
    )
    cfg["min_vol24h_usd"] = round(
        max(0.0, _safe_float(data.get("min_vol24h_usd"), cfg["min_vol24h_usd"])), 2
    )
    cfg["cooldown_sec"] = round(
        min(3600.0, max(15.0, _safe_float(data.get("cooldown_sec"), cfg["cooldown_sec"]))), 1
    )

    sm = str(data.get("side_mode") or cfg["side_mode"]).strip().upper()
    cfg["side_mode"] = sm if sm in ("BOTH", "BID", "ASK") else "BOTH"

    raw_ex = data.get("exchanges")
    if isinstance(raw_ex, list):
        valid = [str(e) for e in raw_ex if str(e) in ALL_EXCHANGES]
        cfg["exchanges"] = valid if valid else list(ALL_EXCHANGES)
    else:
        cfg["exchanges"] = list(ALL_EXCHANGES)

    cfg["watch_coins"] = parse_coin_list(data.get("watch_coins"))
    return cfg


def normalize_discord_config(raw: Any) -> Dict[str, Any]:
    cfg = dict(DEFAULT_DISCORD_CONFIG)
    data: Dict[str, Any] = {}
    if isinstance(raw, dict):
        data = raw
    elif isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                data = parsed
        except Exception:
            data = {}
    cfg["enabled"] = bool(data.get("enabled", True))
    url = str(data.get("webhook_url") or "").strip()
    cfg["webhook_url"] = url if is_valid_discord_webhook(url) else ""
    return cfg


def resolve_discord_webhook(dcfg: Dict[str, Any]) -> str:
    if not dcfg.get("enabled", True):
        return ""
    user_url = str(dcfg.get("webhook_url") or "").strip()
    cand = user_url or ENV_DISCORD_WEBHOOK
    return cand if is_valid_discord_webhook(cand) else ""


def event_matches_user_config(
    ev: DensityEvent,
    cfg: Dict[str, Any],
    bans: List[str],
    allowed: List[str],
) -> bool:
    if not cfg.get("enabled", True):
        return False
    if ev.exchange not in cfg.get("exchanges", ALL_EXCHANGES):
        return False

    mm = cfg.get("market_mode", "BOTH")
    if mm == "FUTURES" and ev.market_type != "FUTURES":
        return False
    if mm == "SPOT" and ev.market_type != "SPOT":
        return False

    sm = cfg.get("side_mode", "BOTH")
    if sm == "BID" and ev.side != "BID":
        return False
    if sm == "ASK" and ev.side != "ASK":
        return False

    if is_symbol_blocked_for_user(ev.symbol, ev.exchange, bans, allowed, cfg.get("watch_coins")):
        return False

    min_wall = (
        float(cfg.get("min_wall_spot_usd", 250_000.0))
        if ev.market_type == "SPOT"
        else float(cfg.get("min_wall_futures_usd", 500_000.0))
    )
    # Для «надкусанной» стены сверяем порог по исходному размеру (чтобы пользователь получил апдейт,
    # даже если после откуса остаток стал чуть меньше его минимального порога)
    effective_wall_usd = max(ev.wall_usd, ev.initial_wall_usd) if ev.event_type == "BITTEN" else ev.wall_usd
    if effective_wall_usd < min_wall:
        return False
    if ev.distance_pct > float(cfg.get("max_distance_pct", 10.0)):
        return False
    if ev.event_type != "BITTEN" and ev.anomaly_x < float(cfg.get("min_anomaly_x", 4.0)):
        return False
    # Для BinanceAlpha не душим суточным объёмом если API не вернул объём
    min_v = float(cfg.get("min_vol24h_usd", 200_000.0))
    if ev.exchange != "BinanceAlpha" and ev.volume_24h_usd < min_v:
        return False
    return True


def fmt_usd(val: float) -> str:
    n = float(val or 0.0)
    if n >= 1_000_000:
        return f"${n / 1_000_000:.2f}M"
    if n >= 1_000:
        return f"${n / 1_000:,.0f}K"
    return f"${n:,.0f}"


def fmt_price(px: float) -> str:
    if px <= 0:
        return "0"
    if px >= 100:
        return f"{px:.2f}"
    if px >= 1:
        return f"{px:.4f}".rstrip("0").rstrip(".")
    return f"{px:.8f}".rstrip("0").rstrip(".")


# ==================== АВТО-ОБНОВЛЕНИЕ TRADFI ЧЁРНОГО СПИСКА ====================

async def refresh_tradfi_metadata(session: aiohttp.ClientSession) -> None:
    """Подтягивает акции, RWA, ETF, металлы и нефть с API бирж в глобальный ЧС."""
    new_pairs: Set[Tuple[str, str]] = set()
    new_bases: Set[str] = set()

    def mark(ex: str, sym: str) -> None:
        if not sym:
            return
        pk = _normalize_pair_key(sym)
        if pk:
            new_pairs.add((ex, pk))
            new_pairs.add(("", pk))
        b = extract_base_asset(sym)
        if b and len(b) >= 2:
            new_bases.add(b)
            new_bases.add(b.replace(".", ""))

    async def fetch_bitget() -> None:
        try:
            url = "https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    return
                data = await r.json()
            for it in data.get("data") or []:
                sym = str(it.get("symbol") or "")
                if (
                    str(it.get("isRwa") or "").upper() == "YES"
                    or _has_tradfi_keyword(it.get("symbolType"))
                    or _has_tradfi_keyword(it.get("category"))
                ):
                    mark("Bitget", sym)
        except Exception:
            pass

    async def fetch_binance_like(ex_name: str, url: str) -> None:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    return
                data = await r.json()
            for s in data.get("symbols") or []:
                sym = str(s.get("symbol") or "")
                utype = str(s.get("underlyingType") or "").upper()
                sub = " ".join(str(x) for x in (s.get("underlyingSubType") or []))
                if _has_tradfi_keyword(s.get("contractType")) or (utype and utype != "COIN") or _has_tradfi_keyword(sub):
                    mark(ex_name, sym)
        except Exception:
            pass

    async def fetch_bybit() -> None:
        try:
            url = "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    return
                data = await r.json()
            for it in (data.get("result") or {}).get("list") or []:
                sym = str(it.get("symbol") or "")
                if (
                    _has_tradfi_keyword(it.get("symbolType"))
                    or _has_tradfi_keyword(it.get("contractType"))
                    or _has_tradfi_keyword(it.get("displayName"))
                ):
                    mark("Bybit", sym)
        except Exception:
            pass

    async def fetch_okx() -> None:
        try:
            url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    return
                data = await r.json()
            for it in data.get("data") or []:
                iid = str(it.get("instId") or "")
                cat = str(it.get("instCategory") or "")
                if (cat and cat != "1") or _has_tradfi_keyword(it.get("instFamily")):
                    mark("OKX", iid)
        except Exception:
            pass

    async def fetch_gate() -> None:
        try:
            url = "https://api.gateio.ws/api/v4/futures/usdt/contracts"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    return
                data = await r.json()
            if isinstance(data, list):
                for it in data:
                    c = str(it.get("name") or "")
                    if (
                        _has_tradfi_keyword(it.get("contract_type"))
                        or _has_tradfi_keyword(it.get("category"))
                        or _has_tradfi_keyword(it.get("tag"))
                    ):
                        mark("Gate", c)
        except Exception:
            pass

    await asyncio.gather(
        fetch_bitget(),
        fetch_binance_like("Binance", "https://fapi.binance.com/fapi/v1/exchangeInfo"),
        fetch_binance_like("Aster", "https://fapi.asterdex.com/fapi/v1/exchangeInfo"),
        fetch_bybit(),
        fetch_okx(),
        fetch_gate(),
        return_exceptions=True,
    )
    if new_pairs or new_bases:
        _dynamic_tradfi_pairs.update(new_pairs)
        _dynamic_tradfi_bases.update(new_bases)
        logger.info(
            "[TradFi] Обновлён авто-ЧС: %s контрактов (%s тикеров)",
            len(_dynamic_tradfi_pairs) // 2,
            len(_dynamic_tradfi_bases),
        )


# ==================== БАЗА ДАННЫХ (PostgreSQL / SQLite) ====================

_pg_pool: Any = None
_use_pg: bool = False
_http_session: Optional[aiohttp.ClientSession] = None
_users_cache: List[Dict[str, Any]] = []
_users_cache_ts: float = 0.0
_api_cache: Dict[str, Any] = {"utc_date": "", "history": [], "ts": 0.0}


def _to_pg_query(query: str) -> str:
    idx = 0
    out: List[str] = []
    for ch in query:
        if ch == "?":
            idx += 1
            out.append(f"${idx}")
        else:
            out.append(ch)
    return "".join(out)


async def init_db() -> None:
    global _pg_pool, _use_pg
    if DATABASE_URL and asyncpg is not None:
        try:
            _pg_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
            async with _pg_pool.acquire() as conn:
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        user_id BIGINT PRIMARY KEY,
                        config TEXT DEFAULT '{}',
                        discord_config TEXT DEFAULT '{}',
                        banned_coins TEXT DEFAULT '[]',
                        allowed_coins TEXT DEFAULT '[]'
                    )
                    """
                )
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS densities (
                        id SERIAL PRIMARY KEY,
                        utc_date TEXT NOT NULL,
                        utc_time TEXT NOT NULL,
                        exchange TEXT NOT NULL,
                        market_type TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        side TEXT NOT NULL,
                        price DOUBLE PRECISION,
                        wall_price DOUBLE PRECISION,
                        wall_usd DOUBLE PRECISION,
                        distance_pct DOUBLE PRECISION,
                        anomaly_x DOUBLE PRECISION,
                        eat_minutes DOUBLE PRECISION,
                        volume_24h_usd DOUBLE PRECISION,
                        event_type TEXT DEFAULT 'NEW',
                        initial_wall_usd DOUBLE PRECISION DEFAULT 0,
                        eaten_pct DOUBLE PRECISION DEFAULT 0
                    )
                    """
                )
                for ddl in (
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS config TEXT DEFAULT '{}'",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS discord_config TEXT DEFAULT '{}'",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS banned_coins TEXT DEFAULT '[]'",
                    "ALTER TABLE users ADD COLUMN IF NOT EXISTS allowed_coins TEXT DEFAULT '[]'",
                    "ALTER TABLE densities ADD COLUMN IF NOT EXISTS event_type TEXT DEFAULT 'NEW'",
                    "ALTER TABLE densities ADD COLUMN IF NOT EXISTS initial_wall_usd DOUBLE PRECISION DEFAULT 0",
                    "ALTER TABLE densities ADD COLUMN IF NOT EXISTS eaten_pct DOUBLE PRECISION DEFAULT 0",
                ):
                    await conn.execute(ddl)
            _use_pg = True
            logger.info("БД: PostgreSQL подключена.")
            return
        except Exception as exc:
            logger.warning("PostgreSQL недоступен (%s) — использую SQLite.", exc)
            _pg_pool = None
            _use_pg = False

    async with aiosqlite.connect(SQLITE_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                config TEXT DEFAULT '{}',
                discord_config TEXT DEFAULT '{}',
                banned_coins TEXT DEFAULT '[]',
                allowed_coins TEXT DEFAULT '[]'
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS densities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                utc_date TEXT NOT NULL,
                utc_time TEXT NOT NULL,
                exchange TEXT NOT NULL,
                market_type TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                price REAL,
                wall_price REAL,
                wall_usd REAL,
                distance_pct REAL,
                anomaly_x REAL,
                eat_minutes REAL,
                volume_24h_usd REAL,
                event_type TEXT DEFAULT 'NEW',
                initial_wall_usd REAL DEFAULT 0,
                eaten_pct REAL DEFAULT 0
            )
            """
        )
        cur = await db.execute("PRAGMA table_info(users)")
        cols = {row[1] for row in await cur.fetchall()}
        if "config" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN config TEXT DEFAULT '{}'")
        if "discord_config" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN discord_config TEXT DEFAULT '{}'")
        if "banned_coins" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN banned_coins TEXT DEFAULT '[]'")
        if "allowed_coins" not in cols:
            await db.execute("ALTER TABLE users ADD COLUMN allowed_coins TEXT DEFAULT '[]'")

        cur_d = await db.execute("PRAGMA table_info(densities)")
        d_cols = {row[1] for row in await cur_d.fetchall()}
        if "event_type" not in d_cols:
            await db.execute("ALTER TABLE densities ADD COLUMN event_type TEXT DEFAULT 'NEW'")
        if "initial_wall_usd" not in d_cols:
            await db.execute("ALTER TABLE densities ADD COLUMN initial_wall_usd REAL DEFAULT 0")
        if "eaten_pct" not in d_cols:
            await db.execute("ALTER TABLE densities ADD COLUMN eaten_pct REAL DEFAULT 0")
        await db.commit()
    logger.info("БД: SQLite (%s) готова.", SQLITE_PATH)


async def db_execute(query: str, params: Tuple[Any, ...] = ()) -> None:
    if _use_pg and _pg_pool is not None:
        async with _pg_pool.acquire() as conn:
            await conn.execute(_to_pg_query(query), *params)
        return
    async with aiosqlite.connect(SQLITE_PATH) as db:
        await db.execute(query, params)
        await db.commit()


async def db_fetchone(query: str, params: Tuple[Any, ...] = ()) -> Optional[Dict[str, Any]]:
    if _use_pg and _pg_pool is not None:
        async with _pg_pool.acquire() as conn:
            row = await conn.fetchrow(_to_pg_query(query), *params)
            return dict(row) if row is not None else None
    async with aiosqlite.connect(SQLITE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(query, params)
        row = await cur.fetchone()
        return dict(row) if row is not None else None


async def db_fetchall(query: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
    if _use_pg and _pg_pool is not None:
        async with _pg_pool.acquire() as conn:
            rows = await conn.fetch(_to_pg_query(query), *params)
            return [dict(r) for r in rows]
    async with aiosqlite.connect(SQLITE_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(query, params)
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


def invalidate_users_cache() -> None:
    global _users_cache_ts
    _users_cache_ts = 0.0


async def get_cached_users(force: bool = False) -> List[Dict[str, Any]]:
    global _users_cache, _users_cache_ts
    now = time.time()
    if not force and _users_cache_ts > 0.0 and (now - _users_cache_ts) < 30.0:
        return _users_cache
    rows = await db_fetchall("SELECT user_id, config, discord_config, banned_coins, allowed_coins FROM users")
    _users_cache = rows
    _users_cache_ts = now
    return rows


async def sync_global_whitelist() -> None:
    """Синхронизирует объединённый Whitelist (allow + watch) всех пользователей для сканеров."""
    global _global_allowed_bases, _whitelist_version
    users = await get_cached_users(force=True)
    merged: Set[str] = set()
    min_spot = DEFAULT_CONFIG["min_wall_spot_usd"]
    min_fut = DEFAULT_CONFIG["min_wall_futures_usd"]
    max_dist = DEFAULT_CONFIG["max_distance_pct"]
    min_anom = DEFAULT_CONFIG["min_anomaly_x"]
    min_life = DEFAULT_CONFIG["min_lifetime_sec"]
    min_cd = DEFAULT_CONFIG["cooldown_sec"]

    for u in users:
        allowed = parse_coin_list(u.get("allowed_coins"))
        cfg = normalize_config(u.get("config"))
        merged.update(allowed)
        merged.update(cfg.get("watch_coins") or [])
        min_spot = min(min_spot, float(cfg["min_wall_spot_usd"]))
        min_fut = min(min_fut, float(cfg["min_wall_futures_usd"]))
        max_dist = max(max_dist, float(cfg["max_distance_pct"]))
        min_anom = min(min_anom, float(cfg["min_anomaly_x"]))
        min_life = min(min_life, float(cfg["min_lifetime_sec"]))
        min_cd = min(min_cd, float(cfg["cooldown_sec"]))

    if merged != _global_allowed_bases:
        _global_allowed_bases = merged
        _whitelist_version += 1

    density_engine.update_scan_floor(
        min_spot_usd=min_spot,
        min_fut_usd=min_fut,
        max_dist_pct=max_dist,
        min_anom_x=min_anom,
        min_life_sec=min_life,
        cooldown_sec=min_cd,
    )


async def get_user_record(user_id: int) -> Tuple[Dict[str, Any], Dict[str, Any], List[str], List[str]]:
    row = await db_fetchone(
        "SELECT config, discord_config, banned_coins, allowed_coins FROM users WHERE user_id = ?",
        (user_id,),
    )
    if not row:
        return dict(DEFAULT_CONFIG), dict(DEFAULT_DISCORD_CONFIG), [], []
    return (
        normalize_config(row.get("config")),
        normalize_discord_config(row.get("discord_config")),
        parse_coin_list(row.get("banned_coins")),
        parse_coin_list(row.get("allowed_coins")),
    )


async def save_user_record(
    user_id: int,
    cfg: Optional[Dict[str, Any]] = None,
    dcfg: Optional[Dict[str, Any]] = None,
    bans: Optional[List[str]] = None,
    allowed: Optional[List[str]] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any], List[str], List[str]]:
    cur_cfg, cur_dcfg, cur_bans, cur_allowed = await get_user_record(user_id)
    new_cfg = normalize_config(cfg) if cfg is not None else cur_cfg
    new_dcfg = normalize_discord_config(dcfg) if dcfg is not None else cur_dcfg
    new_bans = parse_coin_list(bans) if bans is not None else cur_bans
    new_allowed = parse_coin_list(allowed) if allowed is not None else cur_allowed

    await db_execute(
        "INSERT INTO users (user_id, config, discord_config, banned_coins, allowed_coins) "
        "VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET "
        "config = excluded.config, "
        "discord_config = excluded.discord_config, "
        "banned_coins = excluded.banned_coins, "
        "allowed_coins = excluded.allowed_coins",
        (
            user_id,
            json.dumps(new_cfg),
            json.dumps(new_dcfg),
            json.dumps(new_bans),
            json.dumps(new_allowed),
        ),
    )
    invalidate_users_cache()
    await sync_global_whitelist()
    return new_cfg, new_dcfg, new_bans, new_allowed


# ==================== DISCORD WEBHOOKS ====================

async def _post_discord_webhook(
    webhook_url: str,
    payload: Dict[str, Any],
    session: Optional[aiohttp.ClientSession] = None,
) -> Tuple[bool, str]:
    if not is_valid_discord_webhook(webhook_url):
        return False, "Некорректный Discord Webhook URL"
    own = session is None or session.closed
    client = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) if own else session
    try:
        for attempt in range(2):
            async with client.post(
                webhook_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status in (200, 204):
                    return True, "OK"
                if resp.status == 429 and attempt == 0:
                    try:
                        body = await resp.json()
                        wait_s = min(5.0, float(body.get("retry_after") or 1.5))
                    except Exception:
                        wait_s = 1.5
                    await asyncio.sleep(wait_s)
                    continue
                return False, f"HTTP {resp.status}"
        return False, "Rate limited"
    except Exception as exc:
        return False, str(exc)
    finally:
        if own:
            await client.close()


async def send_density_discord(
    webhook_url: str,
    ev: DensityEvent,
    session: Optional[aiohttp.ClientSession] = None,
) -> bool:
    is_bid = ev.side == "BID"
    is_bitten = ev.event_type == "BITTEN"
    side_emoji = "🟢" if is_bid else "🔴"
    side_label = "BID (Лонг / поддержка)" if is_bid else "ASK (Шорт / сопротивление)"
    mkt_label = "Alpha" if ev.exchange == "BinanceAlpha" else ("Спот" if ev.market_type == "SPOT" else "Фьючерс")
    eat_txt = f"~{ev.eat_minutes:.1f} мин" if ev.eat_minutes > 0 else "—"

    if is_bitten:
        init_usd = ev.initial_wall_usd if ev.initial_wall_usd > 0 else ev.wall_usd
        eaten_usd = max(0.0, init_usd - ev.wall_usd)
        title = f"🦷 {side_emoji} НАДКУСАНА {ev.symbol} | {ev.exchange} ({mkt_label})"
        color = 0xF59E0B
        fields = [
            {"name": "Откусили", "value": f"**-{ev.eaten_pct:.0f}%** ({fmt_usd(eaten_usd)})", "inline": True},
            {"name": "Осталось / было", "value": f"**{fmt_usd(ev.wall_usd)}** / {fmt_usd(init_usd)}", "inline": True},
            {"name": "Сторона", "value": f"{side_emoji} **{side_label}**", "inline": True},
            {"name": "Цена стены", "value": f"`{fmt_price(ev.wall_price)}`", "inline": True},
            {"name": "От спреда", "value": f"**{ev.distance_pct:.2f}%** (спред `{fmt_price(ev.price)}`)", "inline": True},
            {"name": "Разъедание остатка", "value": f"**{eat_txt}**", "inline": True},
        ]
    else:
        title = f"🧱 {side_emoji} {ev.symbol} | {ev.exchange} ({mkt_label})"
        color = 0x16C784 if is_bid else 0xEA3943
        fields = [
            {"name": "Плотность", "value": f"**{fmt_usd(ev.wall_usd)}** (x{ev.anomaly_x:.1f})", "inline": True},
            {"name": "Сторона", "value": f"{side_emoji} **{side_label}**", "inline": True},
            {"name": "Цена стены", "value": f"`{fmt_price(ev.wall_price)}`", "inline": True},
            {"name": "От спреда", "value": f"**{ev.distance_pct:.2f}%** (спред `{fmt_price(ev.price)}`)", "inline": True},
            {"name": "Разъедание", "value": f"**{eat_txt}**", "inline": True},
            {"name": "Оборот 24ч", "value": f"**{fmt_usd(ev.volume_24h_usd)}**", "inline": True},
        ]

    payload = {
        "username": "Density Wall Screener",
        "embeds": [
            {
                "title": title,
                "color": color,
                "fields": fields,
                "footer": {"text": f"Density Screener · {ev.timestamp_utc} UTC"},
            }
        ],
    }
    ok, _ = await _post_discord_webhook(webhook_url, payload, session=session)
    return ok


async def send_test_discord(webhook_url: str, session: Optional[aiohttp.ClientSession] = None) -> Tuple[bool, str]:
    payload = {
        "username": "Density Wall Screener",
        "embeds": [
            {
                "title": "✅ Discord Webhook подключён!",
                "description": "Скринер аномальных плотностей (8 бирж · Спот + Фьючерсы + Binance Alpha · ±10%) готов к отправке алертов.",
                "color": 0x16C784,
            }
        ],
    }
    return await _post_discord_webhook(webhook_url, payload, session=session)


# ==================== ДВИЖОК ПОИСКА АНОМАЛЬНЫХ ПЛОТНОСТЕЙ ====================

def analyze_orderbook_levels(
    exchange: str,
    market_type: str,
    symbol: str,
    bids: List[Tuple[float, float]],
    asks: List[Tuple[float, float]],
    volume_24h_usd: float,
    min_wall_usd: float,
    max_distance_pct: float,
    min_anomaly_x: float,
) -> List[DensityEvent]:
    """
    Анализирует стакан (bids/asks в формате [(price, qty_base), ...]) в пределах ±max_distance_pct
    от лучшего бида (для BID / лонг) и лучшего аска (для ASK / шорт).
    """
    if not bids or not asks:
        return []

    bids_sorted = sorted((b for b in bids if b[0] > 0 and b[1] > 0), key=lambda x: x[0], reverse=True)
    asks_sorted = sorted((a for a in asks if a[0] > 0 and a[1] > 0), key=lambda x: x[0])
    if not bids_sorted or not asks_sorted:
        return []

    best_bid = bids_sorted[0][0]
    best_ask = asks_sorted[0][0]
    if best_bid <= 0 or best_ask <= 0 or best_bid > best_ask * 1.05:
        return []

    vol_per_min = (volume_24h_usd / 1440.0) if volume_24h_usd > 0 else 0.0
    now_utc = datetime.now(timezone.utc).strftime("%H:%M:%S")
    found: List[DensityEvent] = []

    for side, levels, ref_px in (
        ("BID", bids_sorted, best_bid),
        ("ASK", asks_sorted, best_ask),
    ):
        in_range: List[Tuple[float, float, float]] = []
        for px, qty in levels:
            dist_pct = ((ref_px - px) / ref_px * 100.0) if side == "BID" else ((px - ref_px) / ref_px * 100.0)
            if dist_pct < -0.05:
                continue
            if dist_pct > max_distance_pct:
                break
            usd = px * qty
            if usd > 0:
                in_range.append((px, usd, max(0.0, dist_pct)))

        if not in_range:
            continue

        usd_vals = [x[1] for x in in_range]
        med_usd = statistics.median(usd_vals) if len(usd_vals) >= 3 else (sum(usd_vals) / len(usd_vals))
        med_usd = max(med_usd, 100.0)

        # Берём самую крупную аномальную плотность на этой стороне
        best_cand: Optional[Tuple[float, float, float, float]] = None
        for px, usd, dist_pct in in_range:
            if usd < min_wall_usd:
                continue
            anom_x = usd / med_usd
            if anom_x < min_anomaly_x:
                continue
            if best_cand is None or usd > best_cand[1]:
                best_cand = (px, usd, dist_pct, anom_x)

        if best_cand is not None:
            w_px, w_usd, dist_pct, anom_x = best_cand
            eat_min = round(w_usd / vol_per_min, 1) if vol_per_min > 0 else 0.0
            found.append(
                DensityEvent(
                    exchange=exchange,
                    market_type=market_type,
                    symbol=symbol,
                    side=side,
                    price=ref_px,
                    wall_price=w_px,
                    wall_usd=round(w_usd, 2),
                    distance_pct=round(dist_pct, 2),
                    anomaly_x=round(anom_x, 1),
                    eat_minutes=eat_min,
                    volume_24h_usd=round(volume_24h_usd, 2),
                    timestamp_utc=now_utc,
                )
            )

    return found


class DensityEngine:
    """
    Отслеживает плотности в режиме «1 алерт на стену + уведомление, если стену надкусили»:
    - Пока одна и та же плотность стоит на своём уровне нетронутой, повторных алертов нет.
    - Как только от неё откусывают заметную часть (>= 15% при подходе цены), бот шлёт
      отдельный алерт «🦷 НАДКУСАННАЯ ПЛОТНОСТЬ» с процентом откуса и остатком.
    """

    def __init__(self, on_density: Callable[[DensityEvent], Any]) -> None:
        self.on_density = on_density
        self.min_spot_usd: float = DEFAULT_CONFIG["min_wall_spot_usd"]
        self.min_fut_usd: float = DEFAULT_CONFIG["min_wall_futures_usd"]
        self.max_dist_pct: float = DEFAULT_CONFIG["max_distance_pct"]
        self.min_anom_x: float = DEFAULT_CONFIG["min_anomaly_x"]
        self.min_life_sec: float = DEFAULT_CONFIG["min_lifetime_sec"]
        self.cooldown_sec: float = DEFAULT_CONFIG["cooldown_sec"]

        # (ex, mkt, sym, side) -> список отслеживаемых стен по их цене
        self._tracked: Dict[Tuple[str, str, str, str], List[_TrackedWall]] = {}
        # (ex, mkt, sym, side) -> (DensityEvent, ts)
        self._active: Dict[Tuple[str, str, str, str], Tuple[DensityEvent, float]] = {}

    def update_scan_floor(
        self,
        min_spot_usd: float,
        min_fut_usd: float,
        max_dist_pct: float,
        min_anom_x: float,
        min_life_sec: float,
        cooldown_sec: float,
    ) -> None:
        self.min_spot_usd = max(5_000.0, min_spot_usd)
        self.min_fut_usd = max(5_000.0, min_fut_usd)
        self.max_dist_pct = min(25.0, max(0.5, max_dist_pct))
        self.min_anom_x = max(1.0, min_anom_x)
        self.min_life_sec = max(0.0, min_life_sec)
        self.cooldown_sec = max(15.0, cooldown_sec)

    @staticmethod
    def _find_matching_wall(walls: List[_TrackedWall], wall_price: float, tol_ratio: float = 0.002) -> Optional[_TrackedWall]:
        """Ищет уже отслеживаемую стену по цене (допуск ±0.20% на случай сдвига на 1–2 тика)."""
        if wall_price <= 0:
            return None
        for tw in walls:
            if tw.wall_price > 0 and abs(wall_price - tw.wall_price) / tw.wall_price <= tol_ratio:
                return tw
        return None

    async def evaluate_orderbook(
        self,
        exchange: str,
        market_type: str,
        symbol: str,
        bids: List[Tuple[float, float]],
        asks: List[Tuple[float, float]],
        volume_24h_usd: float,
        from_full_scan: bool = False,
    ) -> None:
        if is_symbol_blocked_for_scan(symbol, exchange):
            return
        if not bids or not asks:
            return

        bids_sorted = sorted((b for b in bids if b[0] > 0 and b[1] > 0), key=lambda x: x[0], reverse=True)
        asks_sorted = sorted((a for a in asks if a[0] > 0 and a[1] > 0), key=lambda x: x[0])
        if not bids_sorted or not asks_sorted:
            return

        best_bid = bids_sorted[0][0]
        best_ask = asks_sorted[0][0]
        if best_bid <= 0 or best_ask <= 0 or best_bid > best_ask * 1.05:
            return

        min_bid_px = bids_sorted[-1][0]
        max_ask_px = asks_sorted[-1][0]
        vol_per_min = (volume_24h_usd / 1440.0) if volume_24h_usd > 0 else 0.0
        now = time.time()
        now_utc = datetime.now(timezone.utc).strftime("%H:%M:%S")

        # Шаг 1: обновляем уже отслеживаемые стены и проверяем, не надкусили ли их
        for side, levels, ref_px, lo_cov, hi_cov in (
            ("BID", bids_sorted, best_bid, min_bid_px * 0.999, best_bid * 1.002),
            ("ASK", asks_sorted, best_ask, best_ask * 0.998, max_ask_px * 1.001),
        ):
            t_key = (exchange, market_type, symbol, side)
            t_list = self._tracked.get(t_key)
            if not t_list:
                continue

            survivors: List[_TrackedWall] = []
            for tw in t_list:
                # Проверяем, не прошла ли цена уже сквозь эту стену
                crossed = (best_bid < tw.wall_price * 0.998) if side == "BID" else (best_ask > tw.wall_price * 1.002)
                if crossed:
                    if tw.missing_since_ts <= 0.0:
                        tw.missing_since_ts = now
                    act = self._active.get(t_key)
                    if act and abs(act[0].wall_price - tw.wall_price) / max(tw.wall_price, 1e-12) <= 0.002:
                        self._active.pop(t_key, None)
                    if (now - tw.missing_since_ts) < WALL_ABSENCE_FORGET_SEC:
                        survivors.append(tw)
                    continue

                # Если срез стакана (например, WS depth20) не достаёт до уровня стены — не трогаем её
                if not (lo_cov <= tw.wall_price <= hi_cov):
                    if (now - tw.last_seen_ts) < 900.0:
                        survivors.append(tw)
                    continue

                # Суммируем текущий объём прямо на уровне этой стены (±0.08%)
                cur_usd = sum(
                    px * qty
                    for px, qty in levels
                    if abs(px - tw.wall_price) / tw.wall_price <= 0.0008
                )
                dist_pct = (
                    ((ref_px - tw.wall_price) / ref_px * 100.0)
                    if side == "BID"
                    else ((tw.wall_price - ref_px) / ref_px * 100.0)
                )
                dist_pct = max(0.0, dist_pct)
                tw.min_dist_pct = min(tw.min_dist_pct, dist_pct)

                # Если стена исчезла/полностью разъедена — держим в памяти 5 минут (защита от мигания), потом забываем
                if cur_usd < max(10_000.0, tw.peak_usd * 0.08):
                    if tw.missing_since_ts <= 0.0:
                        tw.missing_since_ts = now
                    if (now - tw.missing_since_ts) < WALL_ABSENCE_FORGET_SEC:
                        survivors.append(tw)
                    else:
                        act = self._active.get(t_key)
                        if act and abs(act[0].wall_price - tw.wall_price) / max(tw.wall_price, 1e-12) <= 0.002:
                            self._active.pop(t_key, None)
                    continue

                # Стена на месте — обновляем состояние без повторного спама
                tw.last_seen_ts = now
                tw.missing_since_ts = 0.0
                tw.last_usd = cur_usd
                if volume_24h_usd > 0:
                    tw.volume_24h_usd = volume_24h_usd

                if not tw.alerted_new:
                    tw.peak_usd = max(tw.peak_usd, cur_usd)
                    survivors.append(tw)
                    continue

                # Если стену ещё не кусали, а в неё докинули объёма — тихо обновляем пиковый размер
                if tw.bitten_count == 0 and cur_usd > tw.peak_usd:
                    tw.peak_usd = cur_usd
                    tw.last_alert_usd = cur_usd

                eaten_from_peak_pct = max(0.0, (tw.peak_usd - cur_usd) / max(tw.peak_usd, 1.0) * 100.0)
                drop_from_last_pct = max(0.0, (tw.last_alert_usd - cur_usd) / max(tw.last_alert_usd, 1.0) * 100.0)
                eat_min = round(cur_usd / vol_per_min, 1) if vol_per_min > 0 else 0.0

                live_ev = DensityEvent(
                    exchange=exchange,
                    market_type=market_type,
                    symbol=symbol,
                    side=side,
                    price=ref_px,
                    wall_price=tw.wall_price,
                    wall_usd=round(cur_usd, 2),
                    distance_pct=round(dist_pct, 2),
                    anomaly_x=tw.anomaly_x,
                    eat_minutes=eat_min,
                    volume_24h_usd=round(volume_24h_usd or tw.volume_24h_usd, 2),
                    timestamp_utc=now_utc,
                    event_type="BITTEN" if tw.bitten_count > 0 else "NEW",
                    initial_wall_usd=round(tw.peak_usd, 2),
                    eaten_pct=round(eaten_from_peak_pct, 1),
                )
                self._active[t_key] = (live_ev, now)

                # Отправляем алерт «надкусана», только если откусили >= 15% (повторно >= 25%) и цена подходила близко
                need_drop = WALL_BITE_FIRST_PCT if tw.bitten_count == 0 else WALL_BITE_NEXT_PCT
                near_enough = (dist_pct <= WALL_BITE_MAX_DIST_PCT) or (tw.min_dist_pct <= 1.5)
                if (
                    tw.bitten_count < WALL_BITE_MAX_ALERTS
                    and drop_from_last_pct >= need_drop
                    and eaten_from_peak_pct >= WALL_BITE_FIRST_PCT
                    and cur_usd >= max(15_000.0, tw.peak_usd * 0.12)
                    and near_enough
                    and (now - tw.last_alert_ts) >= WALL_BITE_MIN_GAP_SEC
                ):
                    tw.bitten_count += 1
                    tw.last_alert_usd = cur_usd
                    tw.last_alert_ts = now
                    bitten_ev = DensityEvent(
                        exchange=exchange,
                        market_type=market_type,
                        symbol=symbol,
                        side=side,
                        price=ref_px,
                        wall_price=tw.wall_price,
                        wall_usd=round(cur_usd, 2),
                        distance_pct=round(dist_pct, 2),
                        anomaly_x=tw.anomaly_x,
                        eat_minutes=eat_min,
                        volume_24h_usd=round(volume_24h_usd or tw.volume_24h_usd, 2),
                        timestamp_utc=now_utc,
                        event_type="BITTEN",
                        initial_wall_usd=round(tw.peak_usd, 2),
                        eaten_pct=round(eaten_from_peak_pct, 1),
                    )
                    self._active[t_key] = (bitten_ev, now)
                    await self.on_density(bitten_ev)

                survivors.append(tw)

            if survivors:
                self._tracked[t_key] = survivors
            else:
                self._tracked.pop(t_key, None)

        # Шаг 2: ищем НОВЫЕ аномальные плотности (уже отслеживаемые повторно не шлём!)
        min_wall = self.min_spot_usd if market_type == "SPOT" else self.min_fut_usd
        events = analyze_orderbook_levels(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
            bids=bids_sorted,
            asks=asks_sorted,
            volume_24h_usd=volume_24h_usd,
            min_wall_usd=min_wall,
            max_distance_pct=self.max_dist_pct,
            min_anomaly_x=self.min_anom_x,
        )
        if not events:
            return

        for ev in events:
            t_key = (exchange, market_type, symbol, ev.side)
            t_list = self._tracked.setdefault(t_key, [])
            existing = self._find_matching_wall(t_list, ev.wall_price)

            if existing is not None:
                # Если первый алерт по этой стене уже был — тихо пропускаем (никаких дублей!)
                if existing.alerted_new:
                    continue
                existing.last_seen_ts = now
                existing.peak_usd = max(existing.peak_usd, ev.wall_usd)
                existing.last_usd = ev.wall_usd
                existing.anomaly_x = max(existing.anomaly_x, ev.anomaly_x)
                existing.min_dist_pct = min(existing.min_dist_pct, ev.distance_pct)
                if from_full_scan or self.min_life_sec <= 0.0 or (now - existing.first_seen_ts) >= self.min_life_sec:
                    existing.alerted_new = True
                    existing.last_alert_usd = ev.wall_usd
                    existing.last_alert_ts = now
                    ev.initial_wall_usd = existing.peak_usd
                    self._active[t_key] = (ev, now)
                    await self.on_density(ev)
                continue

            # Совершенно новая плотность на этом уровне
            tw = _TrackedWall(
                wall_price=ev.wall_price,
                peak_usd=ev.wall_usd,
                last_usd=ev.wall_usd,
                last_alert_usd=ev.wall_usd,
                anomaly_x=ev.anomaly_x,
                volume_24h_usd=ev.volume_24h_usd,
                first_seen_ts=now,
                last_seen_ts=now,
                min_dist_pct=ev.distance_pct,
            )
            t_list.append(tw)
            if len(t_list) > 12:
                t_list.sort(key=lambda w: w.last_seen_ts, reverse=True)
                del t_list[12:]

            if from_full_scan or self.min_life_sec <= 0.0:
                tw.alerted_new = True
                tw.last_alert_ts = now
                ev.initial_wall_usd = ev.wall_usd
                self._active[t_key] = (ev, now)
                await self.on_density(ev)

    def get_active(self) -> List[DensityEvent]:
        now = time.time()
        expired = [k for k, (_, ts) in self._active.items() if (now - ts) > ACTIVE_WALL_TTL_SEC]
        for k in expired:
            self._active.pop(k, None)
        # Фоновая чистка старых записей из _tracked
        if len(self._tracked) > 2500:
            for k in list(self._tracked.keys()):
                fresh = [w for w in self._tracked[k] if (now - w.last_seen_ts) < 900.0]
                if fresh:
                    self._tracked[k] = fresh
                else:
                    self._tracked.pop(k, None)
        items = [ev for ev, _ in self._active.values()]
        items.sort(key=lambda x: x.wall_usd, reverse=True)
        return items


# ==================== КЭШ МЕТАДАННЫХ БИРЖ И СКАНИРОВАНИЕ СТАКАНОВ ====================

async def _fetch_json(
    session: aiohttp.ClientSession,
    url: str,
    timeout_sec: float = 8.0,
) -> Optional[Any]:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout_sec)) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception:
        return None


def _parse_pairs(raw_levels: Any, mult: float = 1.0) -> List[Tuple[float, float]]:
    out: List[Tuple[float, float]] = []
    if not isinstance(raw_levels, list):
        return out
    for lv in raw_levels:
        if isinstance(lv, (list, tuple)) and len(lv) >= 2:
            px = _safe_float(lv[0])
            qty = _safe_float(lv[1]) * mult
            if px > 0 and qty > 0:
                out.append((px, qty))
        elif isinstance(lv, dict):
            px = _safe_float(lv.get("p") or lv.get("price"))
            qty = _safe_float(lv.get("s") or lv.get("size") or lv.get("qty")) * mult
            if px > 0 and qty > 0:
                out.append((px, qty))
    return out


class MarketCatalog:
    """Хранит актуальные списки пар, объёмы 24ч и множители контрактов по всем 8 биржам."""

    def __init__(self) -> None:
        # (exchange, market_type) -> Dict[symbol, Dict[str, Any]]
        # item: {"vol": float, "mult": float, "raw_sym": str, "display": str}
        self.markets: Dict[Tuple[str, str], Dict[str, Dict[str, Any]]] = {}
        self.last_refresh_ts: float = 0.0

    async def refresh(self, session: aiohttp.ClientSession) -> None:
        new_m: Dict[Tuple[str, str], Dict[str, Dict[str, Any]]] = {}

        async def load_binance_like(ex: str, fut_url: str, spot_url: str) -> None:
            fut_data = await _fetch_json(session, fut_url, 10.0)
            if isinstance(fut_data, list):
                m_f: Dict[str, Dict[str, Any]] = {}
                for it in fut_data:
                    sym = str(it.get("symbol") or "")
                    if sym.endswith("USDT"):
                        m_f[sym] = {
                            "vol": _safe_float(it.get("quoteVolume")),
                            "mult": 1.0,
                            "raw_sym": sym,
                            "display": sym,
                        }
                new_m[(ex, "FUTURES")] = m_f

            spot_data = await _fetch_json(session, spot_url, 10.0)
            if isinstance(spot_data, list):
                m_s: Dict[str, Dict[str, Any]] = {}
                for it in spot_data:
                    sym = str(it.get("symbol") or "")
                    if sym.endswith("USDT"):
                        m_s[sym] = {
                            "vol": _safe_float(it.get("quoteVolume")),
                            "mult": 1.0,
                            "raw_sym": sym,
                            "display": sym,
                        }
                new_m[(ex, "SPOT")] = m_s

        async def load_binance_alpha() -> None:
            m_a: Dict[str, Dict[str, Any]] = {}
            alpha_vol_by_id: Dict[str, Tuple[str, float]] = {}
            tok_data = await _fetch_json(
                session,
                "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list",
                12.0,
            )
            for it in (tok_data or {}).get("data") or []:
                aid = str(it.get("alphaId") or "").strip()
                ticker = str(it.get("symbol") or "").upper().strip()
                vol = _safe_float(it.get("volume24h"))
                if aid:
                    alpha_vol_by_id[aid.upper()] = (ticker or aid.upper(), vol)

            ex_info = await _fetch_json(
                session,
                "https://www.binance.com/bapi/defi/v1/public/alpha-trade/get-exchange-info",
                12.0,
            )
            symbols_list = ((ex_info or {}).get("data") or {}).get("symbols") or []
            for s in symbols_list:
                raw_sym = str(s.get("symbol") or "").strip()
                if not raw_sym or str(s.get("status") or "").upper() not in ("TRADING", "ONLINE", ""):
                    continue
                base_asset = str(s.get("baseAsset") or "").upper().strip()
                disp_ticker, vol = alpha_vol_by_id.get(base_asset, (base_asset.replace("ALPHA_", ""), 0.0))
                display = f"{disp_ticker}USDT" if disp_ticker and not disp_ticker.endswith("USDT") else raw_sym
                m_a[raw_sym] = {
                    "vol": vol,
                    "mult": 1.0,
                    "raw_sym": raw_sym,
                    "display": display,
                }
            if m_a:
                new_m[("BinanceAlpha", "SPOT")] = m_a

        async def load_bybit() -> None:
            for cat, mkt in (("linear", "FUTURES"), ("spot", "SPOT")):
                data = await _fetch_json(
                    session, f"https://api.bybit.com/v5/market/tickers?category={cat}", 10.0
                )
                m: Dict[str, Dict[str, Any]] = {}
                for it in ((data or {}).get("result") or {}).get("list") or []:
                    sym = str(it.get("symbol") or "")
                    if sym.endswith("USDT"):
                        m[sym] = {
                            "vol": _safe_float(it.get("turnover24h")),
                            "mult": 1.0,
                            "raw_sym": sym,
                            "display": sym,
                        }
                new_m[("Bybit", mkt)] = m

        async def load_okx() -> None:
            ct_vals: Dict[str, float] = {}
            inst = await _fetch_json(
                session, "https://www.okx.com/api/v5/public/instruments?instType=SWAP", 10.0
            )
            for it in (inst or {}).get("data") or []:
                iid = str(it.get("instId") or "")
                if iid.endswith("-USDT-SWAP"):
                    ct_vals[iid] = _safe_float(it.get("ctVal"), 1.0)

            for inst_type, mkt, suf in (("SWAP", "FUTURES", "-USDT-SWAP"), ("SPOT", "SPOT", "-USDT")):
                data = await _fetch_json(
                    session, f"https://www.okx.com/api/v5/market/tickers?instType={inst_type}", 10.0
                )
                m: Dict[str, Dict[str, Any]] = {}
                for it in (data or {}).get("data") or []:
                    iid = str(it.get("instId") or "")
                    if not iid.endswith(suf):
                        continue
                    lp = _safe_float(it.get("last"))
                    vol = _safe_float(it.get("volCcy24h")) * lp if inst_type == "SWAP" else _safe_float(it.get("volCcy24h"))
                    disp = iid.replace("-USDT-SWAP", "USDT").replace("-USDT", "USDT")
                    m[iid] = {
                        "vol": vol,
                        "mult": ct_vals.get(iid, 1.0) if inst_type == "SWAP" else 1.0,
                        "raw_sym": iid,
                        "display": disp,
                    }
                new_m[("OKX", mkt)] = m

        async def load_bitget() -> None:
            f_data = await _fetch_json(
                session, "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES", 10.0
            )
            m_f: Dict[str, Dict[str, Any]] = {}
            for it in (f_data or {}).get("data") or []:
                sym = str(it.get("symbol") or "")
                if sym.endswith("USDT"):
                    m_f[sym] = {
                        "vol": _safe_float(it.get("usdtVolume")),
                        "mult": 1.0,
                        "raw_sym": sym,
                        "display": sym,
                    }
            new_m[("Bitget", "FUTURES")] = m_f

            s_data = await _fetch_json(session, "https://api.bitget.com/api/v2/spot/market/tickers", 10.0)
            m_s: Dict[str, Dict[str, Any]] = {}
            for it in (s_data or {}).get("data") or []:
                sym = str(it.get("symbol") or "")
                if sym.endswith("USDT"):
                    m_s[sym] = {
                        "vol": _safe_float(it.get("usdtVolume") or it.get("quoteVolume")),
                        "mult": 1.0,
                        "raw_sym": sym,
                        "display": sym,
                    }
            new_m[("Bitget", "SPOT")] = m_s

        async def load_gate() -> None:
            q_mults: Dict[str, float] = {}
            c_data = await _fetch_json(session, "https://api.gateio.ws/api/v4/futures/usdt/contracts", 10.0)
            if isinstance(c_data, list):
                for it in c_data:
                    name = str(it.get("name") or "")
                    if name:
                        q_mults[name] = _safe_float(it.get("quanto_multiplier"), 1.0)

            f_data = await _fetch_json(session, "https://api.gateio.ws/api/v4/futures/usdt/tickers", 10.0)
            m_f: Dict[str, Dict[str, Any]] = {}
            if isinstance(f_data, list):
                for it in f_data:
                    c = str(it.get("contract") or "")
                    if c.endswith("_USDT"):
                        m_f[c] = {
                            "vol": _safe_float(it.get("volume_24h_quote") or it.get("volume_24h_settle")),
                            "mult": q_mults.get(c, 1.0),
                            "raw_sym": c,
                            "display": c.replace("_USDT", "USDT"),
                        }
            new_m[("Gate", "FUTURES")] = m_f

            s_data = await _fetch_json(session, "https://api.gateio.ws/api/v4/spot/tickers", 10.0)
            m_s: Dict[str, Dict[str, Any]] = {}
            if isinstance(s_data, list):
                for it in s_data:
                    c = str(it.get("currency_pair") or "")
                    if c.endswith("_USDT"):
                        m_s[c] = {
                            "vol": _safe_float(it.get("quote_volume")),
                            "mult": 1.0,
                            "raw_sym": c,
                            "display": c.replace("_USDT", "USDT"),
                        }
            new_m[("Gate", "SPOT")] = m_s

        async def load_kucoin() -> None:
            f_data = await _fetch_json(session, "https://api-futures.kucoin.com/api/v1/contracts/active", 10.0)
            m_f: Dict[str, Dict[str, Any]] = {}
            for it in (f_data or {}).get("data") or []:
                sym = str(it.get("symbol") or "")
                if sym.endswith("USDTM"):
                    disp = "BTCUSDT" if sym == "XBTUSDTM" else sym[:-1]
                    m_f[sym] = {
                        "vol": _safe_float(it.get("turnoverOf24h")),
                        "mult": _safe_float(it.get("multiplier"), 1.0),
                        "raw_sym": sym,
                        "display": disp,
                    }
            new_m[("KuCoin", "FUTURES")] = m_f

            s_data = await _fetch_json(session, "https://api.kucoin.com/api/v1/market/allTickers", 10.0)
            m_s: Dict[str, Dict[str, Any]] = {}
            for it in ((s_data or {}).get("data") or {}).get("ticker") or []:
                sym = str(it.get("symbol") or "")
                if sym.endswith("-USDT"):
                    m_s[sym] = {
                        "vol": _safe_float(it.get("volValue")),
                        "mult": 1.0,
                        "raw_sym": sym,
                        "display": sym.replace("-USDT", "USDT"),
                    }
            new_m[("KuCoin", "SPOT")] = m_s

        await asyncio.gather(
            load_binance_like(
                "Binance",
                "https://fapi.binance.com/fapi/v1/ticker/24hr",
                "https://api.binance.com/api/v3/ticker/24hr",
            ),
            load_binance_like(
                "Aster",
                "https://fapi.asterdex.com/fapi/v1/ticker/24hr",
                "https://sapi.asterdex.com/api/v1/ticker/24hr",
            ),
            load_binance_alpha(),
            load_bybit(),
            load_okx(),
            load_bitget(),
            load_gate(),
            load_kucoin(),
            return_exceptions=True,
        )

        for k, v in new_m.items():
            if v:
                self.markets[k] = v
        self.last_refresh_ts = time.time()
        total_pairs = sum(len(v) for v in self.markets.values())
        logger.info("[Catalog] Обновлено пар по 8 биржам: %s (рынков: %s)", total_pairs, len(self.markets))


catalog = MarketCatalog()


async def fetch_deep_orderbook(
    session: aiohttp.ClientSession,
    exchange: str,
    market_type: str,
    raw_sym: str,
    mult: float = 1.0,
) -> Optional[Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]]:
    """Загружает глубокий стакан (100–500 уровней) для проверки ±10% от спреда."""
    if exchange == "Binance":
        url = (
            f"https://fapi.binance.com/fapi/v1/depth?symbol={raw_sym}&limit=500"
            if market_type == "FUTURES"
            else f"https://api.binance.com/api/v3/depth?symbol={raw_sym}&limit=500"
        )
        d = await _fetch_json(session, url, 5.0)
        if not isinstance(d, dict):
            return None
        return _parse_pairs(d.get("bids"), mult), _parse_pairs(d.get("asks"), mult)

    if exchange == "Aster":
        url = (
            f"https://fapi.asterdex.com/fapi/v1/depth?symbol={raw_sym}&limit=500"
            if market_type == "FUTURES"
            else f"https://sapi.asterdex.com/api/v1/depth?symbol={raw_sym}&limit=500"
        )
        d = await _fetch_json(session, url, 5.0)
        if not isinstance(d, dict):
            return None
        return _parse_pairs(d.get("bids"), mult), _parse_pairs(d.get("asks"), mult)

    if exchange == "BinanceAlpha":
        url = f"https://www.binance.com/bapi/defi/v1/public/alpha-trade/depth?symbol={raw_sym}&limit=500"
        d = await _fetch_json(session, url, 6.0)
        body = (d or {}).get("data") if isinstance(d, dict) else None
        if not isinstance(body, dict):
            return None
        return _parse_pairs(body.get("bids"), mult), _parse_pairs(body.get("asks"), mult)

    if exchange == "Bybit":
        cat = "linear" if market_type == "FUTURES" else "spot"
        url = f"https://api.bybit.com/v5/market/orderbook?category={cat}&symbol={raw_sym}&limit=200"
        d = await _fetch_json(session, url, 5.0)
        res = (d or {}).get("result") if isinstance(d, dict) else None
        if not isinstance(res, dict):
            return None
        return _parse_pairs(res.get("b"), mult), _parse_pairs(res.get("a"), mult)

    if exchange == "OKX":
        url = f"https://www.okx.com/api/v5/market/books?instId={raw_sym}&sz=200"
        d = await _fetch_json(session, url, 5.0)
        arr = (d or {}).get("data") if isinstance(d, dict) else None
        if not arr or not isinstance(arr[0], dict):
            return None
        return _parse_pairs(arr[0].get("bids"), mult), _parse_pairs(arr[0].get("asks"), mult)

    if exchange == "Bitget":
        url = (
            f"https://api.bitget.com/api/v2/mix/market/merge-depth?productType=USDT-FUTURES&symbol={raw_sym}&limit=100"
            if market_type == "FUTURES"
            else f"https://api.bitget.com/api/v2/spot/market/orderbook?symbol={raw_sym}&type=step0&limit=150"
        )
        d = await _fetch_json(session, url, 5.0)
        res = (d or {}).get("data") if isinstance(d, dict) else None
        if not isinstance(res, dict):
            return None
        return _parse_pairs(res.get("bids"), mult), _parse_pairs(res.get("asks"), mult)

    if exchange == "Gate":
        url = (
            f"https://api.gateio.ws/api/v4/futures/usdt/order_book?contract={raw_sym}&limit=100"
            if market_type == "FUTURES"
            else f"https://api.gateio.ws/api/v4/spot/order_book?currency_pair={raw_sym}&limit=100"
        )
        d = await _fetch_json(session, url, 5.0)
        if not isinstance(d, dict):
            return None
        return _parse_pairs(d.get("bids"), mult), _parse_pairs(d.get("asks"), mult)

    if exchange == "KuCoin":
        url = (
            f"https://api-futures.kucoin.com/api/v1/level2/depth100?symbol={raw_sym}"
            if market_type == "FUTURES"
            else f"https://api.kucoin.com/api/v1/market/orderbook/level2_100?symbol={raw_sym}"
        )
        d = await _fetch_json(session, url, 5.0)
        res = (d or {}).get("data") if isinstance(d, dict) else None
        if not isinstance(res, dict):
            return None
        return _parse_pairs(res.get("bids"), mult), _parse_pairs(res.get("asks"), mult)

    return None


async def scan_single_coin_live(
    session: aiohttp.ClientSession,
    coin_query: str,
    min_spot_usd: float,
    min_fut_usd: float,
    max_dist_pct: float,
    min_anom_x: float = 1.0,
) -> List[DensityEvent]:
    """Живой скан глубоких стаканов по одной монете сразу на всех 8 биржах (Спот + Фьючерсы + Alpha)."""
    target = extract_base_asset(coin_query)
    if not target:
        return []
    if not catalog.markets or (time.time() - catalog.last_refresh_ts) > 600:
        await catalog.refresh(session)

    jobs: List[Tuple[str, str, str, str, float, float]] = []
    for (ex, mkt), mp in catalog.markets.items():
        for raw_sym, info in mp.items():
            disp = str(info.get("display") or raw_sym)
            if extract_base_asset(disp) == target or extract_base_asset(raw_sym) == target:
                jobs.append((ex, mkt, raw_sym, disp, float(info.get("mult") or 1.0), float(info.get("vol") or 0.0)))

    if not jobs:
        return []

    sem = asyncio.Semaphore(12)
    results: List[DensityEvent] = []

    async def _worker(ex: str, mkt: str, raw_sym: str, disp: str, mult: float, vol: float) -> None:
        async with sem:
            ob = await fetch_deep_orderbook(session, ex, mkt, raw_sym, mult)
            if not ob:
                return
            bids, asks = ob
            min_w = min_spot_usd if mkt == "SPOT" else min_fut_usd
            evs = analyze_orderbook_levels(
                exchange=ex,
                market_type=mkt,
                symbol=disp,
                bids=bids,
                asks=asks,
                volume_24h_usd=vol,
                min_wall_usd=min_w,
                max_distance_pct=max_dist_pct,
                min_anomaly_x=min_anom_x,
            )
            results.extend(evs)

    await asyncio.gather(*(_worker(*j) for j in jobs), return_exceptions=True)
    results.sort(key=lambda x: x.wall_usd, reverse=True)
    return results


# ==================== ФОНОВЫЙ ПОЛНЫЙ ОБХОД РАЗ В 3 МИНУТЫ + WS-ПОТОКИ ====================

async def full_market_scan_loop(session: aiohttp.ClientSession, eng: DensityEngine) -> None:
    """
    Полный обход ВСЕХ незаблокированных монет на всех 8 биржах каждые 3 минуты
    с проверкой глубоких стаканов в диапазоне ±10% от спреда.
    """
    await asyncio.sleep(3)
    while True:
        cycle_start = time.time()
        try:
            await catalog.refresh(session)
            # Группируем задания по биржам, чтобы не упираться в rate limit одной биржи
            ex_sems = {ex: asyncio.Semaphore(5 if ex in ("Binance", "Bybit") else 3) for ex in ALL_EXCHANGES}
            checked = 0

            async def scan_exchange(ex: str) -> None:
                nonlocal checked
                sem = ex_sems[ex]
                items: List[Tuple[str, str, str, float, float]] = []
                for mkt in ("FUTURES", "SPOT"):
                    mp = catalog.markets.get((ex, mkt)) or {}
                    for raw_sym, info in mp.items():
                        disp = str(info.get("display") or raw_sym)
                        if is_symbol_blocked_for_scan(disp, ex):
                            continue
                        vol = float(info.get("vol") or 0.0)
                        base = extract_base_asset(disp)
                        # Берём все Whitelist-монеты + BinanceAlpha + все активные монеты с оборотом от $50K
                        if base not in _global_allowed_bases and ex != "BinanceAlpha" and vol < 50_000.0:
                            continue
                        items.append((mkt, raw_sym, disp, float(info.get("mult") or 1.0), vol))

                items.sort(key=lambda x: (extract_base_asset(x[2]) in _global_allowed_bases, x[4]), reverse=True)

                async def one_pair(mkt: str, raw_sym: str, disp: str, mult: float, vol: float) -> None:
                    nonlocal checked
                    async with sem:
                        ob = await fetch_deep_orderbook(session, ex, mkt, raw_sym, mult)
                        if not ob:
                            return
                        checked += 1
                        bids, asks = ob
                        await eng.evaluate_orderbook(
                            exchange=ex,
                            market_type=mkt,
                            symbol=disp,
                            bids=bids,
                            asks=asks,
                            volume_24h_usd=vol,
                            from_full_scan=True,
                        )
                        await asyncio.sleep(0.06)

                await asyncio.gather(*(one_pair(*it) for it in items), return_exceptions=True)

            await asyncio.gather(*(scan_exchange(ex) for ex in ALL_EXCHANGES), return_exceptions=True)
            elapsed = time.time() - cycle_start
            logger.info("[FullScan 3m] Проверено %s стаканов по 8 биржам за %.1fс", checked, elapsed)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[FullScan 3m] Ошибка цикла: %s", exc)

        sleep_for = max(15.0, FULL_SCAN_INTERVAL_SEC - (time.time() - cycle_start))
        await asyncio.sleep(sleep_for)


async def _binance_ws_scanner(
    session: aiohttp.ClientSession,
    exchange_name: str,
    market_type: str,
    ws_base: str,
    eng: DensityEngine,
) -> None:
    """Быстрый WebSocket-сканер топ-60 пар (Binance / Aster, Futures + Spot)."""
    while True:
        try:
            mp = catalog.markets.get((exchange_name, market_type)) or {}
            if not mp:
                await asyncio.sleep(8)
                continue
            wl_ver = _whitelist_version
            cands = [
                (raw_sym, info)
                for raw_sym, info in mp.items()
                if not is_symbol_blocked_for_scan(str(info.get("display") or raw_sym), exchange_name)
                and (
                    extract_base_asset(str(info.get("display") or raw_sym)) in _global_allowed_bases
                    or float(info.get("vol") or 0.0) >= 200_000.0
                )
            ]
            cands.sort(
                key=lambda x: (
                    extract_base_asset(str(x[1].get("display") or x[0])) in _global_allowed_bases,
                    float(x[1].get("vol") or 0.0),
                ),
                reverse=True,
            )
            top = cands[:60]
            if not top:
                await asyncio.sleep(15)
                continue

            vol_map = {raw_sym.upper(): float(info.get("vol") or 0.0) for raw_sym, info in top}
            streams = "/".join(f"{raw_sym.lower()}@depth20@500ms" for raw_sym, _ in top)
            url = f"{ws_base}/stream?streams={streams}"
            started = time.time()

            async with session.ws_connect(url, heartbeat=20, timeout=15) as ws:
                logger.info("[%s %s WS] Подписан на %s пар", exchange_name, market_type, len(top))
                async for msg in ws:
                    if time.time() - started > 1500 or _whitelist_version != wl_ver:
                        break
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                        continue
                    payload = json.loads(msg.data)
                    stream = str(payload.get("stream") or "")
                    d = payload.get("data")
                    if not isinstance(d, dict) or "@" not in stream:
                        continue
                    sym = stream.split("@", 1)[0].upper()
                    bids = _parse_pairs(d.get("b") or d.get("bids"))
                    asks = _parse_pairs(d.get("a") or d.get("asks"))
                    if bids and asks:
                        await eng.evaluate_orderbook(
                            exchange=exchange_name,
                            market_type=market_type,
                            symbol=sym,
                            bids=bids,
                            asks=asks,
                            volume_24h_usd=vol_map.get(sym, 0.0),
                        )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("[%s %s WS] Переподключение через 6с: %s", exchange_name, market_type, exc)
            await asyncio.sleep(6)


async def _bybit_ws_scanner(
    session: aiohttp.ClientSession,
    market_type: str,
    eng: DensityEngine,
) -> None:
    """Быстрый WebSocket-сканер orderbook.50 для Bybit (Linear + Spot)."""
    cat = "linear" if market_type == "FUTURES" else "spot"
    ws_url = f"wss://stream.bybit.com/v5/public/{cat}"
    while True:
        try:
            mp = catalog.markets.get(("Bybit", market_type)) or {}
            if not mp:
                await asyncio.sleep(8)
                continue
            wl_ver = _whitelist_version
            cands = [
                (raw_sym, info)
                for raw_sym, info in mp.items()
                if not is_symbol_blocked_for_scan(raw_sym, "Bybit")
                and (extract_base_asset(raw_sym) in _global_allowed_bases or float(info.get("vol") or 0.0) >= 200_000.0)
            ]
            cands.sort(
                key=lambda x: (extract_base_asset(x[0]) in _global_allowed_bases, float(x[1].get("vol") or 0.0)),
                reverse=True,
            )
            top = cands[:50]
            if not top:
                await asyncio.sleep(15)
                continue

            vol_map = {s: float(info.get("vol") or 0.0) for s, info in top}
            books: Dict[str, Tuple[Dict[float, float], Dict[float, float]]] = {}
            last_eval: Dict[str, float] = {}
            started = time.time()

            async with session.ws_connect(ws_url, heartbeat=20, timeout=15) as ws:
                for i in range(0, len(top), 10):
                    args = [f"orderbook.50.{s}" for s, _ in top[i : i + 10]]
                    await ws.send_json({"op": "subscribe", "args": args})
                logger.info("[Bybit %s WS] Подписан на %s пар", market_type, len(top))

                async def pinger() -> None:
                    while True:
                        await asyncio.sleep(20)
                        try:
                            await ws.send_json({"op": "ping"})
                        except Exception:
                            break

                pt = asyncio.create_task(pinger())
                try:
                    async for msg in ws:
                        if time.time() - started > 1500 or _whitelist_version != wl_ver:
                            break
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                            continue
                        payload = json.loads(msg.data)
                        topic = str(payload.get("topic") or "")
                        if not topic.startswith("orderbook.50."):
                            continue
                        sym = topic.split(".")[-1]
                        d = payload.get("data")
                        if not isinstance(d, dict):
                            continue
                        msg_type = payload.get("type")
                        if msg_type == "snapshot" or sym not in books:
                            b_map = {px: qty for px, qty in _parse_pairs(d.get("b"))}
                            a_map = {px: qty for px, qty in _parse_pairs(d.get("a"))}
                            books[sym] = (b_map, a_map)
                        else:
                            b_map, a_map = books[sym]
                            for lv in d.get("b") or []:
                                if len(lv) >= 2:
                                    px, qty = _safe_float(lv[0]), _safe_float(lv[1])
                                    if qty <= 0:
                                        b_map.pop(px, None)
                                    elif px > 0:
                                        b_map[px] = qty
                            for lv in d.get("a") or []:
                                if len(lv) >= 2:
                                    px, qty = _safe_float(lv[0]), _safe_float(lv[1])
                                    if qty <= 0:
                                        a_map.pop(px, None)
                                    elif px > 0:
                                        a_map[px] = qty

                        now = time.time()
                        if (now - last_eval.get(sym, 0.0)) < 1.0:
                            continue
                        last_eval[sym] = now
                        b_map, a_map = books[sym]
                        if b_map and a_map:
                            await eng.evaluate_orderbook(
                                exchange="Bybit",
                                market_type=market_type,
                                symbol=sym,
                                bids=list(b_map.items()),
                                asks=list(a_map.items()),
                                volume_24h_usd=vol_map.get(sym, 0.0),
                            )
                finally:
                    pt.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("[Bybit %s WS] Переподключение через 6с: %s", market_type, exc)
            await asyncio.sleep(6)


async def _tradfi_refresher_loop(session: aiohttp.ClientSession) -> None:
    while True:
        await asyncio.sleep(1800)
        try:
            await refresh_tradfi_metadata(session)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass


# ==================== TELEGRAM BOT + ОБРАБОТКА СОБЫТИЙ ====================

bot: Optional[Bot] = Bot(token=BOT_TOKEN) if BOT_TOKEN else None
dp = Dispatcher()


def _build_main_keyboard() -> Optional[InlineKeyboardMarkup]:
    if not WEBAPP_URL:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🧱 Открыть Скринер Плотностей", web_app=WebAppInfo(url=WEBAPP_URL))]
        ]
    )


def _build_alert_keyboard(base_coin: str, is_banned: bool = False) -> InlineKeyboardMarkup:
    """Клавиатура под каждым алертом с кнопкой мгновенной блокировки/разблокировки монеты."""
    rows: List[List[InlineKeyboardButton]] = []
    if base_coin:
        if is_banned:
            rows.append(
                [
                    InlineKeyboardButton(
                        text=f"✅ {base_coin} в ЧС (нажми, чтобы вернуть)",
                        callback_data=f"unban_coin:{base_coin}",
                    )
                ]
            )
        else:
            rows.append(
                [
                    InlineKeyboardButton(
                        text=f"🚫 Заблокировать {base_coin}",
                        callback_data=f"ban_coin:{base_coin}",
                    )
                ]
            )
    if WEBAPP_URL:
        rows.append([InlineKeyboardButton(text="🧱 Открыть Скринер Плотностей", web_app=WebAppInfo(url=WEBAPP_URL))])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _format_ban_menu(bans: List[str]) -> Tuple[str, InlineKeyboardMarkup]:
    """Интерактивное меню управления личным чёрным списком монет."""
    if bans:
        text = (
            "🚫 <b>Ваш чёрный список монет</b>\n"
            f"Заблокировано: <b>{len(bans)}</b>\n\n"
            "Нажмите на монету ниже, чтобы вернуть её в алерты.\n"
            "Добавить новые: <code>/ban WIF PEPE</code>"
        )
    else:
        text = (
            "🚫 <b>Ваш чёрный список монет пуст</b>\n\n"
            "Заблокировать монету можно тремя способами:\n"
            "• Кнопкой <b>«🚫 Заблокировать ...»</b> прямо под любым алертом\n"
            "• Командой <code>/ban WIF</code> (или сразу несколько: <code>/ban WIF RIVER</code>)\n"
            "• Кнопкой <b>🚫</b> на карточке в Mini App"
        )

    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for coin in bans[:30]:
        row.append(InlineKeyboardButton(text=f"✕ {coin}", callback_data=f"ban_rm:{coin}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    if bans:
        rows.append([InlineKeyboardButton(text="🗑 Очистить весь ЧС", callback_data="ban_clear")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад к настройкам", callback_data="back_settings")])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def _format_unblock_menu(allowed: List[str]) -> Tuple[str, InlineKeyboardMarkup]:
    """Интерактивное меню разблокировки монет из глобального чёрного списка."""
    cur_txt = ", ".join(allowed) if allowed else "нет"
    text = (
        "🔓 <b>Разблокировка монет из глобального ЧС</b>\n"
        f"Сейчас разблокированы: <b>{cur_txt}</b>\n\n"
        "Нажмите на тикер ниже или введите команду <code>/allow BTC SOL XAU</code>:"
    )
    rows: List[List[InlineKeyboardButton]] = []
    row: List[InlineKeyboardButton] = []
    for coin in QUICK_UNBLOCK_COINS:
        on = coin in allowed
        row.append(
            InlineKeyboardButton(
                text=f"{'✅ ' if on else ''}{coin}",
                callback_data=f"toggle_allow:{coin}",
            )
        )
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="⬅️ Назад к настройкам", callback_data="back_settings")])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


def _format_settings_view(
    cfg: Dict[str, Any],
    dcfg: Dict[str, Any],
    bans: List[str],
    allowed: List[str],
) -> Tuple[str, InlineKeyboardMarkup]:
    mkt_lbl = {"BOTH": "Фьючерсы + Спот", "FUTURES": "Только Фьючерсы", "SPOT": "Только Спот"}[cfg["market_mode"]]
    side_lbl = {"BOTH": "BID + ASK", "BID": "Только BID (Лонг)", "ASK": "Только ASK (Шорт)"}[cfg["side_mode"]]
    dc_url = resolve_discord_webhook(dcfg)
    dc_state = "✅ подключён" if dc_url else "❌ не задан"
    watch_txt = ", ".join(cfg["watch_coins"]) if cfg["watch_coins"] else "весь рынок"
    allow_txt = ", ".join(allowed) if allowed else "нет"

    text = (
        "⚙️ <b>Настройки скринера плотностей</b>\n\n"
        f"• Статус алертов: <b>{'🟢 ВКЛ' if cfg['enabled'] else '🔴 ВЫКЛ'}</b>\n"
        f"• Рынки: <b>{mkt_lbl}</b>\n"
        f"• Мин. плотность Спот: <b>{fmt_usd(cfg['min_wall_spot_usd'])}</b> (<code>/minspot</code>)\n"
        f"• Мин. плотность Фьючерсы: <b>{fmt_usd(cfg['min_wall_futures_usd'])}</b> (<code>/minfut</code>)\n"
        f"• Диапазон от спреда: <b>±{cfg['max_distance_pct']:g}%</b> (<code>/dist</code>)\n"
        f"• Аномальность: <b>от x{cfg['min_anomaly_x']:g}</b> (<code>/anom</code>)\n"
        f"• Задержка удержания: <b>{cfg['min_lifetime_sec']:g}с</b> (<code>/life</code>)\n"
        f"• Мин. оборот 24ч: <b>{fmt_usd(cfg['min_vol24h_usd'])}</b> (<code>/minvol</code>)\n"
        f"• Сторона: <b>{side_lbl}</b> (<code>/side</code>)\n"
        f"• Биржи ({len(cfg['exchanges'])}/8): <b>{', '.join(cfg['exchanges'])}</b>\n"
        f"• Watchlist: <b>{watch_txt}</b> (<code>/watch</code>)\n"
        f"• Разблокировано из ЧС: <b>{allow_txt}</b> (<code>/allow</code>)\n"
        f"• Личный ЧС: <b>{len(bans)}</b> монет (<code>/banlist</code>)\n"
        f"• Discord Webhook: <b>{dc_state}</b> (<code>/discord URL</code>)"
    )

    rows = [
        [
            InlineKeyboardButton(
                text="🔴 Выкл алерты" if cfg["enabled"] else "🟢 Вкл алерты",
                callback_data="toggle_enabled",
            ),
            InlineKeyboardButton(text=f"📊 {mkt_lbl}", callback_data="cycle_market"),
        ],
        [
            InlineKeyboardButton(text="⚡ 100K/250K", callback_data="preset:light"),
            InlineKeyboardButton(text="⚖️ 250K/500K", callback_data="preset:std"),
            InlineKeyboardButton(text="🐋 500K/1M", callback_data="preset:whale"),
        ],
        [
            InlineKeyboardButton(text=f"🎯 {side_lbl}", callback_data="cycle_side"),
            InlineKeyboardButton(text=f"📏 ±{cfg['max_distance_pct']:g}%", callback_data="cycle_dist"),
        ],
        [
            InlineKeyboardButton(text="🔓 Из ЧС (вкл)", callback_data="unblock_menu"),
            InlineKeyboardButton(text=f"🚫 Чёрный список ({len(bans)})", callback_data="ban_menu"),
        ],
        [
            InlineKeyboardButton(text="💬 Тест Discord", callback_data="dc_test"),
        ],
    ]
    if WEBAPP_URL:
        rows.append([InlineKeyboardButton(text="🧱 Открыть Mini App", web_app=WebAppInfo(url=WEBAPP_URL))])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


async def _broadcast_messages(
    user_ids: List[int],
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
) -> None:
    if bot is None or not user_ids:
        return

    async def _send_one(uid: int) -> None:
        try:
            await bot.send_message(uid, text, parse_mode="HTML", reply_markup=reply_markup)
        except Exception as exc:
            logger.debug("Ошибка отправки %s: %s", uid, exc)

    for i in range(0, len(user_ids), 20):
        batch = user_ids[i : i + 20]
        await asyncio.gather(*(_send_one(uid) for uid in batch))
        if i + 20 < len(user_ids):
            await asyncio.sleep(0.05)


async def handle_density_event(ev: DensityEvent) -> None:
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    is_bid = ev.side == "BID"
    is_bitten = ev.event_type == "BITTEN"
    side_emoji = "🟢" if is_bid else "🔴"
    side_ru = "BID (Лонг)" if is_bid else "ASK (Шорт)"
    mkt_ru = "Alpha" if ev.exchange == "BinanceAlpha" else ("Спот" if ev.market_type == "SPOT" else "Фьючерс")
    eat_txt = f"~{ev.eat_minutes:.1f} мин" if ev.eat_minutes > 0 else "—"

    if is_bitten:
        init_usd = ev.initial_wall_usd if ev.initial_wall_usd > 0 else ev.wall_usd
        text = (
            f"🦷 <b>НАДКУСАННАЯ ПЛОТНОСТЬ | {ev.exchange} ({mkt_ru})</b>\n"
            f"Монета: <code>{ev.symbol}</code> — {side_emoji} <b>{side_ru}</b>\n"
            f"✂️ Откусили: <b>-{ev.eaten_pct:.0f}%</b> (было <b>{fmt_usd(init_usd)}</b> → осталось <b>{fmt_usd(ev.wall_usd)}</b>)\n"
            f"📍 Цена стены: <code>{fmt_price(ev.wall_price)}</code> ({ev.distance_pct:.2f}% от спреда)\n"
            f"⏳ Разъедание остатка: <b>{eat_txt}</b> | 24ч: <b>{fmt_usd(ev.volume_24h_usd)}</b>\n"
            f"Время (UTC): {ev.timestamp_utc}"
        )
    else:
        text = (
            f"🧱 <b>ПЛОТНОСТЬ | {ev.exchange} ({mkt_ru})</b>\n"
            f"Монета: <code>{ev.symbol}</code> — {side_emoji} <b>{side_ru}</b>\n"
            f"💰 Стена: <b>{fmt_usd(ev.wall_usd)}</b> (<b>x{ev.anomaly_x:.1f}</b>)\n"
            f"📍 Цена стены: <code>{fmt_price(ev.wall_price)}</code> ({ev.distance_pct:.2f}% от спреда)\n"
            f"⏳ Разъедание: <b>{eat_txt}</b> | 24ч: <b>{fmt_usd(ev.volume_24h_usd)}</b>\n"
            f"Время (UTC): {ev.timestamp_utc}"
        )

    users = await get_cached_users()
    base = extract_base_asset(ev.symbol)
    target_ids: List[int] = []
    discord_urls: List[str] = []

    for u in users:
        uid = int(u.get("user_id") or 0)
        if uid <= 0:
            continue
        cfg = normalize_config(u.get("config"))
        bans = parse_coin_list(u.get("banned_coins"))
        allowed = parse_coin_list(u.get("allowed_coins"))
        if event_matches_user_config(ev, cfg, bans, allowed):
            target_ids.append(uid)
            dcfg = normalize_discord_config(u.get("discord_config"))
            wurl = resolve_discord_webhook(dcfg)
            if wurl and wurl not in discord_urls:
                discord_urls.append(wurl)

    if not users and is_valid_discord_webhook(ENV_DISCORD_WEBHOOK):
        if event_matches_user_config(ev, DEFAULT_CONFIG, [], []):
            discord_urls.append(ENV_DISCORD_WEBHOOK)

    for wurl in discord_urls:
        asyncio.create_task(send_density_discord(wurl, ev, session=_http_session))
    if target_ids:
        asyncio.create_task(_broadcast_messages(target_ids, text, _build_alert_keyboard(base)))

    await db_execute(
        "INSERT INTO densities (utc_date, utc_time, exchange, market_type, symbol, side, price, "
        "wall_price, wall_usd, distance_pct, anomaly_x, eat_minutes, volume_24h_usd, "
        "event_type, initial_wall_usd, eaten_pct) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            today_utc,
            ev.timestamp_utc,
            ev.exchange,
            ev.market_type,
            ev.symbol,
            ev.side,
            ev.price,
            ev.wall_price,
            ev.wall_usd,
            ev.distance_pct,
            ev.anomaly_x,
            ev.eat_minutes,
            ev.volume_24h_usd,
            ev.event_type,
            ev.initial_wall_usd,
            ev.eaten_pct,
        ),
    )
    _api_cache["ts"] = 0.0


density_engine = DensityEngine(on_density=handle_density_event)


async def refresh_api_cache(force: bool = False) -> None:
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now = time.time()
    if not force and _api_cache["utc_date"] == today_utc and (now - _api_cache["ts"]) < 1.5:
        return
    rows = await db_fetchall(
        "SELECT id, utc_time, exchange, market_type, symbol, side, price, wall_price, "
        "wall_usd, distance_pct, anomaly_x, eat_minutes, volume_24h_usd, "
        "event_type, initial_wall_usd, eaten_pct "
        "FROM densities WHERE utc_date = ? ORDER BY id DESC LIMIT 200",
        (today_utc,),
    )
    _api_cache["utc_date"] = today_utc
    _api_cache["history"] = rows
    _api_cache["ts"] = now


async def notify_startup() -> None:
    await asyncio.sleep(4)
    try:
        users = await get_cached_users(force=True)
        now_utc = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        ids = [int(u["user_id"]) for u in users if int(u.get("user_id") or 0) > 0]
        if ids:
            text = (
                "🚀 <b>Скринер аномальных плотностей запущен!</b>\n"
                f"Время: <code>{now_utc}</code>\n"
                "• 8 бирж: Binance, BinanceAlpha, Bybit, OKX, Bitget, Gate, KuCoin, Aster\n"
                "• Диапазон: <b>±10% от спреда</b> · Полный обход каждые <b>3 мин</b>\n"
                "• Спот от <b>$250K</b> · Фьючерсы от <b>$500K</b> (настраивается в /settings)"
            )
            await _broadcast_messages(ids, text, _build_main_keyboard())
    except Exception:
        pass


# ==================== КОМАНДЫ TELEGRAM-БОТА ====================

@dp.message(Command("start", "help"))
async def cmd_start(message: types.Message) -> None:
    if not message.from_user:
        return
    await get_user_record(message.from_user.id)
    await db_execute(
        "INSERT INTO users (user_id) VALUES (?) ON CONFLICT(user_id) DO NOTHING",
        (message.from_user.id,),
    )
    invalidate_users_cache()
    await sync_global_whitelist()

    text = (
        "🧱 <b>Скринер аномальных плотностей (8 бирж · Спот + Фьючерсы + Alpha)</b>\n\n"
        "• Биржи: Binance, BinanceAlpha, Bybit, OKX, Bitget, Gate, KuCoin, Aster\n"
        "• По умолчанию: Спот от <b>$250K</b>, Фьючерсы от <b>$500K</b>, диапазон <b>±10%</b> от спреда\n"
        "• Полный обход всех монет каждые <b>3 минуты</b> + мгновенные алерты\n\n"
        "<b>Основные команды:</b>\n"
        "/settings — интерактивные настройки\n"
        "/active — активные плотности сейчас\n"
        "/find SOL — мгновенно просканировать стаканы по монете\n"
        "/watch SOL WIF — следить только за выбранными (/watch clear — сброс)\n"
        "/minspot 250000 — мин. стена на Спот ($)\n"
        "/minfut 500000 — мин. стена на Фьючерсы ($)\n"
        "/minwall 250000 500000 — сразу Спот и Фьючерсы\n"
        "/dist 10 — макс. дистанция от спреда (%)\n"
        "/anom 4 — мин. аномальность (x)\n"
        "/life 0 — задержка удержания (сек)\n"
        "/minvol 200000 — мин. оборот 24ч ($)\n"
        "/market both|futures|spot — выбор рынка\n"
        "/side both|bid|ask — сторона стакана\n"
        "/exchanges all — выбор бирж\n"
        "/allow BTC SOL — разблокировать из глобального ЧС (/unallow)\n"
        "/ban WIF — заблокировать монету (/banlist, /unban)\n"
        "/discord URL — подключить Discord Webhook (/discord test|off|on)"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=_build_main_keyboard())


@dp.message(Command("settings"))
async def cmd_settings(message: types.Message) -> None:
    if not message.from_user:
        return
    cfg, dcfg, bans, allowed = await get_user_record(message.from_user.id)
    text, kb = _format_settings_view(cfg, dcfg, bans, allowed)
    await message.answer(text, parse_mode="HTML", reply_markup=kb)


@dp.message(Command("active"))
async def cmd_active(message: types.Message) -> None:
    if not message.from_user:
        return
    cfg, _, bans, allowed = await get_user_record(message.from_user.id)
    active = [ev for ev in density_engine.get_active() if event_matches_user_config(ev, cfg, bans, allowed)]
    if not active:
        await message.answer("🧱 Активных плотностей под ваши фильтры сейчас нет.", reply_markup=_build_main_keyboard())
        return
    lines = ["🧱 <b>Активные плотности сейчас:</b>"]
    for ev in active[:15]:
        em = "🟢" if ev.side == "BID" else "🔴"
        mkt = "Alpha" if ev.exchange == "BinanceAlpha" else ("Спот" if ev.market_type == "SPOT" else "Фьюч")
        lines.append(
            f"• {em} <b>{ev.exchange}</b> ({mkt}) <code>{ev.symbol}</code> — "
            f"<b>{fmt_usd(ev.wall_usd)}</b> (x{ev.anomaly_x:.1f}) @ <code>{fmt_price(ev.wall_price)}</code> ({ev.distance_pct:.2f}%)"
        )
    await message.answer("\n".join(lines), parse_mode="HTML", reply_markup=_build_main_keyboard())


@dp.message(Command("find"))
async def cmd_find(message: types.Message) -> None:
    if not message.from_user or _http_session is None:
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer(
            "Использование: <code>/find SOL</code> или <code>/find SOL 150000</code> или <code>/find SOL 100000 300000</code>",
            parse_mode="HTML",
        )
        return
    cfg, _, _, _ = await get_user_record(message.from_user.id)
    coin = parts[1]
    min_s = float(cfg["min_wall_spot_usd"])
    min_f = float(cfg["min_wall_futures_usd"])
    if len(parts) == 3:
        v = max(1_000.0, _safe_float(parts[2], min_s))
        min_s, min_f = v, v
    elif len(parts) >= 4:
        min_s = max(1_000.0, _safe_float(parts[2], min_s))
        min_f = max(1_000.0, _safe_float(parts[3], min_f))

    wait_msg = await message.answer(f"🔎 Сканирую стаканы <b>{extract_base_asset(coin)}</b> на 8 биржах...", parse_mode="HTML")
    walls = await scan_single_coin_live(
        _http_session, coin, min_s, min_f, float(cfg["max_distance_pct"]), min_anom_x=1.5
    )
    if not walls:
        await wait_msg.edit_text(
            f"По <b>{extract_base_asset(coin)}</b> не найдено плотностей в ±{cfg['max_distance_pct']:g}% "
            f"(Спот ≥ {fmt_usd(min_s)}, Фьюч ≥ {fmt_usd(min_f)}).",
            parse_mode="HTML",
        )
        return
    lines = [f"🔎 <b>Найденные плотности по {extract_base_asset(coin)} (±{cfg['max_distance_pct']:g}%):</b>"]
    for ev in walls[:20]:
        em = "🟢" if ev.side == "BID" else "🔴"
        mkt = "Alpha" if ev.exchange == "BinanceAlpha" else ("Спот" if ev.market_type == "SPOT" else "Фьюч")
        lines.append(
            f"• {em} <b>{ev.exchange}</b> ({mkt}) <code>{ev.symbol}</code> — "
            f"<b>{fmt_usd(ev.wall_usd)}</b> (x{ev.anomaly_x:.1f}) @ <code>{fmt_price(ev.wall_price)}</code> ({ev.distance_pct:.2f}%)"
        )
    await wait_msg.edit_text("\n".join(lines), parse_mode="HTML")


@dp.message(Command("watch"))
async def cmd_watch(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split(maxsplit=1)
    cfg, _, bans, allowed = await get_user_record(message.from_user.id)
    if len(parts) < 2:
        cur = ", ".join(cfg["watch_coins"]) if cfg["watch_coins"] else "весь рынок"
        await message.answer(
            f"👁 Текущий Watchlist: <b>{cur}</b>\n"
            "Задать: <code>/watch SOL WIF</code> · Сбросить: <code>/watch clear</code>",
            parse_mode="HTML",
        )
        return
    arg = parts[1].strip().lower()
    if arg in ("clear", "reset", "all", "off"):
        cfg["watch_coins"] = []
        await save_user_record(message.from_user.id, cfg=cfg)
        await message.answer("✅ Watchlist очищен — сканируется весь рынок.", parse_mode="HTML")
        return
    coins = parse_coin_list(parts[1])
    if not coins:
        return
    cfg["watch_coins"] = coins
    new_allowed = list(dict.fromkeys(allowed + coins))
    new_bans = [b for b in bans if b not in coins]
    await save_user_record(message.from_user.id, cfg=cfg, bans=new_bans, allowed=new_allowed)
    await message.answer(f"👁 Отслеживаются только монеты: <b>{', '.join(coins)}</b>", parse_mode="HTML")


@dp.message(Command("minspot"))
async def cmd_minspot(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Пример: <code>/minspot 250000</code>", parse_mode="HTML")
        return
    cfg, _, _, _ = await get_user_record(message.from_user.id)
    cfg["min_wall_spot_usd"] = max(5_000.0, _safe_float(parts[1], cfg["min_wall_spot_usd"]))
    cfg, _, _, _ = await save_user_record(message.from_user.id, cfg=cfg)
    await message.answer(f"✅ Мин. плотность Спот: <b>{fmt_usd(cfg['min_wall_spot_usd'])}</b>", parse_mode="HTML")


@dp.message(Command("minfut"))
async def cmd_minfut(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Пример: <code>/minfut 500000</code>", parse_mode="HTML")
        return
    cfg, _, _, _ = await get_user_record(message.from_user.id)
    cfg["min_wall_futures_usd"] = max(5_000.0, _safe_float(parts[1], cfg["min_wall_futures_usd"]))
    cfg, _, _, _ = await save_user_record(message.from_user.id, cfg=cfg)
    await message.answer(f"✅ Мин. плотность Фьючерсы: <b>{fmt_usd(cfg['min_wall_futures_usd'])}</b>", parse_mode="HTML")


@dp.message(Command("minwall"))
async def cmd_minwall(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Пример: <code>/minwall 250000 500000</code> (Спот Фьючерсы)", parse_mode="HTML")
        return
    cfg, _, _, _ = await get_user_record(message.from_user.id)
    v1 = max(5_000.0, _safe_float(parts[1], cfg["min_wall_spot_usd"]))
    v2 = max(5_000.0, _safe_float(parts[2], v1)) if len(parts) >= 3 else v1
    cfg["min_wall_spot_usd"] = v1
    cfg["min_wall_futures_usd"] = v2
    cfg, _, _, _ = await save_user_record(message.from_user.id, cfg=cfg)
    await message.answer(
        f"✅ Спот: <b>{fmt_usd(cfg['min_wall_spot_usd'])}</b> | Фьючерсы: <b>{fmt_usd(cfg['min_wall_futures_usd'])}</b>",
        parse_mode="HTML",
    )


@dp.message(Command("dist"))
async def cmd_dist(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Пример: <code>/dist 10</code> (±10% от спреда)", parse_mode="HTML")
        return
    cfg, _, _, _ = await get_user_record(message.from_user.id)
    cfg["max_distance_pct"] = _safe_float(parts[1], cfg["max_distance_pct"])
    cfg, _, _, _ = await save_user_record(message.from_user.id, cfg=cfg)
    await message.answer(f"✅ Диапазон от спреда: <b>±{cfg['max_distance_pct']:g}%</b>", parse_mode="HTML")


@dp.message(Command("anom"))
async def cmd_anom(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Пример: <code>/anom 4</code>", parse_mode="HTML")
        return
    cfg, _, _, _ = await get_user_record(message.from_user.id)
    cfg["min_anomaly_x"] = _safe_float(parts[1], cfg["min_anomaly_x"])
    cfg, _, _, _ = await save_user_record(message.from_user.id, cfg=cfg)
    await message.answer(f"✅ Мин. аномальность: <b>x{cfg['min_anomaly_x']:g}</b>", parse_mode="HTML")


@dp.message(Command("life"))
async def cmd_life(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Пример: <code>/life 0</code> (0 = сразу при нахождении)", parse_mode="HTML")
        return
    cfg, _, _, _ = await get_user_record(message.from_user.id)
    cfg["min_lifetime_sec"] = _safe_float(parts[1], cfg["min_lifetime_sec"])
    cfg, _, _, _ = await save_user_record(message.from_user.id, cfg=cfg)
    await message.answer(f"✅ Задержка удержания: <b>{cfg['min_lifetime_sec']:g}с</b>", parse_mode="HTML")


@dp.message(Command("minvol"))
async def cmd_minvol(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Пример: <code>/minvol 200000</code>", parse_mode="HTML")
        return
    cfg, _, _, _ = await get_user_record(message.from_user.id)
    cfg["min_vol24h_usd"] = _safe_float(parts[1], cfg["min_vol24h_usd"])
    cfg, _, _, _ = await save_user_record(message.from_user.id, cfg=cfg)
    await message.answer(f"✅ Мин. оборот 24ч: <b>{fmt_usd(cfg['min_vol24h_usd'])}</b>", parse_mode="HTML")


@dp.message(Command("market"))
async def cmd_market(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split()
    arg = parts[1].strip().upper() if len(parts) > 1 else ""
    if arg not in ("BOTH", "FUTURES", "SPOT"):
        await message.answer("Использование: <code>/market both</code>, <code>/market futures</code> или <code>/market spot</code>", parse_mode="HTML")
        return
    cfg, _, _, _ = await get_user_record(message.from_user.id)
    cfg["market_mode"] = arg
    await save_user_record(message.from_user.id, cfg=cfg)
    await message.answer(f"✅ Режим рынков: <b>{arg}</b>", parse_mode="HTML")


@dp.message(Command("side"))
async def cmd_side(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split()
    arg = parts[1].strip().upper() if len(parts) > 1 else ""
    if arg not in ("BOTH", "BID", "ASK"):
        await message.answer("Использование: <code>/side both</code>, <code>/side bid</code> или <code>/side ask</code>", parse_mode="HTML")
        return
    cfg, _, _, _ = await get_user_record(message.from_user.id)
    cfg["side_mode"] = arg
    await save_user_record(message.from_user.id, cfg=cfg)
    await message.answer(f"✅ Сторона стакана: <b>{arg}</b>", parse_mode="HTML")


@dp.message(Command("exchanges"))
async def cmd_exchanges(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer(
            f"Доступные биржи: <code>{', '.join(ALL_EXCHANGES)}</code>\nПример: <code>/exchanges all</code>",
            parse_mode="HTML",
        )
        return
    arg = parts[1].strip()
    cfg, _, _, _ = await get_user_record(message.from_user.id)
    if arg.lower() == "all":
        cfg["exchanges"] = list(ALL_EXCHANGES)
    else:
        lookup = {e.lower(): e for e in ALL_EXCHANGES}
        chosen = [lookup[tok.lower()] for tok in re.split(r"[\s,;]+", arg) if tok.lower() in lookup]
        if not chosen:
            await message.answer("Не распознаны биржи. Пример: <code>/exchanges Binance BinanceAlpha Bybit</code>", parse_mode="HTML")
            return
        cfg["exchanges"] = list(dict.fromkeys(chosen))
    cfg, _, _, _ = await save_user_record(message.from_user.id, cfg=cfg)
    await message.answer(f"✅ Активные биржи: <b>{', '.join(cfg['exchanges'])}</b>", parse_mode="HTML")


@dp.message(Command("allow"))
async def cmd_allow(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split(maxsplit=1)
    _, _, bans, allowed = await get_user_record(message.from_user.id)
    if len(parts) < 2:
        text, kb = _format_unblock_menu(allowed)
        await message.answer(text, parse_mode="HTML", reply_markup=kb)
        return
    coins = parse_coin_list(parts[1])
    if not coins:
        return
    new_allowed = list(dict.fromkeys(allowed + coins))
    new_bans = [b for b in bans if b not in coins]
    await save_user_record(message.from_user.id, bans=new_bans, allowed=new_allowed)
    await message.answer(f"🔓 Разблокированы из глобального ЧС: <b>{', '.join(coins)}</b>", parse_mode="HTML")


@dp.message(Command("unallow"))
async def cmd_unallow(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Пример: <code>/unallow BTC SOL</code>", parse_mode="HTML")
        return
    rem = set(parse_coin_list(parts[1]))
    _, _, _, allowed = await get_user_record(message.from_user.id)
    new_allowed = [c for c in allowed if c not in rem]
    await save_user_record(message.from_user.id, allowed=new_allowed)
    await message.answer(f"🔒 Возвращены в глобальный ЧС: <b>{', '.join(sorted(rem))}</b>", parse_mode="HTML")


@dp.message(Command("ban", "banlist"))
async def cmd_ban(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split(maxsplit=1)
    cfg, _, bans, allowed = await get_user_record(message.from_user.id)
    if len(parts) < 2:
        text, kb = _format_ban_menu(bans)
        await message.answer(text, parse_mode="HTML", reply_markup=kb)
        return
    coins = parse_coin_list(parts[1])
    if not coins:
        await message.answer("Пример: <code>/ban WIF</code> или <code>/ban WIF RIVER</code>", parse_mode="HTML")
        return
    new_bans = list(dict.fromkeys(bans + coins))
    new_allowed = [c for c in allowed if c not in coins]
    cfg["watch_coins"] = [c for c in (cfg.get("watch_coins") or []) if c not in coins]
    await save_user_record(message.from_user.id, cfg=cfg, bans=new_bans, allowed=new_allowed)
    await message.answer(
        f"🚫 Заблокированы (Telegram, Discord и Mini App): <b>{', '.join(coins)}</b>\n"
        "Посмотреть список: <code>/banlist</code> · Вернуть: <code>/unban ТИКЕР</code>",
        parse_mode="HTML",
    )


@dp.message(Command("unban"))
async def cmd_unban(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Пример: <code>/unban WIF</code> или <code>/unban all</code>", parse_mode="HTML")
        return
    raw_arg = parts[1].strip().lower()
    if raw_arg in ("all", "clear", "reset", "все", "очистить"):
        await save_user_record(message.from_user.id, bans=[])
        await message.answer("✅ Ваш личный чёрный список полностью очищен.", parse_mode="HTML")
        return
    rem = set(parse_coin_list(parts[1]))
    if not rem:
        return
    _, _, bans, _ = await get_user_record(message.from_user.id)
    new_bans = [c for c in bans if c not in rem]
    await save_user_record(message.from_user.id, bans=new_bans)
    await message.answer(f"✅ Разблокированы: <b>{', '.join(sorted(rem))}</b>", parse_mode="HTML")


@dp.message(Command("discord"))
async def cmd_discord(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split(maxsplit=1)
    _, dcfg, _, _ = await get_user_record(message.from_user.id)
    if len(parts) < 2:
        wurl = resolve_discord_webhook(dcfg)
        st = "✅ подключён" if wurl else "❌ не задан"
        await message.answer(
            f"💬 Discord Webhook: <b>{st}</b>\n"
            "Подключить: <code>/discord https://discord.com/api/webhooks/...</code>\n"
            "Управление: <code>/discord test</code> | <code>/discord off</code> | <code>/discord on</code>",
            parse_mode="HTML",
        )
        return

    arg = parts[1].strip()
    low = arg.lower()
    if low == "off":
        dcfg["enabled"] = False
        await save_user_record(message.from_user.id, dcfg=dcfg)
        await message.answer("💬 Отправка в Discord <b>выключена</b>.", parse_mode="HTML")
        return
    if low == "on":
        dcfg["enabled"] = True
        await save_user_record(message.from_user.id, dcfg=dcfg)
        await message.answer("💬 Отправка в Discord <b>включена</b>.", parse_mode="HTML")
        return
    if low == "test":
        wurl = resolve_discord_webhook(dcfg)
        ok, err = await send_test_discord(wurl, session=_http_session)
        await message.answer("✅ Тест отправлен в Discord!" if ok else f"❌ Ошибка Discord: {err}")
        return

    if not is_valid_discord_webhook(arg):
        await message.answer("❌ Некорректный URL. Нужна ссылка вида <code>https://discord.com/api/webhooks/...</code>", parse_mode="HTML")
        return
    dcfg["webhook_url"] = arg
    dcfg["enabled"] = True
    await save_user_record(message.from_user.id, dcfg=dcfg)
    ok, err = await send_test_discord(arg, session=_http_session)
    await message.answer(
        "✅ Discord Webhook сохранён и проверен!" if ok else f"⚠️ Webhook сохранён, но тест вернул ошибку: {err}"
    )


# ==================== INLINE-КНОПКИ TELEGRAM ====================

@dp.callback_query(F.data)
async def on_callback(query: types.CallbackQuery) -> None:
    if not query.from_user or not query.data:
        return
    uid = query.from_user.id
    data = query.data
    cfg, dcfg, bans, allowed = await get_user_record(uid)

    if data == "toggle_enabled":
        cfg["enabled"] = not cfg["enabled"]
        cfg, dcfg, bans, allowed = await save_user_record(uid, cfg=cfg)
        text, kb = _format_settings_view(cfg, dcfg, bans, allowed)
        if query.message:
            try:
                await query.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
            except Exception:
                pass
        await query.answer("Обновлено")
        return

    if data == "cycle_market":
        order = ["BOTH", "FUTURES", "SPOT"]
        cfg["market_mode"] = order[(order.index(cfg["market_mode"]) + 1) % len(order)]
        cfg, dcfg, bans, allowed = await save_user_record(uid, cfg=cfg)
        text, kb = _format_settings_view(cfg, dcfg, bans, allowed)
        if query.message:
            try:
                await query.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
            except Exception:
                pass
        await query.answer(f"Рынок: {cfg['market_mode']}")
        return

    if data == "cycle_side":
        order = ["BOTH", "BID", "ASK"]
        cfg["side_mode"] = order[(order.index(cfg["side_mode"]) + 1) % len(order)]
        cfg, dcfg, bans, allowed = await save_user_record(uid, cfg=cfg)
        text, kb = _format_settings_view(cfg, dcfg, bans, allowed)
        if query.message:
            try:
                await query.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
            except Exception:
                pass
        await query.answer(f"Сторона: {cfg['side_mode']}")
        return

    if data == "cycle_dist":
        dists = [2.5, 5.0, 10.0, 15.0]
        cur = float(cfg["max_distance_pct"])
        nxt = next((d for d in dists if d > cur + 0.01), dists[0])
        cfg["max_distance_pct"] = nxt
        cfg, dcfg, bans, allowed = await save_user_record(uid, cfg=cfg)
        text, kb = _format_settings_view(cfg, dcfg, bans, allowed)
        if query.message:
            try:
                await query.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
            except Exception:
                pass
        await query.answer(f"Дистанция: ±{nxt:g}%")
        return

    if data.startswith("preset:"):
        p = data.split(":", 1)[1]
        presets = {
            "light": (100_000.0, 250_000.0),
            "std": (250_000.0, 500_000.0),
            "whale": (500_000.0, 1_000_000.0),
        }
        if p in presets:
            cfg["min_wall_spot_usd"], cfg["min_wall_futures_usd"] = presets[p]
            cfg, dcfg, bans, allowed = await save_user_record(uid, cfg=cfg)
            text, kb = _format_settings_view(cfg, dcfg, bans, allowed)
            if query.message:
                try:
                    await query.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
                except Exception:
                    pass
        await query.answer("Пресет применён")
        return

    if data == "unblock_menu":
        text, kb = _format_unblock_menu(allowed)
        if query.message:
            try:
                await query.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
            except Exception:
                pass
        await query.answer()
        return

    if data.startswith("toggle_allow:"):
        coin = extract_base_asset(data.split(":", 1)[1])
        if coin:
            if coin in allowed:
                new_allowed = [c for c in allowed if c != coin]
                new_bans = bans
                msg = f"🔒 {coin} возвращена в ЧС"
            else:
                new_allowed = list(dict.fromkeys(allowed + [coin]))
                new_bans = [b for b in bans if b != coin]
                msg = f"🔓 {coin} разблокирована"
            _, _, _, new_allowed = await save_user_record(uid, bans=new_bans, allowed=new_allowed)
            text, kb = _format_unblock_menu(new_allowed)
            if query.message:
                try:
                    await query.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
                except Exception:
                    pass
            await query.answer(msg)
        else:
            await query.answer()
        return

    if data.startswith("ban_coin:"):
        coin = extract_base_asset(data.split(":", 1)[1])
        if coin:
            new_bans = list(dict.fromkeys(bans + [coin]))
            new_allowed = [c for c in allowed if c != coin]
            cfg["watch_coins"] = [c for c in (cfg.get("watch_coins") or []) if c != coin]
            await save_user_record(uid, cfg=cfg, bans=new_bans, allowed=new_allowed)
            await query.answer(f"🚫 {coin} заблокирована (Telegram + Discord)")
            if query.message:
                try:
                    await query.message.edit_reply_markup(reply_markup=_build_alert_keyboard(coin, is_banned=True))
                except Exception:
                    pass
        else:
            await query.answer()
        return

    if data.startswith("unban_coin:"):
        coin = extract_base_asset(data.split(":", 1)[1])
        if coin:
            new_bans = [b for b in bans if b != coin]
            await save_user_record(uid, bans=new_bans)
            await query.answer(f"✅ {coin} возвращена в алерты")
            if query.message:
                try:
                    await query.message.edit_reply_markup(reply_markup=_build_alert_keyboard(coin, is_banned=False))
                except Exception:
                    pass
        else:
            await query.answer()
        return

    if data == "ban_menu":
        text, kb = _format_ban_menu(bans)
        if query.message:
            try:
                await query.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
            except Exception:
                pass
        await query.answer()
        return

    if data.startswith("ban_rm:"):
        coin = extract_base_asset(data.split(":", 1)[1])
        new_bans = [b for b in bans if b != coin]
        _, _, new_bans, _ = await save_user_record(uid, bans=new_bans)
        await query.answer(f"✅ {coin} разблокирована")
        text, kb = _format_ban_menu(new_bans)
        if query.message:
            try:
                await query.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
            except Exception:
                pass
        return

    if data == "ban_clear":
        _, _, new_bans, _ = await save_user_record(uid, bans=[])
        await query.answer("✅ Чёрный список очищен")
        text, kb = _format_ban_menu(new_bans)
        if query.message:
            try:
                await query.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
            except Exception:
                pass
        return

    if data == "back_settings":
        text, kb = _format_settings_view(cfg, dcfg, bans, allowed)
        if query.message:
            try:
                await query.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
            except Exception:
                pass
        await query.answer()
        return

    if data == "dc_test":
        wurl = resolve_discord_webhook(dcfg)
        ok, err = await send_test_discord(wurl, session=_http_session)
        await query.answer("✅ Тест отправлен в Discord!" if ok else f"❌ {err}", show_alert=not ok)
        return

    await query.answer()


# ==================== MINI APP HTML ====================

WEBAPP_HTML = r"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>Density Wall Screener</title>
    <script src="https://telegram.org/js/telegram-web-app.js"></script>
    <style>
        :root {
            --bg: #0b0f16;
            --card: #141a24;
            --card2: #1b2332;
            --text: #eef2f8;
            --hint: #8b95a7;
            --border: rgba(255,255,255,0.08);
            --bid: #16c784;
            --ask: #ea3943;
            --gold: #f5b942;
            --accent: #3b82f6;
        }
        * { box-sizing: border-box; -webkit-tap-highlight-color: transparent; }
        body {
            margin: 0; padding: 12px 12px 76px; background: var(--bg); color: var(--text);
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; font-size: 14px;
        }
        .topbar { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; gap: 8px; }
        .brand { font-size: 16px; font-weight: 800; display: flex; align-items: center; gap: 6px; }
        .chip {
            border: 1px solid var(--border); background: var(--card); color: var(--text);
            padding: 6px 10px; border-radius: 10px; font-size: 12px; font-weight: 700; cursor: pointer;
        }
        .chip.on { border-color: rgba(22,199,132,0.5); background: rgba(22,199,132,0.14); color: var(--bid); }
        .tabs {
            display: grid; grid-template-columns: repeat(3, 1fr); gap: 6px;
            background: var(--card); padding: 4px; border-radius: 12px; border: 1px solid var(--border); margin-bottom: 12px;
        }
        .tab-btn { border: none; background: transparent; color: var(--hint); padding: 8px; border-radius: 9px; font-size: 12px; font-weight: 700; cursor: pointer; }
        .tab-btn.active { background: var(--accent); color: #fff; }
        .panel { display: none; }
        .panel.active { display: block; }
        .box { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 12px; margin-bottom: 10px; }
        .lbl { font-size: 13px; font-weight: 800; margin-bottom: 8px; display: block; }
        .sub { font-size: 11px; color: var(--hint); font-weight: 600; margin-bottom: 4px; display: block; }
        .row2 { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 8px; }
        .row3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 6px; margin-bottom: 8px; }
        input[type="text"], input[type="number"], select {
            width: 100%; background: var(--card2); color: var(--text); border: 1px solid var(--border);
            border-radius: 9px; padding: 8px 10px; font-size: 13px; outline: none;
        }
        .btn {
            border: none; background: var(--accent); color: #fff; padding: 9px 12px;
            border-radius: 9px; font-size: 13px; font-weight: 700; cursor: pointer; width: 100%;
        }
        .btn.sec { background: var(--card2); color: var(--text); border: 1px solid var(--border); }
        .card {
            background: var(--card); border: 1px solid var(--border); border-radius: 12px;
            padding: 10px 12px; margin-bottom: 8px; border-left: 3px solid var(--accent); cursor: pointer;
        }
        .card.bid { border-left-color: var(--bid); }
        .card.ask { border-left-color: var(--ask); }
        .card-top { display: flex; justify-content: space-between; align-items: center; gap: 6px; margin-bottom: 5px; }
        .sym { display: flex; align-items: center; gap: 5px; font-weight: 800; font-size: 14px; flex-wrap: wrap; }
        .tag { font-size: 10px; font-weight: 700; padding: 2px 6px; border-radius: 6px; background: rgba(255,255,255,0.08); }
        .tag.spot { background: rgba(59,130,246,0.22); color: #93c5fd; }
        .tag.alpha { background: rgba(245,185,66,0.22); color: var(--gold); }
        .badge { font-size: 11px; font-weight: 800; padding: 2px 7px; border-radius: 6px; }
        .badge.bid { background: rgba(22,199,132,0.18); color: var(--bid); }
        .badge.ask { background: rgba(234,57,67,0.18); color: var(--ask); }
        .badge.gold { background: rgba(245,185,66,0.18); color: var(--gold); }
        .ban-x {
            border: none; background: rgba(234,57,67,0.16); color: var(--ask);
            width: 24px; height: 24px; border-radius: 6px; font-size: 12px; font-weight: 800;
            cursor: pointer; display: inline-flex; align-items: center; justify-content: center;
        }
        .meta { font-size: 12px; color: var(--hint); display: flex; justify-content: space-between; flex-wrap: wrap; gap: 6px; }
        .meta b { color: var(--text); }
        .pills { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 6px; }
        .pill {
            padding: 5px 9px; border-radius: 8px; border: 1px solid var(--border);
            background: rgba(255,255,255,0.04); color: var(--hint); font-size: 12px; font-weight: 700; cursor: pointer;
        }
        .pill.on { border-color: rgba(22,199,132,0.5); background: rgba(22,199,132,0.16); color: var(--text); }
        .pill.ban { border-color: rgba(234,57,67,0.45); background: rgba(234,57,67,0.14); color: #fda4af; }
        .empty { text-align: center; color: var(--hint); font-size: 12px; padding: 14px 4px; }
        .toast {
            position: fixed; bottom: 18px; left: 50%; transform: translateX(-50%);
            background: #1f2937; color: #fff; padding: 9px 14px; border-radius: 10px;
            font-size: 12px; font-weight: 700; opacity: 0; pointer-events: none; transition: opacity 0.2s; z-index: 99;
        }
        .toast.show { opacity: 1; }
    </style>
</head>
<body>
    <div class="topbar">
        <div class="brand">🧱 Density Screener <span style="font-size:11px;color:var(--bid)">8 бирж</span></div>
        <button id="tiger-btn" class="chip" onclick="connectTiger(true)">🐯 Tiger</button>
    </div>

    <div class="tabs">
        <button class="tab-btn active" id="tb-live" onclick="switchTab('live')">🔥 Лайв</button>
        <button class="tab-btn" id="tb-hist" onclick="switchTab('hist')">📜 История</button>
        <button class="tab-btn" id="tb-set" onclick="switchTab('set')">⚙️ Настройки</button>
    </div>

    <!-- ВКЛАДКА ЛАЙВ + ЖИВОЙ ПОИСК -->
    <div id="p-live" class="panel active">
        <div class="box">
            <span class="lbl">🔎 Живой скан стаканов по монете (8 бирж)</span>
            <div class="row2">
                <input type="text" id="find-coin" placeholder="Тикер, напр. SOL">
                <button class="btn" onclick="runLiveFind()">🔎 Стаканы</button>
            </div>
            <div class="row3">
                <div><span class="sub">Спот ($)</span><input type="number" id="find-spot" value="250000"></div>
                <div><span class="sub">Фьюч ($)</span><input type="number" id="find-fut" value="500000"></div>
                <div><span class="sub">Дист (%)</span><input type="number" id="find-dist" value="10" step="0.5"></div>
            </div>
            <div id="find-results"></div>
        </div>
        <div id="live-list"></div>
    </div>

    <!-- ВКЛАДКА ИСТОРИЯ -->
    <div id="p-hist" class="panel">
        <div id="hist-list"></div>
    </div>

    <!-- ВКЛАДКА НАСТРОЙКИ -->
    <div id="p-set" class="panel">
        <div class="box">
            <span class="lbl">🎯 Фильтры плотностей</span>
            <div class="row2">
                <div><span class="sub">Спот от ($)</span><input type="number" id="s-spot" value="250000" step="25000"></div>
                <div><span class="sub">Фьючерсы от ($)</span><input type="number" id="s-fut" value="500000" step="50000"></div>
            </div>
            <div class="row2">
                <div><span class="sub">Дистанция от спреда (±%)</span><input type="number" id="s-dist" value="10" step="0.5"></div>
                <div><span class="sub">Аномальность (x)</span><input type="number" id="s-anom" value="4" step="0.5"></div>
            </div>
            <div class="row2">
                <div><span class="sub">Задержка удержания (с)</span><input type="number" id="s-life" value="0" step="1"></div>
                <div><span class="sub">Мин. оборот 24ч ($)</span><input type="number" id="s-vol" value="200000" step="50000"></div>
            </div>
            <div class="row2">
                <div>
                    <span class="sub">Рынки</span>
                    <select id="s-mkt">
                        <option value="BOTH">Фьючерсы + Спот</option>
                        <option value="FUTURES">Только Фьючерсы</option>
                        <option value="SPOT">Только Спот</option>
                    </select>
                </div>
                <div>
                    <span class="sub">Сторона</span>
                    <select id="s-side">
                        <option value="BOTH">BID + ASK</option>
                        <option value="BID">Только BID (Лонг)</option>
                        <option value="ASK">Только ASK (Шорт)</option>
                    </select>
                </div>
            </div>
            <span class="sub">Биржи:</span>
            <div class="pills" id="ex-pills" style="margin-bottom:10px;"></div>
            <button class="btn" onclick="saveSettings()">💾 Сохранить настройки</button>
        </div>

        <div class="box">
            <span class="lbl">💬 Discord Webhook</span>
            <input type="text" id="dc-url" placeholder="https://discord.com/api/webhooks/..." style="margin-bottom:8px;">
            <div class="row2">
                <button class="btn" onclick="saveSettings()">💾 Сохранить</button>
                <button class="btn sec" onclick="testDiscord()">🧪 Тест Discord</button>
            </div>
        </div>

        <div class="box">
            <span class="lbl">👁 Отслеживать только выбранные (Watchlist)</span>
            <div class="row2">
                <input type="text" id="watch-inp" placeholder="Напр. SOL WIF">
                <button class="btn" onclick="saveWatch()">Применить</button>
            </div>
            <div class="pills" id="watch-pills"></div>
        </div>

        <div class="box">
            <span class="lbl">🔓 Разблокировать из глобального ЧС</span>
            <div class="row2">
                <input type="text" id="allow-inp" placeholder="Напр. BTC XAU TSLA">
                <button class="btn" onclick="addAllow()">Разблокировать</button>
            </div>
            <div class="pills" id="quick-allow"></div>
            <div class="pills" id="allow-pills"></div>
        </div>

        <div class="box">
            <span class="lbl">🚫 Личный чёрный список монет</span>
            <div class="row2">
                <input type="text" id="ban-inp" placeholder="Напр. WIF PEPE">
                <button class="btn" onclick="addBan()">Заблокировать</button>
            </div>
            <div class="pills" id="ban-pills"></div>
        </div>
    </div>

    <div id="toast" class="toast"></div>

    <script>
        const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
        if (tg) { tg.ready(); tg.expand(); }
        const userId = (tg && tg.initDataUnsafe && tg.initDataUnsafe.user && tg.initDataUnsafe.user.id)
            ? Number(tg.initDataUnsafe.user.id)
            : Number(new URLSearchParams(location.search).get('user_id') || 0);

        const ALL_EX = ['Binance', 'BinanceAlpha', 'Bybit', 'OKX', 'Bitget', 'Gate', 'KuCoin', 'Aster'];
        const QUICK_ALLOW = ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'SUI', 'PEPE', 'WIF', 'XAU', 'TSLA', 'NVDA'];
        let selEx = [...ALL_EX];
        let bannedCoins = [];
        let allowedCoins = [];
        let watchCoins = [];
        let inited = false;
        let tigerWs = null;

        function toast(m) {
            const t = document.getElementById('toast');
            t.textContent = m;
            t.classList.add('show');
            clearTimeout(window._tt);
            window._tt = setTimeout(() => t.classList.remove('show'), 2200);
        }
        function fmtUsd(v) {
            const n = Number(v || 0);
            if (n >= 1e6) return '$' + (n / 1e6).toFixed(2) + 'M';
            if (n >= 1e3) return '$' + Math.round(n / 1e3) + 'K';
            return '$' + Math.round(n);
        }
        function baseCoin(sym) {
            return String(sym || '').toUpperCase()
                .replace(/(-USDT-SWAP|-USDC-SWAP|USDTM|_USDT|-USDT|USDT|USDC)$/i, '')
                .replace(/^(1000000|100000|10000|1000)/, '');
        }
        function switchTab(t) {
            ['live', 'hist', 'set'].forEach(k => {
                document.getElementById('p-' + k).classList.toggle('active', k === t);
                document.getElementById('tb-' + k).classList.toggle('active', k === t);
            });
        }
        function connectTiger(manual) {
            const btn = document.getElementById('tiger-btn');
            try {
                if (tigerWs && tigerWs.readyState === 1) {
                    if (manual) toast('🐯 Tiger уже подключён');
                    return;
                }
                tigerWs = new WebSocket('ws://127.0.0.1:21310');
                tigerWs.onopen = () => { btn.className = 'chip on'; btn.textContent = '🐯 ON'; if (manual) toast('🐯 Подключено'); };
                tigerWs.onclose = () => { btn.className = 'chip'; btn.textContent = '🐯 Tiger'; };
            } catch {}
        }
        function openTiger(ex, sym, mkt) {
            if (!tigerWs || tigerWs.readyState !== 1) return;
            const futMap = { Binance: 'BINANCEF', Bybit: 'BYBITF', OKX: 'OKXF', Bitget: 'BITGETF', Gate: 'GATEIOF', KuCoin: 'KUCOINF', Aster: 'BINANCEF' };
            const spotMap = { Binance: 'BINANCE', BinanceAlpha: 'BINANCE', Bybit: 'BYBIT', OKX: 'OKX', Bitget: 'BITGET', Gate: 'GATEIO', KuCoin: 'KUCOIN', Aster: 'BINANCE' };
            try {
                tigerWs.send(JSON.stringify({
                    action: 'SetTicker',
                    exchange: (mkt === 'SPOT' ? spotMap[ex] : futMap[ex]) || 'BINANCEF',
                    ticker: sym,
                    group: 1
                }));
                toast('🐯 ' + sym);
            } catch {}
        }
        function cardHtml(x) {
            const isB = x.side === 'BID';
            const isBitten = x.event_type === 'BITTEN' || Number(x.eaten_pct || 0) >= 15;
            const mTag = x.exchange === 'BinanceAlpha'
                ? '<span class="tag alpha">Alpha</span>'
                : (x.market_type === 'SPOT' ? '<span class="tag spot">Спот</span>' : '<span class="tag">Фьюч</span>');
            const biteBadge = isBitten
                ? `<span class="badge" style="background:rgba(245,158,11,0.22);color:#fbbf24">🦷 -${Math.round(x.eaten_pct || 0)}%</span>`
                : '';
            const wasTxt = (isBitten && Number(x.initial_wall_usd || 0) > Number(x.wall_usd || 0))
                ? ` (было ${fmtUsd(x.initial_wall_usd)})`
                : '';
            return `<div class="card ${isB ? 'bid' : 'ask'}" onclick="openTiger('${x.exchange}','${x.symbol}','${x.market_type}')">
                <div class="card-top">
                    <div class="sym"><span>${x.symbol}</span><span class="tag">${x.exchange}</span>${mTag}${biteBadge}</div>
                    <div style="display:flex;gap:5px;align-items:center">
                        <span class="badge gold">${fmtUsd(x.wall_usd)}${wasTxt}</span>
                        <span class="badge ${isB ? 'bid' : 'ask'}">${x.side} · x${x.anomaly_x}</span>
                        <button class="ban-x" title="Заблокировать монету" onclick="banFromCard('${x.symbol}', event)">🚫</button>
                    </div>
                </div>
                <div class="meta">
                    <span>Стена: <b>${x.wall_price}</b> (${x.distance_pct}% от спреда)</span>
                    <span>${x.eat_minutes ? ('~' + x.eat_minutes + 'м · ') : ''}${x.utc_time || ''}</span>
                </div>
            </div>`;
        }
        async function banFromCard(sym, ev) {
            if (ev) ev.stopPropagation();
            const coin = baseCoin(sym);
            if (!coin) return;
            await updateBan(coin, 'ban');
        }
        function renderExPills() {
            document.getElementById('ex-pills').innerHTML = ALL_EX.map(e =>
                `<button type="button" class="pill ${selEx.includes(e) ? 'on' : ''}" onclick="toggleEx('${e}')">${e}</button>`
            ).join('');
        }
        function toggleEx(e) {
            if (selEx.includes(e)) {
                if (selEx.length <= 1) return;
                selEx = selEx.filter(x => x !== e);
            } else selEx.push(e);
            renderExPills();
        }
        function renderLists() {
            document.getElementById('quick-allow').innerHTML = QUICK_ALLOW.map(c =>
                `<button type="button" class="pill ${allowedCoins.includes(c) ? 'on' : ''}" onclick="updateAllow('${c}','${allowedCoins.includes(c) ? 'unallow' : 'allow'}')">${allowedCoins.includes(c) ? '✅ ' : ''}${c}</button>`
            ).join('');
            document.getElementById('allow-pills').innerHTML = allowedCoins.map(c =>
                `<span class="pill on" onclick="updateAllow('${c}','unallow')">🔓 ${c} ✕</span>`
            ).join('');
            document.getElementById('ban-pills').innerHTML = bannedCoins.length
                ? bannedCoins.map(c => `<span class="pill ban" onclick="updateBan('${c}','unban')">🚫 ${c} ✕</span>`).join('')
                : '<span class="sub">Личный ЧС пуст</span>';
            document.getElementById('watch-pills').innerHTML = watchCoins.length
                ? watchCoins.map(c => `<span class="pill on">${c}</span>`).join('') + `<button type="button" class="pill ban" onclick="clearWatch()">Сбросить</button>`
                : '<span class="sub">Сканируется весь рынок</span>';
        }
        async function runLiveFind() {
            const coin = document.getElementById('find-coin').value.trim();
            if (!coin) return;
            const box = document.getElementById('find-results');
            box.innerHTML = '<div class="empty">Сканирую 8 бирж...</div>';
            try {
                const url = `/api/find?coin=${encodeURIComponent(coin)}&min_spot=${encodeURIComponent(document.getElementById('find-spot').value)}&min_fut=${encodeURIComponent(document.getElementById('find-fut').value)}&max_dist=${encodeURIComponent(document.getElementById('find-dist').value)}`;
                const r = await fetch(url);
                const d = await r.json();
                box.innerHTML = (d.walls && d.walls.length) ? d.walls.map(cardHtml).join('') : '<div class="empty">Плотностей не найдено</div>';
            } catch { box.innerHTML = '<div class="empty">Ошибка запроса</div>'; }
        }
        async function saveSettings() {
            const body = {
                user_id: userId,
                config: {
                    enabled: true,
                    market_mode: document.getElementById('s-mkt').value,
                    min_wall_spot_usd: Number(document.getElementById('s-spot').value || 250000),
                    min_wall_futures_usd: Number(document.getElementById('s-fut').value || 500000),
                    max_distance_pct: Number(document.getElementById('s-dist').value || 10),
                    min_anomaly_x: Number(document.getElementById('s-anom').value || 4),
                    min_lifetime_sec: Number(document.getElementById('s-life').value || 0),
                    min_vol24h_usd: Number(document.getElementById('s-vol').value || 200000),
                    side_mode: document.getElementById('s-side').value,
                    exchanges: selEx,
                    watch_coins: watchCoins
                },
                discord_config: { enabled: true, webhook_url: document.getElementById('dc-url').value.trim() }
            };
            const r = await fetch('/api/settings', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(body) });
            if (r.ok) { toast('✅ Сохранено'); loadData(); }
        }
        async function testDiscord() {
            const r = await fetch('/api/discord/test', {
                method: 'POST', headers: {'Content-Type':'application/json'},
                body: JSON.stringify({ user_id: userId, webhook_url: document.getElementById('dc-url').value.trim() })
            });
            const d = await r.json();
            toast(d.ok ? '✅ Тест отправлен!' : ('❌ ' + (d.detail || 'Ошибка')));
        }
        async function addAllow() {
            const inp = document.getElementById('allow-inp');
            if (!inp.value.trim()) return;
            await updateAllow(inp.value.trim(), 'allow');
            inp.value = '';
        }
        async function updateAllow(coin, action) {
            const r = await fetch('/api/allow', {
                method: 'POST', headers: {'Content-Type':'application/json'},
                body: JSON.stringify({ user_id: userId, coin, action })
            });
            if (r.ok) {
                const d = await r.json();
                allowedCoins = d.allowed_coins || [];
                bannedCoins = d.banned_coins || [];
                renderLists();
                toast(action === 'allow' ? '🔓 Разблокировано' : '🔒 В глобальный ЧС');
                loadData();
            }
        }
        async function addBan() {
            const inp = document.getElementById('ban-inp');
            if (!inp.value.trim()) return;
            await updateBan(inp.value.trim(), 'ban');
            inp.value = '';
        }
        async function updateBan(coin, action) {
            const r = await fetch('/api/ban', {
                method: 'POST', headers: {'Content-Type':'application/json'},
                body: JSON.stringify({ user_id: userId, coin, action })
            });
            if (r.ok) {
                const d = await r.json();
                bannedCoins = d.banned_coins || [];
                allowedCoins = d.allowed_coins || [];
                if (Array.isArray(d.watch_coins)) watchCoins = d.watch_coins;
                renderLists();
                toast(action === 'ban' ? ('🚫 ' + coin + ' заблокирована') : ('✅ ' + coin + ' разблокирована'));
                loadData();
            }
        }
        async function saveWatch() {
            const inp = document.getElementById('watch-inp');
            const r = await fetch('/api/watch', {
                method: 'POST', headers: {'Content-Type':'application/json'},
                body: JSON.stringify({ user_id: userId, coin: inp.value.trim(), action: 'set' })
            });
            if (r.ok) {
                const d = await r.json();
                watchCoins = d.watch_coins || [];
                allowedCoins = d.allowed_coins || allowedCoins;
                inp.value = '';
                renderLists();
                toast('👁 Watchlist обновлён');
                loadData();
            }
        }
        async function clearWatch() {
            const r = await fetch('/api/watch', {
                method: 'POST', headers: {'Content-Type':'application/json'},
                body: JSON.stringify({ user_id: userId, coin: '', action: 'clear' })
            });
            if (r.ok) { watchCoins = []; renderLists(); toast('✅ Watchlist очищен'); loadData(); }
        }
        async function loadData() {
            try {
                const r = await fetch('/api/data?user_id=' + encodeURIComponent(userId));
                if (!r.ok) return;
                const d = await r.json();
                if (!inited && d.config) {
                    document.getElementById('s-spot').value = d.config.min_wall_spot_usd;
                    document.getElementById('s-fut').value = d.config.min_wall_futures_usd;
                    document.getElementById('s-dist').value = d.config.max_distance_pct;
                    document.getElementById('s-anom').value = d.config.min_anomaly_x;
                    document.getElementById('s-life').value = d.config.min_lifetime_sec;
                    document.getElementById('s-vol').value = d.config.min_vol24h_usd;
                    document.getElementById('s-mkt').value = d.config.market_mode;
                    document.getElementById('s-side').value = d.config.side_mode;
                    document.getElementById('find-spot').value = d.config.min_wall_spot_usd;
                    document.getElementById('find-fut').value = d.config.min_wall_futures_usd;
                    document.getElementById('find-dist').value = d.config.max_distance_pct;
                    selEx = d.config.exchanges || [...ALL_EX];
                    watchCoins = d.config.watch_coins || [];
                    bannedCoins = d.banned_coins || [];
                    allowedCoins = d.allowed_coins || [];
                    if (d.discord_config) document.getElementById('dc-url').value = d.discord_config.webhook_url || '';
                    renderExPills();
                    renderLists();
                    inited = true;
                }
                const live = d.active || [];
                const hist = d.history || [];
                document.getElementById('live-list').innerHTML = live.length ? live.map(cardHtml).join('') : '<div class="empty">Активных плотностей сейчас нет</div>';
                document.getElementById('hist-list').innerHTML = hist.length ? hist.map(cardHtml).join('') : '<div class="empty">За сегодня (UTC) плотностей пока нет</div>';
            } catch {}
        }
        renderExPills();
        connectTiger(false);
        loadData();
        setInterval(loadData, 2000);
    </script>
</body>
</html>
"""


# ==================== FASTAPI СЕРВЕР ====================

class SettingsPayload(BaseModel):
    user_id: int
    config: Dict[str, Any]
    discord_config: Optional[Dict[str, Any]] = None


class DiscordTestPayload(BaseModel):
    user_id: int = 0
    webhook_url: str = ""


class CoinActionPayload(BaseModel):
    user_id: int
    coin: str = ""
    action: str = "ban"


async def _run_telegram_polling() -> None:
    if bot is None:
        logger.warning("BOT_TOKEN не задан — Telegram-бот не запущен.")
        return
    while True:
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            await dp.start_polling(bot, handle_signals=False)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Ошибка polling Telegram (%s) — перезапуск через 5с", exc)
            await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global _http_session
    await init_db()
    await sync_global_whitelist()

    ws_session = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=100, ttl_dns_cache=300),
        headers={"User-Agent": "Mozilla/5.0"},
    )
    _http_session = ws_session
    await refresh_tradfi_metadata(ws_session)
    await catalog.refresh(ws_session)

    tasks: List[asyncio.Task[Any]] = [
        asyncio.create_task(_run_telegram_polling()),
        asyncio.create_task(notify_startup()),
        asyncio.create_task(_tradfi_refresher_loop(ws_session)),
        asyncio.create_task(full_market_scan_loop(ws_session, density_engine)),
        asyncio.create_task(
            _binance_ws_scanner(ws_session, "Binance", "FUTURES", "wss://fstream.binance.com", density_engine)
        ),
        asyncio.create_task(
            _binance_ws_scanner(ws_session, "Binance", "SPOT", "wss://stream.binance.com:9443", density_engine)
        ),
        asyncio.create_task(
            _binance_ws_scanner(ws_session, "Aster", "FUTURES", "wss://fstream.asterdex.com", density_engine)
        ),
        asyncio.create_task(_bybit_ws_scanner(ws_session, "FUTURES", density_engine)),
        asyncio.create_task(_bybit_ws_scanner(ws_session, "SPOT", density_engine)),
    ]

    logger.info("Скринер плотностей запущен: 8 бирж, ±10%% от спреда, полный обход каждые 3 мин.")
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        _http_session = None
        await ws_session.close()
        if bot is not None:
            await bot.session.close()
        if _pg_pool is not None:
            await _pg_pool.close()


app = FastAPI(lifespan=lifespan)


@app.api_route("/", methods=["GET", "HEAD"])
async def root_status() -> JSONResponse:
    return JSONResponse({"status": "ok", "service": "density-screener", "exchanges": ALL_EXCHANGES})


@app.api_route("/ping", methods=["GET", "HEAD"])
async def ping() -> JSONResponse:
    return JSONResponse({"status": "ok", "exchanges": len(ALL_EXCHANGES), "active": len(density_engine.get_active())})


@app.get("/webapp", response_class=HTMLResponse)
async def webapp() -> HTMLResponse:
    return HTMLResponse(WEBAPP_HTML)


@app.get("/api/data")
async def api_data(user_id: int = Query(default=0)) -> JSONResponse:
    await refresh_api_cache()
    cfg, dcfg, bans, allowed = (
        await get_user_record(user_id)
        if user_id > 0
        else (dict(DEFAULT_CONFIG), dict(DEFAULT_DISCORD_CONFIG), [], [])
    )

    active_rows = []
    for ev in density_engine.get_active():
        if event_matches_user_config(ev, cfg, bans, allowed):
            active_rows.append(
                {
                    "exchange": ev.exchange,
                    "market_type": ev.market_type,
                    "symbol": ev.symbol,
                    "side": ev.side,
                    "price": fmt_price(ev.price),
                    "wall_price": fmt_price(ev.wall_price),
                    "wall_usd": ev.wall_usd,
                    "distance_pct": ev.distance_pct,
                    "anomaly_x": ev.anomaly_x,
                    "eat_minutes": ev.eat_minutes,
                    "volume_24h_usd": ev.volume_24h_usd,
                    "utc_time": ev.timestamp_utc,
                    "event_type": ev.event_type,
                    "initial_wall_usd": ev.initial_wall_usd,
                    "eaten_pct": ev.eaten_pct,
                }
            )

    history_rows = []
    for h in _api_cache["history"]:
        ev = DensityEvent(
            exchange=str(h.get("exchange") or ""),
            market_type=str(h.get("market_type") or "FUTURES"),
            symbol=str(h.get("symbol") or ""),
            side=str(h.get("side") or "BID"),
            price=_safe_float(h.get("price")),
            wall_price=_safe_float(h.get("wall_price")),
            wall_usd=_safe_float(h.get("wall_usd")),
            distance_pct=_safe_float(h.get("distance_pct")),
            anomaly_x=_safe_float(h.get("anomaly_x")),
            eat_minutes=_safe_float(h.get("eat_minutes")),
            volume_24h_usd=_safe_float(h.get("volume_24h_usd")),
            timestamp_utc=str(h.get("utc_time") or ""),
            event_type=str(h.get("event_type") or "NEW"),
            initial_wall_usd=_safe_float(h.get("initial_wall_usd")),
            eaten_pct=_safe_float(h.get("eaten_pct")),
        )
        if event_matches_user_config(ev, cfg, bans, allowed):
            history_rows.append(
                {
                    "id": h.get("id"),
                    "exchange": ev.exchange,
                    "market_type": ev.market_type,
                    "symbol": ev.symbol,
                    "side": ev.side,
                    "price": fmt_price(ev.price),
                    "wall_price": fmt_price(ev.wall_price),
                    "wall_usd": ev.wall_usd,
                    "distance_pct": ev.distance_pct,
                    "anomaly_x": ev.anomaly_x,
                    "eat_minutes": ev.eat_minutes,
                    "volume_24h_usd": ev.volume_24h_usd,
                    "utc_time": ev.timestamp_utc,
                    "event_type": ev.event_type,
                    "initial_wall_usd": ev.initial_wall_usd,
                    "eaten_pct": ev.eaten_pct,
                }
            )

    history_rows = []
    for h in _api_cache["history"]:
        ev = DensityEvent(
            exchange=str(h.get("exchange") or ""),
            market_type=str(h.get("market_type") or "FUTURES"),
            symbol=str(h.get("symbol") or ""),
            side=str(h.get("side") or "BID"),
            price=_safe_float(h.get("price")),
            wall_price=_safe_float(h.get("wall_price")),
            wall_usd=_safe_float(h.get("wall_usd")),
            distance_pct=_safe_float(h.get("distance_pct")),
            anomaly_x=_safe_float(h.get("anomaly_x")),
            eat_minutes=_safe_float(h.get("eat_minutes")),
            volume_24h_usd=_safe_float(h.get("volume_24h_usd")),
            timestamp_utc=str(h.get("utc_time") or ""),
        )
        if event_matches_user_config(ev, cfg, bans, allowed):
            history_rows.append(
                {
                    "id": h.get("id"),
                    "exchange": ev.exchange,
                    "market_type": ev.market_type,
                    "symbol": ev.symbol,
                    "side": ev.side,
                    "price": fmt_price(ev.price),
                    "wall_price": fmt_price(ev.wall_price),
                    "wall_usd": ev.wall_usd,
                    "distance_pct": ev.distance_pct,
                    "anomaly_x": ev.anomaly_x,
                    "eat_minutes": ev.eat_minutes,
                    "volume_24h_usd": ev.volume_24h_usd,
                    "utc_time": ev.timestamp_utc,
                }
            )

    return JSONResponse(
        {
            "active": active_rows,
            "history": history_rows,
            "config": cfg,
            "discord_config": dcfg,
            "banned_coins": bans,
            "allowed_coins": allowed,
        }
    )


@app.get("/api/find")
async def api_find(
    coin: str = Query(default=""),
    min_spot: float = Query(default=250_000.0),
    min_fut: float = Query(default=500_000.0),
    max_dist: float = Query(default=10.0),
) -> JSONResponse:
    if not coin.strip() or _http_session is None:
        return JSONResponse({"walls": []})
    walls = await scan_single_coin_live(_http_session, coin, max(1_000.0, min_spot), max(1_000.0, min_fut), min(25.0, max(0.5, max_dist)), 1.5)
    return JSONResponse(
        {
            "walls": [
                {
                    "exchange": w.exchange,
                    "market_type": w.market_type,
                    "symbol": w.symbol,
                    "side": w.side,
                    "price": fmt_price(w.price),
                    "wall_price": fmt_price(w.wall_price),
                    "wall_usd": w.wall_usd,
                    "distance_pct": w.distance_pct,
                    "anomaly_x": w.anomaly_x,
                    "eat_minutes": w.eat_minutes,
                    "volume_24h_usd": w.volume_24h_usd,
                    "utc_time": w.timestamp_utc,
                }
                for w in walls
            ]
        }
    )


@app.post("/api/settings")
async def api_settings(payload: SettingsPayload) -> JSONResponse:
    if payload.user_id > 0:
        cfg, dcfg, bans, allowed = await save_user_record(
            payload.user_id,
            cfg=payload.config,
            dcfg=payload.discord_config,
        )
        return JSONResponse({"status": "ok", "config": cfg, "discord_config": dcfg, "banned_coins": bans, "allowed_coins": allowed})
    return JSONResponse({"status": "ok"})


@app.post("/api/discord/test")
async def api_discord_test(payload: DiscordTestPayload) -> JSONResponse:
    url = payload.webhook_url.strip()
    if not url and payload.user_id > 0:
        _, dcfg, _, _ = await get_user_record(payload.user_id)
        url = resolve_discord_webhook(dcfg)
    if not url:
        url = ENV_DISCORD_WEBHOOK
    ok, err = await send_test_discord(url, session=_http_session)
    return JSONResponse({"ok": ok, "detail": err})


@app.post("/api/allow")
async def api_allow(payload: CoinActionPayload) -> JSONResponse:
    coins = parse_coin_list(payload.coin)
    if not coins:
        return JSONResponse({"status": "error"}, status_code=400)
    if payload.user_id > 0:
        _, _, bans, allowed = await get_user_record(payload.user_id)
        if payload.action == "unallow":
            allowed = [c for c in allowed if c not in coins]
        else:
            allowed = list(dict.fromkeys(allowed + coins))
            bans = [b for b in bans if b not in coins]
        _, _, bans, allowed = await save_user_record(payload.user_id, bans=bans, allowed=allowed)
        return JSONResponse({"status": "ok", "allowed_coins": allowed, "banned_coins": bans})
    return JSONResponse({"status": "ok", "allowed_coins": coins, "banned_coins": []})


@app.post("/api/ban")
async def api_ban(payload: CoinActionPayload) -> JSONResponse:
    coins = parse_coin_list(payload.coin)
    if not coins:
        return JSONResponse({"status": "error"}, status_code=400)
    if payload.user_id > 0:
        cfg, _, bans, allowed = await get_user_record(payload.user_id)
        if payload.action == "unban":
            bans = [c for c in bans if c not in coins]
        else:
            bans = list(dict.fromkeys(bans + coins))
            allowed = [a for a in allowed if a not in coins]
            cfg["watch_coins"] = [w for w in (cfg.get("watch_coins") or []) if w not in coins]
        cfg, _, bans, allowed = await save_user_record(payload.user_id, cfg=cfg, bans=bans, allowed=allowed)
        return JSONResponse(
            {
                "status": "ok",
                "banned_coins": bans,
                "allowed_coins": allowed,
                "watch_coins": cfg.get("watch_coins") or [],
            }
        )
    return JSONResponse({"status": "ok", "banned_coins": coins, "allowed_coins": [], "watch_coins": []})


@app.post("/api/watch")
async def api_watch(payload: CoinActionPayload) -> JSONResponse:
    if payload.user_id > 0:
        cfg, _, bans, allowed = await get_user_record(payload.user_id)
        if payload.action == "clear":
            cfg["watch_coins"] = []
        else:
            coins = parse_coin_list(payload.coin)
            cfg["watch_coins"] = coins
            allowed = list(dict.fromkeys(allowed + coins))
            bans = [b for b in bans if b not in coins]
        cfg, _, bans, allowed = await save_user_record(payload.user_id, cfg=cfg, bans=bans, allowed=allowed)
        return JSONResponse({"status": "ok", "watch_coins": cfg["watch_coins"], "allowed_coins": allowed, "banned_coins": bans})
    return JSONResponse({"status": "ok", "watch_coins": []})


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)