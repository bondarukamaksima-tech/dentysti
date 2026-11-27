"""
Отдельный самостоятельный скринер аномальных плотностей в стакане (Orderbook Walls).

Возможности:
- WebSocket-сканирование ФЬЮЧЕРСОВ и СПОТА в реальном времени по 7 биржам:
  Binance, Bybit, OKX, Bitget, Gate, KuCoin, Aster.
- Раздельные настраиваемые пороги плотности:
    * Спот: от $250,000 по умолчанию (меняется через /minspot или Mini App).
    * Фьючерсы: от $500,000 по умолчанию (меняется через /minfut или Mini App).
- Поиск и фильтрация по отдельным монетам:
    * Мгновенное сканирование стаканов конкретной монеты по 7 биржам (/find COIN [порог] или в Mini App).
    * Режим отслеживания только выбранных монет (/watch COIN1 COIN2 | /watch clear).
- Гибкое управление чёрным списком монет:
    * По умолчанию заблокированы тяжёлые крипто-монеты, стейблкоины, форекс, акции/RWA, ETF, металлы и нефть.
    * Можно в 1 клик РАЗБЛОКИРОВАТЬ любую монету из чёрного списка (/allow BTC SOL XAU или кнопки в /settings и Mini App).
    * Личный чёрный список пользователя (/ban, /unban).
- Отправка алертов в Telegram и Discord-канал через Webhooks (/discord URL).
- База данных PostgreSQL (если указан DATABASE_URL) с автоматическим резервом на SQLite.
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
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import aiohttp
import aiosqlite
import uvicorn
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

try:
    import asyncpg  # type: ignore
except ImportError:
    asyncpg = None

# ==================== ЛОГИРОВАНИЕ И ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ ====================

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

ALL_EXCHANGES = ["Binance", "Bybit", "OKX", "Bitget", "Gate", "KuCoin", "Aster"]
ALL_MARKETS = ["FUTURES", "SPOT"]
QUICK_UNBLOCK_COINS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "SUI", "PEPE", "WIF", "XAU", "XAG", "TSLA", "NVDA"]

DEFAULT_CONFIG: Dict[str, Any] = {
    "enabled": True,
    "min_wall_spot_usd": 250_000.0,     # Мин. размер плотности на СПОТЕ ($)
    "min_wall_futures_usd": 500_000.0,  # Мин. размер плотности на ФЬЮЧЕРСАХ ($)
    "max_distance_pct": 2.5,            # Макс. дистанция до цены (%)
    "min_anomaly_x": 5.0,               # Во сколько раз больше медианного уровня стакана
    "min_lifetime_sec": 15.0,           # Мин. время удержания в стакане (анти-спуфинг, сек)
    "min_vol24h": 500_000.0,            # Мин. суточный оборот монеты ($)
    "cooldown_sec": 300.0,              # Пауза повтора по одной стороне монеты (сек)
    "side_mode": "BOTH",                # BOTH | BID | ASK
    "market_mode": "BOTH",              # BOTH | FUTURES | SPOT
    "exchanges": list(ALL_EXCHANGES),
    "watch_coins": [],                  # Пусто = весь рынок; иначе алерты только по указанным монетам
}

DEFAULT_DISCORD_CONFIG: Dict[str, Any] = {
    "enabled": True,
    "webhook_url": "",
}

VALID_WEBHOOK_PREFIXES = (
    "https://discord.com/api/webhooks/",
    "https://discordapp.com/api/webhooks/",
    "https://canary.discord.com/api/webhooks/",
    "https://ptb.discord.com/api/webhooks/",
)

# ==================== ЧЁРНЫЙ СПИСОК МОНЕТ (ТЯЖИ, АКЦИИ, МЕТАЛЛЫ, НЕФТЬ, СТЕЙБЛЫ) ====================

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
    "NG", "NATGAS", "NGAS", "UNG", "BOIL", "KOLD", "GAS", "CORN", "WHEAT",
    "SOYB", "SUGAR", "COFFEE", "COCOA", "COTTON", "URANIUM", "URA", "XLE",
    # Индексы и ETF
    "SPY", "QQQ", "DIA", "IWM", "VOO", "VTI", "TQQQ", "SQQQ", "SOXL", "SOXS",
    "UPRO", "SPXU", "UVXY", "VIX", "TLT", "TMF", "HYG", "LQD", "EEM", "FXI",
    "KWEB", "ARKK", "XLF", "XLK", "XLV", "XLI", "XLP", "XLY", "XLU", "SMH",
    "US500", "SPX", "SPX500", "NAS100", "NDX", "USTEC", "US30", "DJI", "DJ30",
    "US2000", "RUT", "GER40", "DAX", "DE40", "UK100", "FTSE", "JP225", "NIKKEI",
    "HK50", "HSI", "FR40", "EU50", "CN50", "A50", "MAG7", "FAANG",
    # Акции и токенизированные акции (RWA)
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
    "U", "TTD", "AFRM", "SOFI", "UPST", "NU", "SVV", "SNDK", "SNDKB", "WDC",
    "ANET", "VRT", "VST", "CEG", "OKLO", "SMR", "IONQ", "RGTI", "QBTS",
    "RKLB", "ASTS", "LUNR", "HIMS", "TEM", "CVNA", "CAVA", "CMG", "DUOL", "SPCX",
}

_TRADFI_KEYWORDS = (
    "STOCK", "EQUITY", "EQUITIES", "RWA", "TRADFI", "TRADIFI", "METAL",
    "GOLD", "SILVER", "OIL", "COMMODIT", "FOREX", "FX", "ETF", "INDEX",
    "INDICES", "SHARE", "ENERGY", "PRECIOUS",
)

# Автоматически пополняемые списки TradFi-контрактов напрямую из API бирж
_dynamic_tradfi_pairs: Set[Tuple[str, str]] = set()
_dynamic_tradfi_bases: Set[str] = set()

# Глобальный белый список монет, разблокированных из ЧС или добавленных в отслеживание (/allow, /watch)
_runtime_allowed_bases: Set[str] = set()
_whitelist_version: int = 0


class WallSide(str, Enum):
    BID = "BID"   # Поддержка (плотность на покупку)
    ASK = "ASK"   # Сопротивление (плотность на продажу)


@dataclass
class DensityEvent:
    exchange: str
    symbol: str
    side: WallSide
    wall_price: float
    current_price: float
    distance_pct: float
    wall_usd: float
    median_level_usd: float
    anomaly_x: float
    lifetime_sec: float
    eat_minutes: float
    volume_24h_usd: float
    timestamp_utc: str
    market_type: str = "FUTURES"   # FUTURES | SPOT


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
    """Извлекает базовый тикер (1000PEPEUSDT, WIF-USDT-SWAP, SUI_USDT, XBTUSDTM -> PEPE, WIF, SUI, BTC)."""
    s = (symbol or "").upper().strip()
    if not s:
        return ""
    for sep in ("-USDT-SWAP", "-USDC-SWAP", "-USD-SWAP", "_USDT", "-USDT", "/USDT", "_USDC", "-USDC"):
        if s.endswith(sep):
            s = s[: -len(sep)]
            break
    else:
        for suf in ("USDTM", "USDCM", "USDT", "USDC", "BUSD", "PERP"):
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
    if s.endswith("USDTM"):
        s = s[:-1]
    s = s.replace("-USDT-SWAP", "USDT").replace("_USDT", "USDT").replace("-USDT", "USDT")
    if s.startswith("XBT"):
        s = "BTC" + s[3:]
    return s


def is_blacklisted_symbol(
    symbol: str,
    exchange: str = "",
    allowed_bases: Optional[Set[str]] = None,
) -> bool:
    """
    Полный фильтр монет: блокирует тяжеловесы, стейблкоины, форекс,
    акции, RWA, ETF, индексы, металлы и нефть/газ.
    Если базовый тикер добавлен пользователем в белый список (allowed_bases / _runtime_allowed_bases),
    монета РАЗБЛОКИРОВАНА и проходит в скринер.
    """
    raw_up = (symbol or "").upper().strip()
    if not raw_up:
        return True

    base = extract_base_asset(raw_up)
    if not base:
        return True
    clean = base.replace(".", "")

    eff_allowed = _runtime_allowed_bases if allowed_bases is None else allowed_bases
    if eff_allowed and (base in eff_allowed or clean in eff_allowed):
        return False

    pk = _normalize_pair_key(raw_up)
    if exchange and (exchange, pk) in _dynamic_tradfi_pairs:
        return True
    if ("", pk) in _dynamic_tradfi_pairs:
        return True

    # Префикс r* (RWA-акции на Bitget, например rSVVUSDT)
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

    if any(tok in clean for tok in ("XAU", "XAG", "XPT", "XPD", "OIL", "BRENT", "WTI", "GOLD", "SILVER", "NATGAS", "US500", "NAS100")):
        return True

    for suf in ("ON", "X", "2L", "2S", "3L", "3S", "5L", "5S"):
        if len(clean) >= len(suf) + 2 and clean.endswith(suf):
            stem = clean[: -len(suf)]
            if stem in TRADFI_STATIC_BLACKLIST or stem in HEAVY_AND_STABLE_BLACKLIST or stem in _dynamic_tradfi_bases:
                return True

    return False


async def refresh_tradfi_metadata(session: aiohttp.ClientSession) -> None:
    """Загружает метаданные контрактов с 7 бирж и автоматически блокирует акции/RWA/металлы/нефть."""
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

    async def _get(url: str) -> Optional[Any]:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception:
            return None
        return None

    async def load_binance() -> None:
        data = await _get("https://fapi.binance.com/fapi/v1/exchangeInfo")
        for s in (data or {}).get("symbols") or []:
            sym = str(s.get("symbol") or "")
            ctype = str(s.get("contractType") or "").upper()
            utype = str(s.get("underlyingType") or "").upper()
            sub = " ".join(str(x) for x in (s.get("underlyingSubType") or []))
            if _has_tradfi_keyword(ctype) or (utype and utype != "COIN") or _has_tradfi_keyword(sub):
                mark("Binance", sym)

    async def load_bybit() -> None:
        data = await _get("https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000")
        for it in ((data or {}).get("result") or {}).get("list") or []:
            sym = str(it.get("symbol") or "")
            if (
                _has_tradfi_keyword(it.get("symbolType"))
                or _has_tradfi_keyword(it.get("contractType"))
                or _has_tradfi_keyword(it.get("displayName"))
            ):
                mark("Bybit", sym)

    async def load_okx() -> None:
        data = await _get("https://www.okx.com/api/v5/public/instruments?instType=SWAP")
        for it in (data or {}).get("data") or []:
            iid = str(it.get("instId") or "")
            cat = str(it.get("instCategory") or "")
            if (cat and cat != "1") or _has_tradfi_keyword(it.get("instFamily")):
                mark("OKX", iid)

    async def load_bitget() -> None:
        data = await _get("https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES")
        for it in (data or {}).get("data") or []:
            sym = str(it.get("symbol") or "")
            if (
                str(it.get("isRwa") or "").upper() == "YES"
                or _has_tradfi_keyword(it.get("symbolType"))
                or _has_tradfi_keyword(it.get("category"))
            ):
                mark("Bitget", sym)

    async def load_gate() -> None:
        data = await _get("https://api.gateio.ws/api/v4/futures/usdt/contracts")
        if isinstance(data, list):
            for it in data:
                c = str(it.get("name") or "")
                if (
                    _has_tradfi_keyword(it.get("contract_type"))
                    or _has_tradfi_keyword(it.get("category"))
                    or _has_tradfi_keyword(it.get("tag"))
                ):
                    mark("Gate", c)

    async def load_aster() -> None:
        data = await _get("https://fapi.asterdex.com/fapi/v1/exchangeInfo")
        for s in (data or {}).get("symbols") or []:
            sym = str(s.get("symbol") or "")
            ctype = str(s.get("contractType") or "").upper()
            utype = str(s.get("underlyingType") or "").upper()
            sub = " ".join(str(x) for x in (s.get("underlyingSubType") or []))
            if _has_tradfi_keyword(ctype) or (utype and utype != "COIN") or _has_tradfi_keyword(sub):
                mark("Aster", sym)

    async def load_kucoin() -> None:
        data = await _get("https://api-futures.kucoin.com/api/v1/contracts/active")
        for it in (data or {}).get("data") or []:
            sym = str(it.get("symbol") or "")
            if _has_tradfi_keyword(it.get("type")) or _has_tradfi_keyword(it.get("baseCurrency")):
                mark("KuCoin", sym)

    await asyncio.gather(
        load_binance(), load_bybit(), load_okx(), load_bitget(), load_gate(), load_aster(), load_kucoin(),
        return_exceptions=True,
    )
    if new_pairs or new_bases:
        _dynamic_tradfi_pairs.update(new_pairs)
        _dynamic_tradfi_bases.update(new_bases)
        logger.info(
            "[TradFi] Заблокировано %s контрактов (%s базовых тикеров) из API 7 бирж.",
            len(_dynamic_tradfi_pairs) // 2,
            len(_dynamic_tradfi_bases),
        )


# ==================== НОРМАЛИЗАЦИЯ КОНФИГОВ ====================

def is_valid_discord_webhook(url: str) -> bool:
    clean = (url or "").strip()
    return bool(clean) and clean.startswith(VALID_WEBHOOK_PREFIXES) and len(clean) > 45


def normalize_config(raw: Any) -> Dict[str, Any]:
    """Приводит настройки пользователя к безопасным значениям."""
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

    legacy_wall = _safe_float(data.get("min_wall_usd"), 0.0)
    spot_default = legacy_wall if (legacy_wall >= 10_000.0 and "min_wall_spot_usd" not in data) else cfg["min_wall_spot_usd"]
    fut_default = legacy_wall if (legacy_wall >= 10_000.0 and "min_wall_futures_usd" not in data) else cfg["min_wall_futures_usd"]

    cfg["min_wall_spot_usd"] = round(max(10_000.0, _safe_float(data.get("min_wall_spot_usd"), spot_default)), 2)
    cfg["min_wall_futures_usd"] = round(max(10_000.0, _safe_float(data.get("min_wall_futures_usd"), fut_default)), 2)
    cfg["max_distance_pct"] = round(min(15.0, max(0.1, _safe_float(data.get("max_distance_pct"), cfg["max_distance_pct"]))), 2)
    cfg["min_anomaly_x"] = round(min(100.0, max(1.5, _safe_float(data.get("min_anomaly_x"), cfg["min_anomaly_x"]))), 1)
    cfg["min_lifetime_sec"] = round(min(600.0, max(0.0, _safe_float(data.get("min_lifetime_sec"), cfg["min_lifetime_sec"]))), 1)
    cfg["min_vol24h"] = round(max(0.0, _safe_float(data.get("min_vol24h"), cfg["min_vol24h"])), 2)
    cfg["cooldown_sec"] = round(min(3600.0, max(30.0, _safe_float(data.get("cooldown_sec"), cfg["cooldown_sec"]))), 1)

    sm = str(data.get("side_mode") or cfg["side_mode"]).strip().upper()
    cfg["side_mode"] = sm if sm in ("BOTH", "BID", "ASK") else "BOTH"

    mm = str(data.get("market_mode") or cfg["market_mode"]).strip().upper()
    cfg["market_mode"] = mm if mm in ("BOTH", "FUTURES", "SPOT") else "BOTH"

    raw_ex = data.get("exchanges")
    if isinstance(raw_ex, list):
        valid = [str(e) for e in raw_ex if str(e) in ALL_EXCHANGES]
        cfg["exchanges"] = valid if valid else list(ALL_EXCHANGES)
    else:
        cfg["exchanges"] = list(ALL_EXCHANGES)

    raw_watch = data.get("watch_coins")
    if isinstance(raw_watch, str):
        raw_watch = [x.strip() for x in re.split(r"[,\s]+", raw_watch) if x.strip()]
    if isinstance(raw_watch, list):
        w_out: List[str] = []
        for x in raw_watch:
            b = extract_base_asset(str(x))
            if b and b not in w_out:
                w_out.append(b)
        cfg["watch_coins"] = w_out
    else:
        cfg["watch_coins"] = []
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


def parse_banned_coins(raw: Any) -> List[str]:
    if not raw:
        return []
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            items = parsed if isinstance(parsed, list) else []
        except Exception:
            items = []
    else:
        items = []
    out: List[str] = []
    for x in items:
        b = extract_base_asset(str(x))
        if b and b not in out:
            out.append(b)
    return out


def wall_matches_config(
    exchange: str,
    side: str,
    wall_usd: float,
    distance_pct: float,
    anomaly_x: float,
    lifetime_sec: float,
    volume_24h_usd: float,
    cfg: Dict[str, Any],
    market_type: str = "FUTURES",
    symbol: str = "",
) -> bool:
    if exchange not in cfg.get("exchanges", ALL_EXCHANGES):
        return False
    watch_coins = cfg.get("watch_coins") or []
    if watch_coins and symbol:
        base = extract_base_asset(symbol)
        if base not in watch_coins:
            return False
    mm = cfg.get("market_mode", "BOTH")
    mt = (market_type or "FUTURES").upper()
    if mm == "FUTURES" and mt != "FUTURES":
        return False
    if mm == "SPOT" and mt != "SPOT":
        return False
    sm = cfg.get("side_mode", "BOTH")
    if sm == "BID" and side != "BID":
        return False
    if sm == "ASK" and side != "ASK":
        return False
    min_wall = (
        float(cfg.get("min_wall_spot_usd", 250_000.0))
        if mt == "SPOT"
        else float(cfg.get("min_wall_futures_usd", 500_000.0))
    )
    if wall_usd < min_wall:
        return False
    if distance_pct > float(cfg.get("max_distance_pct", 2.5)):
        return False
    if anomaly_x < float(cfg.get("min_anomaly_x", 5.0)):
        return False
    if lifetime_sec < float(cfg.get("min_lifetime_sec", 15.0)):
        return False
    if volume_24h_usd < float(cfg.get("min_vol24h", 500_000.0)):
        return False
    return True


# ==================== БАЗА ДАННЫХ (PostgreSQL / SQLite) ====================

_pg_pool: Any = None
_use_pg: bool = False
_http_session: Optional[aiohttp.ClientSession] = None


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
                        market_type TEXT DEFAULT 'FUTURES',
                        symbol TEXT NOT NULL,
                        side TEXT NOT NULL,
                        wall_price DOUBLE PRECISION,
                        current_price DOUBLE PRECISION,
                        distance_pct DOUBLE PRECISION,
                        wall_usd DOUBLE PRECISION,
                        median_level_usd DOUBLE PRECISION,
                        anomaly_x DOUBLE PRECISION,
                        lifetime_sec DOUBLE PRECISION,
                        eat_minutes DOUBLE PRECISION,
                        volume_24h_usd DOUBLE PRECISION
                    )
                    """
                )
                await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS allowed_coins TEXT DEFAULT '[]'")
                await conn.execute("ALTER TABLE densities ADD COLUMN IF NOT EXISTS market_type TEXT DEFAULT 'FUTURES'")
            _use_pg = True
            logger.info("БД: PostgreSQL подключена.")
            return
        except Exception as exc:
            logger.warning("Ошибка PostgreSQL (%s) — переключаюсь на SQLite.", exc)
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
                market_type TEXT DEFAULT 'FUTURES',
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                wall_price REAL,
                current_price REAL,
                distance_pct REAL,
                wall_usd REAL,
                median_level_usd REAL,
                anomaly_x REAL,
                lifetime_sec REAL,
                eat_minutes REAL,
                volume_24h_usd REAL
            )
            """
        )
        cur_u = await db.execute("PRAGMA table_info(users)")
        u_cols = {row[1] for row in await cur_u.fetchall()}
        if "allowed_coins" not in u_cols:
            await db.execute("ALTER TABLE users ADD COLUMN allowed_coins TEXT DEFAULT '[]'")
        cur_d = await db.execute("PRAGMA table_info(densities)")
        d_cols = {row[1] for row in await cur_d.fetchall()}
        if "market_type" not in d_cols:
            await db.execute("ALTER TABLE densities ADD COLUMN market_type TEXT DEFAULT 'FUTURES'")
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


# ==================== DISCORD WEBHOOK NOTIFIER ====================

async def post_discord_webhook(
    webhook_url: str,
    payload: Dict[str, Any],
    session: Optional[aiohttp.ClientSession] = None,
) -> Tuple[bool, str]:
    """Отправляет сообщение в Discord Webhook с обработкой 429 rate-limit."""
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
        logger.warning("[Discord] Ошибка отправки: %s", exc)
        return False, str(exc)
    finally:
        if own:
            await client.close()


async def send_density_discord(
    webhook_url: str,
    ev: DensityEvent,
    session: Optional[aiohttp.ClientSession] = None,
) -> bool:
    """Отправляет карточку аномальной плотности в Discord-канал."""
    is_bid = ev.side == WallSide.BID
    emoji = "🟢" if is_bid else "🔴"
    side_ru = "BID (Поддержка)" if is_bid else "ASK (Сопротивление)"
    mkt_ru = "Спот" if ev.market_type == "SPOT" else "Фьючерс"
    color = 0x16C784 if is_bid else 0xEA3943
    payload = {
        "username": "Density Wall Screener",
        "embeds": [
            {
                "title": f"🧱 {ev.symbol} ({mkt_ru}) · {emoji} {side_ru}",
                "color": color,
                "fields": [
                    {"name": "Биржа", "value": f"**{ev.exchange}** ({mkt_ru})", "inline": True},
                    {"name": "Плотность", "value": f"**${ev.wall_usd:,.0f}**", "inline": True},
                    {"name": "Аномалия", "value": f"**x{ev.anomaly_x:.1f}** (~{ev.eat_minutes:.1f} мин)", "inline": True},
                    {"name": "Цена плотности", "value": f"`{ev.wall_price:g}`", "inline": True},
                    {"name": "До цены", "value": f"**{ev.distance_pct:.2f}%** (`{ev.current_price:g}`)", "inline": True},
                    {"name": "Стоит / 24ч", "value": f"**{ev.lifetime_sec:.0f}с** · ${ev.volume_24h_usd:,.0f}", "inline": True},
                ],
                "footer": {"text": f"{ev.timestamp_utc} UTC"},
            }
        ],
    }
    ok, _ = await post_discord_webhook(webhook_url, payload, session=session)
    return ok


async def send_test_discord(webhook_url: str, session: Optional[aiohttp.ClientSession] = None) -> Tuple[bool, str]:
    payload = {
        "username": "Density Wall Screener",
        "embeds": [
            {
                "title": "✅ Тест Discord (Аномальные плотности)",
                "description": "Вебхук успешно подключён! Алерты по крупным плотностям будут приходить сюда.",
                "color": 0x16C784,
            }
        ],
    }
    return await post_discord_webhook(webhook_url, payload, session=session)


# ==================== ДВИЖОК ПОИСКА АНОМАЛЬНЫХ ПЛОТНОСТЕЙ ====================

class DensityEngine:
    """
    Анализирует WebSocket-стаканы (Фьючерсы и Спот) и находит устойчивые аномальные плотности:
    - Считает медианный уровень стакана (в USD) и кратность аномалии (wall_usd / median_level_usd).
    - Замеряет время удержания плотности на уровне (анти-спуфинг).
    - Считает расчётное время разъедания (wall_usd / средний объём за 1 минуту).
    """

    def __init__(self, on_density: Callable[[DensityEvent], Any]) -> None:
        self.on_density = on_density
        self.config: Dict[str, Any] = dict(DEFAULT_CONFIG)
        # (exchange, market_type, symbol) -> last_eval_ts
        self._last_eval: Dict[Tuple[str, str, str], float] = {}
        # (exchange, market_type, symbol, side, price_key) -> first_seen_ts
        self._tracked: Dict[Tuple[str, str, str, WallSide, str], float] = {}
        # (exchange, market_type, symbol, side) -> (last_alert_ts, last_wall_price, last_wall_usd)
        self._cooldown: Dict[Tuple[str, str, str, WallSide], Tuple[float, float, float]] = {}
        # (exchange, market_type, symbol, side, price_key) -> (DensityEvent, last_seen_ts)
        self._active: Dict[Tuple[str, str, str, WallSide, str], Tuple[DensityEvent, float]] = {}

    def update_config(self, cfg: Dict[str, Any]) -> None:
        self.config = normalize_config(cfg)
        logger.info(
            "[DensityEngine] Конфиг: Спот>=$%s, Фьюч>=$%s, дист<=%s%%, x>=%s, жизнь>=%sс, 24ч>=$%s, кд=%sс, сторона=%s, рынок=%s, монеты=%s",
            self.config["min_wall_spot_usd"],
            self.config["min_wall_futures_usd"],
            self.config["max_distance_pct"],
            self.config["min_anomaly_x"],
            self.config["min_lifetime_sec"],
            self.config["min_vol24h"],
            self.config["cooldown_sec"],
            self.config["side_mode"],
            self.config["market_mode"],
            ",".join(self.config["watch_coins"]) if self.config["watch_coins"] else "ВСЕ",
        )

    @staticmethod
    def _price_key(px: float) -> str:
        return f"{px:.7g}"

    async def process_orderbook(
        self,
        exchange: str,
        symbol: str,
        bids: List[Tuple[float, float]],
        asks: List[Tuple[float, float]],
        volume_24h_usd: float,
        market_type: str = "FUTURES",
    ) -> None:
        """
        bids: список (price, base_qty) отсортированный по убыванию цены (лучший bid первый)
        asks: список (price, base_qty) отсортированный по возрастанию цены (лучший ask первый)
        """
        if not bids or not asks or is_blacklisted_symbol(symbol, exchange):
            return

        cfg = self.config
        if exchange not in cfg["exchanges"]:
            return
        mt = market_type.upper()
        mm = cfg.get("market_mode", "BOTH")
        if (mm == "FUTURES" and mt != "FUTURES") or (mm == "SPOT" and mt != "SPOT"):
            return

        watch_coins = cfg.get("watch_coins") or []
        if watch_coins and extract_base_asset(symbol) not in watch_coins:
            return

        now = time.time()
        pair_key = (exchange, mt, symbol)
        if now - self._last_eval.get(pair_key, 0.0) < 1.0:
            return
        self._last_eval[pair_key] = now

        min_vol = float(cfg["min_vol24h"])
        if volume_24h_usd < min_vol:
            return

        best_bid = bids[0][0]
        best_ask = asks[0][0]
        if best_bid <= 0 or best_ask <= 0 or best_bid > best_ask:
            return
        mid = (best_bid + best_ask) * 0.5

        top_bids = [(px, px * qty) for px, qty in bids[:30] if px > 0 and qty > 0]
        top_asks = [(px, px * qty) for px, qty in asks[:30] if px > 0 and qty > 0]
        if len(top_bids) < 4 or len(top_asks) < 4:
            return

        all_sizes = [u for _, u in top_bids] + [u for _, u in top_asks]
        median_lvl = statistics.median(all_sizes)
        if median_lvl <= 0:
            return

        min_wall = (
            float(cfg["min_wall_spot_usd"])
            if mt == "SPOT"
            else float(cfg["min_wall_futures_usd"])
        )
        max_dist = float(cfg["max_distance_pct"])
        min_anom = float(cfg["min_anomaly_x"])
        min_life = float(cfg["min_lifetime_sec"])
        cooldown_sec = float(cfg["cooldown_sec"])
        side_mode = cfg["side_mode"]

        # Порог поддержания отслеживания (75% от min_wall), чтобы мелкие частичные филы не сбрасывали таймер жизни
        track_floor = max(10_000.0, min_wall * 0.75)
        seen_keys_now: Set[Tuple[str, str, str, WallSide, str]] = set()
        avg_vol_per_min = max(volume_24h_usd / 1440.0, 1.0)

        def check_side(side: WallSide, levels: List[Tuple[float, float]]) -> List[Tuple[Tuple[str, str, str, WallSide, str], DensityEvent]]:
            found: List[Tuple[Tuple[str, str, str, WallSide, str], DensityEvent]] = []
            if side_mode == "BID" and side != WallSide.BID:
                return found
            if side_mode == "ASK" and side != WallSide.ASK:
                return found

            for px, usd in levels:
                dist_pct = abs(px - mid) / mid * 100.0
                if dist_pct > max_dist:
                    continue
                if usd < track_floor:
                    continue
                anom_x = usd / median_lvl
                if anom_x < max(1.5, min_anom * 0.75):
                    continue

                wkey = (exchange, mt, symbol, side, self._price_key(px))
                seen_keys_now.add(wkey)
                first_ts = self._tracked.get(wkey)
                if first_ts is None:
                    self._tracked[wkey] = now
                    first_ts = now
                life_sec = now - first_ts

                if usd >= min_wall and anom_x >= min_anom and life_sec >= min_life:
                    eat_min = round(usd / avg_vol_per_min, 1)
                    ev = DensityEvent(
                        exchange=exchange,
                        symbol=symbol,
                        side=side,
                        wall_price=px,
                        current_price=mid,
                        distance_pct=round(dist_pct, 2),
                        wall_usd=round(usd, 2),
                        median_level_usd=round(median_lvl, 2),
                        anomaly_x=round(anom_x, 1),
                        lifetime_sec=round(life_sec, 1),
                        eat_minutes=eat_min,
                        volume_24h_usd=volume_24h_usd,
                        timestamp_utc=datetime.now(timezone.utc).strftime("%H:%M:%S"),
                        market_type=mt,
                    )
                    found.append((wkey, ev))
            return found

        candidates = check_side(WallSide.BID, top_bids) + check_side(WallSide.ASK, top_asks)

        # Удаляем из трекинга плотности этой монеты, которые исчезли из стакана (спуфинг или разъели)
        stale = [
            k
            for k in self._tracked
            if k[0] == exchange and k[1] == mt and k[2] == symbol and k not in seen_keys_now
        ]
        for k in stale:
            self._tracked.pop(k, None)
            self._active.pop(k, None)

        for wkey, ev in candidates:
            self._active[wkey] = (ev, now)
            cd_key = (exchange, mt, symbol, ev.side)
            prev = self._cooldown.get(cd_key)
            can_alert = False
            if prev is None:
                can_alert = True
            else:
                prev_ts, prev_px, prev_usd = prev
                price_moved = prev_px > 0 and (abs(ev.wall_price - prev_px) / prev_px * 100.0) >= 0.8
                much_bigger = ev.wall_usd >= prev_usd * 1.6
                if (now - prev_ts) >= cooldown_sec or (price_moved and much_bigger and (now - prev_ts) >= 30.0):
                    can_alert = True

            if can_alert:
                self._cooldown[cd_key] = (now, ev.wall_price, ev.wall_usd)
                await self.on_density(ev)

    def get_active_walls(self) -> List[DensityEvent]:
        now = time.time()
        expired = [k for k, (_, ts) in self._active.items() if now - ts > 25.0]
        for k in expired:
            self._active.pop(k, None)
        events = [ev for ev, _ in self._active.values()]
        events.sort(key=lambda e: e.wall_usd, reverse=True)
        return events


# ==================== ЖИВОЙ ПОИСК ПЛОТНОСТЕЙ ПО ОТДЕЛЬНОЙ МОНЕТЕ (7 БИРЖ · ФЬЮЧЕРСЫ + СПОТ) ====================

async def live_search_coin_densities(
    session: Optional[aiohttp.ClientSession],
    coin_raw: str,
    cfg: Dict[str, Any],
    override_min_spot: Optional[float] = None,
    override_min_fut: Optional[float] = None,
    override_max_dist: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Делает параллельный живой REST-снимок стаканов выбранной монеты по 7 биржам
    (Фьючерсы и Спот) и возвращает найденные плотности с учётом текущих или заданных фильтров.
    """
    base = extract_base_asset(coin_raw)
    if not base:
        return {"coin": "", "matches": [], "top_seen": [], "checked_books": 0}

    min_spot = max(1_000.0, float(override_min_spot)) if override_min_spot is not None else float(cfg.get("min_wall_spot_usd", 250_000.0))
    min_fut = max(1_000.0, float(override_min_fut)) if override_min_fut is not None else float(cfg.get("min_wall_futures_usd", 500_000.0))
    max_dist = min(20.0, max(0.1, float(override_max_dist))) if override_max_dist is not None else float(cfg.get("max_distance_pct", 2.5))
    min_anom = float(cfg.get("min_anomaly_x", 5.0))
    side_mode = str(cfg.get("side_mode", "BOTH")).upper()
    market_mode = str(cfg.get("market_mode", "BOTH")).upper()
    exchanges = list(cfg.get("exchanges", ALL_EXCHANGES))

    own_session = session is None or session.closed
    client = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) if own_session else session

    async def _get_json(url: str) -> Optional[Any]:
        try:
            async with client.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception:
            return None
        return None

    async def _fetch_book(ex: str, mt: str) -> Optional[Tuple[str, str, str, List[Tuple[float, float]], List[Tuple[float, float]]]]:
        sym_std = f"{base}USDT"
        try:
            if ex == "Binance":
                url = (
                    f"https://fapi.binance.com/fapi/v1/depth?symbol={sym_std}&limit=50"
                    if mt == "FUTURES"
                    else f"https://api.binance.com/api/v3/depth?symbol={sym_std}&limit=50"
                )
                d = await _get_json(url)
                if not isinstance(d, dict):
                    return None
                bids = [(_safe_float(p), _safe_float(q)) for p, q in (d.get("bids") or [])]
                asks = [(_safe_float(p), _safe_float(q)) for p, q in (d.get("asks") or [])]
                return ex, mt, sym_std, bids, asks

            if ex == "Aster":
                url = (
                    f"https://fapi.asterdex.com/fapi/v1/depth?symbol={sym_std}&limit=50"
                    if mt == "FUTURES"
                    else f"https://sapi.asterdex.com/api/v1/depth?symbol={sym_std}&limit=50"
                )
                d = await _get_json(url)
                if not isinstance(d, dict):
                    return None
                bids = [(_safe_float(p), _safe_float(q)) for p, q in (d.get("bids") or [])]
                asks = [(_safe_float(p), _safe_float(q)) for p, q in (d.get("asks") or [])]
                return ex, mt, sym_std, bids, asks

            if ex == "Bybit":
                cat = "linear" if mt == "FUTURES" else "spot"
                d = await _get_json(f"https://api.bybit.com/v5/market/orderbook?category={cat}&symbol={sym_std}&limit=50")
                res = (d or {}).get("result") if isinstance(d, dict) else None
                if not isinstance(res, dict):
                    return None
                bids = [(_safe_float(p), _safe_float(q)) for p, q in (res.get("b") or [])]
                asks = [(_safe_float(p), _safe_float(q)) for p, q in (res.get("a") or [])]
                return ex, mt, sym_std, bids, asks

            if ex == "OKX":
                iid = f"{base}-USDT-SWAP" if mt == "FUTURES" else f"{base}-USDT"
                ct = 1.0
                if mt == "FUTURES":
                    inst = await _get_json(f"https://www.okx.com/api/v5/public/instruments?instType=SWAP&instId={iid}")
                    i_arr = (inst or {}).get("data") or []
                    if i_arr:
                        ct = _safe_float(i_arr[0].get("ctVal"), 1.0)
                d = await _get_json(f"https://www.okx.com/api/v5/market/books?instId={iid}&sz=40")
                arr = (d or {}).get("data") or []
                if not arr:
                    return None
                b_raw = arr[0].get("bids") or []
                a_raw = arr[0].get("asks") or []
                bids = [(_safe_float(x[0]), _safe_float(x[1]) * ct) for x in b_raw if len(x) >= 2]
                asks = [(_safe_float(x[0]), _safe_float(x[1]) * ct) for x in a_raw if len(x) >= 2]
                return ex, mt, sym_std, bids, asks

            if ex == "Bitget":
                url = (
                    f"https://api.bitget.com/api/v2/mix/market/merge-depth?productType=USDT-FUTURES&symbol={sym_std}&limit=50"
                    if mt == "FUTURES"
                    else f"https://api.bitget.com/api/v2/spot/market/merge-depth?symbol={sym_std}&limit=50"
                )
                d = await _get_json(url)
                res = (d or {}).get("data") if isinstance(d, dict) else None
                if not isinstance(res, dict):
                    return None
                bids = [(_safe_float(p), _safe_float(q)) for p, q in (res.get("bids") or [])]
                asks = [(_safe_float(p), _safe_float(q)) for p, q in (res.get("asks") or [])]
                return ex, mt, sym_std, bids, asks

            if ex == "Gate":
                pair = f"{base}_USDT"
                if mt == "FUTURES":
                    c_info, d = await asyncio.gather(
                        _get_json(f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{pair}"),
                        _get_json(f"https://api.gateio.ws/api/v4/futures/usdt/order_book?contract={pair}&limit=50"),
                    )
                    if not isinstance(d, dict):
                        return None
                    mult = _safe_float((c_info or {}).get("quanto_multiplier"), 1.0) if isinstance(c_info, dict) else 1.0
                    bids = [(_safe_float(x.get("p")), abs(_safe_float(x.get("s"))) * mult) for x in (d.get("bids") or []) if isinstance(x, dict)]
                    asks = [(_safe_float(x.get("p")), abs(_safe_float(x.get("s"))) * mult) for x in (d.get("asks") or []) if isinstance(x, dict)]
                    return ex, mt, sym_std, bids, asks
                d = await _get_json(f"https://api.gateio.ws/api/v4/spot/order_book?currency_pair={pair}&limit=50")
                if not isinstance(d, dict):
                    return None
                bids = [(_safe_float(x[0]), _safe_float(x[1])) for x in (d.get("bids") or []) if isinstance(x, list) and len(x) >= 2]
                asks = [(_safe_float(x[0]), _safe_float(x[1])) for x in (d.get("asks") or []) if isinstance(x, list) and len(x) >= 2]
                return ex, mt, sym_std, bids, asks

            if ex == "KuCoin":
                if mt == "FUTURES":
                    kbase = "XBT" if base == "BTC" else base
                    ksym = f"{kbase}USDTM"
                    c_info, d = await asyncio.gather(
                        _get_json(f"https://api-futures.kucoin.com/api/v1/contracts/{ksym}"),
                        _get_json(f"https://api-futures.kucoin.com/api/v1/level2/depth50?symbol={ksym}"),
                    )
                    res = (d or {}).get("data") if isinstance(d, dict) else None
                    if not isinstance(res, dict):
                        return None
                    c_dat = (c_info or {}).get("data") if isinstance(c_info, dict) else {}
                    mult = _safe_float((c_dat or {}).get("multiplier"), 1.0)
                    bids = [(_safe_float(x[0]), _safe_float(x[1]) * mult) for x in (res.get("bids") or []) if len(x) >= 2]
                    asks = [(_safe_float(x[0]), _safe_float(x[1]) * mult) for x in (res.get("asks") or []) if len(x) >= 2]
                    return ex, mt, sym_std, bids, asks
                ksym = f"{base}-USDT"
                d = await _get_json(f"https://api.kucoin.com/api/v1/market/orderbook/level2_20?symbol={ksym}")
                res = (d or {}).get("data") if isinstance(d, dict) else None
                if not isinstance(res, dict):
                    return None
                bids = [(_safe_float(x[0]), _safe_float(x[1])) for x in (res.get("bids") or []) if len(x) >= 2]
                asks = [(_safe_float(x[0]), _safe_float(x[1])) for x in (res.get("asks") or []) if len(x) >= 2]
                return ex, mt, sym_std, bids, asks
        except Exception:
            return None
        return None

    markets_to_scan: List[str] = ["FUTURES", "SPOT"] if market_mode == "BOTH" else [market_mode]
    jobs = [_fetch_book(ex, mt) for ex in exchanges for mt in markets_to_scan]
    try:
        results = await asyncio.gather(*jobs, return_exceptions=True)
    finally:
        if own_session:
            await client.close()

    matches: List[Dict[str, Any]] = []
    top_seen: List[Dict[str, Any]] = []
    checked = 0
    now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")

    for res in results:
        if not isinstance(res, tuple):
            continue
        ex, mt, sym, bids, asks = res
        if not bids or not asks:
            continue
        best_bid = bids[0][0]
        best_ask = asks[0][0]
        if best_bid <= 0 or best_ask <= 0 or best_bid > best_ask:
            continue
        checked += 1
        mid = (best_bid + best_ask) * 0.5
        top_b = [(px, px * q) for px, q in bids[:40] if px > 0 and q > 0]
        top_a = [(px, px * q) for px, q in asks[:40] if px > 0 and q > 0]
        all_sz = [u for _, u in top_b] + [u for _, u in top_a]
        if len(all_sz) < 4:
            continue
        med = statistics.median(all_sz)
        if med <= 0:
            continue

        min_wall = min_spot if mt == "SPOT" else min_fut
        best_book_wall: Optional[Dict[str, Any]] = None

        for side_str, lvls in (("BID", top_b), ("ASK", top_a)):
            if side_mode != "BOTH" and side_mode != side_str:
                continue
            for px, usd in lvls:
                dist = abs(px - mid) / mid * 100.0
                if dist > max_dist:
                    continue
                anom = usd / med
                item = {
                    "exchange": ex,
                    "market_type": mt,
                    "symbol": sym,
                    "side": side_str,
                    "wall_price": px,
                    "current_price": round(mid, 8),
                    "distance_pct": round(dist, 2),
                    "wall_usd": round(usd, 2),
                    "anomaly_x": round(anom, 1),
                    "utc_time": now_str,
                }
                if best_book_wall is None or usd > best_book_wall["wall_usd"]:
                    best_book_wall = item
                if usd >= min_wall and anom >= min_anom:
                    matches.append(item)

        if best_book_wall is not None:
            top_seen.append(best_book_wall)

    matches.sort(key=lambda x: x["wall_usd"], reverse=True)
    top_seen.sort(key=lambda x: x["wall_usd"], reverse=True)
    return {
        "coin": base,
        "min_wall_spot_usd": min_spot,
        "min_wall_futures_usd": min_fut,
        "max_distance_pct": max_dist,
        "checked_books": checked,
        "matches": matches[:40],
        "top_seen": top_seen[:8],
    }


# ==================== WEBSOCKET-СКАНЕРЫ СТАКАНОВ (7 БИРЖ · ФЬЮЧЕРСЫ + СПОТ) ====================

def _select_top_symbols(vols: Dict[str, float], limit: int) -> List[str]:
    """
    Сортирует пары по обороту, но ВСЕГДА ставит в приоритет монеты,
    разблокированные пользователем из чёрного списка (_runtime_allowed_bases).
    """
    priority = [s for s in vols if extract_base_asset(s) in _runtime_allowed_bases]
    priority.sort(key=lambda s: vols[s], reverse=True)
    others = [s for s in vols if extract_base_asset(s) not in _runtime_allowed_bases]
    others.sort(key=lambda s: vols[s], reverse=True)
    return list(dict.fromkeys(priority + others))[:limit]


async def _binance_like_depth_ws(
    session: aiohttp.ClientSession,
    eng: DensityEngine,
    exchange_name: str,
    market_type: str,
    ticker_url: str,
    ws_base: str,
    depth_suffix: str,
    max_syms: int,
) -> None:
    """Универсальный WS-сканер стаканов для Binance и Aster (Фьючерсы и Спот)."""
    while True:
        local_wl_ver = _whitelist_version
        try:
            async with session.get(ticker_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    await asyncio.sleep(15)
                    continue
                tickers = await resp.json()

            vols: Dict[str, float] = {}
            for it in tickers if isinstance(tickers, list) else []:
                sym = str(it.get("symbol") or "")
                if not sym.endswith("USDT") or is_blacklisted_symbol(sym, exchange_name):
                    continue
                v = _safe_float(it.get("quoteVolume"))
                is_whitelisted = extract_base_asset(sym) in _runtime_allowed_bases
                if v >= 150_000 or (is_whitelisted and v >= 10_000):
                    vols[sym] = v

            top_syms = _select_top_symbols(vols, max_syms)
            if not top_syms:
                await asyncio.sleep(20)
                continue

            streams = "/".join(f"{s.lower()}@{depth_suffix}" for s in top_syms)
            ws_url = f"{ws_base}/stream?streams={streams}"
            started = time.time()

            async with session.ws_connect(ws_url, heartbeat=20, timeout=15) as ws:
                logger.info("[%s %s Depth WS] Подписан на %s стаканов", exchange_name, market_type, len(top_syms))
                async for msg in ws:
                    if time.time() - started > 1800 or _whitelist_version != local_wl_ver:
                        break
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                        continue
                    payload = json.loads(msg.data)
                    d = payload.get("data")
                    if not isinstance(d, dict):
                        continue
                    stream = str(payload.get("stream") or "")
                    sym = str(d.get("s") or (stream.split("@")[0].upper() if "@" in stream else ""))
                    if not sym:
                        continue
                    raw_bids = d.get("b") if d.get("b") is not None else (d.get("bids") or [])
                    raw_asks = d.get("a") if d.get("a") is not None else (d.get("asks") or [])
                    bids = [(_safe_float(p), _safe_float(q)) for p, q in raw_bids]
                    asks = [(_safe_float(p), _safe_float(q)) for p, q in raw_asks]
                    await eng.process_orderbook(exchange_name, sym, bids, asks, vols.get(sym, 0.0), market_type=market_type)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[%s %s Depth WS] Переподключение через 8с: %s", exchange_name, market_type, exc)
            await asyncio.sleep(8)


async def bybit_depth_ws(session: aiohttp.ClientSession, eng: DensityEngine, market_type: str = "FUTURES") -> None:
    """Bybit Linear (Фьючерсы) и Spot (Спот): orderbook.50.{symbol}."""
    category = "linear" if market_type == "FUTURES" else "spot"
    ws_url = f"wss://stream.bybit.com/v5/public/{category}"
    max_syms = 80 if market_type == "FUTURES" else 60
    while True:
        local_wl_ver = _whitelist_version
        try:
            async with session.get(
                f"https://api.bybit.com/v5/market/tickers?category={category}",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = await resp.json()

            vols: Dict[str, float] = {}
            for it in (data.get("result") or {}).get("list") or []:
                sym = str(it.get("symbol") or "")
                if not sym.endswith("USDT") or is_blacklisted_symbol(sym, "Bybit"):
                    continue
                v = _safe_float(it.get("turnover24h"))
                is_whitelisted = extract_base_asset(sym) in _runtime_allowed_bases
                if v >= 150_000 or (is_whitelisted and v >= 10_000):
                    vols[sym] = v

            top_syms = _select_top_symbols(vols, max_syms)
            if not top_syms:
                await asyncio.sleep(15)
                continue

            books: Dict[str, Tuple[Dict[float, float], Dict[float, float]]] = {}
            started = time.time()
            async with session.ws_connect(ws_url, heartbeat=20, timeout=15) as ws:
                for i in range(0, len(top_syms), 10):
                    args = [f"orderbook.50.{s}" for s in top_syms[i : i + 10]]
                    await ws.send_json({"op": "subscribe", "args": args})
                    await asyncio.sleep(0.08)
                logger.info("[Bybit %s Depth WS] Подписан на %s стаканов", market_type, len(top_syms))

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
                        if time.time() - started > 1800 or _whitelist_version != local_wl_ver:
                            break
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                            continue
                        payload = json.loads(msg.data)
                        topic = str(payload.get("topic") or "")
                        if not topic.startswith("orderbook.50."):
                            continue
                        d = payload.get("data")
                        if not isinstance(d, dict):
                            continue
                        sym = str(d.get("s") or topic.split(".")[-1])
                        msg_type = payload.get("type")
                        if msg_type == "snapshot" or sym not in books:
                            b_map = {_safe_float(p): _safe_float(q) for p, q in (d.get("b") or []) if _safe_float(q) > 0}
                            a_map = {_safe_float(p): _safe_float(q) for p, q in (d.get("a") or []) if _safe_float(q) > 0}
                            books[sym] = (b_map, a_map)
                        else:
                            b_map, a_map = books[sym]
                            for p_raw, q_raw in d.get("b") or []:
                                px, qty = _safe_float(p_raw), _safe_float(q_raw)
                                if qty <= 0:
                                    b_map.pop(px, None)
                                else:
                                    b_map[px] = qty
                            for p_raw, q_raw in d.get("a") or []:
                                px, qty = _safe_float(p_raw), _safe_float(q_raw)
                                if qty <= 0:
                                    a_map.pop(px, None)
                                else:
                                    a_map[px] = qty

                        b_map, a_map = books[sym]
                        bids_sorted = sorted(b_map.items(), key=lambda x: x[0], reverse=True)[:40]
                        asks_sorted = sorted(a_map.items(), key=lambda x: x[0])[:40]
                        if len(b_map) > 100:
                            books[sym] = (dict(bids_sorted), dict(asks_sorted))
                        await eng.process_orderbook("Bybit", sym, bids_sorted, asks_sorted, vols.get(sym, 0.0), market_type=market_type)
                finally:
                    pt.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[Bybit %s Depth WS] Переподключение через 5с: %s", market_type, exc)
            await asyncio.sleep(5)


async def okx_depth_ws(session: aiohttp.ClientSession, eng: DensityEngine, market_type: str = "FUTURES") -> None:
    """OKX USDT-SWAP (Фьючерсы) и SPOT (Спот): канал books."""
    inst_type = "SWAP" if market_type == "FUTURES" else "SPOT"
    suffix = "-USDT-SWAP" if market_type == "FUTURES" else "-USDT"
    max_syms = 50 if market_type == "FUTURES" else 40
    while True:
        local_wl_ver = _whitelist_version
        try:
            ct_vals: Dict[str, float] = {}
            if market_type == "FUTURES":
                async with session.get(
                    "https://www.okx.com/api/v5/public/instruments?instType=SWAP",
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    inst_data = await resp.json()
                for it in inst_data.get("data") or []:
                    iid = str(it.get("instId") or "")
                    if iid.endswith("-USDT-SWAP"):
                        ct_vals[iid] = _safe_float(it.get("ctVal"), 1.0)

            async with session.get(
                f"https://www.okx.com/api/v5/market/tickers?instType={inst_type}",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                tk_data = await resp.json()

            vols: Dict[str, float] = {}
            for it in tk_data.get("data") or []:
                iid = str(it.get("instId") or "")
                if not iid.endswith(suffix) or is_blacklisted_symbol(iid, "OKX"):
                    continue
                if market_type == "SPOT":
                    v = _safe_float(it.get("volCcy24h"))
                else:
                    v = _safe_float(it.get("volCcy24h")) * _safe_float(it.get("last"))
                is_whitelisted = extract_base_asset(iid) in _runtime_allowed_bases
                if v >= 150_000 or (is_whitelisted and v >= 10_000):
                    vols[iid] = v

            top_ids = _select_top_symbols(vols, max_syms)
            if not top_ids:
                await asyncio.sleep(15)
                continue

            books: Dict[str, Tuple[Dict[float, float], Dict[float, float]]] = {}
            started = time.time()
            async with session.ws_connect("wss://ws.okx.com:8443/ws/v5/public", heartbeat=20, timeout=15) as ws:
                for i in range(0, len(top_ids), 10):
                    args = [{"channel": "books", "instId": iid} for iid in top_ids[i : i + 10]]
                    await ws.send_json({"op": "subscribe", "args": args})
                    await asyncio.sleep(0.12)
                logger.info("[OKX %s Depth WS] Подписан на %s стаканов", market_type, len(top_ids))

                async def pinger() -> None:
                    while True:
                        await asyncio.sleep(20)
                        try:
                            await ws.send_str("ping")
                        except Exception:
                            break

                pt = asyncio.create_task(pinger())
                try:
                    async for msg in ws:
                        if time.time() - started > 1800 or _whitelist_version != local_wl_ver:
                            break
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                            continue
                        if msg.data == "pong":
                            continue
                        payload = json.loads(msg.data)
                        arg = payload.get("arg") or {}
                        if arg.get("channel") != "books":
                            continue
                        iid = str(arg.get("instId") or "")
                        arr = payload.get("data") or []
                        if not iid or not arr:
                            continue
                        d = arr[0]
                        ct = ct_vals.get(iid, 1.0)
                        action = payload.get("action")
                        if action == "snapshot" or iid not in books:
                            b_map = {_safe_float(lvl[0]): _safe_float(lvl[1]) * ct for lvl in (d.get("bids") or []) if _safe_float(lvl[1]) > 0}
                            a_map = {_safe_float(lvl[0]): _safe_float(lvl[1]) * ct for lvl in (d.get("asks") or []) if _safe_float(lvl[1]) > 0}
                            books[iid] = (b_map, a_map)
                        else:
                            b_map, a_map = books[iid]
                            for lvl in d.get("bids") or []:
                                px, qty = _safe_float(lvl[0]), _safe_float(lvl[1]) * ct
                                if qty <= 0:
                                    b_map.pop(px, None)
                                else:
                                    b_map[px] = qty
                            for lvl in d.get("asks") or []:
                                px, qty = _safe_float(lvl[0]), _safe_float(lvl[1]) * ct
                                if qty <= 0:
                                    a_map.pop(px, None)
                                else:
                                    a_map[px] = qty

                        b_map, a_map = books[iid]
                        bids_sorted = sorted(b_map.items(), key=lambda x: x[0], reverse=True)[:40]
                        asks_sorted = sorted(a_map.items(), key=lambda x: x[0])[:40]
                        if len(b_map) > 120:
                            books[iid] = (dict(bids_sorted), dict(asks_sorted))
                        sym_clean = iid.replace("-USDT-SWAP", "USDT").replace("-USDT", "USDT")
                        await eng.process_orderbook("OKX", sym_clean, bids_sorted, asks_sorted, vols.get(iid, 0.0), market_type=market_type)
                finally:
                    pt.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[OKX %s Depth WS] Переподключение через 5с: %s", market_type, exc)
            await asyncio.sleep(5)


async def bitget_depth_ws(session: aiohttp.ClientSession, eng: DensityEngine, market_type: str = "FUTURES") -> None:
    """Bitget USDT-FUTURES и SPOT: канал books15."""
    inst_type = "USDT-FUTURES" if market_type == "FUTURES" else "SPOT"
    ticker_url = (
        "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES"
        if market_type == "FUTURES"
        else "https://api.bitget.com/api/v2/spot/market/tickers"
    )
    max_syms = 70 if market_type == "FUTURES" else 50
    while True:
        local_wl_ver = _whitelist_version
        try:
            async with session.get(ticker_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()

            vols: Dict[str, float] = {}
            for it in data.get("data") or []:
                sym = str(it.get("symbol") or "")
                if not sym.endswith("USDT") or is_blacklisted_symbol(sym, "Bitget"):
                    continue
                v = _safe_float(it.get("usdtVolume"))
                is_whitelisted = extract_base_asset(sym) in _runtime_allowed_bases
                if v >= 150_000 or (is_whitelisted and v >= 10_000):
                    vols[sym] = v

            top_syms = _select_top_symbols(vols, max_syms)
            if not top_syms:
                await asyncio.sleep(15)
                continue

            started = time.time()
            async with session.ws_connect("wss://ws.bitget.com/v2/ws/public", heartbeat=20, timeout=15) as ws:
                for i in range(0, len(top_syms), 15):
                    args = [
                        {"instType": inst_type, "channel": "books15", "instId": s}
                        for s in top_syms[i : i + 15]
                    ]
                    await ws.send_json({"op": "subscribe", "args": args})
                    await asyncio.sleep(0.1)
                logger.info("[Bitget %s Depth WS] Подписан на %s стаканов", market_type, len(top_syms))

                async def pinger() -> None:
                    while True:
                        await asyncio.sleep(25)
                        try:
                            await ws.send_str("ping")
                        except Exception:
                            break

                pt = asyncio.create_task(pinger())
                try:
                    async for msg in ws:
                        if time.time() - started > 1800 or _whitelist_version != local_wl_ver:
                            break
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                            continue
                        if msg.data == "pong":
                            continue
                        payload = json.loads(msg.data)
                        arg = payload.get("arg") or {}
                        if arg.get("channel") != "books15":
                            continue
                        sym = str(arg.get("instId") or "")
                        arr = payload.get("data") or []
                        if not sym or not arr:
                            continue
                        d = arr[0]
                        bids = [(_safe_float(p), _safe_float(q)) for p, q in (d.get("bids") or [])]
                        asks = [(_safe_float(p), _safe_float(q)) for p, q in (d.get("asks") or [])]
                        await eng.process_orderbook("Bitget", sym, bids, asks, vols.get(sym, 0.0), market_type=market_type)
                finally:
                    pt.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[Bitget %s Depth WS] Переподключение через 5с: %s", market_type, exc)
            await asyncio.sleep(5)


async def gate_depth_ws(session: aiohttp.ClientSession, eng: DensityEngine, market_type: str = "FUTURES") -> None:
    """Gate.io USDT Perpetual (Фьючерсы) и Spot (Спот): 20-уровневые снимки стакана."""
    is_fut = market_type == "FUTURES"
    ws_url = "wss://fx-ws.gateio.ws/v4/ws/usdt" if is_fut else "wss://api.gateio.ws/ws/v4/"
    channel = "futures.order_book" if is_fut else "spot.order_book"
    ping_ch = "futures.ping" if is_fut else "spot.ping"
    max_syms = 50 if is_fut else 40
    while True:
        local_wl_ver = _whitelist_version
        try:
            q_mults: Dict[str, float] = {}
            if is_fut:
                async with session.get(
                    "https://api.gateio.ws/api/v4/futures/usdt/contracts",
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    c_data = await resp.json()
                if isinstance(c_data, list):
                    for it in c_data:
                        name = str(it.get("name") or "")
                        if name:
                            q_mults[name] = _safe_float(it.get("quanto_multiplier"), 1.0)

            tk_url = (
                "https://api.gateio.ws/api/v4/futures/usdt/tickers"
                if is_fut
                else "https://api.gateio.ws/api/v4/spot/tickers"
            )
            async with session.get(tk_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                tk_data = await resp.json()

            vols: Dict[str, float] = {}
            if isinstance(tk_data, list):
                for it in tk_data:
                    c = str(it.get("contract") if is_fut else it.get("currency_pair") or "")
                    if not c.endswith("_USDT") or is_blacklisted_symbol(c, "Gate"):
                        continue
                    v = (
                        _safe_float(it.get("volume_24h_quote")) or _safe_float(it.get("volume_24h_settle"))
                        if is_fut
                        else _safe_float(it.get("quote_volume"))
                    )
                    is_whitelisted = extract_base_asset(c) in _runtime_allowed_bases
                    if v >= 150_000 or (is_whitelisted and v >= 10_000):
                        vols[c] = v

            top_contracts = _select_top_symbols(vols, max_syms)
            if not top_contracts:
                await asyncio.sleep(15)
                continue

            started = time.time()
            async with session.ws_connect(ws_url, heartbeat=20, timeout=15) as ws:
                for c in top_contracts:
                    sub_payload = [c, "20", "0"] if is_fut else [c, "20", "1000ms"]
                    await ws.send_json(
                        {
                            "time": int(time.time()),
                            "channel": channel,
                            "event": "subscribe",
                            "payload": sub_payload,
                        }
                    )
                    await asyncio.sleep(0.03)
                logger.info("[Gate %s Depth WS] Подписан на %s стаканов", market_type, len(top_contracts))

                async def pinger() -> None:
                    while True:
                        await asyncio.sleep(20)
                        try:
                            await ws.send_json({"time": int(time.time()), "channel": ping_ch})
                        except Exception:
                            break

                pt = asyncio.create_task(pinger())
                try:
                    async for msg in ws:
                        if time.time() - started > 1800 or _whitelist_version != local_wl_ver:
                            break
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                            continue
                        payload = json.loads(msg.data)
                        if payload.get("channel") != channel:
                            continue
                        res = payload.get("result")
                        if not isinstance(res, dict):
                            continue
                        c = str(res.get("contract") if is_fut else res.get("s") or "")
                        if not c:
                            continue
                        bids_raw = res.get("bids") or []
                        asks_raw = res.get("asks") or []
                        if not bids_raw or not asks_raw:
                            continue
                        if is_fut:
                            mult = q_mults.get(c, 1.0)
                            bids = [(_safe_float(x.get("p")), abs(_safe_float(x.get("s"))) * mult) for x in bids_raw if isinstance(x, dict)]
                            asks = [(_safe_float(x.get("p")), abs(_safe_float(x.get("s"))) * mult) for x in asks_raw if isinstance(x, dict)]
                        else:
                            bids = [(_safe_float(x[0]), _safe_float(x[1])) for x in bids_raw if isinstance(x, list) and len(x) >= 2]
                            asks = [(_safe_float(x[0]), _safe_float(x[1])) for x in asks_raw if isinstance(x, list) and len(x) >= 2]
                        sym_clean = c.replace("_USDT", "USDT")
                        await eng.process_orderbook("Gate", sym_clean, bids, asks, vols.get(c, 0.0), market_type=market_type)
                finally:
                    pt.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[Gate %s Depth WS] Переподключение через 5с: %s", market_type, exc)
            await asyncio.sleep(5)


async def kucoin_depth_ws(session: aiohttp.ClientSession, eng: DensityEngine, market_type: str = "FUTURES") -> None:
    """KuCoin Futures (/contractMarket/level2Depth50) и Spot (/spotMarket/level2Depth50)."""
    is_fut = market_type == "FUTURES"
    bullet_url = (
        "https://api-futures.kucoin.com/api/v1/bullet-public"
        if is_fut
        else "https://api.kucoin.com/api/v1/bullet-public"
    )
    max_syms = 45 if is_fut else 40
    while True:
        local_wl_ver = _whitelist_version
        try:
            mults: Dict[str, float] = {}
            vols: Dict[str, float] = {}

            if is_fut:
                async with session.get(
                    "https://api-futures.kucoin.com/api/v1/contracts/active",
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    c_data = await resp.json()
                for it in (c_data or {}).get("data") or []:
                    sym = str(it.get("symbol") or "")
                    if not sym.endswith("USDTM") or str(it.get("status") or "") != "Open":
                        continue
                    if is_blacklisted_symbol(sym, "KuCoin"):
                        continue
                    v = _safe_float(it.get("turnoverOf24h"))
                    is_whitelisted = extract_base_asset(sym) in _runtime_allowed_bases
                    if v >= 150_000 or (is_whitelisted and v >= 10_000):
                        vols[sym] = v
                        mults[sym] = _safe_float(it.get("multiplier"), 1.0)
            else:
                async with session.get(
                    "https://api.kucoin.com/api/v1/market/allTickers",
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    tk_data = await resp.json()
                for it in ((tk_data or {}).get("data") or {}).get("ticker") or []:
                    sym = str(it.get("symbol") or "")
                    if not sym.endswith("-USDT") or is_blacklisted_symbol(sym, "KuCoin"):
                        continue
                    v = _safe_float(it.get("volValue"))
                    is_whitelisted = extract_base_asset(sym) in _runtime_allowed_bases
                    if v >= 150_000 or (is_whitelisted and v >= 10_000):
                        vols[sym] = v

            top_syms = _select_top_symbols(vols, max_syms)
            if not top_syms:
                await asyncio.sleep(15)
                continue

            # Получаем публичный токен и адрес WebSocket-сервера KuCoin
            async with session.post(bullet_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                b_data = await resp.json()
            b_res = (b_data or {}).get("data") or {}
            token = str(b_res.get("token") or "")
            servers = b_res.get("instanceServers") or []
            if not token or not servers:
                await asyncio.sleep(10)
                continue
            endpoint = str(servers[0].get("endpoint") or "").rstrip("/")
            ping_sec = max(10.0, _safe_float(servers[0].get("pingInterval"), 18000.0) / 1000.0 * 0.7)
            ws_url = f"{endpoint}?token={token}&connectId={int(time.time() * 1000)}"

            started = time.time()
            async with session.ws_connect(ws_url, heartbeat=25, timeout=15) as ws:
                if is_fut:
                    for idx, sym in enumerate(top_syms):
                        await ws.send_json(
                            {
                                "id": f"kf_{idx}",
                                "type": "subscribe",
                                "topic": f"/contractMarket/level2Depth50:{sym}",
                                "privateChannel": False,
                                "response": False,
                            }
                        )
                        await asyncio.sleep(0.04)
                else:
                    for i in range(0, len(top_syms), 10):
                        chunk = ",".join(top_syms[i : i + 10])
                        await ws.send_json(
                            {
                                "id": f"ks_{i}",
                                "type": "subscribe",
                                "topic": f"/spotMarket/level2Depth50:{chunk}",
                                "privateChannel": False,
                                "response": False,
                            }
                        )
                        await asyncio.sleep(0.08)

                logger.info("[KuCoin %s Depth WS] Подписан на %s стаканов", market_type, len(top_syms))

                async def pinger() -> None:
                    while True:
                        await asyncio.sleep(ping_sec)
                        try:
                            await ws.send_json({"id": str(int(time.time() * 1000)), "type": "ping"})
                        except Exception:
                            break

                pt = asyncio.create_task(pinger())
                try:
                    async for msg in ws:
                        if time.time() - started > 1800 or _whitelist_version != local_wl_ver:
                            break
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                            continue
                        payload = json.loads(msg.data)
                        if payload.get("type") != "message":
                            continue
                        topic = str(payload.get("topic") or "")
                        if "level2Depth50:" not in topic:
                            continue
                        raw_sym = topic.split("level2Depth50:", 1)[1]
                        d = payload.get("data")
                        if not isinstance(d, dict) or not raw_sym:
                            continue
                        mult = mults.get(raw_sym, 1.0) if is_fut else 1.0
                        bids = [(_safe_float(lvl[0]), _safe_float(lvl[1]) * mult) for lvl in (d.get("bids") or []) if len(lvl) >= 2]
                        asks = [(_safe_float(lvl[0]), _safe_float(lvl[1]) * mult) for lvl in (d.get("asks") or []) if len(lvl) >= 2]
                        if not bids or not asks:
                            continue
                        sym_clean = _normalize_pair_key(raw_sym)
                        await eng.process_orderbook("KuCoin", sym_clean, bids, asks, vols.get(raw_sym, 0.0), market_type=market_type)
                finally:
                    pt.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[KuCoin %s Depth WS] Переподключение через 6с: %s", market_type, exc)
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


# ==================== TELEGRAM BOT + РАССЫЛКА ====================

bot: Optional[Bot] = Bot(token=BOT_TOKEN) if BOT_TOKEN else None
dp = Dispatcher()

_users_cache: List[Dict[str, Any]] = []
_users_cache_ts: float = 0.0
_api_cache: Dict[str, Any] = {"utc_date": "", "history": [], "ts": 0.0}


def invalidate_users_cache() -> None:
    global _users_cache_ts
    _users_cache_ts = 0.0


async def get_cached_users(force: bool = False) -> List[Dict[str, Any]]:
    global _users_cache, _users_cache_ts
    now = time.time()
    if not force and _users_cache_ts > 0 and (now - _users_cache_ts) < 60.0:
        return _users_cache
    rows = await db_fetchall("SELECT user_id, config, discord_config, banned_coins, allowed_coins FROM users")
    _users_cache = rows
    _users_cache_ts = now
    return rows


async def sync_runtime_allowed_bases() -> None:
    """
    Обновляет глобальный белый список разблокированных монет (/allow и /watch)
    и переподключает WS при изменении.
    """
    global _whitelist_version
    rows = await db_fetchall("SELECT config, allowed_coins FROM users")
    merged: Set[str] = set()
    for r in rows:
        for coin in parse_banned_coins(r.get("allowed_coins")):
            merged.add(coin)
            merged.add(coin.replace(".", ""))
        ucfg = normalize_config(r.get("config"))
        for coin in ucfg.get("watch_coins") or []:
            merged.add(coin)
            merged.add(coin.replace(".", ""))
    if merged != _runtime_allowed_bases:
        _runtime_allowed_bases.clear()
        _runtime_allowed_bases.update(merged)
        _whitelist_version += 1
        logger.info("[Whitelist] Разблокированы/в отслеживании: %s", sorted(_runtime_allowed_bases) if _runtime_allowed_bases else "нет")


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
        parse_banned_coins(row.get("banned_coins")),
        parse_banned_coins(row.get("allowed_coins")),
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
    new_bans = bans if bans is not None else cur_bans
    new_allowed = allowed if allowed is not None else cur_allowed

    await db_execute(
        "INSERT INTO users (user_id, config, discord_config, banned_coins, allowed_coins) VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET "
        "config = excluded.config, discord_config = excluded.discord_config, "
        "banned_coins = excluded.banned_coins, allowed_coins = excluded.allowed_coins",
        (user_id, json.dumps(new_cfg), json.dumps(new_dcfg), json.dumps(new_bans), json.dumps(new_allowed)),
    )
    invalidate_users_cache()
    density_engine.update_config(new_cfg)
    if allowed is not None or cfg is not None:
        await sync_runtime_allowed_bases()
    return new_cfg, new_dcfg, new_bans, new_allowed


def _build_main_keyboard() -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    if WEBAPP_URL:
        rows.append([InlineKeyboardButton(text="🧱 Открыть Скринер (Mini App)", web_app=WebAppInfo(url=WEBAPP_URL))])
    rows.append(
        [
            InlineKeyboardButton(text="🧱 Активные сейчас", callback_data="menu:active"),
            InlineKeyboardButton(text="⚙️ Настройки", callback_data="menu:settings"),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_settings_keyboard(cfg: Dict[str, Any], dcfg: Dict[str, Any]) -> InlineKeyboardMarkup:
    en_txt = "🟢 Алерты: ВКЛ" if cfg["enabled"] else "🔴 Алерты: ВЫКЛ"
    side_txt = {"BOTH": "Сторона: BID + ASK", "BID": "Сторона: только BID", "ASK": "Сторона: только ASK"}[
        cfg["side_mode"]
    ]
    mkt_txt = {"BOTH": "Рынок: Фьюч + Спот", "FUTURES": "Рынок: Фьючерсы", "SPOT": "Рынок: Спот"}[
        cfg["market_mode"]
    ]
    dc_txt = "💬 Discord: ВКЛ" if dcfg["enabled"] else "💬 Discord: ВЫКЛ"
    rows: List[List[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(text=en_txt, callback_data="set:toggle_enabled"),
            InlineKeyboardButton(text=side_txt, callback_data="set:cycle_side"),
        ],
        [
            InlineKeyboardButton(text=mkt_txt, callback_data="set:cycle_market"),
            InlineKeyboardButton(text="🔓 Добавить из ЧС", callback_data="menu:allowlist"),
        ],
        [
            InlineKeyboardButton(text="⚡ Скальп (100K/200K)", callback_data="preset:scalp"),
            InlineKeyboardButton(text="⚖️ Стандарт (250K/500K)", callback_data="preset:standard"),
            InlineKeyboardButton(text="🐋 Кит (500K/1M)", callback_data="preset:whale"),
        ],
        [
            InlineKeyboardButton(text=dc_txt, callback_data="set:toggle_discord"),
            InlineKeyboardButton(text="🧪 Тест Discord", callback_data="set:test_discord"),
        ],
    ]
    if WEBAPP_URL:
        rows.append([InlineKeyboardButton(text="📊 Открыть Mini App", web_app=WebAppInfo(url=WEBAPP_URL))])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _build_allowlist_keyboard(allowed: List[str]) -> InlineKeyboardMarkup:
    allowed_set = set(allowed)
    rows: List[List[InlineKeyboardButton]] = []
    for i in range(0, len(QUICK_UNBLOCK_COINS), 3):
        row = []
        for coin in QUICK_UNBLOCK_COINS[i : i + 3]:
            mark = "✅ " if coin in allowed_set else "🚫 "
            row.append(InlineKeyboardButton(text=f"{mark}{coin}", callback_data=f"allow:{coin}"))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="⬅️ Назад в настройки", callback_data="menu:settings")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _format_settings_text(cfg: Dict[str, Any], dcfg: Dict[str, Any], bans: List[str], allowed: List[str]) -> str:
    side_ru = {"BOTH": "🟢 BID + 🔴 ASK", "BID": "🟢 Только BID (поддержка)", "ASK": "🔴 Только ASK (сопротивление)"}[
        cfg["side_mode"]
    ]
    mkt_ru = {"BOTH": "Фьючерсы + Спот", "FUTURES": "Только Фьючерсы", "SPOT": "Только Спот"}[cfg["market_mode"]]
    dc_url = resolve_discord_webhook(dcfg)
    dc_status = "✅ Подключён" if dc_url else "❌ Не задан"
    if dc_url and not dcfg.get("webhook_url"):
        dc_status += " (из Render Env)"
    bans_txt = ", ".join(bans[:15]) if bans else "нет"
    allow_txt = ", ".join(allowed[:15]) if allowed else "нет"
    watch_coins = cfg.get("watch_coins") or []
    watch_txt = ", ".join(watch_coins[:15]) if watch_coins else "ВЕСЬ РЫНОК"
    return (
        "⚙️ <b>Настройки скринера аномальных плотностей</b>\n\n"
        f"• Статус алертов: <b>{'ВКЛ' if cfg['enabled'] else 'ВЫКЛ'}</b>\n"
        f"• Рынок: <b>{mkt_ru}</b> (<code>/market both|futures|spot</code>)\n"
        f"• Мин. плотность <b>СПОТ</b>: <b>${cfg['min_wall_spot_usd']:,.0f}</b> (<code>/minspot 250000</code>)\n"
        f"• Мин. плотность <b>ФЬЮЧЕРСЫ</b>: <b>${cfg['min_wall_futures_usd']:,.0f}</b> (<code>/minfut 500000</code>)\n"
        f"• Макс. дистанция: <b>{cfg['max_distance_pct']:g}%</b> (<code>/dist 2.5</code>)\n"
        f"• Мин. аномальность: <b>x{cfg['min_anomaly_x']:g}</b> к стакану (<code>/anom 5</code>)\n"
        f"• Мин. время жизни: <b>{cfg['min_lifetime_sec']:g} сек</b> (<code>/life 15</code>)\n"
        f"• Мин. оборот 24ч: <b>${cfg['min_vol24h']:,.0f}</b> (<code>/minvol 500000</code>)\n"
        f"• Кулдаун повтора: <b>{cfg['cooldown_sec']:g} сек</b> (<code>/cooldown 300</code>)\n"
        f"• Сторона: <b>{side_ru}</b> (<code>/side both|bid|ask</code>)\n"
        f"• Биржи (7): <b>{', '.join(cfg['exchanges'])}</b>\n"
        f"• 🎯 Фильтр монет: <code>{watch_txt}</code> (<code>/watch COIN</code> | <code>/watch clear</code>)\n"
        f"• 🔎 Живой поиск по монете: <code>/find SOL</code> или <code>/find SOL 150000</code>\n"
        f"• Discord Webhook: <b>{dc_status}</b> (<code>/discord URL</code>)\n"
        f"• 🔓 Разблокированы из ЧС: <code>{allow_txt}</code> (<code>/allow BTC SOL</code>)\n"
        f"• 🚫 Личный бан-лист: <code>{bans_txt}</code> (<code>/ban COIN</code>)"
    )


async def _broadcast_telegram(user_ids: List[int], text: str) -> None:
    if bot is None or not user_ids:
        return

    async def _send(uid: int) -> None:
        try:
            await bot.send_message(uid, text, parse_mode="HTML", reply_markup=_build_main_keyboard())
        except Exception as exc:
            logger.debug("Ошибка Telegram (%s): %s", uid, exc)

    for i in range(0, len(user_ids), 20):
        await asyncio.gather(*(_send(uid) for uid in user_ids[i : i + 20]))
        if i + 20 < len(user_ids):
            await asyncio.sleep(0.05)


async def handle_density_event(ev: DensityEvent) -> None:
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    is_bid = ev.side == WallSide.BID
    emoji = "🟢" if is_bid else "🔴"
    side_ru = "BID (Поддержка)" if is_bid else "ASK (Сопротивление)"
    mkt_ru = "Спот" if ev.market_type == "SPOT" else "Фьючерс"
    text = (
        f"🧱 <b>ПЛОТНОСТЬ | {ev.exchange} ({mkt_ru})</b>\n"
        f"Монета: <code>{ev.symbol}</code> — {emoji} <b>{side_ru}</b>\n"
        f"Объём стены: <b>${ev.wall_usd:,.0f}</b> (<b>x{ev.anomaly_x:.1f}</b> к стакану · ~{ev.eat_minutes:.1f} мин)\n"
        f"Цена стены: <code>{ev.wall_price:g}</code> | До цены: <b>{ev.distance_pct:.2f}%</b> (<code>{ev.current_price:g}</code>)\n"
        f"Стоит: <b>{ev.lifetime_sec:.0f}с</b> | 24ч: <b>${ev.volume_24h_usd:,.0f}</b>\n"
        f"Время (UTC): {ev.timestamp_utc}"
    )

    users = await get_cached_users()
    base = extract_base_asset(ev.symbol)
    tg_targets: List[int] = []
    dc_urls: List[str] = []

    for u in users:
        uid = int(u["user_id"])
        if uid <= 0:
            continue
        ucfg = normalize_config(u.get("config"))
        if not ucfg.get("enabled", True):
            continue
        ubans = parse_banned_coins(u.get("banned_coins"))
        if base in ubans:
            continue
        uallowed = set(parse_banned_coins(u.get("allowed_coins"))) | set(ucfg.get("watch_coins") or [])
        if is_blacklisted_symbol(ev.symbol, ev.exchange, allowed_bases=uallowed):
            continue
        if wall_matches_config(
            ev.exchange,
            ev.side.value,
            ev.wall_usd,
            ev.distance_pct,
            ev.anomaly_x,
            ev.lifetime_sec,
            ev.volume_24h_usd,
            ucfg,
            market_type=ev.market_type,
            symbol=ev.symbol,
        ):
            tg_targets.append(uid)
            dcfg = normalize_discord_config(u.get("discord_config"))
            wurl = resolve_discord_webhook(dcfg)
            if wurl and wurl not in dc_urls:
                dc_urls.append(wurl)

    # Если пользователей в БД ещё нет, но задан DISCORD_WEBHOOK_URL в Render Environment
    if not users and is_valid_discord_webhook(ENV_DISCORD_WEBHOOK):
        dc_urls.append(ENV_DISCORD_WEBHOOK)

    # Мгновенная параллельная отправка в Discord и Telegram до записи в БД
    for wurl in dc_urls:
        asyncio.create_task(send_density_discord(wurl, ev, session=_http_session))
    if tg_targets:
        asyncio.create_task(_broadcast_telegram(tg_targets, text))

    await db_execute(
        "INSERT INTO densities (utc_date, utc_time, exchange, market_type, symbol, side, wall_price, current_price, "
        "distance_pct, wall_usd, median_level_usd, anomaly_x, lifetime_sec, eat_minutes, volume_24h_usd) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            today_utc,
            ev.timestamp_utc,
            ev.exchange,
            ev.market_type,
            ev.symbol,
            ev.side.value,
            ev.wall_price,
            ev.current_price,
            ev.distance_pct,
            ev.wall_usd,
            ev.median_level_usd,
            ev.anomaly_x,
            ev.lifetime_sec,
            ev.eat_minutes,
            ev.volume_24h_usd,
        ),
    )
    _api_cache["ts"] = 0.0


density_engine = DensityEngine(on_density=handle_density_event)


# ==================== КОМАНДЫ TELEGRAM-БОТА ====================

@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    if not message.from_user:
        return
    await db_execute("INSERT INTO users (user_id) VALUES (?) ON CONFLICT(user_id) DO NOTHING", (message.from_user.id,))
    invalidate_users_cache()
    text = (
        "🧱 <b>Скринер аномальных плотностей в стакане (7 бирж · Фьючерсы + Спот · WS)</b>\n\n"
        "Биржи: <b>Binance, Bybit, OKX, Bitget, Gate, KuCoin, Aster</b>.\n"
        "Тяжёлые монеты, акции/RWA, ETF, металлы, нефть и стейблкоины заблокированы по умолчанию, "
        "но любую из них можно разблокировать командой <code>/allow</code> или кнопкой в <code>/settings</code>.\n\n"
        "<b>Команды настройки прямо в боте:</b>\n"
        "/settings — панель настроек, пресеты и разблокировка монет из ЧС\n"
        "/active — активные аномальные плотности прямо сейчас\n"
        "/find SOL [сумма] — мгновенно найти плотности по отдельной монете (7 бирж, Спот+Фьюч)\n"
        "/watch SOL WIF — отслеживать только выбранные монеты (/watch clear — весь рынок)\n"
        "/unwatch SOL — убрать монету из фильтра отслеживания\n"
        "/market both|futures|spot — Фьючерсы + Спот, только Фьючерсы или только Спот\n"
        "/minspot 250000 — мин. плотность на СПОТЕ ($)\n"
        "/minfut 500000 — мин. плотность на ФЬЮЧЕРСАХ ($)\n"
        "/minwall 250000 500000 — задать Спот и Фьючерсы одной командой\n"
        "/dist 2.5 — макс. дистанция от цены (%)\n"
        "/anom 5 — мин. кратность к медианному уровню (x)\n"
        "/life 15 — мин. время удержания в стакане (сек, анти-спуфинг)\n"
        "/minvol 500000 — мин. суточный оборот ($)\n"
        "/cooldown 300 — пауза повтора (сек)\n"
        "/side both|bid|ask — фильтр стороны\n"
        "/exchanges Binance,Bybit,OKX,Bitget,Gate,KuCoin,Aster — выбор бирж\n"
        "/allow BTC SOL XAU — разблокировать монеты из чёрного списка\n"
        "/unallow BTC — вернуть монету обратно в чёрный список\n"
        "/discord https://discord.com/api/webhooks/... — подключить Discord\n"
        "/discord test | on | off — тест или вкл/выкл Discord\n"
        "/ban COIN / /unban COIN — скрыть или вернуть монету"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=_build_main_keyboard())


@dp.message(Command("settings"))
async def cmd_settings(message: types.Message) -> None:
    if not message.from_user:
        return
    cfg, dcfg, bans, allowed = await get_user_record(message.from_user.id)
    await message.answer(
        _format_settings_text(cfg, dcfg, bans, allowed),
        parse_mode="HTML",
        reply_markup=_build_settings_keyboard(cfg, dcfg),
    )


@dp.message(Command("active"))
async def cmd_active(message: types.Message) -> None:
    walls = density_engine.get_active_walls()
    if not walls:
        await message.answer("🧱 Сейчас активных аномальных плотностей нет.", reply_markup=_build_main_keyboard())
        return
    lines = ["🧱 <b>Активные аномальные плотности сейчас:</b>\n"]
    for w in walls[:15]:
        em = "🟢" if w.side == WallSide.BID else "🔴"
        mt = "Спот" if w.market_type == "SPOT" else "Фьюч"
        lines.append(
            f"• <b>{w.exchange}</b> ({mt}) | <code>{w.symbol}</code> {em} <b>{w.side.value}</b> — "
            f"<b>${w.wall_usd:,.0f}</b> (x{w.anomaly_x:.1f}, {w.distance_pct:.2f}%, {w.lifetime_sec:.0f}с)"
        )
    await message.answer("\n".join(lines), parse_mode="HTML", reply_markup=_build_main_keyboard())


@dp.message(Command("market"))
async def cmd_market(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split()
    arg = parts[1].strip().upper() if len(parts) > 1 else ""
    if arg not in ("BOTH", "FUTURES", "SPOT"):
        await message.answer(
            "Использование: <code>/market both</code>, <code>/market futures</code> или <code>/market spot</code>",
            parse_mode="HTML",
        )
        return
    cfg, _, _, _ = await get_user_record(message.from_user.id)
    cfg["market_mode"] = arg
    await save_user_record(message.from_user.id, cfg=cfg)
    lbl = {"BOTH": "Фьючерсы + Спот", "FUTURES": "Только Фьючерсы", "SPOT": "Только Спот"}[arg]
    await message.answer(f"✅ Режим рынка: <b>{lbl}</b>", parse_mode="HTML")


@dp.message(Command("allow"))
async def cmd_allow(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split(maxsplit=1)
    _, _, bans, allowed = await get_user_record(message.from_user.id)
    if len(parts) < 2:
        cur = ", ".join(allowed) if allowed else "нет"
        await message.answer(
            f"🔓 <b>Разблокированные монеты из ЧС:</b> <code>{cur}</code>\n\n"
            "Пример: <code>/allow BTC SOL XAU</code> — добавить в скринер\n"
            "<code>/unallow BTC</code> — вернуть обратно в чёрный список",
            parse_mode="HTML",
            reply_markup=_build_allowlist_keyboard(allowed),
        )
        return
    tokens = [extract_base_asset(x.strip()) for x in re.split(r"[,\s]+", parts[1]) if x.strip()]
    tokens = [t for t in tokens if t]
    if not tokens:
        await message.answer("Укажите тикеры, например: <code>/allow BTC SOL</code>", parse_mode="HTML")
        return
    for t in tokens:
        if t not in allowed:
            allowed.append(t)
    bans = [b for b in bans if b not in tokens]
    await save_user_record(message.from_user.id, bans=bans, allowed=allowed)
    await message.answer(
        f"🔓 Разблокированы из чёрного списка: <b>{', '.join(tokens)}</b>\n"
        "<i>WebSocket-сканеры подключат их автоматически через несколько секунд.</i>",
        parse_mode="HTML",
    )


@dp.message(Command("unallow"))
async def cmd_unallow(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Пример: <code>/unallow BTC</code>", parse_mode="HTML")
        return
    tokens = [extract_base_asset(x.strip()) for x in re.split(r"[,\s]+", parts[1]) if x.strip()]
    tokens = [t for t in tokens if t]
    _, _, _, allowed = await get_user_record(message.from_user.id)
    allowed = [c for c in allowed if c not in tokens]
    await save_user_record(message.from_user.id, allowed=allowed)
    await message.answer(f"🔒 Возвращены в чёрный список: <b>{', '.join(tokens)}</b>", parse_mode="HTML")


@dp.message(Command("minspot"))
async def cmd_minspot(message: types.Message) -> None:
    if not message.from_user:
        return
    try:
        val = max(10_000.0, float((message.text or "").split()[1]))
        cfg, _, _, _ = await get_user_record(message.from_user.id)
        cfg["min_wall_spot_usd"] = val
        cfg, _, _, _ = await save_user_record(message.from_user.id, cfg=cfg)
        await message.answer(f"✅ Мин. плотность на <b>СПОТЕ</b>: <b>${cfg['min_wall_spot_usd']:,.0f}</b>", parse_mode="HTML")
    except Exception:
        await message.answer("Пример: <code>/minspot 250000</code>", parse_mode="HTML")


@dp.message(Command("minfut"))
async def cmd_minfut(message: types.Message) -> None:
    if not message.from_user:
        return
    try:
        val = max(10_000.0, float((message.text or "").split()[1]))
        cfg, _, _, _ = await get_user_record(message.from_user.id)
        cfg["min_wall_futures_usd"] = val
        cfg, _, _, _ = await save_user_record(message.from_user.id, cfg=cfg)
        await message.answer(f"✅ Мин. плотность на <b>ФЬЮЧЕРСАХ</b>: <b>${cfg['min_wall_futures_usd']:,.0f}</b>", parse_mode="HTML")
    except Exception:
        await message.answer("Пример: <code>/minfut 500000</code>", parse_mode="HTML")


@dp.message(Command("minwall"))
async def cmd_minwall(message: types.Message) -> None:
    if not message.from_user:
        return
    try:
        parts = (message.text or "").split()
        if len(parts) >= 3:
            spot_val = max(10_000.0, float(parts[1]))
            fut_val = max(10_000.0, float(parts[2]))
        else:
            spot_val = max(10_000.0, float(parts[1]))
            fut_val = spot_val
        cfg, _, _, _ = await get_user_record(message.from_user.id)
        cfg["min_wall_spot_usd"] = spot_val
        cfg["min_wall_futures_usd"] = fut_val
        cfg, _, _, _ = await save_user_record(message.from_user.id, cfg=cfg)
        await message.answer(
            f"✅ Мин. плотность обновлена:\n"
            f"• Спот: <b>${cfg['min_wall_spot_usd']:,.0f}</b>\n"
            f"• Фьючерсы: <b>${cfg['min_wall_futures_usd']:,.0f}</b>",
            parse_mode="HTML",
        )
    except Exception:
        await message.answer(
            "Использование:\n"
            "<code>/minspot 250000</code> — только Спот\n"
            "<code>/minfut 500000</code> — только Фьючерсы\n"
            "<code>/minwall 250000 500000</code> — Спот и Фьючерсы сразу",
            parse_mode="HTML",
        )


@dp.message(Command("watch"))
async def cmd_watch(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split(maxsplit=1)
    cfg, _, bans, _ = await get_user_record(message.from_user.id)
    if len(parts) < 2:
        cur = ", ".join(cfg["watch_coins"]) if cfg["watch_coins"] else "ВЕСЬ РЫНОК"
        await message.answer(
            f"🎯 <b>Фильтр по отдельным монетам</b>\n"
            f"Сейчас отслеживаются: <code>{cur}</code>\n\n"
            "Добавить монеты (будут приходить алерты только по ним):\n"
            "<code>/watch SOL WIF RIVER</code>\n"
            "Убрать одну монету: <code>/unwatch SOL</code>\n"
            "Сбросить и снова сканировать весь рынок: <code>/watch clear</code>",
            parse_mode="HTML",
        )
        return

    arg = parts[1].strip()
    if arg.lower() in ("clear", "all", "reset", "off"):
        cfg["watch_coins"] = []
        await save_user_record(message.from_user.id, cfg=cfg)
        await message.answer("✅ Фильтр монет сброшен — скринер снова сканирует <b>весь рынок</b>.", parse_mode="HTML")
        return

    tokens = [extract_base_asset(x.strip()) for x in re.split(r"[,\s]+", arg) if x.strip()]
    tokens = [t for t in tokens if t]
    if not tokens:
        await message.answer("Укажите тикеры, например: <code>/watch SOL WIF</code>", parse_mode="HTML")
        return

    cur_watch = list(cfg.get("watch_coins") or [])
    for t in tokens:
        if t not in cur_watch:
            cur_watch.append(t)
    bans = [b for b in bans if b not in tokens]
    cfg["watch_coins"] = cur_watch
    await save_user_record(message.from_user.id, cfg=cfg, bans=bans)
    await message.answer(
        f"🎯 Теперь алерты приходят только по монетам: <b>{', '.join(cur_watch)}</b>\n"
        "<i>(Если среди них были монеты из ЧС — они автоматически разблокированы и подключатся через пару секунд.)</i>\n"
        "Вернуть весь рынок: <code>/watch clear</code>",
        parse_mode="HTML",
    )


@dp.message(Command("unwatch"))
async def cmd_unwatch(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Пример: <code>/unwatch SOL</code>", parse_mode="HTML")
        return
    tokens = [extract_base_asset(x.strip()) for x in re.split(r"[,\s]+", parts[1]) if x.strip()]
    tokens = [t for t in tokens if t]
    cfg, _, _, _ = await get_user_record(message.from_user.id)
    cfg["watch_coins"] = [c for c in (cfg.get("watch_coins") or []) if c not in tokens]
    await save_user_record(message.from_user.id, cfg=cfg)
    left = ", ".join(cfg["watch_coins"]) if cfg["watch_coins"] else "ВЕСЬ РЫНОК"
    await message.answer(f"✅ Обновлённый список отслеживания: <b>{left}</b>", parse_mode="HTML")


@dp.message(Command("find", "search"))
async def cmd_find(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer(
            "🔎 <b>Живой поиск плотностей по отдельной монете (7 бирж · Спот + Фьючерсы)</b>\n\n"
            "Примеры:\n"
            "<code>/find SOL</code> — искать по вашим текущим фильтрам (Спот / Фьючерсы)\n"
            "<code>/find SOL 150000</code> — временно искать плотности от $150,000\n"
            "<code>/find SOL 100000 300000</code> — Спот от $100K, Фьючерсы от $300K",
            parse_mode="HTML",
        )
        return

    coin = parts[1].strip()
    ov_spot: Optional[float] = None
    ov_fut: Optional[float] = None
    if len(parts) >= 4:
        ov_spot = _safe_float(parts[2], 0.0) or None
        ov_fut = _safe_float(parts[3], 0.0) or None
    elif len(parts) == 3:
        v = _safe_float(parts[2], 0.0) or None
        ov_spot = v
        ov_fut = v

    cfg, _, _, _ = await get_user_record(message.from_user.id)
    wait_msg = await message.answer(f"🔎 Сканирую живые стаканы <code>{extract_base_asset(coin)}</code> на 7 биржах...", parse_mode="HTML")
    res = await live_search_coin_densities(
        _http_session, coin, cfg, override_min_spot=ov_spot, override_min_fut=ov_fut
    )
    base = res["coin"]
    if not base:
        await wait_msg.edit_text("❌ Некорректный тикер монеты.")
        return

    matches = res["matches"]
    top_seen = res["top_seen"]
    header = (
        f"🔎 <b>Стаканы {base}USDT</b> (проверено: {res['checked_books']} стаканов)\n"
        f"Фильтр: Спот ≥ <b>${res['min_wall_spot_usd']:,.0f}</b> · Фьюч ≥ <b>${res['min_wall_futures_usd']:,.0f}</b> · до <b>{res['max_distance_pct']:g}%</b>\n\n"
    )
    if matches:
        lines = [header + "<b>Найденные аномальные плотности:</b>"]
        for m in matches[:15]:
            em = "🟢" if m["side"] == "BID" else "🔴"
            mt = "Спот" if m["market_type"] == "SPOT" else "Фьюч"
            lines.append(
                f"• <b>{m['exchange']}</b> ({mt}) {em} <b>{m['side']}</b> — "
                f"<b>${m['wall_usd']:,.0f}</b> (x{m['anomaly_x']}) @ <code>{m['wall_price']:g}</code> ({m['distance_pct']}%)"
            )
    else:
        lines = [header + "По текущему фильтру плотностей сейчас нет."]
        if top_seen:
            lines.append("\n<b>Крупнейшие стенки в стаканах прямо сейчас:</b>")
            for m in top_seen[:6]:
                em = "🟢" if m["side"] == "BID" else "🔴"
                mt = "Спот" if m["market_type"] == "SPOT" else "Фьюч"
                lines.append(
                    f"• <b>{m['exchange']}</b> ({mt}) {em} {m['side']} — "
                    f"<b>${m['wall_usd']:,.0f}</b> (x{m['anomaly_x']}) @ <code>{m['wall_price']:g}</code> ({m['distance_pct']}%)"
                )
    await wait_msg.edit_text("\n".join(lines), parse_mode="HTML")


@dp.message(Command("dist"))
async def cmd_dist(message: types.Message) -> None:
    if not message.from_user:
        return
    try:
        val = float((message.text or "").split()[1])
        cfg, _, _, _ = await get_user_record(message.from_user.id)
        cfg["max_distance_pct"] = val
        cfg, _, _, _ = await save_user_record(message.from_user.id, cfg=cfg)
        await message.answer(f"✅ Макс. дистанция до цены: <b>{cfg['max_distance_pct']:g}%</b>", parse_mode="HTML")
    except Exception:
        await message.answer("Пример: <code>/dist 2.5</code>", parse_mode="HTML")


@dp.message(Command("anom"))
async def cmd_anom(message: types.Message) -> None:
    if not message.from_user:
        return
    try:
        val = float((message.text or "").split()[1])
        cfg, _, _, _ = await get_user_record(message.from_user.id)
        cfg["min_anomaly_x"] = val
        cfg, _, _, _ = await save_user_record(message.from_user.id, cfg=cfg)
        await message.answer(f"✅ Мин. аномальность: <b>x{cfg['min_anomaly_x']:g}</b> к стакану", parse_mode="HTML")
    except Exception:
        await message.answer("Пример: <code>/anom 5</code>", parse_mode="HTML")


@dp.message(Command("life"))
async def cmd_life(message: types.Message) -> None:
    if not message.from_user:
        return
    try:
        val = float((message.text or "").split()[1])
        cfg, _, _, _ = await get_user_record(message.from_user.id)
        cfg["min_lifetime_sec"] = val
        cfg, _, _, _ = await save_user_record(message.from_user.id, cfg=cfg)
        await message.answer(f"✅ Мин. время удержания: <b>{cfg['min_lifetime_sec']:g} сек</b>", parse_mode="HTML")
    except Exception:
        await message.answer("Пример: <code>/life 15</code>", parse_mode="HTML")


@dp.message(Command("minvol"))
async def cmd_minvol(message: types.Message) -> None:
    if not message.from_user:
        return
    try:
        val = float((message.text or "").split()[1])
        cfg, _, _, _ = await get_user_record(message.from_user.id)
        cfg["min_vol24h"] = val
        cfg, _, _, _ = await save_user_record(message.from_user.id, cfg=cfg)
        await message.answer(f"✅ Мин. оборот 24ч: <b>${cfg['min_vol24h']:,.0f}</b>", parse_mode="HTML")
    except Exception:
        await message.answer("Пример: <code>/minvol 500000</code>", parse_mode="HTML")


@dp.message(Command("cooldown"))
async def cmd_cooldown(message: types.Message) -> None:
    if not message.from_user:
        return
    try:
        val = float((message.text or "").split()[1])
        cfg, _, _, _ = await get_user_record(message.from_user.id)
        cfg["cooldown_sec"] = val
        cfg, _, _, _ = await save_user_record(message.from_user.id, cfg=cfg)
        await message.answer(f"✅ Кулдаун повтора: <b>{cfg['cooldown_sec']:g} сек</b>", parse_mode="HTML")
    except Exception:
        await message.answer("Пример: <code>/cooldown 300</code>", parse_mode="HTML")


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
    await message.answer(f"✅ Фильтр стороны: <b>{arg}</b>", parse_mode="HTML")


@dp.message(Command("exchanges"))
async def cmd_exchanges(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer(
            "Пример: <code>/exchanges Binance,Bybit,OKX,Bitget,Gate,KuCoin,Aster</code>",
            parse_mode="HTML",
        )
        return
    raw_items = [x.strip() for x in re.split(r"[,\s]+", parts[1]) if x.strip()]
    lookup = {e.lower(): e for e in ALL_EXCHANGES}
    chosen = [lookup[x.lower()] for x in raw_items if x.lower() in lookup]
    chosen = list(dict.fromkeys(chosen))
    if not chosen:
        await message.answer(f"Доступные биржи: <code>{', '.join(ALL_EXCHANGES)}</code>", parse_mode="HTML")
        return
    cfg, _, _, _ = await get_user_record(message.from_user.id)
    cfg["exchanges"] = chosen
    await save_user_record(message.from_user.id, cfg=cfg)
    await message.answer(f"✅ Активные биржи: <b>{', '.join(chosen)}</b>", parse_mode="HTML")


@dp.message(Command("discord"))
async def cmd_discord(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer(
            "Использование:\n"
            "<code>/discord https://discord.com/api/webhooks/...</code> — подключить канал\n"
            "<code>/discord test</code> — отправить тест\n"
            "<code>/discord on</code> / <code>/discord off</code> — вкл/выкл\n"
            "<code>/discord clear</code> — удалить вебхук",
            parse_mode="HTML",
        )
        return

    arg = parts[1].strip()
    _, dcfg, _, _ = await get_user_record(message.from_user.id)
    low = arg.lower()

    if low in ("on", "off"):
        dcfg["enabled"] = low == "on"
        await save_user_record(message.from_user.id, dcfg=dcfg)
        await message.answer(f"💬 Discord: <b>{'ВКЛЮЧЁН' if dcfg['enabled'] else 'ВЫКЛЮЧЕН'}</b>", parse_mode="HTML")
        return

    if low == "clear":
        dcfg["webhook_url"] = ""
        await save_user_record(message.from_user.id, dcfg=dcfg)
        await message.answer("🗑 Пользовательский Discord Webhook очищен.")
        return

    if low == "test":
        wurl = resolve_discord_webhook(dcfg)
        if not wurl:
            await message.answer("❌ Сначала задайте Webhook: <code>/discord https://discord.com/api/webhooks/...</code>", parse_mode="HTML")
            return
        ok, err = await send_test_discord(wurl, session=_http_session)
        await message.answer("✅ Тест отправлен в Discord!" if ok else f"❌ Ошибка Discord: {err}")
        return

    if not is_valid_discord_webhook(arg):
        await message.answer("❌ Некорректный URL. Ссылка должна начинаться с <code>https://discord.com/api/webhooks/</code>", parse_mode="HTML")
        return

    dcfg["webhook_url"] = arg
    dcfg["enabled"] = True
    await save_user_record(message.from_user.id, dcfg=dcfg)
    ok, err = await send_test_discord(arg, session=_http_session)
    if ok:
        await message.answer("✅ Discord Webhook сохранён, тестовое сообщение успешно отправлено в канал!")
    else:
        await message.answer(f"⚠️ Webhook сохранён, но тест вернул ошибку: {err}")


@dp.message(Command("ban"))
async def cmd_ban(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Пример: <code>/ban RIVER</code>", parse_mode="HTML")
        return
    base = extract_base_asset(parts[1])
    if not base:
        await message.answer("Некорректный тикер.")
        return
    _, _, bans, allowed = await get_user_record(message.from_user.id)
    if base not in bans:
        bans.append(base)
    if base in allowed:
        allowed = [c for c in allowed if c != base]
    await save_user_record(message.from_user.id, bans=bans, allowed=allowed)
    await message.answer(f"🚫 Монета <b>{base}</b> добавлена в личный чёрный список.", parse_mode="HTML")


@dp.message(Command("unban"))
async def cmd_unban(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer("Пример: <code>/unban RIVER</code>", parse_mode="HTML")
        return
    base = extract_base_asset(parts[1])
    _, _, bans, _ = await get_user_record(message.from_user.id)
    bans = [b for b in bans if b != base]
    await save_user_record(message.from_user.id, bans=bans)
    await message.answer(f"✅ Монета <b>{base}</b> возвращена в скринер.", parse_mode="HTML")


@dp.callback_query(F.data.startswith(("menu:", "set:", "preset:", "allow:")))
async def cb_handler(query: CallbackQuery) -> None:
    if not query.from_user or not query.message:
        return
    uid = query.from_user.id
    data = query.data or ""
    cfg, dcfg, bans, allowed = await get_user_record(uid)

    if data == "menu:active":
        walls = density_engine.get_active_walls()
        if not walls:
            await query.answer("Сейчас активных плотностей нет", show_alert=False)
            return
        lines = ["🧱 <b>Активные аномальные плотности сейчас:</b>\n"]
        for w in walls[:12]:
            em = "🟢" if w.side == WallSide.BID else "🔴"
            mt = "Спот" if w.market_type == "SPOT" else "Фьюч"
            lines.append(
                f"• <b>{w.exchange}</b> ({mt}) | <code>{w.symbol}</code> {em} <b>{w.side.value}</b> — "
                f"<b>${w.wall_usd:,.0f}</b> (x{w.anomaly_x:.1f}, {w.distance_pct:.2f}%)"
            )
        await query.message.answer("\n".join(lines), parse_mode="HTML")
        await query.answer()
        return

    if data == "menu:allowlist":
        cur = ", ".join(allowed) if allowed else "нет"
        try:
            await query.message.edit_text(
                "🔓 <b>Разблокировать монеты из чёрного списка</b>\n"
                f"Сейчас добавлены: <code>{cur}</code>\n\n"
                "Нажмите на монету ниже, чтобы включить (✅) или вернуть в ЧС (🚫).\n"
                "Для любого другого тикера используйте <code>/allow TICKER</code>.",
                parse_mode="HTML",
                reply_markup=_build_allowlist_keyboard(allowed),
            )
        except Exception:
            await query.message.answer(
                "🔓 <b>Разблокировать монеты из чёрного списка</b>",
                parse_mode="HTML",
                reply_markup=_build_allowlist_keyboard(allowed),
            )
        await query.answer()
        return

    if data.startswith("allow:"):
        coin = extract_base_asset(data.split(":", 1)[1])
        if coin:
            if coin in allowed:
                allowed = [c for c in allowed if c != coin]
                msg_toast = f"🔒 {coin} возвращена в ЧС"
            else:
                allowed.append(coin)
                bans = [b for b in bans if b != coin]
                msg_toast = f"🔓 {coin} разблокирована!"
            _, _, bans, allowed = await save_user_record(uid, bans=bans, allowed=allowed)
            cur = ", ".join(allowed) if allowed else "нет"
            try:
                await query.message.edit_text(
                    "🔓 <b>Разблокировать монеты из чёрного списка</b>\n"
                    f"Сейчас добавлены: <code>{cur}</code>\n\n"
                    "Нажмите на монету ниже, чтобы включить (✅) или вернуть в ЧС (🚫).\n"
                    "Для любого другого тикера используйте <code>/allow TICKER</code>.",
                    parse_mode="HTML",
                    reply_markup=_build_allowlist_keyboard(allowed),
                )
            except Exception:
                pass
            await query.answer(msg_toast)
        return

    if data == "menu:settings":
        try:
            await query.message.edit_text(
                _format_settings_text(cfg, dcfg, bans, allowed),
                parse_mode="HTML",
                reply_markup=_build_settings_keyboard(cfg, dcfg),
            )
        except Exception:
            await query.message.answer(
                _format_settings_text(cfg, dcfg, bans, allowed),
                parse_mode="HTML",
                reply_markup=_build_settings_keyboard(cfg, dcfg),
            )
        await query.answer()
        return

    if data == "set:toggle_enabled":
        cfg["enabled"] = not cfg["enabled"]
        cfg, dcfg, bans, allowed = await save_user_record(uid, cfg=cfg)
        await query.answer("Статус алертов изменён")
    elif data == "set:cycle_side":
        order = ["BOTH", "BID", "ASK"]
        cfg["side_mode"] = order[(order.index(cfg["side_mode"]) + 1) % len(order)]
        cfg, dcfg, bans, allowed = await save_user_record(uid, cfg=cfg)
        await query.answer(f"Сторона: {cfg['side_mode']}")
    elif data == "set:cycle_market":
        order = ["BOTH", "FUTURES", "SPOT"]
        cfg["market_mode"] = order[(order.index(cfg["market_mode"]) + 1) % len(order)]
        cfg, dcfg, bans, allowed = await save_user_record(uid, cfg=cfg)
        await query.answer(f"Рынок: {cfg['market_mode']}")
    elif data == "set:toggle_discord":
        dcfg["enabled"] = not dcfg["enabled"]
        cfg, dcfg, bans, allowed = await save_user_record(uid, dcfg=dcfg)
        await query.answer("Discord переключён")
    elif data == "set:test_discord":
        wurl = resolve_discord_webhook(dcfg)
        if not wurl:
            await query.answer("Сначала задайте /discord URL", show_alert=False)
            return
        ok, err = await send_test_discord(wurl, session=_http_session)
        await query.answer("✅ Тест отправлен!" if ok else f"❌ {err}", show_alert=False)
        return
    elif data.startswith("preset:"):
        p = data.split(":", 1)[1]
        presets = {
            "scalp": {"min_wall_spot_usd": 100_000.0, "min_wall_futures_usd": 200_000.0, "max_distance_pct": 1.5, "min_anomaly_x": 4.0, "min_lifetime_sec": 8.0, "cooldown_sec": 180.0},
            "standard": {"min_wall_spot_usd": 250_000.0, "min_wall_futures_usd": 500_000.0, "max_distance_pct": 2.5, "min_anomaly_x": 5.0, "min_lifetime_sec": 15.0, "cooldown_sec": 300.0},
            "whale": {"min_wall_spot_usd": 500_000.0, "min_wall_futures_usd": 1_000_000.0, "max_distance_pct": 4.0, "min_anomaly_x": 7.0, "min_lifetime_sec": 30.0, "cooldown_sec": 600.0},
        }
        if p in presets:
            cfg.update(presets[p])
            cfg, dcfg, bans, allowed = await save_user_record(uid, cfg=cfg)
            await query.answer(f"Пресет применён: {p}")

    try:
        await query.message.edit_text(
            _format_settings_text(cfg, dcfg, bans, allowed),
            parse_mode="HTML",
            reply_markup=_build_settings_keyboard(cfg, dcfg),
        )
    except Exception:
        pass


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
            --bg: #0b0f17; --card: #141a26; --card-2: #1c2434;
            --text: #eef2f8; --hint: #8b96aa; --border: rgba(255,255,255,0.08);
            --bid: #16c784; --ask: #ea3943; --gold: #f5b942; --accent: #3b82f6;
        }
        * { box-sizing: border-box; -webkit-tap-highlight-color: transparent; }
        body {
            margin: 0; padding: 12px 12px 72px; background: var(--bg); color: var(--text);
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; font-size: 14px;
        }
        .topbar { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; gap: 8px; }
        .brand { font-weight: 800; font-size: 16px; display: flex; align-items: center; gap: 6px; }
        .chip {
            border: 1px solid var(--border); background: var(--card); color: var(--text);
            padding: 6px 10px; border-radius: 10px; font-size: 12px; font-weight: 700; cursor: pointer;
        }
        .chip.on { border-color: rgba(22,199,132,0.55); background: rgba(22,199,132,0.14); color: var(--bid); }
        .tabs {
            display: grid; grid-template-columns: 1fr 1fr; gap: 6px; background: var(--card);
            padding: 4px; border-radius: 12px; border: 1px solid var(--border); margin-bottom: 12px;
        }
        .tab-btn {
            border: none; background: transparent; color: var(--hint);
            padding: 9px 6px; border-radius: 9px; font-size: 13px; font-weight: 700; cursor: pointer;
        }
        .tab-btn.active { background: var(--accent); color: #fff; }
        .panel { display: none; }
        .panel.active { display: block; }
        .sec-title {
            display: flex; justify-content: space-between; font-size: 11px; font-weight: 800;
            color: var(--hint); text-transform: uppercase; letter-spacing: 0.4px; margin: 12px 2px 8px;
        }
        .filters { display: flex; gap: 6px; overflow-x: auto; padding-bottom: 6px; margin-bottom: 4px; }
        .pill {
            border: 1px solid var(--border); background: var(--card); color: var(--hint);
            padding: 5px 10px; border-radius: 999px; font-size: 11px; font-weight: 700; cursor: pointer; white-space: nowrap;
        }
        .pill.active { background: var(--text); color: var(--bg); border-color: var(--text); }
        .card {
            background: var(--card); border: 1px solid var(--border); border-radius: 12px;
            padding: 10px 12px; margin-bottom: 8px; border-left: 3px solid var(--accent); cursor: pointer;
        }
        .card.bid { border-left-color: var(--bid); }
        .card.ask { border-left-color: var(--ask); }
        .card-top { display: flex; justify-content: space-between; align-items: center; gap: 6px; margin-bottom: 5px; }
        .sym { font-weight: 800; font-size: 14px; display: flex; align-items: center; gap: 5px; flex-wrap: wrap; }
        .ex { font-size: 10px; font-weight: 700; padding: 2px 6px; border-radius: 6px; background: rgba(255,255,255,0.08); }
        .mkt { font-size: 9px; font-weight: 800; padding: 2px 5px; border-radius: 5px; background: rgba(59,130,246,0.2); color: #93c5fd; }
        .mkt.spot { background: rgba(245,185,66,0.2); color: var(--gold); }
        .badge { font-size: 11px; font-weight: 800; padding: 2px 7px; border-radius: 6px; }
        .badge.bid { background: rgba(22,199,132,0.18); color: var(--bid); }
        .badge.ask { background: rgba(234,57,67,0.18); color: var(--ask); }
        .badge.gold { background: rgba(245,185,66,0.18); color: var(--gold); }
        .meta { display: flex; justify-content: space-between; flex-wrap: wrap; gap: 6px; font-size: 12px; color: var(--hint); }
        .meta b { color: var(--text); }
        .ban-x {
            border: none; background: rgba(234,57,67,0.16); color: var(--ask);
            width: 22px; height: 22px; border-radius: 6px; font-weight: 800; cursor: pointer;
        }
        .box { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 12px; margin-bottom: 10px; }
        .box-title { font-size: 13px; font-weight: 800; margin-bottom: 8px; display: block; }
        .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 8px; }
        .fg { display: flex; flex-direction: column; gap: 4px; }
        .fl { font-size: 11px; color: var(--hint); font-weight: 600; }
        input[type="number"], input[type="text"], select {
            width: 100%; background: var(--card-2); color: var(--text); border: 1px solid var(--border);
            border-radius: 9px; padding: 8px 10px; font-size: 13px; outline: none;
        }
        .toggle { display: flex; justify-content: space-between; align-items: center; padding: 6px 0; font-size: 13px; }
        .toggle input { width: 18px; height: 18px; accent-color: var(--bid); }
        .btn {
            border: none; background: var(--accent); color: #fff; padding: 9px 12px;
            border-radius: 9px; font-size: 13px; font-weight: 700; cursor: pointer; width: 100%;
        }
        .btn.sec { background: var(--card-2); color: var(--text); border: 1px solid var(--border); }
        .ex-wrap { display: flex; flex-wrap: wrap; gap: 6px; margin: 6px 0 10px; }
        .ex-btn {
            padding: 6px 10px; border-radius: 9px; border: 1px solid var(--border);
            background: rgba(255,255,255,0.03); color: var(--hint); font-size: 12px; font-weight: 700; cursor: pointer;
        }
        .ex-btn.on { border-color: rgba(22,199,132,0.55); background: rgba(22,199,132,0.14); color: var(--text); }
        .ban-tags { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; }
        .ban-tag {
            display: inline-flex; align-items: center; gap: 6px; padding: 4px 8px; border-radius: 8px;
            background: rgba(234,57,67,0.14); border: 1px solid rgba(234,57,67,0.35); font-size: 12px; font-weight: 700;
        }
        .ban-tag.allow {
            background: rgba(22,199,132,0.14); border-color: rgba(22,199,132,0.45); color: var(--bid);
        }
        .ban-tag button { border: none; background: transparent; color: inherit; font-weight: 800; cursor: pointer; }
        .empty { text-align: center; color: var(--hint); font-size: 12px; padding: 14px 4px; }
        .toast {
            position: fixed; bottom: 16px; left: 50%; transform: translateX(-50%);
            background: #1f2937; color: #fff; padding: 9px 14px; border-radius: 10px;
            font-size: 12px; font-weight: 700; opacity: 0; pointer-events: none; transition: opacity 0.2s; z-index: 200;
        }
        .toast.show { opacity: 1; }
    </style>
</head>
<body>
    <div class="topbar">
        <div class="brand">🧱 Density Screener <span style="font-size:11px;color:var(--bid)">7 EX · FUT+SPOT</span></div>
        <div style="display:flex;gap:6px">
            <button id="snd-btn" class="chip on" onclick="toggleSound()">🔊 Звук</button>
            <button id="tiger-btn" class="chip" onclick="connectTiger(true)">🐯 Tiger</button>
        </div>
    </div>

    <div class="tabs">
        <button id="tb-feed" class="tab-btn active" onclick="switchTab('feed')">🧱 Плотности</button>
        <button id="tb-cfg" class="tab-btn" onclick="switchTab('cfg')">⚙️ Настройки</button>
    </div>

    <div id="tab-feed" class="panel active">
        <div class="box" style="padding:10px;margin-bottom:10px">
            <div style="display:flex;gap:6px;margin-bottom:6px">
                <input type="text" id="coin-search" placeholder="🔎 Поиск монеты (напр. SOL, WIF, RIVER)" oninput="renderLists()">
                <button class="btn" style="width:auto;white-space:nowrap" onclick="scanSingleCoin()">🔎 Стаканы</button>
                <button class="btn sec" style="width:auto;padding:8px 10px" onclick="clearCoinSearch()" title="Сбросить">✕</button>
            </div>
            <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px">
                <div class="fg"><span class="fl">Спот ($)</span><input type="number" id="s-spot" value="250000" step="25000"></div>
                <div class="fg"><span class="fl">Фьючерс ($)</span><input type="number" id="s-fut" value="500000" step="50000"></div>
                <div class="fg"><span class="fl">Дист (%)</span><input type="number" id="s-dist" value="2.5" step="0.5"></div>
            </div>
            <div id="scan-res" style="margin-top:8px"></div>
        </div>

        <div class="filters" id="mkt-filters"></div>
        <div class="filters" id="ex-filters"></div>

        <div class="sec-title"><span>🔴 Активные стены прямо сейчас</span><span id="live-cnt">0</span></div>
        <div id="live-list"></div>

        <div class="sec-title"><span>📜 История аномальных плотностей (UTC)</span></div>
        <div id="hist-list"></div>
    </div>

    <div id="tab-cfg" class="panel">
        <div class="box">
            <span class="box-title">🧱 Параметры аномальной плотности</span>
            <div class="toggle">
                <span>Включить уведомления</span>
                <input type="checkbox" id="c-en" checked>
            </div>
            <div class="grid-2">
                <div class="fg">
                    <span class="fl">Рынок</span>
                    <select id="c-mkt">
                        <option value="BOTH">Фьючерсы + Спот</option>
                        <option value="FUTURES">Только Фьючерсы</option>
                        <option value="SPOT">Только Спот</option>
                    </select>
                </div>
                <div class="fg">
                    <span class="fl">Сторона стакана</span>
                    <select id="c-side">
                        <option value="BOTH">🟢🔴 Оба (BID + ASK)</option>
                        <option value="BID">🟢 Только BID</option>
                        <option value="ASK">🔴 Только ASK</option>
                    </select>
                </div>
            </div>
            <div class="grid-2">
                <div class="fg"><span class="fl">Мин. плотность СПОТ ($)</span><input type="number" id="c-wall-spot" value="250000" step="25000"></div>
                <div class="fg"><span class="fl">Мин. плотность ФЬЮЧЕРС ($)</span><input type="number" id="c-wall-fut" value="500000" step="50000"></div>
            </div>
            <div class="grid-2">
                <div class="fg"><span class="fl">Макс. дистанция (%)</span><input type="number" id="c-dist" value="2.5" step="0.2"></div>
                <div class="fg"><span class="fl">Аномальность (x к стакану)</span><input type="number" id="c-anom" value="5" step="0.5"></div>
            </div>
            <div class="grid-2">
                <div class="fg"><span class="fl">Мин. время жизни (сек)</span><input type="number" id="c-life" value="15" step="5"></div>
                <div class="fg"><span class="fl">Мин. оборот 24ч ($)</span><input type="number" id="c-vol" value="500000" step="100000"></div>
            </div>
            <div class="fg" style="margin-bottom:8px">
                <span class="fl">Кулдаун повтора (сек)</span><input type="number" id="c-cd" value="300" step="30">
            </div>
            <span class="fl">Биржи (7):</span>
            <div class="ex-wrap" id="ex-checks"></div>
            <button class="btn" onclick="saveSettings()">💾 Сохранить настройки</button>
        </div>

        <div class="box">
            <span class="box-title">🎯 Отслеживать только выбранные монеты</span>
            <div class="fl" style="margin-bottom:6px">Если пусто — скринер ищет по всему рынку. Если добавить монеты — алерты будут приходить только по ним:</div>
            <div class="grid-2">
                <input type="text" id="watch-inp" placeholder="Напр. SOL, WIF, RIVER">
                <button class="btn" onclick="addWatch()">🎯 Добавить</button>
            </div>
            <div class="ban-tags" id="watch-list"></div>
        </div>

        <div class="box">
            <span class="box-title">🔓 Разблокировать монеты из чёрного списка</span>
            <div class="fl" style="margin-bottom:6px">Добавьте любую заблокированную монету (BTC, SOL, XAU, TSLA и др.) обратно в скринер:</div>
            <div class="grid-2">
                <input type="text" id="allow-inp" placeholder="Напр. BTC, SOL, XAU">
                <button class="btn" onclick="addAllow()">🔓 Добавить</button>
            </div>
            <div class="ex-wrap" id="quick-allow"></div>
            <div class="ban-tags" id="allow-list"></div>
        </div>

        <div class="box">
            <span class="box-title">💬 Discord-канал (Webhook)</span>
            <div class="toggle">
                <span>Отправлять алерты в Discord</span>
                <input type="checkbox" id="d-en" checked>
            </div>
            <div class="fg" style="margin-bottom:10px">
                <span class="fl">Discord Webhook URL</span>
                <input type="text" id="d-url" placeholder="https://discord.com/api/webhooks/...">
            </div>
            <div class="grid-2">
                <button class="btn" onclick="saveSettings()">💾 Сохранить</button>
                <button class="btn sec" onclick="testDiscord()">🧪 Тест Discord</button>
            </div>
        </div>

        <div class="box">
            <span class="box-title">🚫 Личный чёрный список монет</span>
            <div class="grid-2">
                <input type="text" id="ban-inp" placeholder="Тикер, напр. RIVER">
                <button class="btn sec" onclick="addBan()">Скрыть</button>
            </div>
            <div class="ban-tags" id="ban-list"></div>
        </div>
    </div>

    <div id="toast" class="toast"></div>

    <script>
        const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
        if (tg) { tg.ready(); tg.expand(); }
        const userId = (tg && tg.initDataUnsafe && tg.initDataUnsafe.user && tg.initDataUnsafe.user.id)
            ? Number(tg.initDataUnsafe.user.id) : Number(new URLSearchParams(location.search).get('user_id') || 0);

        const ALL_EX = ['Binance', 'Bybit', 'OKX', 'Bitget', 'Gate', 'KuCoin', 'Aster'];
        const QUICK_UNBLOCK = ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'SUI', 'PEPE', 'WIF', 'XAU', 'XAG', 'TSLA', 'NVDA'];
        let exFilter = 'ALL';
        let mktFilter = 'ALL';
        let selectedEx = [...ALL_EX];
        let soundOn = true;
        let inited = false;
        let firstDone = false;
        let lastId = 0;
        let activeWalls = [];
        let histWalls = [];
        let bans = [];
        let allowedCoins = [];
        let watchCoins = [];
        let tigerWs = null;

        function toast(m) {
            const t = document.getElementById('toast');
            t.textContent = m; t.classList.add('show');
            clearTimeout(window._tt); window._tt = setTimeout(() => t.classList.remove('show'), 2300);
        }
        function fmtUsd(v) {
            const n = Number(v || 0);
            if (n >= 1e6) return '$' + (n / 1e6).toFixed(2) + 'M';
            if (n >= 1e3) return '$' + Math.round(n / 1e3) + 'K';
            return '$' + Math.round(n);
        }
        function speak(txt) {
            if (!soundOn || !('speechSynthesis' in window)) return;
            try {
                window.speechSynthesis.cancel();
                const u = new SpeechSynthesisUtterance(txt);
                u.lang = 'ru-RU'; u.rate = 1.08;
                window.speechSynthesis.speak(u);
            } catch {}
        }
        function toggleSound() {
            soundOn = !soundOn;
            const b = document.getElementById('snd-btn');
            b.className = 'chip ' + (soundOn ? 'on' : '');
            b.textContent = soundOn ? '🔊 Звук' : '🔇 Выкл';
        }
        function switchTab(name) {
            ['feed', 'cfg'].forEach(t => {
                document.getElementById('tab-' + t).classList.toggle('active', t === name);
                document.getElementById('tb-' + t).classList.toggle('active', t === name);
            });
        }
        function connectTiger(manual) {
            const btn = document.getElementById('tiger-btn');
            try {
                if (tigerWs && tigerWs.readyState === 1) { if (manual) toast('🐯 Уже подключён'); return; }
                tigerWs = new WebSocket('ws://127.0.0.1:21310');
                tigerWs.onopen = () => { btn.className = 'chip on'; btn.textContent = '🐯 ON'; if (manual) toast('🐯 Tiger подключён'); };
                tigerWs.onclose = () => { btn.className = 'chip'; btn.textContent = '🐯 Tiger'; };
            } catch {}
        }
        function openTiger(ex, sym, mkt) {
            if (!tigerWs || tigerWs.readyState !== 1) return;
            const futMap = { Binance: 'BINANCEF', Bybit: 'BYBITF', OKX: 'OKXF', Bitget: 'BITGETF', Gate: 'GATEIOF', KuCoin: 'KUCOINF', Aster: 'BINANCEF' };
            const spotMap = { Binance: 'BINANCE', Bybit: 'BYBIT', OKX: 'OKX', Bitget: 'BITGET', Gate: 'GATEIO', KuCoin: 'KUCOIN', Aster: 'BINANCE' };
            const exCode = mkt === 'SPOT' ? (spotMap[ex] || 'BINANCE') : (futMap[ex] || 'BINANCEF');
            try { tigerWs.send(JSON.stringify({ action: 'SetTicker', exchange: exCode, ticker: sym, group: 1 })); toast('🐯 ' + sym); } catch {}
        }
        function renderExUI() {
            const mkts = [['ALL', 'Все рынки'], ['FUTURES', 'Фьючерсы'], ['SPOT', 'Спот']];
            document.getElementById('mkt-filters').innerHTML = mkts.map(([k, lbl]) =>
                `<button class="pill ${mktFilter === k ? 'active' : ''}" onclick="mktFilter='${k}';renderExUI();renderLists()">${lbl}</button>`
            ).join('');
            document.getElementById('ex-filters').innerHTML = ['ALL', ...ALL_EX].map(e =>
                `<button class="pill ${exFilter === e ? 'active' : ''}" onclick="exFilter='${e}';renderExUI();renderLists()">${e === 'ALL' ? 'Все биржи' : e}</button>`
            ).join('');
            document.getElementById('ex-checks').innerHTML = ALL_EX.map(e =>
                `<button type="button" class="ex-btn ${selectedEx.includes(e) ? 'on' : ''}" onclick="toggleEx('${e}')">${e}</button>`
            ).join('');
        }
        function toggleEx(e) {
            if (selectedEx.includes(e)) {
                if (selectedEx.length <= 1) return toast('Оставьте минимум 1 биржу');
                selectedEx = selectedEx.filter(x => x !== e);
            } else selectedEx.push(e);
            renderExUI();
        }
        function cardHtml(r, isLive) {
            const isBid = r.side === 'BID';
            const isSpot = (r.market_type || 'FUTURES') === 'SPOT';
            const mktLbl = isSpot ? 'SPOT' : 'FUT';
            return `<div class="card ${isBid ? 'bid' : 'ask'}" onclick="openTiger('${r.exchange}','${r.symbol}','${r.market_type || 'FUTURES'}')">
                <div class="card-top">
                    <div class="sym">
                        <span>${r.symbol}</span>
                        <span class="ex">${r.exchange}</span>
                        <span class="mkt ${isSpot ? 'spot' : ''}">${mktLbl}</span>
                    </div>
                    <div style="display:flex;gap:5px;align-items:center">
                        <span class="badge gold">🧱 ${fmtUsd(r.wall_usd)}</span>
                        <span class="badge ${isBid ? 'bid' : 'ask'}">${r.side}</span>
                        <button class="ban-x" onclick="event.stopPropagation();banCoin('${r.symbol}','ban')">✕</button>
                    </div>
                </div>
                <div class="meta">
                    <span>Цена: <b>${r.wall_price}</b> (${r.distance_pct}%) · <b>x${r.anomaly_x}</b> · ~${r.eat_minutes}м</span>
                    <span>${Math.round(r.lifetime_sec)}с · ${isLive ? fmtUsd(r.volume_24h_usd) : r.utc_time}</span>
                </div>
            </div>`;
        }
        function matchFeed(x) {
            const mt = (x.market_type || 'FUTURES').toUpperCase();
            const q = (document.getElementById('coin-search')?.value || '').trim().toUpperCase();
            const coinOk = !q || String(x.symbol || '').toUpperCase().includes(q);
            return coinOk && (exFilter === 'ALL' || x.exchange === exFilter) && (mktFilter === 'ALL' || mt === mktFilter);
        }
        function clearCoinSearch() {
            const el = document.getElementById('coin-search');
            if (el) el.value = '';
            const sr = document.getElementById('scan-res');
            if (sr) sr.innerHTML = '';
            renderLists();
        }
        async function scanSingleCoin() {
            const q = (document.getElementById('coin-search')?.value || '').trim();
            const box = document.getElementById('scan-res');
            if (!q) return toast('Введите тикер монеты, напр. SOL');
            box.innerHTML = '<div class="empty">⏳ Сканирую стаканы на 7 биржах (Спот + Фьючерсы)...</div>';
            const sp = Number(document.getElementById('s-spot').value || 250000);
            const ft = Number(document.getElementById('s-fut').value || 500000);
            const ds = Number(document.getElementById('s-dist').value || 2.5);
            try {
                const url = `/api/search?user_id=${encodeURIComponent(userId)}&coin=${encodeURIComponent(q)}&min_spot=${sp}&min_fut=${ft}&max_dist=${ds}`;
                const r = await fetch(url);
                const d = await r.json();
                if (!r.ok || !d.coin) {
                    box.innerHTML = '<div class="empty">Не удалось найти монету</div>';
                    return;
                }
                const list = (d.matches && d.matches.length) ? d.matches : (d.top_seen || []);
                const hdr = (d.matches && d.matches.length)
                    ? `Найдено по фильтру (${d.coin}): <b>${d.matches.length}</b>`
                    : `По фильтру нет (крупнейшие стенки ${d.coin} из ${d.checked_books} стаканов):`;
                const watchBtn = `<button class="pill active" onclick="watchCoin('${d.coin}','add')">🎯 В отслеживаемые</button>`;
                box.innerHTML = `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;font-size:11px;color:var(--hint)"><span>${hdr}</span>${watchBtn}</div>` +
                    (list.length ? list.map(m => {
                        const isBid = m.side === 'BID';
                        const isSpot = m.market_type === 'SPOT';
                        return `<div class="card ${isBid ? 'bid' : 'ask'}" style="margin-bottom:6px;padding:8px 10px" onclick="openTiger('${m.exchange}','${m.symbol}','${m.market_type}')">
                            <div class="card-top" style="margin-bottom:3px">
                                <div class="sym"><span>${m.symbol}</span><span class="ex">${m.exchange}</span><span class="mkt ${isSpot ? 'spot' : ''}">${isSpot ? 'SPOT' : 'FUT'}</span></div>
                                <div style="display:flex;gap:4px"><span class="badge gold">🧱 ${fmtUsd(m.wall_usd)}</span><span class="badge ${isBid ? 'bid' : 'ask'}">${m.side}</span></div>
                            </div>
                            <div class="meta"><span>Цена: <b>${m.wall_price}</b> (${m.distance_pct}%) · <b>x${m.anomaly_x}</b></span><span>${m.utc_time} UTC</span></div>
                        </div>`;
                    }).join('') : '<div class="empty">Стаканы по этой монете не найдены</div>');
            } catch {
                box.innerHTML = '<div class="empty">Ошибка запроса стаканов</div>';
            }
        }
        function renderWatch() {
            const box = document.getElementById('watch-list');
            if (!box) return;
            box.innerHTML = watchCoins.length
                ? watchCoins.map(c => `<span class="ban-tag allow">🎯 ${c}<button onclick="watchCoin('${c}','remove')">✕</button></span>`).join('') +
                  `<button class="pill" onclick="watchCoin('','clear')">Сбросить (весь рынок)</button>`
                : '<div class="empty" style="padding:4px">Режим: весь рынок</div>';
        }
        async function watchCoin(coin, action) {
            try {
                const r = await fetch('/api/watch', {
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ user_id: userId, coin, action })
                });
                if (r.ok) {
                    const d = await r.json();
                    watchCoins = d.watch_coins || [];
                    renderWatch();
                    toast(action === 'clear' ? '🌍 Режим: весь рынок' : (action === 'add' ? '🎯 Добавлено в отслеживание' : 'Убрано из отслеживания'));
                    loadData();
                }
            } catch {}
        }
        function addWatch() {
            const i = document.getElementById('watch-inp');
            if (!i.value.trim()) return;
            watchCoin(i.value.trim(), 'add'); i.value = '';
        }
        function renderLists() {
            const act = activeWalls.filter(matchFeed);
            document.getElementById('live-cnt').textContent = act.length;
            document.getElementById('live-list').innerHTML = act.length
                ? act.map(x => cardHtml(x, true)).join('')
                : '<div class="empty">Сейчас активных аномальных плотностей нет</div>';

            const hist = histWalls.filter(matchFeed);
            document.getElementById('hist-list').innerHTML = hist.length
                ? hist.map(x => cardHtml(x, false)).join('')
                : '<div class="empty">За сегодня (UTC) плотностей пока не было</div>';
        }
        function renderBans() {
            const box = document.getElementById('ban-list');
            box.innerHTML = bans.length
                ? bans.map(c => `<span class="ban-tag">${c}<button onclick="banCoin('${c}','unban')">✕</button></span>`).join('')
                : '<div class="empty" style="padding:4px">Пусто</div>';
        }
        function renderAllowed() {
            document.getElementById('quick-allow').innerHTML = QUICK_UNBLOCK.map(c => {
                const on = allowedCoins.includes(c);
                return `<button type="button" class="ex-btn ${on ? 'on' : ''}" onclick="toggleAllow('${c}')">${on ? '✅ ' : '+ '}${c}</button>`;
            }).join('');
            const box = document.getElementById('allow-list');
            box.innerHTML = allowedCoins.length
                ? allowedCoins.map(c => `<span class="ban-tag allow">🔓 ${c}<button onclick="allowCoin('${c}','unallow')">✕</button></span>`).join('')
                : '<div class="empty" style="padding:4px">Нет разблокированных монет из ЧС</div>';
        }
        function toggleAllow(c) {
            allowCoin(c, allowedCoins.includes(c) ? 'unallow' : 'allow');
        }
        async function allowCoin(coin, action) {
            try {
                const r = await fetch('/api/allow', {
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ user_id: userId, coin, action })
                });
                if (r.ok) {
                    const d = await r.json();
                    allowedCoins = d.allowed_coins || [];
                    bans = d.banned_coins || bans;
                    renderAllowed(); renderBans();
                    toast(action === 'allow' ? '🔓 Разблокировано из ЧС' : '🔒 Возвращено в ЧС');
                    loadData();
                }
            } catch {}
        }
        function addAllow() {
            const i = document.getElementById('allow-inp');
            if (!i.value.trim()) return;
            allowCoin(i.value.trim(), 'allow'); i.value = '';
        }
        async function banCoin(coin, action) {
            try {
                const r = await fetch('/api/ban', {
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ user_id: userId, coin, action })
                });
                if (r.ok) {
                    const d = await r.json();
                    bans = d.banned_coins || [];
                    allowedCoins = d.allowed_coins || allowedCoins;
                    renderBans(); renderAllowed();
                    toast(action === 'ban' ? '🚫 Скрыто' : '✅ Возвращено');
                    loadData();
                }
            } catch {}
        }
        function addBan() {
            const i = document.getElementById('ban-inp');
            if (!i.value.trim()) return;
            banCoin(i.value.trim(), 'ban'); i.value = '';
        }
        async function saveSettings() {
            const cfg = {
                enabled: document.getElementById('c-en').checked,
                market_mode: document.getElementById('c-mkt').value,
                min_wall_spot_usd: Number(document.getElementById('c-wall-spot').value || 250000),
                min_wall_futures_usd: Number(document.getElementById('c-wall-fut').value || 500000),
                max_distance_pct: Number(document.getElementById('c-dist').value || 2.5),
                min_anomaly_x: Number(document.getElementById('c-anom').value || 5),
                min_lifetime_sec: Number(document.getElementById('c-life').value || 15),
                min_vol24h: Number(document.getElementById('c-vol').value || 500000),
                cooldown_sec: Number(document.getElementById('c-cd').value || 300),
                side_mode: document.getElementById('c-side').value,
                exchanges: [...selectedEx],
                watch_coins: [...watchCoins]
            };
            const dcfg = {
                enabled: document.getElementById('d-en').checked,
                webhook_url: (document.getElementById('d-url').value || '').trim()
            };
            try {
                const r = await fetch('/api/settings', {
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ user_id: userId, config: cfg, discord_config: dcfg })
                });
                if (r.ok) {
                    document.getElementById('s-spot').value = cfg.min_wall_spot_usd;
                    document.getElementById('s-fut').value = cfg.min_wall_futures_usd;
                    document.getElementById('s-dist').value = cfg.max_distance_pct;
                    toast('✅ Настройки сохранены');
                    loadData();
                }
            } catch { toast('❌ Ошибка сохранения'); }
        }
        async function testDiscord() {
            const dcfg = { enabled: document.getElementById('d-en').checked, webhook_url: (document.getElementById('d-url').value || '').trim() };
            try {
                const r = await fetch('/api/discord/test', {
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ user_id: userId, discord_config: dcfg })
                });
                const d = await r.json();
                toast(d.ok ? '✅ Тест отправлен в Discord!' : ('❌ ' + (d.detail || 'Ошибка')));
            } catch { toast('❌ Ошибка Discord'); }
        }
        async function loadData() {
            try {
                const r = await fetch('/api/data?user_id=' + encodeURIComponent(userId));
                if (!r.ok) return;
                const d = await r.json();
                if (!inited) {
                    const c = d.config || {};
                    document.getElementById('c-en').checked = c.enabled !== false;
                    document.getElementById('c-mkt').value = c.market_mode || 'BOTH';
                    document.getElementById('c-wall-spot').value = c.min_wall_spot_usd ?? 250000;
                    document.getElementById('c-wall-fut').value = c.min_wall_futures_usd ?? 500000;
                    document.getElementById('c-dist').value = c.max_distance_pct ?? 2.5;
                    document.getElementById('s-spot').value = c.min_wall_spot_usd ?? 250000;
                    document.getElementById('s-fut').value = c.min_wall_futures_usd ?? 500000;
                    document.getElementById('s-dist').value = c.max_distance_pct ?? 2.5;
                    document.getElementById('c-anom').value = c.min_anomaly_x ?? 5;
                    document.getElementById('c-life').value = c.min_lifetime_sec ?? 15;
                    document.getElementById('c-vol').value = c.min_vol24h ?? 500000;
                    document.getElementById('c-cd').value = c.cooldown_sec ?? 300;
                    document.getElementById('c-side').value = c.side_mode || 'BOTH';
                    if (Array.isArray(c.exchanges) && c.exchanges.length) selectedEx = c.exchanges;
                    watchCoins = Array.isArray(c.watch_coins) ? c.watch_coins : [];
                    const dc = d.discord_config || {};
                    document.getElementById('d-en').checked = dc.enabled !== false;
                    document.getElementById('d-url').value = dc.webhook_url || '';
                    bans = d.banned_coins || [];
                    allowedCoins = d.allowed_coins || [];
                    renderExUI(); renderBans(); renderAllowed(); renderWatch(); inited = true;
                }
                activeWalls = d.active || [];
                histWalls = d.history || [];
                if (histWalls.length) {
                    const top = histWalls[0];
                    if (firstDone && top.id > lastId) {
                        const s = top.side === 'BID' ? 'Поддержка' : 'Сопротивление';
                        const m = top.market_type === 'SPOT' ? 'Спот' : 'Фьючерс';
                        speak(`Плотность ${top.exchange} ${m}. ${top.symbol.replace('USDT','')}. ${s}. ${Math.round(top.wall_usd/1000)} тысяч`);
                    }
                    lastId = Math.max(lastId, top.id);
                }
                firstDone = true;
                renderLists();
            } catch {}
        }
        renderExUI(); renderAllowed(); connectTiger(false); loadData(); setInterval(loadData, 2000);
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
    discord_config: Optional[Dict[str, Any]] = None


class BanPayload(BaseModel):
    user_id: int
    coin: str
    action: str = "ban"


class AllowPayload(BaseModel):
    user_id: int
    coin: str
    action: str = "allow"


class WatchPayload(BaseModel):
    user_id: int
    coin: str = ""
    action: str = "add"  # add | remove | clear


async def _run_telegram_polling() -> None:
    if bot is None:
        logger.warning("BOT_TOKEN не задан — Telegram-бот не запущен (Discord-режим работает).")
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


async def _load_initial_engine_config() -> None:
    rows = await db_fetchall("SELECT config FROM users WHERE config IS NOT NULL AND config != '{}'")
    if rows:
        density_engine.update_config(normalize_config(rows[-1].get("config")))


async def _refresh_api_cache() -> None:
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now = time.time()
    if _api_cache["utc_date"] == today_utc and (now - _api_cache["ts"]) < 1.5:
        return
    rows = await db_fetchall(
        "SELECT id, utc_time, exchange, market_type, symbol, side, wall_price, current_price, distance_pct, "
        "wall_usd, median_level_usd, anomaly_x, lifetime_sec, eat_minutes, volume_24h_usd "
        "FROM densities WHERE utc_date = ? ORDER BY id DESC LIMIT 150",
        (today_utc,),
    )
    _api_cache["utc_date"] = today_utc
    _api_cache["history"] = rows
    _api_cache["ts"] = now


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global _http_session
    await init_db()
    await _load_initial_engine_config()
    await sync_runtime_allowed_bases()
    await get_cached_users(force=True)

    ws_session = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=120, ttl_dns_cache=300),
        headers={"User-Agent": "Mozilla/5.0"},
    )
    _http_session = ws_session
    await refresh_tradfi_metadata(ws_session)

    tasks: List[asyncio.Task[Any]] = [
        asyncio.create_task(_run_telegram_polling()),
        asyncio.create_task(_tradfi_refresher_loop(ws_session)),
        # Binance (Фьючерсы + Спот)
        asyncio.create_task(
            _binance_like_depth_ws(
                ws_session, density_engine, "Binance", "FUTURES",
                "https://fapi.binance.com/fapi/v1/ticker/24hr", "wss://fstream.binance.com", "depth20@500ms", 90,
            )
        ),
        asyncio.create_task(
            _binance_like_depth_ws(
                ws_session, density_engine, "Binance", "SPOT",
                "https://api.binance.com/api/v3/ticker/24hr", "wss://stream.binance.com:9443", "depth20@1000ms", 70,
            )
        ),
        # Aster (Фьючерсы + Спот)
        asyncio.create_task(
            _binance_like_depth_ws(
                ws_session, density_engine, "Aster", "FUTURES",
                "https://fapi.asterdex.com/fapi/v1/ticker/24hr", "wss://fstream.asterdex.com", "depth20@500ms", 70,
            )
        ),
        asyncio.create_task(
            _binance_like_depth_ws(
                ws_session, density_engine, "Aster", "SPOT",
                "https://sapi.asterdex.com/api/v1/ticker/24hr", "wss://sstream.asterdex.com", "depth20@500ms", 40,
            )
        ),
        # Bybit (Фьючерсы + Спот)
        asyncio.create_task(bybit_depth_ws(ws_session, density_engine, "FUTURES")),
        asyncio.create_task(bybit_depth_ws(ws_session, density_engine, "SPOT")),
        # OKX (Фьючерсы + Спот)
        asyncio.create_task(okx_depth_ws(ws_session, density_engine, "FUTURES")),
        asyncio.create_task(okx_depth_ws(ws_session, density_engine, "SPOT")),
        # Bitget (Фьючерсы + Спот)
        asyncio.create_task(bitget_depth_ws(ws_session, density_engine, "FUTURES")),
        asyncio.create_task(bitget_depth_ws(ws_session, density_engine, "SPOT")),
        # Gate (Фьючерсы + Спот)
        asyncio.create_task(gate_depth_ws(ws_session, density_engine, "FUTURES")),
        asyncio.create_task(gate_depth_ws(ws_session, density_engine, "SPOT")),
        # KuCoin (Фьючерсы + Спот)
        asyncio.create_task(kucoin_depth_ws(ws_session, density_engine, "FUTURES")),
        asyncio.create_task(kucoin_depth_ws(ws_session, density_engine, "SPOT")),
    ]
    logger.info("Запущен скринер аномальных плотностей (7 бирж · Фьючерсы + Спот · WebSocket).")
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
    return JSONResponse({"status": "ok", "service": "density-wall-screener", "exchanges": 7, "markets": ["FUTURES", "SPOT"]})


@app.api_route("/ping", methods=["GET", "HEAD"])
async def ping() -> JSONResponse:
    return JSONResponse({"status": "ok", "exchanges": 7, "markets": 2})


@app.get("/webapp", response_class=HTMLResponse)
async def webapp() -> HTMLResponse:
    return HTMLResponse(WEBAPP_HTML)


@app.get("/api/data")
async def api_data(user_id: int = Query(default=0)) -> JSONResponse:
    await _refresh_api_cache()
    if user_id:
        cfg, dcfg, bans, allowed = await get_user_record(user_id)
    else:
        cfg, dcfg, bans, allowed = dict(density_engine.config), dict(DEFAULT_DISCORD_CONFIG), [], sorted(_runtime_allowed_bases)
    allowed_set = set(allowed) | set(cfg.get("watch_coins") or [])

    active = [
        {
            "exchange": w.exchange,
            "market_type": w.market_type,
            "symbol": w.symbol,
            "side": w.side.value,
            "wall_price": w.wall_price,
            "current_price": w.current_price,
            "distance_pct": w.distance_pct,
            "wall_usd": w.wall_usd,
            "anomaly_x": w.anomaly_x,
            "lifetime_sec": w.lifetime_sec,
            "eat_minutes": w.eat_minutes,
            "volume_24h_usd": w.volume_24h_usd,
            "utc_time": w.timestamp_utc,
        }
        for w in density_engine.get_active_walls()
        if not is_blacklisted_symbol(w.symbol, w.exchange, allowed_bases=allowed_set)
        and extract_base_asset(w.symbol) not in bans
        and wall_matches_config(
            w.exchange, w.side.value, w.wall_usd, w.distance_pct, w.anomaly_x,
            w.lifetime_sec, w.volume_24h_usd, cfg, market_type=w.market_type, symbol=w.symbol,
        )
    ]

    history = [
        h
        for h in _api_cache["history"]
        if not is_blacklisted_symbol(str(h.get("symbol") or ""), str(h.get("exchange") or ""), allowed_bases=allowed_set)
        and extract_base_asset(str(h.get("symbol") or "")) not in bans
        and wall_matches_config(
            str(h.get("exchange") or ""),
            str(h.get("side") or ""),
            float(h.get("wall_usd") or 0),
            float(h.get("distance_pct") or 0),
            float(h.get("anomaly_x") or 0),
            float(h.get("lifetime_sec") or 0),
            float(h.get("volume_24h_usd") or 0),
            cfg,
            market_type=str(h.get("market_type") or "FUTURES"),
            symbol=str(h.get("symbol") or ""),
        )
    ]

    return JSONResponse(
        {
            "utc_date": _api_cache["utc_date"],
            "active": active,
            "history": history,
            "config": cfg,
            "discord_config": dcfg,
            "banned_coins": bans,
            "allowed_coins": allowed,
        }
    )


@app.post("/api/settings")
async def api_settings(payload: SettingsPayload) -> JSONResponse:
    if payload.user_id > 0:
        cfg, dcfg, _, _ = await save_user_record(payload.user_id, cfg=payload.config, dcfg=payload.discord_config)
        return JSONResponse({"status": "ok", "config": cfg, "discord_config": dcfg})
    cfg = normalize_config(payload.config)
    density_engine.update_config(cfg)
    return JSONResponse({"status": "ok", "config": cfg})


@app.post("/api/discord/test")
async def api_discord_test(payload: DiscordTestPayload) -> JSONResponse:
    dcfg = normalize_discord_config(payload.discord_config)
    wurl = resolve_discord_webhook(dcfg)
    if not wurl:
        return JSONResponse({"ok": False, "detail": "Укажите корректный Discord Webhook URL"}, status_code=400)
    ok, err = await send_test_discord(wurl, session=_http_session)
    return JSONResponse({"ok": ok, "detail": "OK" if ok else err})


@app.post("/api/ban")
async def api_ban(payload: BanPayload) -> JSONResponse:
    base = extract_base_asset(payload.coin)
    if not base:
        return JSONResponse({"status": "error"}, status_code=400)
    bans: List[str] = []
    allowed: List[str] = []
    if payload.user_id > 0:
        _, _, bans, allowed = await get_user_record(payload.user_id)
        if payload.action == "unban":
            bans = [b for b in bans if b != base]
        else:
            if base not in bans:
                bans.append(base)
            allowed = [c for c in allowed if c != base]
        _, _, bans, allowed = await save_user_record(payload.user_id, bans=bans, allowed=allowed)
    return JSONResponse({"status": "ok", "banned_coins": bans, "allowed_coins": allowed})


@app.post("/api/allow")
async def api_allow(payload: AllowPayload) -> JSONResponse:
    tokens = [extract_base_asset(x.strip()) for x in re.split(r"[,\s]+", payload.coin or "") if x.strip()]
    tokens = [t for t in tokens if t]
    if not tokens:
        return JSONResponse({"status": "error"}, status_code=400)
    bans: List[str] = []
    allowed: List[str] = []
    if payload.user_id > 0:
        _, _, bans, allowed = await get_user_record(payload.user_id)
        if payload.action == "unallow":
            allowed = [c for c in allowed if c not in tokens]
        else:
            for t in tokens:
                if t not in allowed:
                    allowed.append(t)
            bans = [b for b in bans if b not in tokens]
        _, _, bans, allowed = await save_user_record(payload.user_id, bans=bans, allowed=allowed)
    return JSONResponse({"status": "ok", "allowed_coins": allowed, "banned_coins": bans})


@app.get("/api/search")
async def api_search(
    coin: str = Query(default=""),
    user_id: int = Query(default=0),
    min_spot: Optional[float] = Query(default=None),
    min_fut: Optional[float] = Query(default=None),
    max_dist: Optional[float] = Query(default=None),
) -> JSONResponse:
    if not coin.strip():
        return JSONResponse({"coin": "", "matches": [], "top_seen": [], "checked_books": 0}, status_code=400)
    if user_id:
        cfg, _, _, _ = await get_user_record(user_id)
    else:
        cfg = dict(density_engine.config)
    res = await live_search_coin_densities(
        _http_session,
        coin,
        cfg,
        override_min_spot=min_spot,
        override_min_fut=min_fut,
        override_max_dist=max_dist,
    )
    return JSONResponse(res)


@app.post("/api/watch")
async def api_watch(payload: WatchPayload) -> JSONResponse:
    if payload.user_id > 0:
        cfg, _, bans, _ = await get_user_record(payload.user_id)
    else:
        cfg, bans = dict(density_engine.config), []

    watch_list = list(cfg.get("watch_coins") or [])
    act = (payload.action or "add").lower()
    if act == "clear":
        watch_list = []
    else:
        tokens = [extract_base_asset(x.strip()) for x in re.split(r"[,\s]+", payload.coin or "") if x.strip()]
        tokens = [t for t in tokens if t]
        if act == "remove":
            watch_list = [c for c in watch_list if c not in tokens]
        else:
            for t in tokens:
                if t not in watch_list:
                    watch_list.append(t)
            bans = [b for b in bans if b not in tokens]

    cfg["watch_coins"] = watch_list
    if payload.user_id > 0:
        cfg, _, _, _ = await save_user_record(payload.user_id, cfg=cfg, bans=bans)
    else:
        density_engine.update_config(cfg)
    return JSONResponse({"status": "ok", "watch_coins": cfg["watch_coins"]})


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
