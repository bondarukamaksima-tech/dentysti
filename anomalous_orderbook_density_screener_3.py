"""
Отдельный самостоятельный скринер аномальных плотностей в стакане (Orderbook Walls).

Возможности:
- Полный обход ВСЕХ монет каждые 3 минуты (глубокие стаканы до ±10% от спреда) +
  WebSocket-сканирование топ-пар в реальном времени по 8 биржам:
  Binance, BinanceAlpha, Bybit, OKX, Bitget, Gate, KuCoin, Aster (Фьючерсы + Спот).
- Мгновенная отправка алерта сразу при обнаружении плотности (min_lifetime_sec = 0с по умолчанию).
- Диапазон поиска по умолчанию: 10% лонг (BID) и 10% шорт (ASK) от спреда.
- Раздельные настраиваемые пороги плотности:
    * Спот / BinanceAlpha: от $250,000 по умолчанию (меняется через /minspot или Mini App).
    * Фьючерсы: от $500,000 по умолчанию (меняется через /minfut или Mini App).
- Поиск и фильтрация по отдельным монетам (/find, /watch) и гибкий чёрный/белый список (/allow, /ban).
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
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

try:
    import asyncpg  # type: ignore
except ImportError:
    asyncpg = None

# ==================== ЛОГИРОВАНИЕ И КОНФИГ ====================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("density_screener")

BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
WEBAPP_URL = (os.getenv("WEBAPP_URL") or "").strip()
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
SQLITE_PATH = os.getenv("SQLITE_PATH", "density_screener.db")
ENV_DISCORD_WEBHOOK = (os.getenv("DISCORD_WEBHOOK_URL") or "").strip()

ALL_EXCHANGES = ["Binance", "BinanceAlpha", "Bybit", "OKX", "Bitget", "Gate", "KuCoin", "Aster"]
ALL_MARKETS = ["FUTURES", "SPOT"]
QUICK_UNBLOCK_COINS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "SUI", "PEPE", "WIF", "XAU", "XAG", "TSLA", "NVDA"]
FULL_SCAN_INTERVAL_SEC = 180.0          # Полная проверка всех монет на биржах раз в 3 минуты

DEFAULT_CONFIG: Dict[str, Any] = {
    "enabled": True,
    "min_wall_spot_usd": 250_000.0,     # Мин. размер плотности на СПОТЕ и Binance Alpha ($)
    "min_wall_futures_usd": 500_000.0,  # Мин. размер плотности на ФЬЮЧЕРСАХ ($)
    "max_distance_pct": 10.0,           # Макс. дистанция от спреда: 10% лонг (BID) и 10% шорт (ASK)
    "min_anomaly_x": 5.0,               # Во сколько раз больше медианного уровня стакана
    "min_lifetime_sec": 0.0,            # 0с = слать алерт СРАЗУ как найдёт плотность
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

COLOR_GREEN = 0x16C784
COLOR_RED = 0xEA3943
COLOR_BLUE = 0x3B82F6


# ==================== ЧЁРНЫЙ И БЕЛЫЙ СПИСКИ МОНЕТ ====================

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
    "USOIL", "UKOIL", "WTI", "BRENT", "CRUDE", "OIL", "CL", "USO", "BNO",
    "NATGAS", "NGAS", "UNG", "BOIL", "KOLD", "GAS", "CORN", "WHEAT", "SOYB",
    "SUGAR", "COFFEE", "COCOA", "COTTON", "URANIUM", "URA", "XLE",
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
    "U", "TTD", "AFRM", "SOFI", "UPST", "NU", "SVV", "SNDK", "SNDKB", "WDC",
    "STX", "ANET", "VRT", "VST", "CEG", "OKLO", "SMR", "IONQ", "RGTI", "QBTS",
    "RKLB", "ASTS", "LUNR", "HIMS", "TEM", "CVNA", "CAVA", "CMG", "DUOL",
}

_TRADFI_KEYWORDS = (
    "STOCK", "EQUITY", "EQUITIES", "RWA", "TRADFI", "TRADIFI", "METAL",
    "GOLD", "SILVER", "OIL", "COMMODIT", "FOREX", "FX", "ETF", "INDEX",
    "INDICES", "SHARE", "ENERGY", "PRECIOUS",
)

# Динамически пополняемый чёрный список TradFi/RWA/акций/металлов/нефти из API бирж
_dynamic_tradfi_pairs: Set[Tuple[str, str]] = set()
_dynamic_tradfi_bases: Set[str] = set()

# Глобальный белый список монет, разблокированных из ЧС или добавленных в отслеживание (/allow, /watch)
_runtime_allowed_bases: Set[str] = set()
_whitelist_version: int = 0

# Кэш токенов Binance Alpha: display_symbol (напр. BUBBUSDT) -> (alpha_pair, base_ticker, vol24h_usd)
_alpha_markets: Dict[str, Tuple[str, str, float]] = {}
_alpha_by_base: Dict[str, Tuple[str, str, float]] = {}
_alpha_last_refresh: float = 0.0


class WallSide(str, Enum):
    BID = "BID"   # Поддержка / крупная покупка (лонг)
    ASK = "ASK"   # Сопротивление / крупная продажа (шорт)


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


def _safe_int(val: Any, default: int = 0) -> int:
    try:
        if val is None or val == "":
            return default
        return int(float(val))
    except (TypeError, ValueError):
        return default


def _has_tradfi_keyword(text: Any) -> bool:
    up = str(text or "").upper()
    if not up:
        return False
    return any(k in up for k in _TRADFI_KEYWORDS)


def extract_base_asset(symbol: str) -> str:
    """Извлекает базовый тикер (1000PEPEUSDT, WIF-USDT-SWAP, XBTUSDTM, SOL_USDT -> PEPE, WIF, BTC, SOL)."""
    s = (symbol or "").upper().strip()
    if not s:
        return ""
    for sep in (
        "-USDT-SWAP", "-USDC-SWAP", "-USD-SWAP", "USDTM", "USDCM",
        "_USDT", "-USDT", "/USDT", "_USDC", "-USDC", "/USDC",
    ):
        if s.endswith(sep) and len(s) > len(sep):
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
    if s.endswith("USDTM"):
        s = s[:-5] + "USDT"
    if s.startswith("XBT"):
        s = "BTC" + s[3:]
    s = s.replace("-USDT-SWAP", "USDT").replace("_USDT", "USDT").replace("-USDT", "USDT")
    return s


def is_blacklisted_symbol(symbol: str, exchange: str = "", extra_allowed: Optional[Set[str]] = None) -> bool:
    """
    Проверяет, заблокирована ли монета.
    Если базовая монета добавлена в белый список (/allow или /watch), она РАЗРЕШЕНА даже если была в ЧС.
    """
    raw_up = (symbol or "").upper().strip()
    if not raw_up:
        return True

    base = extract_base_asset(raw_up)
    if not base:
        return True
    clean = base.replace(".", "")

    allowed_pool = _runtime_allowed_bases | (extra_allowed or set())
    if base in allowed_pool or clean in allowed_pool:
        return False

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

    if any(tok in clean for tok in ("XAU", "XAG", "XPT", "XPD", "OIL", "BRENT", "WTI", "GOLD", "SILVER", "NATGAS", "US500", "NAS100")):
        return True

    for suf in ("ON", "X", "2L", "2S", "3L", "3S", "5L", "5S"):
        if len(clean) >= len(suf) + 2 and clean.endswith(suf):
            stem = clean[: -len(suf)]
            if stem in TRADFI_STATIC_BLACKLIST or stem in HEAVY_AND_STABLE_BLACKLIST or stem in _dynamic_tradfi_bases:
                return True

    return False


async def refresh_tradfi_metadata(session: aiohttp.ClientSession) -> None:
    """Автоматически подгружает акции/RWA/металлы/нефть/ETF из API бирж в чёрный список."""
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
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
            for it in data.get("data") or []:
                sym = str(it.get("symbol") or "")
                is_rwa = str(it.get("isRwa") or "").upper() == "YES"
                if (
                    is_rwa
                    or _has_tradfi_keyword(it.get("symbolType"))
                    or _has_tradfi_keyword(it.get("category"))
                    or _has_tradfi_keyword(it.get("baseCoin"))
                ):
                    mark("Bitget", sym)
        except Exception:
            pass

    async def fetch_binance_like(ex_name: str, url: str) -> None:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
            for s in data.get("symbols") or []:
                sym = str(s.get("symbol") or "")
                ctype = str(s.get("contractType") or "").upper()
                utype = str(s.get("underlyingType") or "").upper()
                sub = " ".join(str(x) for x in (s.get("underlyingSubType") or []))
                if _has_tradfi_keyword(ctype) or (utype and utype != "COIN") or _has_tradfi_keyword(sub):
                    mark(ex_name, sym)
        except Exception:
            pass

    async def fetch_bybit() -> None:
        try:
            url = "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
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
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
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
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
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
            "[TradFi] Заблокировано %s контрактов (%s базовых тикеров) из API бирж.",
            len(_dynamic_tradfi_pairs) // 2,
            len(_dynamic_tradfi_bases),
        )


async def refresh_binance_alpha_tokens(session: aiohttp.ClientSession, force: bool = False) -> None:
    """
    Загружает актуальный список всех токенов Binance Alpha и их торговые пары
    (ALPHA_xxxUSDT / ALPHA_xxxUSDC) из официальных публичных эндпоинтов Binance Alpha.
    """
    global _alpha_last_refresh
    now = time.time()
    if not force and _alpha_markets and (now - _alpha_last_refresh) < 150.0:
        return

    async def _get(url: str) -> Optional[Any]:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception:
            return None
        return None

    tok_data, info_data = await asyncio.gather(
        _get("https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"),
        _get("https://www.binance.com/bapi/defi/v1/public/alpha-trade/get-exchange-info"),
    )

    pair_by_alpha_id: Dict[str, Tuple[str, str]] = {}
    info_symbols = ((info_data or {}).get("data") or {}).get("symbols") or []
    for s in info_symbols:
        if not isinstance(s, dict):
            continue
        status = str(s.get("status") or "").upper()
        if status and status != "TRADING":
            continue
        tsym = str(s.get("symbol") or "").strip().upper()
        b_asset = str(s.get("baseAsset") or "").strip().upper()
        q_asset = str(s.get("quoteAsset") or "").strip().upper()
        if not tsym or not b_asset or q_asset not in ("USDT", "USDC"):
            continue
        prev = pair_by_alpha_id.get(b_asset)
        if prev is None or (prev[1] != "USDT" and q_asset == "USDT"):
            pair_by_alpha_id[b_asset] = (tsym, q_asset)

    new_markets: Dict[str, Tuple[str, str, float]] = {}
    new_by_base: Dict[str, Tuple[str, str, float]] = {}

    tok_list = (tok_data or {}).get("data") or []
    for item in tok_list if isinstance(tok_list, list) else []:
        if not isinstance(item, dict):
            continue
        if item.get("offsell") is True:
            continue
        alpha_id = str(item.get("alphaId") or "").strip().upper()
        raw_sym = re.sub(r"[^A-Z0-9]", "", str(item.get("symbol") or "").strip().upper())
        if not alpha_id or not raw_sym:
            continue

        pair_info = pair_by_alpha_id.get(alpha_id)
        if pair_info is None:
            if pair_by_alpha_id:
                continue
            trade_pair, quote = f"{alpha_id}USDT", "USDT"
        else:
            trade_pair, quote = pair_info

        vol24 = _safe_float(item.get("volume24h"))
        display_sym = f"{raw_sym}{quote}"
        prev_m = new_markets.get(display_sym)
        if prev_m is None or vol24 > prev_m[2]:
            new_markets[display_sym] = (trade_pair, raw_sym, vol24)
        prev_b = new_by_base.get(raw_sym)
        if prev_b is None or vol24 > prev_b[2]:
            new_by_base[raw_sym] = (trade_pair, display_sym, vol24)

    if new_markets:
        _alpha_markets.clear()
        _alpha_markets.update(new_markets)
        _alpha_by_base.clear()
        _alpha_by_base.update(new_by_base)
        _alpha_last_refresh = now
        logger.info("[BinanceAlpha] Загружено %s активных Alpha-токенов.", len(_alpha_markets))


# ==================== НОРМАЛИЗАЦИЯ НАСТРОЕК И DISCORD ====================

def is_valid_discord_webhook(url: str) -> bool:
    clean = (url or "").strip()
    return bool(clean) and clean.startswith(VALID_WEBHOOK_PREFIXES) and len(clean) > 45


def parse_coin_list(raw: Any) -> List[str]:
    if not raw:
        return []
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            items = parsed if isinstance(parsed, list) else re.split(r"[,\s]+", raw)
        except Exception:
            items = re.split(r"[,\s]+", raw)
    else:
        return []
    out: List[str] = []
    for x in items:
        b = extract_base_asset(str(x))
        if b and b not in out:
            out.append(b)
    return out


def normalize_config(raw: Any) -> Dict[str, Any]:
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
    spot_default = legacy_wall if legacy_wall > 0 else cfg["min_wall_spot_usd"]
    fut_default = legacy_wall if legacy_wall > 0 else cfg["min_wall_futures_usd"]

    cfg["min_wall_spot_usd"] = round(max(10_000.0, _safe_float(data.get("min_wall_spot_usd"), spot_default)), 2)
    cfg["min_wall_futures_usd"] = round(max(10_000.0, _safe_float(data.get("min_wall_futures_usd"), fut_default)), 2)
    cfg["max_distance_pct"] = round(min(50.0, max(0.1, _safe_float(data.get("max_distance_pct"), cfg["max_distance_pct"]))), 2)
    cfg["min_anomaly_x"] = round(min(100.0, max(1.5, _safe_float(data.get("min_anomaly_x"), cfg["min_anomaly_x"]))), 1)
    cfg["min_lifetime_sec"] = round(min(600.0, max(0.0, _safe_float(data.get("min_lifetime_sec"), cfg["min_lifetime_sec"]))), 1)
    cfg["min_vol24h"] = round(max(0.0, _safe_float(data.get("min_vol24h"), cfg["min_vol24h"])), 2)
    cfg["cooldown_sec"] = round(min(7200.0, max(15.0, _safe_float(data.get("cooldown_sec"), cfg["cooldown_sec"]))), 1)

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
    u = str(dcfg.get("webhook_url") or "").strip()
    cand = u or ENV_DISCORD_WEBHOOK
    return cand if is_valid_discord_webhook(cand) else ""


def wall_matches_user_config(
    exchange: str,
    market_type: str,
    symbol: str,
    side: str,
    wall_usd: float,
    distance_pct: float,
    anomaly_x: float,
    lifetime_sec: float,
    volume_24h_usd: float,
    cfg: Dict[str, Any],
) -> bool:
    if exchange not in cfg.get("exchanges", ALL_EXCHANGES):
        return False
    mt = (market_type or "FUTURES").upper()
    mm = cfg.get("market_mode", "BOTH")
    if mm == "FUTURES" and mt != "FUTURES":
        return False
    if mm == "SPOT" and mt != "SPOT":
        return False
    watch = cfg.get("watch_coins") or []
    if watch and extract_base_asset(symbol) not in watch:
        return False
    sm = cfg.get("side_mode", "BOTH")
    if sm == "BID" and side != "BID":
        return False
    if sm == "ASK" and side != "ASK":
        return False
    min_wall = float(cfg.get("min_wall_spot_usd", 250_000.0)) if mt == "SPOT" else float(cfg.get("min_wall_futures_usd", 500_000.0))
    if wall_usd < min_wall:
        return False
    if distance_pct > float(cfg.get("max_distance_pct", 10.0)):
        return False
    if anomaly_x < float(cfg.get("min_anomaly_x", 5.0)):
        return False
    if lifetime_sec < float(cfg.get("min_lifetime_sec", 0.0)):
        return False
    if volume_24h_usd < float(cfg.get("min_vol24h", 500_000.0)):
        return False
    return True


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
            async with client.post(webhook_url, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status in (200, 204):
                    return True, "OK"
                if resp.status == 429 and attempt == 0:
                    try:
                        body = await resp.json()
                        wait = min(5.0, float(body.get("retry_after") or 1.5))
                    except Exception:
                        wait = 1.5
                    await asyncio.sleep(wait)
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
    is_bid = ev.side == WallSide.BID
    side_emoji = "🟢" if is_bid else "🔴"
    side_lbl = "BID (Лонг / Поддержка)" if is_bid else "ASK (Шорт / Сопротивление)"
    mkt_lbl = "Alpha" if ev.exchange == "BinanceAlpha" else ("Спот" if ev.market_type == "SPOT" else "Фьючерс")
    payload = {
        "username": "Density Wall Screener",
        "embeds": [
            {
                "title": f"🧱 {side_emoji} {ev.symbol} | {ev.exchange} ({mkt_lbl})",
                "color": COLOR_GREEN if is_bid else COLOR_RED,
                "fields": [
                    {"name": "Плотность", "value": f"**${ev.wall_usd:,.0f}** (`x{ev.anomaly_x:g}`)", "inline": True},
                    {"name": "Цена стены", "value": f"`{ev.wall_price:g}` ({ev.distance_pct:.2f}% от спреда)", "inline": True},
                    {"name": "Сторона", "value": side_lbl, "inline": True},
                    {"name": "Разъедание", "value": f"~**{ev.eat_minutes:g} мин**", "inline": True},
                    {"name": "Оборот 24ч", "value": f"**${ev.volume_24h_usd:,.0f}**", "inline": True},
                    {"name": "Текущая цена", "value": f"`{ev.current_price:g}`", "inline": True},
                ],
                "footer": {"text": f"Density Screener · {ev.timestamp_utc} UTC"},
            }
        ],
    }
    ok, _ = await _post_discord_webhook(webhook_url, payload, session=session)
    return ok


async def send_test_discord(
    webhook_url: str,
    session: Optional[aiohttp.ClientSession] = None,
) -> Tuple[bool, str]:
    payload = {
        "username": "Density Wall Screener",
        "embeds": [
            {
                "title": "✅ Тест Discord (Аномальные плотности)",
                "description": "Вебхук успешно подключён! Алерты по плотностям (8 бирж · Фьючерсы + Спот + Binance Alpha) будут приходить сюда.",
                "color": COLOR_BLUE,
            }
        ],
    }
    return await _post_discord_webhook(webhook_url, payload, session=session)


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
            logger.warning("PostgreSQL недоступна (%s) — переключаюсь на SQLite.", exc)
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
        u_cols = {r[1] for r in await cur_u.fetchall()}
        if "allowed_coins" not in u_cols:
            await db.execute("ALTER TABLE users ADD COLUMN allowed_coins TEXT DEFAULT '[]'")
        cur_d = await db.execute("PRAGMA table_info(densities)")
        d_cols = {r[1] for r in await cur_d.fetchall()}
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
    """Объединяет разблокированные (/allow) и отслеживаемые (/watch) монеты всех пользователей для сканеров."""
    global _whitelist_version
    users = await get_cached_users(force=True)
    merged: Set[str] = set()
    for u in users:
        merged.update(parse_coin_list(u.get("allowed_coins")))
        ucfg = normalize_config(u.get("config"))
        merged.update(ucfg.get("watch_coins") or [])
    if merged != _runtime_allowed_bases:
        _runtime_allowed_bases.clear()
        _runtime_allowed_bases.update(merged)
        _whitelist_version += 1
        logger.info("[Whitelist] Разблокированы монеты: %s", sorted(_runtime_allowed_bases) or "нет")


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
    density_engine.update_config(new_cfg)
    await sync_runtime_allowed_bases()
    return new_cfg, new_dcfg, new_bans, new_allowed


# ==================== ДВИЖОК АНОМАЛЬНЫХ ПЛОТНОСТЕЙ ====================

class DensityEngine:
    """
    Анализирует WebSocket и полные REST-стаканы (Фьючерсы, Спот, Binance Alpha)
    и находит аномальные плотности в диапазоне ±max_distance_pct от спреда:
    - Считает медианный уровень стакана (в USD) и кратность аномалии (wall_usd / median_level_usd).
    - При min_lifetime_sec = 0 отправляет алерт МГНОВЕННО при обнаружении плотности.
    - Считает расчётное время разъедания (wall_usd / средний объём за 1 минуту).
    """

    def __init__(self, on_density: Callable[[DensityEvent], Any]) -> None:
        self.on_density = on_density
        self.config: Dict[str, Any] = dict(DEFAULT_CONFIG)
        # (exchange, market_type, symbol) -> last_eval_ts
        self._last_eval: Dict[Tuple[str, str, str], float] = {}
        # (exchange, market_type, symbol, side, price_key) -> (first_seen_ts, wall_price)
        self._tracked: Dict[Tuple[str, str, str, WallSide, str], Tuple[float, float]] = {}
        # (exchange, market_type, symbol, side) -> (last_alert_ts, last_wall_price, last_wall_usd)
        self._cooldown: Dict[Tuple[str, str, str, WallSide], Tuple[float, float, float]] = {}
        # (exchange, market_type, symbol, side, price_key) -> (DensityEvent, last_seen_ts)
        self._active: Dict[Tuple[str, str, str, WallSide, str], Tuple[DensityEvent, float]] = {}

    def update_config(self, cfg: Dict[str, Any]) -> None:
        self.config = normalize_config(cfg)

    @staticmethod
    def _price_key(price: float) -> str:
        return f"{price:.8g}"

    async def process_orderbook(
        self,
        exchange: str,
        symbol: str,
        bids: List[Tuple[float, float]],
        asks: List[Tuple[float, float]],
        volume_24h_usd: float,
        market_type: str = "FUTURES",
        bypass_throttle: bool = False,
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
        if not bypass_throttle and (now - self._last_eval.get(pair_key, 0.0)) < 1.0:
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

        max_dist = float(cfg["max_distance_pct"])
        min_bid_bound = best_bid * (1.0 - max_dist / 100.0)
        max_ask_bound = best_ask * (1.0 + max_dist / 100.0)

        top_bids = [(px, px * qty) for px, qty in bids[:500] if px > 0 and qty > 0 and px >= min_bid_bound]
        top_asks = [(px, px * qty) for px, qty in asks[:500] if px > 0 and qty > 0 and px <= max_ask_bound]
        if len(top_bids) < 2 or len(top_asks) < 2:
            return

        all_sizes = [u for _, u in top_bids[:200]] + [u for _, u in top_asks[:200]]
        median_lvl = statistics.median(all_sizes)
        if median_lvl <= 0:
            return

        min_wall = (
            float(cfg["min_wall_spot_usd"])
            if mt == "SPOT"
            else float(cfg["min_wall_futures_usd"])
        )
        min_anom = float(cfg["min_anomaly_x"])
        min_life = float(cfg["min_lifetime_sec"])
        cooldown_sec = float(cfg["cooldown_sec"])
        side_mode = cfg["side_mode"]

        track_floor = max(10_000.0, min_wall * 0.75)
        seen_keys_now: Set[Tuple[str, str, str, WallSide, str]] = set()
        avg_vol_per_min = max(volume_24h_usd / 1440.0, 1.0)

        def check_side(
            side: WallSide,
            levels: List[Tuple[float, float]],
        ) -> List[Tuple[Tuple[str, str, str, WallSide, str], DensityEvent]]:
            found: List[Tuple[Tuple[str, str, str, WallSide, str], DensityEvent]] = []
            if side_mode == "BID" and side != WallSide.BID:
                return found
            if side_mode == "ASK" and side != WallSide.ASK:
                return found

            for px, usd in levels:
                if side == WallSide.BID:
                    dist_pct = max(0.0, (best_bid - px) / best_bid * 100.0)
                else:
                    dist_pct = max(0.0, (px - best_ask) / best_ask * 100.0)
                if dist_pct > max_dist:
                    continue
                if usd < track_floor:
                    continue
                anom_x = usd / median_lvl
                if anom_x < max(1.5, min_anom * 0.75):
                    continue

                wkey = (exchange, mt, symbol, side, self._price_key(px))
                seen_keys_now.add(wkey)
                tr_prev = self._tracked.get(wkey)
                if tr_prev is None:
                    self._tracked[wkey] = (now, px)
                    first_ts = now
                else:
                    first_ts = tr_prev[0]
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

        # Удаляем только исчезнувшие плотности внутри глубины текущего снимка стакана,
        # чтобы мелкие WS-обновления (depth20) не стирали глубокие стены (3–10%) из 3-минутного обхода
        deepest_bid = top_bids[-1][0]
        deepest_ask = top_asks[-1][0]
        stale: List[Tuple[str, str, str, WallSide, str]] = []
        for k, (_, tracked_px) in self._tracked.items():
            if k[0] == exchange and k[1] == mt and k[2] == symbol and k not in seen_keys_now:
                if (k[3] == WallSide.BID and tracked_px >= deepest_bid) or (
                    k[3] == WallSide.ASK and tracked_px <= deepest_ask
                ):
                    stale.append(k)
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
        expired = [k for k, (_, ts) in self._active.items() if now - ts > 210.0]
        for k in expired:
            self._active.pop(k, None)
        events = [ev for ev, _ in self._active.values()]
        events.sort(key=lambda e: e.wall_usd, reverse=True)
        return events


# ==================== ЖИВОЙ ПОИСК ПЛОТНОСТЕЙ ПО ОТДЕЛЬНОЙ МОНЕТЕ (8 БИРЖ) ====================

async def live_search_coin_densities(
    session: Optional[aiohttp.ClientSession],
    coin_raw: str,
    cfg: Dict[str, Any],
    override_min_spot: Optional[float] = None,
    override_min_fut: Optional[float] = None,
    override_max_dist: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Параллельный живой REST-снимок глубоких стаканов выбранной монеты по 8 биржам
    (Фьючерсы, Спот и Binance Alpha) в диапазоне ±max_distance_pct от спреда.
    """
    base = extract_base_asset(coin_raw)
    if not base:
        return {"coin": "", "matches": [], "top_seen": [], "checked_books": 0}

    min_spot = max(1_000.0, float(override_min_spot)) if override_min_spot is not None else float(cfg.get("min_wall_spot_usd", 250_000.0))
    min_fut = max(1_000.0, float(override_min_fut)) if override_min_fut is not None else float(cfg.get("min_wall_futures_usd", 500_000.0))
    max_dist = min(50.0, max(0.1, float(override_max_dist))) if override_max_dist is not None else float(cfg.get("max_distance_pct", 10.0))
    min_anom = float(cfg.get("min_anomaly_x", 5.0))
    side_mode = str(cfg.get("side_mode", "BOTH")).upper()
    market_mode = str(cfg.get("market_mode", "BOTH")).upper()
    exchanges = list(cfg.get("exchanges", ALL_EXCHANGES))

    own_session = session is None or session.closed
    client = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) if own_session else session

    if "BinanceAlpha" in exchanges and not _alpha_by_base:
        await refresh_binance_alpha_tokens(client, force=True)

    async def _get_json(url: str) -> Optional[Any]:
        try:
            async with client.get(url, timeout=aiohttp.ClientTimeout(total=6)) as resp:
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
                    f"https://fapi.binance.com/fapi/v1/depth?symbol={sym_std}&limit=500"
                    if mt == "FUTURES"
                    else f"https://api.binance.com/api/v3/depth?symbol={sym_std}&limit=500"
                )
                d = await _get_json(url)
                if not isinstance(d, dict):
                    return None
                bids = [(_safe_float(p), _safe_float(q)) for p, q in (d.get("bids") or [])]
                asks = [(_safe_float(p), _safe_float(q)) for p, q in (d.get("asks") or [])]
                return ex, mt, sym_std, bids, asks

            if ex == "BinanceAlpha":
                if mt != "SPOT":
                    return None
                alpha_info = _alpha_by_base.get(base)
                if not alpha_info:
                    return None
                trade_pair, display_sym, _ = alpha_info
                d = await _get_json(
                    f"https://www.binance.com/bapi/defi/v1/public/alpha-trade/fullDepth?symbol={trade_pair}&limit=500"
                )
                body = (d or {}).get("data") if isinstance(d, dict) and isinstance(d.get("data"), dict) else d
                if not isinstance(body, dict):
                    return None
                bids = [(_safe_float(p), _safe_float(q)) for p, q in (body.get("bids") or [])]
                asks = [(_safe_float(p), _safe_float(q)) for p, q in (body.get("asks") or [])]
                return ex, "SPOT", display_sym, bids, asks

            if ex == "Aster":
                url = (
                    f"https://fapi.asterdex.com/fapi/v1/depth?symbol={sym_std}&limit=500"
                    if mt == "FUTURES"
                    else f"https://sapi.asterdex.com/api/v1/depth?symbol={sym_std}&limit=500"
                )
                d = await _get_json(url)
                if not isinstance(d, dict):
                    return None
                bids = [(_safe_float(p), _safe_float(q)) for p, q in (d.get("bids") or [])]
                asks = [(_safe_float(p), _safe_float(q)) for p, q in (d.get("asks") or [])]
                return ex, mt, sym_std, bids, asks

            if ex == "Bybit":
                cat = "linear" if mt == "FUTURES" else "spot"
                d = await _get_json(f"https://api.bybit.com/v5/market/orderbook?category={cat}&symbol={sym_std}&limit=200")
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
                d = await _get_json(f"https://www.okx.com/api/v5/market/books?instId={iid}&sz=200")
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
                    f"https://api.bitget.com/api/v2/mix/market/merge-depth?productType=USDT-FUTURES&symbol={sym_std}&limit=100"
                    if mt == "FUTURES"
                    else f"https://api.bitget.com/api/v2/spot/market/merge-depth?symbol={sym_std}&limit=100"
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
                        _get_json(f"https://api.gateio.ws/api/v4/futures/usdt/order_book?contract={pair}&limit=100"),
                    )
                    if not isinstance(d, dict):
                        return None
                    mult = _safe_float((c_info or {}).get("quanto_multiplier"), 1.0) if isinstance(c_info, dict) else 1.0
                    bids = [(_safe_float(x.get("p")), abs(_safe_float(x.get("s"))) * mult) for x in (d.get("bids") or []) if isinstance(x, dict)]
                    asks = [(_safe_float(x.get("p")), abs(_safe_float(x.get("s"))) * mult) for x in (d.get("asks") or []) if isinstance(x, dict)]
                    return ex, mt, sym_std, bids, asks
                d = await _get_json(f"https://api.gateio.ws/api/v4/spot/order_book?currency_pair={pair}&limit=100")
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
                        _get_json(f"https://api-futures.kucoin.com/api/v1/level2/depth100?symbol={ksym}"),
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
                d = await _get_json(f"https://api.kucoin.com/api/v1/market/orderbook/level2_100?symbol={ksym}")
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
    jobs = [
        _fetch_book(ex, mt)
        for ex in exchanges
        for mt in markets_to_scan
        if not (ex == "BinanceAlpha" and mt != "SPOT")
    ]
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
        min_b = best_bid * (1.0 - max_dist / 100.0)
        max_a = best_ask * (1.0 + max_dist / 100.0)
        top_b = [(px, px * q) for px, q in bids[:500] if px > 0 and q > 0 and px >= min_b]
        top_a = [(px, px * q) for px, q in asks[:500] if px > 0 and q > 0 and px <= max_a]
        all_sz = [u for _, u in top_b[:200]] + [u for _, u in top_a[:200]]
        if len(all_sz) < 2:
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
                dist = (
                    max(0.0, (best_bid - px) / best_bid * 100.0)
                    if side_str == "BID"
                    else max(0.0, (px - best_ask) / best_ask * 100.0)
                )
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


# ==================== WEBSOCKET-СКАНЕРЫ СТАКАНОВ (ТОП-ПАРЫ В РЕАЛТАЙМЕ) ====================

def _prioritize_symbols(vols: Dict[str, float], limit: int) -> List[str]:
    """Ставит разблокированные (/allow) и отслеживаемые (/watch) монеты в начало списка подписки."""
    prio: List[str] = []
    rest: List[str] = []
    for sym in sorted(vols.keys(), key=lambda s: vols[s], reverse=True):
        if extract_base_asset(sym) in _runtime_allowed_bases:
            prio.append(sym)
        else:
            rest.append(sym)
    return (prio + rest)[:limit]


async def _binance_like_depth_ws(
    session: aiohttp.ClientSession,
    eng: DensityEngine,
    exchange: str,
    market_type: str,
    ticker_url: str,
    ws_base: str,
    stream_suffix: str,
    max_pairs: int,
) -> None:
    """WebSocket стаканов для Binance и Aster (Фьючерсы и Спот)."""
    while True:
        wl_ver = _whitelist_version
        try:
            async with session.get(ticker_url, timeout=aiohttp.ClientTimeout(total=12)) as resp:
                if resp.status != 200:
                    await asyncio.sleep(15)
                    continue
                tickers = await resp.json()

            vols: Dict[str, float] = {}
            for it in tickers if isinstance(tickers, list) else []:
                sym = str(it.get("symbol") or "")
                if not sym.endswith("USDT") or is_blacklisted_symbol(sym, exchange):
                    continue
                v = _safe_float(it.get("quoteVolume"))
                if v >= 150_000 or extract_base_asset(sym) in _runtime_allowed_bases:
                    vols[sym] = v

            top_syms = _prioritize_symbols(vols, max_pairs)
            if not top_syms:
                await asyncio.sleep(20)
                continue

            streams = "/".join(f"{s.lower()}@{stream_suffix}" for s in top_syms)
            ws_url = f"{ws_base}/stream?streams={streams}"
            started = time.time()

            async with session.ws_connect(ws_url, heartbeat=20, timeout=15) as ws:
                logger.info("[%s %s Depth WS] Подписан на %s пар", exchange, market_type, len(top_syms))
                async for msg in ws:
                    if time.time() - started > 1800 or wl_ver != _whitelist_version:
                        break
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                        continue
                    payload = json.loads(msg.data)
                    stream_name = str(payload.get("stream") or "")
                    d = payload.get("data")
                    if not isinstance(d, dict):
                        continue
                    sym = d.get("s") or stream_name.split("@")[0].upper()
                    raw_b = d.get("b") or d.get("bids") or []
                    raw_a = d.get("a") or d.get("asks") or []
                    if not sym or not raw_b or not raw_a:
                        continue
                    bids = [(_safe_float(p), _safe_float(q)) for p, q in raw_b]
                    asks = [(_safe_float(p), _safe_float(q)) for p, q in raw_a]
                    await eng.process_orderbook(exchange, sym, bids, asks, vols.get(sym, 0.0), market_type=market_type)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[%s %s Depth WS] Переподключение через 6с: %s", exchange, market_type, exc)
            await asyncio.sleep(6)


async def bybit_depth_ws(session: aiohttp.ClientSession, eng: DensityEngine, market_type: str) -> None:
    """WebSocket стаканов для Bybit (linear / spot)."""
    is_fut = market_type == "FUTURES"
    cat = "linear" if is_fut else "spot"
    ws_url = f"wss://stream.bybit.com/v5/public/{cat}"
    while True:
        wl_ver = _whitelist_version
        try:
            async with session.get(
                f"https://api.bybit.com/v5/market/tickers?category={cat}",
                timeout=aiohttp.ClientTimeout(total=12),
            ) as resp:
                data = await resp.json()

            vols: Dict[str, float] = {}
            for it in (data.get("result") or {}).get("list") or []:
                sym = str(it.get("symbol") or "")
                if not sym.endswith("USDT") or is_blacklisted_symbol(sym, "Bybit"):
                    continue
                v = _safe_float(it.get("turnover24h"))
                if v >= 150_000 or extract_base_asset(sym) in _runtime_allowed_bases:
                    vols[sym] = v

            top_syms = _prioritize_symbols(vols, 70 if is_fut else 50)
            if not top_syms:
                await asyncio.sleep(20)
                continue

            books: Dict[str, Tuple[Dict[float, float], Dict[float, float]]] = {}
            started = time.time()
            async with session.ws_connect(ws_url, heartbeat=20, timeout=15) as ws:
                for i in range(0, len(top_syms), 10):
                    args = [f"orderbook.50.{s}" for s in top_syms[i : i + 10]]
                    await ws.send_json({"op": "subscribe", "args": args})
                logger.info("[Bybit %s Depth WS] Подписан на %s пар", market_type, len(top_syms))

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
                        if time.time() - started > 1800 or wl_ver != _whitelist_version:
                            break
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                            continue
                        payload = json.loads(msg.data)
                        topic = str(payload.get("topic") or "")
                        if not topic.startswith("orderbook."):
                            continue
                        d = payload.get("data")
                        if not isinstance(d, dict):
                            continue
                        sym = d.get("s") or topic.split(".")[-1]
                        if not sym:
                            continue
                        ev_type = payload.get("type")
                        if ev_type == "snapshot" or sym not in books:
                            b_map: Dict[float, float] = {}
                            a_map: Dict[float, float] = {}
                            books[sym] = (b_map, a_map)
                        else:
                            b_map, a_map = books[sym]

                        for p_s, q_s in d.get("b") or []:
                            px, qty = _safe_float(p_s), _safe_float(q_s)
                            if px > 0:
                                if qty == 0:
                                    b_map.pop(px, None)
                                else:
                                    b_map[px] = qty
                        for p_s, q_s in d.get("a") or []:
                            px, qty = _safe_float(p_s), _safe_float(q_s)
                            if px > 0:
                                if qty == 0:
                                    a_map.pop(px, None)
                                else:
                                    a_map[px] = qty

                        bids = sorted(b_map.items(), key=lambda x: x[0], reverse=True)[:50]
                        asks = sorted(a_map.items(), key=lambda x: x[0])[:50]
                        await eng.process_orderbook("Bybit", sym, bids, asks, vols.get(sym, 0.0), market_type=market_type)
                finally:
                    pt.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[Bybit %s Depth WS] Переподключение через 6с: %s", market_type, exc)
            await asyncio.sleep(6)


async def okx_depth_ws(session: aiohttp.ClientSession, eng: DensityEngine, market_type: str) -> None:
    """WebSocket стаканов для OKX (SWAP / SPOT)."""
    is_fut = market_type == "FUTURES"
    inst_type = "SWAP" if is_fut else "SPOT"
    suffix = "-USDT-SWAP" if is_fut else "-USDT"
    while True:
        wl_ver = _whitelist_version
        try:
            ct_vals: Dict[str, float] = {}
            if is_fut:
                async with session.get(
                    "https://www.okx.com/api/v5/public/instruments?instType=SWAP",
                    timeout=aiohttp.ClientTimeout(total=12),
                ) as resp:
                    inst_data = await resp.json()
                for it in inst_data.get("data") or []:
                    iid = str(it.get("instId") or "")
                    if iid.endswith("-USDT-SWAP"):
                        ct_vals[iid] = _safe_float(it.get("ctVal"), 1.0)

            async with session.get(
                f"https://www.okx.com/api/v5/market/tickers?instType={inst_type}",
                timeout=aiohttp.ClientTimeout(total=12),
            ) as resp:
                tk_data = await resp.json()

            vols: Dict[str, float] = {}
            for it in tk_data.get("data") or []:
                iid = str(it.get("instId") or "")
                if not iid.endswith(suffix) or is_blacklisted_symbol(iid, "OKX"):
                    continue
                v = _safe_float(it.get("volCcy24h")) if not is_fut else _safe_float(it.get("volCcy24h")) * _safe_float(it.get("last"))
                if v >= 150_000 or extract_base_asset(iid) in _runtime_allowed_bases:
                    vols[iid] = v

            top_ids = _prioritize_symbols(vols, 55 if is_fut else 45)
            if not top_ids:
                await asyncio.sleep(20)
                continue

            books: Dict[str, Tuple[Dict[float, float], Dict[float, float]]] = {}
            started = time.time()
            async with session.ws_connect("wss://ws.okx.com:8443/ws/v5/public", heartbeat=20, timeout=15) as ws:
                for i in range(0, len(top_ids), 15):
                    args = [{"channel": "books", "instId": iid} for iid in top_ids[i : i + 15]]
                    await ws.send_json({"op": "subscribe", "args": args})
                logger.info("[OKX %s Depth WS] Подписан на %s пар", market_type, len(top_ids))

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
                        if time.time() - started > 1800 or wl_ver != _whitelist_version:
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
                        iid = arg.get("instId") or ""
                        arr = payload.get("data") or []
                        if not iid or not arr:
                            continue
                        d = arr[0]
                        ct = ct_vals.get(iid, 1.0) if is_fut else 1.0
                        action = payload.get("action")
                        if action == "snapshot" or iid not in books:
                            b_map: Dict[float, float] = {}
                            a_map: Dict[float, float] = {}
                            books[iid] = (b_map, a_map)
                        else:
                            b_map, a_map = books[iid]

                        for item in d.get("bids") or []:
                            if len(item) >= 2:
                                px, qty = _safe_float(item[0]), _safe_float(item[1]) * ct
                                if px > 0:
                                    if qty == 0:
                                        b_map.pop(px, None)
                                    else:
                                        b_map[px] = qty
                        for item in d.get("asks") or []:
                            if len(item) >= 2:
                                px, qty = _safe_float(item[0]), _safe_float(item[1]) * ct
                                if px > 0:
                                    if qty == 0:
                                        a_map.pop(px, None)
                                    else:
                                        a_map[px] = qty

                        bids = sorted(b_map.items(), key=lambda x: x[0], reverse=True)[:50]
                        asks = sorted(a_map.items(), key=lambda x: x[0])[:50]
                        sym_clean = iid.replace("-USDT-SWAP", "USDT").replace("-USDT", "USDT")
                        await eng.process_orderbook("OKX", sym_clean, bids, asks, vols.get(iid, 0.0), market_type=market_type)
                finally:
                    pt.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[OKX %s Depth WS] Переподключение через 6с: %s", market_type, exc)
            await asyncio.sleep(6)


async def bitget_depth_ws(session: aiohttp.ClientSession, eng: DensityEngine, market_type: str) -> None:
    """WebSocket стаканов для Bitget (USDT-FUTURES / SPOT)."""
    is_fut = market_type == "FUTURES"
    inst_type = "USDT-FUTURES" if is_fut else "SPOT"
    tk_url = (
        "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES"
        if is_fut
        else "https://api.bitget.com/api/v2/spot/market/tickers"
    )
    while True:
        wl_ver = _whitelist_version
        try:
            async with session.get(tk_url, timeout=aiohttp.ClientTimeout(total=12)) as resp:
                data = await resp.json()

            vols: Dict[str, float] = {}
            for it in data.get("data") or []:
                sym = str(it.get("symbol") or "")
                if not sym.endswith("USDT") or is_blacklisted_symbol(sym, "Bitget"):
                    continue
                v = _safe_float(it.get("usdtVolume"))
                if v >= 150_000 or extract_base_asset(sym) in _runtime_allowed_bases:
                    vols[sym] = v

            top_syms = _prioritize_symbols(vols, 60 if is_fut else 45)
            if not top_syms:
                await asyncio.sleep(20)
                continue

            started = time.time()
            async with session.ws_connect("wss://ws.bitget.com/v2/ws/public", heartbeat=20, timeout=15) as ws:
                for i in range(0, len(top_syms), 15):
                    args = [
                        {"instType": inst_type, "channel": "books15", "instId": s}
                        for s in top_syms[i : i + 15]
                    ]
                    await ws.send_json({"op": "subscribe", "args": args})
                logger.info("[Bitget %s Depth WS] Подписан на %s пар", market_type, len(top_syms))

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
                        if time.time() - started > 1800 or wl_ver != _whitelist_version:
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
            logger.warning("[Bitget %s Depth WS] Переподключение через 6с: %s", market_type, exc)
            await asyncio.sleep(6)


async def gate_depth_ws(session: aiohttp.ClientSession, eng: DensityEngine, market_type: str) -> None:
    """WebSocket стаканов для Gate.io (Фьючерсы и Спот)."""
    is_fut = market_type == "FUTURES"
    while True:
        wl_ver = _whitelist_version
        try:
            q_mults: Dict[str, float] = {}
            if is_fut:
                async with session.get(
                    "https://api.gateio.ws/api/v4/futures/usdt/contracts",
                    timeout=aiohttp.ClientTimeout(total=12),
                ) as resp:
                    c_data = await resp.json()
                if isinstance(c_data, list):
                    for it in c_data:
                        n = str(it.get("name") or "")
                        if n:
                            q_mults[n] = _safe_float(it.get("quanto_multiplier"), 1.0)

            tk_url = (
                "https://api.gateio.ws/api/v4/futures/usdt/tickers"
                if is_fut
                else "https://api.gateio.ws/api/v4/spot/tickers"
            )
            async with session.get(tk_url, timeout=aiohttp.ClientTimeout(total=12)) as resp:
                tk_data = await resp.json()

            vols: Dict[str, float] = {}
            if isinstance(tk_data, list):
                for it in tk_data:
                    c = str(it.get("contract") if is_fut else it.get("currency_pair") or "")
                    if not c.endswith("_USDT") or is_blacklisted_symbol(c, "Gate"):
                        continue
                    v = (
                        (_safe_float(it.get("volume_24h_quote")) or _safe_float(it.get("volume_24h_settle")))
                        if is_fut
                        else _safe_float(it.get("quote_volume"))
                    )
                    if v >= 150_000 or extract_base_asset(c) in _runtime_allowed_bases:
                        vols[c] = v

            top_pairs = _prioritize_symbols(vols, 55 if is_fut else 45)
            if not top_pairs:
                await asyncio.sleep(20)
                continue

            ws_url = "wss://fx-ws.gateio.ws/v4/ws/usdt" if is_fut else "wss://api.gateio.ws/ws/v4/"
            ch_name = "futures.order_book" if is_fut else "spot.order_book"
            ping_ch = "futures.ping" if is_fut else "spot.ping"
            started = time.time()
            async with session.ws_connect(ws_url, heartbeat=20, timeout=15) as ws:
                for p in top_pairs:
                    await ws.send_json(
                        {
                            "time": int(time.time()),
                            "channel": ch_name,
                            "event": "subscribe",
                            "payload": [p, "20", "1000ms"] if not is_fut else [p, "20", "0"],
                        }
                    )
                    await asyncio.sleep(0.03)
                logger.info("[Gate %s Depth WS] Подписан на %s пар", market_type, len(top_pairs))

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
                        if time.time() - started > 1800 or wl_ver != _whitelist_version:
                            break
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                            continue
                        payload = json.loads(msg.data)
                        if payload.get("channel") != ch_name:
                            continue
                        res = payload.get("result")
                        if not isinstance(res, dict):
                            continue
                        if is_fut:
                            c = res.get("contract") or ""
                            if not c:
                                continue
                            mult = q_mults.get(c, 1.0)
                            bids = [(_safe_float(x.get("p")), abs(_safe_float(x.get("s"))) * mult) for x in (res.get("bids") or []) if isinstance(x, dict)]
                            asks = [(_safe_float(x.get("p")), abs(_safe_float(x.get("s"))) * mult) for x in (res.get("asks") or []) if isinstance(x, dict)]
                        else:
                            c = res.get("s") or ""
                            if not c:
                                continue
                            bids = [(_safe_float(x[0]), _safe_float(x[1])) for x in (res.get("bids") or []) if isinstance(x, list) and len(x) >= 2]
                            asks = [(_safe_float(x[0]), _safe_float(x[1])) for x in (res.get("asks") or []) if isinstance(x, list) and len(x) >= 2]
                        if bids and asks:
                            await eng.process_orderbook("Gate", c.replace("_USDT", "USDT"), bids, asks, vols.get(c, 0.0), market_type=market_type)
                finally:
                    pt.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[Gate %s Depth WS] Переподключение через 6с: %s", market_type, exc)
            await asyncio.sleep(6)


async def kucoin_depth_ws(session: aiohttp.ClientSession, eng: DensityEngine, market_type: str) -> None:
    """WebSocket стаканов для KuCoin (Фьючерсы и Спот)."""
    is_fut = market_type == "FUTURES"
    bullet_url = (
        "https://api-futures.kucoin.com/api/v1/bullet-public"
        if is_fut
        else "https://api.kucoin.com/api/v1/bullet-public"
    )
    while True:
        wl_ver = _whitelist_version
        try:
            mults: Dict[str, float] = {}
            vols: Dict[str, float] = {}

            if is_fut:
                async with session.get(
                    "https://api-futures.kucoin.com/api/v1/contracts/active",
                    timeout=aiohttp.ClientTimeout(total=12),
                ) as resp:
                    c_data = await resp.json()
                for it in c_data.get("data") or []:
                    sym = str(it.get("symbol") or "")
                    if not sym.endswith("USDTM") or str(it.get("status") or "") != "Open":
                        continue
                    if is_blacklisted_symbol(sym, "KuCoin"):
                        continue
                    v = _safe_float(it.get("turnoverOf24h"))
                    if v >= 150_000 or extract_base_asset(sym) in _runtime_allowed_bases:
                        vols[sym] = v
                        mults[sym] = _safe_float(it.get("multiplier"), 1.0)
            else:
                async with session.get(
                    "https://api.kucoin.com/api/v1/market/allTickers",
                    timeout=aiohttp.ClientTimeout(total=12),
                ) as resp:
                    tk_data = await resp.json()
                for it in (tk_data.get("data") or {}).get("ticker") or []:
                    sym = str(it.get("symbol") or "")
                    if not sym.endswith("-USDT") or is_blacklisted_symbol(sym, "KuCoin"):
                        continue
                    v = _safe_float(it.get("volValue"))
                    if v >= 150_000 or extract_base_asset(sym) in _runtime_allowed_bases:
                        vols[sym] = v

            top_syms = _prioritize_symbols(vols, 50 if is_fut else 40)
            if not top_syms:
                await asyncio.sleep(20)
                continue

            async with session.post(bullet_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                b_data = await resp.json()
            b_info = b_data.get("data") or {}
            token = b_info.get("token")
            servers = b_info.get("instanceServers") or []
            if not token or not servers:
                await asyncio.sleep(15)
                continue
            endpoint = servers[0].get("endpoint")
            ping_iv = max(10.0, _safe_float(servers[0].get("pingInterval"), 18000.0) / 1000.0 * 0.7)
            ws_url = f"{endpoint}?token={token}&connectId={int(time.time() * 1000)}"

            started = time.time()
            async with session.ws_connect(ws_url, heartbeat=25, timeout=15) as ws:
                if is_fut:
                    for idx, s in enumerate(top_syms):
                        await ws.send_json(
                            {
                                "id": f"sub_{idx}",
                                "type": "subscribe",
                                "topic": f"/contractMarket/level2Depth50:{s}",
                                "privateChannel": False,
                                "response": False,
                            }
                        )
                        await asyncio.sleep(0.05)
                else:
                    for i in range(0, len(top_syms), 10):
                        chunk = ",".join(top_syms[i : i + 10])
                        await ws.send_json(
                            {
                                "id": f"sub_{i}",
                                "type": "subscribe",
                                "topic": f"/spotMarket/level2Depth50:{chunk}",
                                "privateChannel": False,
                                "response": False,
                            }
                        )
                        await asyncio.sleep(0.08)

                logger.info("[KuCoin %s Depth WS] Подписан на %s пар", market_type, len(top_syms))

                async def pinger() -> None:
                    while True:
                        await asyncio.sleep(ping_iv)
                        try:
                            await ws.send_json({"id": str(int(time.time())), "type": "ping"})
                        except Exception:
                            break

                pt = asyncio.create_task(pinger())
                try:
                    async for msg in ws:
                        if time.time() - started > 1800 or wl_ver != _whitelist_version:
                            break
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                            continue
                        payload = json.loads(msg.data)
                        if payload.get("type") != "message":
                            continue
                        topic = str(payload.get("topic") or "")
                        if ":" not in topic:
                            continue
                        raw_sym = topic.split(":", 1)[1]
                        d = payload.get("data") or {}
                        raw_b = d.get("bids") or []
                        raw_a = d.get("asks") or []
                        if not raw_b or not raw_a:
                            continue
                        mult = mults.get(raw_sym, 1.0) if is_fut else 1.0
                        bids = [(_safe_float(x[0]), _safe_float(x[1]) * mult) for x in raw_b if len(x) >= 2]
                        asks = [(_safe_float(x[0]), _safe_float(x[1]) * mult) for x in raw_a if len(x) >= 2]
                        sym_clean = _normalize_pair_key(raw_sym)
                        await eng.process_orderbook("KuCoin", sym_clean, bids, asks, vols.get(raw_sym, 0.0), market_type=market_type)
                finally:
                    pt.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[KuCoin %s Depth WS] Переподключение через 8с: %s", market_type, exc)
            await asyncio.sleep(8)


async def _tradfi_refresher_loop(session: aiohttp.ClientSession) -> None:
    while True:
        await asyncio.sleep(1800)
        try:
            await refresh_tradfi_metadata(session)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass


# ==================== ПОЛНЫЙ ОБХОД ВСЕХ МОНЕТ НА 8 БИРЖАХ РАЗ В 3 МИНУТЫ (±10% ОТ СПРЕДА) ====================

async def _fetch_rest_safe(session: aiohttp.ClientSession, url: str, timeout_sec: float = 8.0) -> Tuple[int, Optional[Any]]:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout_sec)) as resp:
            if resp.status == 200:
                return 200, await resp.json()
            return resp.status, None
    except Exception:
        return 0, None


async def full_market_scan_loop(
    session: aiohttp.ClientSession,
    eng: DensityEngine,
    exchange: str,
    market_type: str,
) -> None:
    """
    Каждые 3 минуты (FULL_SCAN_INTERVAL_SEC) проверяет глубокие стаканы (до ±10% от спреда)
    по ВСЕМ подходящим монетам на бирже (включая BinanceAlpha) и при обнаружении плотности
    сразу отправляет алерт.
    """
    stagger = (ALL_EXCHANGES.index(exchange) * 1.5 if exchange in ALL_EXCHANGES else 1.0) + (3.0 if market_type == "SPOT" else 0.0)
    await asyncio.sleep(stagger)

    pace_sec = {
        ("Binance", "FUTURES"): 0.30,
        ("Binance", "SPOT"): 0.38,
        ("BinanceAlpha", "SPOT"): 0.25,
        ("Bybit", "FUTURES"): 0.14,
        ("Bybit", "SPOT"): 0.14,
        ("OKX", "FUTURES"): 0.15,
        ("OKX", "SPOT"): 0.15,
        ("Bitget", "FUTURES"): 0.14,
        ("Bitget", "SPOT"): 0.14,
        ("Gate", "FUTURES"): 0.14,
        ("Gate", "SPOT"): 0.14,
        ("KuCoin", "FUTURES"): 0.18,
        ("KuCoin", "SPOT"): 0.18,
        ("Aster", "FUTURES"): 0.22,
        ("Aster", "SPOT"): 0.22,
    }.get((exchange, market_type), 0.20)

    while True:
        started = time.time()
        try:
            cfg = eng.config
            if exchange not in cfg.get("exchanges", ALL_EXCHANGES):
                await asyncio.sleep(30.0)
                continue
            mm = cfg.get("market_mode", "BOTH")
            if (mm == "FUTURES" and market_type != "FUTURES") or (mm == "SPOT" and market_type != "SPOT"):
                await asyncio.sleep(30.0)
                continue

            min_vol = float(cfg.get("min_vol24h", 500_000.0))
            watch_coins = set(cfg.get("watch_coins") or [])
            # Список целей: (api_symbol, display_symbol, vol24h_usd, contract_mult)
            targets: List[Tuple[str, str, float, float]] = []

            if exchange == "BinanceAlpha":
                await refresh_binance_alpha_tokens(session, force=True)
                for disp_sym, (trade_pair, base_t, vol) in _alpha_markets.items():
                    if is_blacklisted_symbol(disp_sym, "BinanceAlpha"):
                        continue
                    if watch_coins and base_t not in watch_coins:
                        continue
                    if vol >= min_vol or base_t in _runtime_allowed_bases:
                        targets.append((trade_pair, disp_sym, vol, 1.0))

            elif exchange in ("Binance", "Aster"):
                base_url = (
                    ("https://fapi.binance.com" if market_type == "FUTURES" else "https://api.binance.com")
                    if exchange == "Binance"
                    else ("https://fapi.asterdex.com" if market_type == "FUTURES" else "https://sapi.asterdex.com")
                )
                tk_path = (
                    "/fapi/v1/ticker/24hr"
                    if market_type == "FUTURES"
                    else ("/api/v3/ticker/24hr" if exchange == "Binance" else "/api/v1/ticker/24hr")
                )
                st, tk_data = await _fetch_rest_safe(session, base_url + tk_path, timeout_sec=12.0)
                if st == 200 and isinstance(tk_data, list):
                    for it in tk_data:
                        sym = str(it.get("symbol") or "")
                        if not sym.endswith("USDT") or is_blacklisted_symbol(sym, exchange):
                            continue
                        base_t = extract_base_asset(sym)
                        if watch_coins and base_t not in watch_coins:
                            continue
                        vol = _safe_float(it.get("quoteVolume"))
                        if vol >= min_vol or base_t in _runtime_allowed_bases:
                            targets.append((sym, sym, vol, 1.0))

            elif exchange == "Bybit":
                cat = "linear" if market_type == "FUTURES" else "spot"
                st, tk_data = await _fetch_rest_safe(
                    session, f"https://api.bybit.com/v5/market/tickers?category={cat}", timeout_sec=12.0
                )
                if st == 200 and isinstance(tk_data, dict):
                    for it in (tk_data.get("result") or {}).get("list") or []:
                        sym = str(it.get("symbol") or "")
                        if not sym.endswith("USDT") or is_blacklisted_symbol(sym, "Bybit"):
                            continue
                        base_t = extract_base_asset(sym)
                        if watch_coins and base_t not in watch_coins:
                            continue
                        vol = _safe_float(it.get("turnover24h"))
                        if vol >= min_vol or base_t in _runtime_allowed_bases:
                            targets.append((sym, sym, vol, 1.0))

            elif exchange == "OKX":
                inst_type = "SWAP" if market_type == "FUTURES" else "SPOT"
                suffix = "-USDT-SWAP" if market_type == "FUTURES" else "-USDT"
                ct_vals: Dict[str, float] = {}
                if market_type == "FUTURES":
                    st_i, inst_d = await _fetch_rest_safe(
                        session, "https://www.okx.com/api/v5/public/instruments?instType=SWAP", timeout_sec=12.0
                    )
                    if st_i == 200 and isinstance(inst_d, dict):
                        for it in inst_d.get("data") or []:
                            iid = str(it.get("instId") or "")
                            if iid.endswith("-USDT-SWAP"):
                                ct_vals[iid] = _safe_float(it.get("ctVal"), 1.0)
                st, tk_data = await _fetch_rest_safe(
                    session, f"https://www.okx.com/api/v5/market/tickers?instType={inst_type}", timeout_sec=12.0
                )
                if st == 200 and isinstance(tk_data, dict):
                    for it in tk_data.get("data") or []:
                        iid = str(it.get("instId") or "")
                        if not iid.endswith(suffix) or is_blacklisted_symbol(iid, "OKX"):
                            continue
                        base_t = extract_base_asset(iid)
                        if watch_coins and base_t not in watch_coins:
                            continue
                        vol = (
                            _safe_float(it.get("volCcy24h"))
                            if market_type == "SPOT"
                            else _safe_float(it.get("volCcy24h")) * _safe_float(it.get("last"))
                        )
                        if vol >= min_vol or base_t in _runtime_allowed_bases:
                            disp = iid.replace("-USDT-SWAP", "USDT").replace("-USDT", "USDT")
                            targets.append((iid, disp, vol, ct_vals.get(iid, 1.0)))

            elif exchange == "Bitget":
                tk_url = (
                    "https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES"
                    if market_type == "FUTURES"
                    else "https://api.bitget.com/api/v2/spot/market/tickers"
                )
                st, tk_data = await _fetch_rest_safe(session, tk_url, timeout_sec=12.0)
                if st == 200 and isinstance(tk_data, dict):
                    for it in tk_data.get("data") or []:
                        sym = str(it.get("symbol") or "")
                        if not sym.endswith("USDT") or is_blacklisted_symbol(sym, "Bitget"):
                            continue
                        base_t = extract_base_asset(sym)
                        if watch_coins and base_t not in watch_coins:
                            continue
                        vol = _safe_float(it.get("usdtVolume"))
                        if vol >= min_vol or base_t in _runtime_allowed_bases:
                            targets.append((sym, sym, vol, 1.0))

            elif exchange == "Gate":
                is_fut = market_type == "FUTURES"
                q_mults: Dict[str, float] = {}
                if is_fut:
                    st_c, c_data = await _fetch_rest_safe(
                        session, "https://api.gateio.ws/api/v4/futures/usdt/contracts", timeout_sec=12.0
                    )
                    if st_c == 200 and isinstance(c_data, list):
                        for it in c_data:
                            n = str(it.get("name") or "")
                            if n:
                                q_mults[n] = _safe_float(it.get("quanto_multiplier"), 1.0)
                tk_url = (
                    "https://api.gateio.ws/api/v4/futures/usdt/tickers"
                    if is_fut
                    else "https://api.gateio.ws/api/v4/spot/tickers"
                )
                st, tk_data = await _fetch_rest_safe(session, tk_url, timeout_sec=12.0)
                if st == 200 and isinstance(tk_data, list):
                    for it in tk_data:
                        c = str(it.get("contract") if is_fut else it.get("currency_pair") or "")
                        if not c.endswith("_USDT") or is_blacklisted_symbol(c, "Gate"):
                            continue
                        base_t = extract_base_asset(c)
                        if watch_coins and base_t not in watch_coins:
                            continue
                        vol = (
                            (_safe_float(it.get("volume_24h_quote")) or _safe_float(it.get("volume_24h_settle")))
                            if is_fut
                            else _safe_float(it.get("quote_volume"))
                        )
                        if vol >= min_vol or base_t in _runtime_allowed_bases:
                            targets.append((c, c.replace("_USDT", "USDT"), vol, q_mults.get(c, 1.0)))

            elif exchange == "KuCoin":
                if market_type == "FUTURES":
                    st, c_data = await _fetch_rest_safe(
                        session, "https://api-futures.kucoin.com/api/v1/contracts/active", timeout_sec=12.0
                    )
                    if st == 200 and isinstance(c_data, dict):
                        for it in c_data.get("data") or []:
                            sym = str(it.get("symbol") or "")
                            if not sym.endswith("USDTM") or str(it.get("status") or "") != "Open":
                                continue
                            if is_blacklisted_symbol(sym, "KuCoin"):
                                continue
                            base_t = extract_base_asset(sym)
                            if watch_coins and base_t not in watch_coins:
                                continue
                            vol = _safe_float(it.get("turnoverOf24h"))
                            if vol >= min_vol or base_t in _runtime_allowed_bases:
                                targets.append((sym, _normalize_pair_key(sym), vol, _safe_float(it.get("multiplier"), 1.0)))
                else:
                    st, tk_data = await _fetch_rest_safe(
                        session, "https://api.kucoin.com/api/v1/market/allTickers", timeout_sec=12.0
                    )
                    if st == 200 and isinstance(tk_data, dict):
                        for it in (tk_data.get("data") or {}).get("ticker") or []:
                            sym = str(it.get("symbol") or "")
                            if not sym.endswith("-USDT") or is_blacklisted_symbol(sym, "KuCoin"):
                                continue
                            base_t = extract_base_asset(sym)
                            if watch_coins and base_t not in watch_coins:
                                continue
                            vol = _safe_float(it.get("volValue"))
                            if vol >= min_vol or base_t in _runtime_allowed_bases:
                                targets.append((sym, _normalize_pair_key(sym), vol, 1.0))

            targets.sort(key=lambda x: x[2], reverse=True)
            checked_cnt = 0

            for api_sym, disp_sym, vol, mult in targets:
                bids: List[Tuple[float, float]] = []
                asks: List[Tuple[float, float]] = []
                status = 0

                if exchange == "Binance":
                    url = (
                        f"https://fapi.binance.com/fapi/v1/depth?symbol={api_sym}&limit=500"
                        if market_type == "FUTURES"
                        else f"https://api.binance.com/api/v3/depth?symbol={api_sym}&limit=500"
                    )
                    status, d = await _fetch_rest_safe(session, url)
                    if status == 200 and isinstance(d, dict):
                        bids = [(_safe_float(p), _safe_float(q)) for p, q in (d.get("bids") or [])]
                        asks = [(_safe_float(p), _safe_float(q)) for p, q in (d.get("asks") or [])]

                elif exchange == "BinanceAlpha":
                    url = f"https://www.binance.com/bapi/defi/v1/public/alpha-trade/fullDepth?symbol={api_sym}&limit=500"
                    status, d = await _fetch_rest_safe(session, url)
                    body = (d or {}).get("data") if isinstance(d, dict) and isinstance(d.get("data"), dict) else d
                    if status == 200 and isinstance(body, dict):
                        bids = [(_safe_float(p), _safe_float(q)) for p, q in (body.get("bids") or [])]
                        asks = [(_safe_float(p), _safe_float(q)) for p, q in (body.get("asks") or [])]

                elif exchange == "Aster":
                    url = (
                        f"https://fapi.asterdex.com/fapi/v1/depth?symbol={api_sym}&limit=500"
                        if market_type == "FUTURES"
                        else f"https://sapi.asterdex.com/api/v1/depth?symbol={api_sym}&limit=500"
                    )
                    status, d = await _fetch_rest_safe(session, url)
                    if status == 200 and isinstance(d, dict):
                        bids = [(_safe_float(p), _safe_float(q)) for p, q in (d.get("bids") or [])]
                        asks = [(_safe_float(p), _safe_float(q)) for p, q in (d.get("asks") or [])]

                elif exchange == "Bybit":
                    cat = "linear" if market_type == "FUTURES" else "spot"
                    status, d = await _fetch_rest_safe(
                        session, f"https://api.bybit.com/v5/market/orderbook?category={cat}&symbol={api_sym}&limit=200"
                    )
                    res = (d or {}).get("result") if status == 200 and isinstance(d, dict) else None
                    if isinstance(res, dict):
                        bids = [(_safe_float(p), _safe_float(q)) for p, q in (res.get("b") or [])]
                        asks = [(_safe_float(p), _safe_float(q)) for p, q in (res.get("a") or [])]

                elif exchange == "OKX":
                    status, d = await _fetch_rest_safe(
                        session, f"https://www.okx.com/api/v5/market/books?instId={api_sym}&sz=200"
                    )
                    arr = (d or {}).get("data") if status == 200 and isinstance(d, dict) else None
                    if arr:
                        bids = [(_safe_float(x[0]), _safe_float(x[1]) * mult) for x in (arr[0].get("bids") or []) if len(x) >= 2]
                        asks = [(_safe_float(x[0]), _safe_float(x[1]) * mult) for x in (arr[0].get("asks") or []) if len(x) >= 2]

                elif exchange == "Bitget":
                    url = (
                        f"https://api.bitget.com/api/v2/mix/market/merge-depth?productType=USDT-FUTURES&symbol={api_sym}&limit=100"
                        if market_type == "FUTURES"
                        else f"https://api.bitget.com/api/v2/spot/market/merge-depth?symbol={api_sym}&limit=100"
                    )
                    status, d = await _fetch_rest_safe(session, url)
                    res = (d or {}).get("data") if status == 200 and isinstance(d, dict) else None
                    if isinstance(res, dict):
                        bids = [(_safe_float(p), _safe_float(q)) for p, q in (res.get("bids") or [])]
                        asks = [(_safe_float(p), _safe_float(q)) for p, q in (res.get("asks") or [])]

                elif exchange == "Gate":
                    if market_type == "FUTURES":
                        url = f"https://api.gateio.ws/api/v4/futures/usdt/order_book?contract={api_sym}&limit=100"
                        status, d = await _fetch_rest_safe(session, url)
                        if status == 200 and isinstance(d, dict):
                            bids = [(_safe_float(x.get("p")), abs(_safe_float(x.get("s"))) * mult) for x in (d.get("bids") or []) if isinstance(x, dict)]
                            asks = [(_safe_float(x.get("p")), abs(_safe_float(x.get("s"))) * mult) for x in (d.get("asks") or []) if isinstance(x, dict)]
                    else:
                        url = f"https://api.gateio.ws/api/v4/spot/order_book?currency_pair={api_sym}&limit=100"
                        status, d = await _fetch_rest_safe(session, url)
                        if status == 200 and isinstance(d, dict):
                            bids = [(_safe_float(x[0]), _safe_float(x[1])) for x in (d.get("bids") or []) if isinstance(x, list) and len(x) >= 2]
                            asks = [(_safe_float(x[0]), _safe_float(x[1])) for x in (d.get("asks") or []) if isinstance(x, list) and len(x) >= 2]

                elif exchange == "KuCoin":
                    if market_type == "FUTURES":
                        url = f"https://api-futures.kucoin.com/api/v1/level2/depth100?symbol={api_sym}"
                    else:
                        url = f"https://api.kucoin.com/api/v1/market/orderbook/level2_100?symbol={api_sym}"
                    status, d = await _fetch_rest_safe(session, url)
                    res = (d or {}).get("data") if status == 200 and isinstance(d, dict) else None
                    if isinstance(res, dict):
                        bids = [(_safe_float(x[0]), _safe_float(x[1]) * mult) for x in (res.get("bids") or []) if len(x) >= 2]
                        asks = [(_safe_float(x[0]), _safe_float(x[1]) * mult) for x in (res.get("asks") or []) if len(x) >= 2]

                if status in (418, 429):
                    logger.warning("[3m FullScan | %s %s] Rate limit (%s) — пауза 20с", exchange, market_type, status)
                    await asyncio.sleep(20.0)
                    continue

                if bids and asks:
                    checked_cnt += 1
                    await eng.process_orderbook(
                        exchange,
                        disp_sym,
                        bids,
                        asks,
                        vol,
                        market_type=market_type,
                        bypass_throttle=True,
                    )
                await asyncio.sleep(pace_sec)

            elapsed = time.time() - started
            logger.info(
                "[3m FullScan | %s %s] Проверено %s/%s монет за %.1fс",
                exchange,
                market_type,
                checked_cnt,
                len(targets),
                elapsed,
            )
            sleep_left = max(10.0, FULL_SCAN_INTERVAL_SEC - elapsed)
            await asyncio.sleep(sleep_left)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[3m FullScan | %s %s] Ошибка обхода: %s", exchange, market_type, exc)
            await asyncio.sleep(20.0)


# ==================== ОБРАБОТКА СОБЫТИЙ И TELEGRAM BOT ====================

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
        except Exception:
            pass

    for i in range(0, len(user_ids), 20):
        await asyncio.gather(*(_send_one(uid) for uid in user_ids[i : i + 20]))
        if i + 20 < len(user_ids):
            await asyncio.sleep(0.05)


async def handle_density_event(ev: DensityEvent) -> None:
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    is_bid = ev.side == WallSide.BID
    side_emoji = "🟢" if is_bid else "🔴"
    side_lbl = "BID (Лонг)" if is_bid else "ASK (Шорт)"
    mkt_lbl = "Alpha" if ev.exchange == "BinanceAlpha" else ("Спот" if ev.market_type == "SPOT" else "Фьючерс")

    text = (
        f"🧱 <b>ПЛОТНОСТЬ | {ev.exchange} ({mkt_lbl})</b>\n"
        f"Монета: <code>{ev.symbol}</code> — {side_emoji} <b>{side_lbl}</b>\n"
        f"💰 Объём: <b>${ev.wall_usd:,.0f}</b> (<b>x{ev.anomaly_x:g}</b> к стакану)\n"
        f"📍 Цена стены: <code>{ev.wall_price:g}</code> ({ev.distance_pct:.2f}% от спреда)\n"
        f"⏳ Разъедание: ~<b>{ev.eat_minutes:g} мин</b> | 24ч: <b>${ev.volume_24h_usd:,.0f}</b>\n"
        f"Время (UTC): {ev.timestamp_utc}"
    )

    users = await get_cached_users()
    base = extract_base_asset(ev.symbol)
    target_ids: List[int] = []
    discord_urls: List[str] = []

    for u in users:
        uid = int(u["user_id"])
        if uid <= 0:
            continue
        ucfg = normalize_config(u.get("config"))
        if not ucfg.get("enabled", True):
            continue
        bans = set(parse_coin_list(u.get("banned_coins")))
        if base in bans:
            continue
        allowed = set(parse_coin_list(u.get("allowed_coins"))) | set(ucfg.get("watch_coins") or [])
        if is_blacklisted_symbol(ev.symbol, ev.exchange, extra_allowed=allowed):
            continue
        if wall_matches_user_config(
            ev.exchange,
            ev.market_type,
            ev.symbol,
            ev.side.value,
            ev.wall_usd,
            ev.distance_pct,
            ev.anomaly_x,
            ev.lifetime_sec,
            ev.volume_24h_usd,
            ucfg,
        ):
            target_ids.append(uid)
            dcfg = normalize_discord_config(u.get("discord_config"))
            wurl = resolve_discord_webhook(dcfg)
            if wurl and wurl not in discord_urls:
                discord_urls.append(wurl)

    if not users and ENV_DISCORD_WEBHOOK and is_valid_discord_webhook(ENV_DISCORD_WEBHOOK):
        discord_urls.append(ENV_DISCORD_WEBHOOK)

    for wurl in discord_urls:
        asyncio.create_task(send_density_discord(wurl, ev, session=_http_session))
    if target_ids:
        asyncio.create_task(_broadcast_messages(target_ids, text, _build_main_keyboard()))

    await db_execute(
        "INSERT INTO densities (utc_date, utc_time, exchange, market_type, symbol, side, wall_price, "
        "current_price, distance_pct, wall_usd, median_level_usd, anomaly_x, lifetime_sec, eat_minutes, volume_24h_usd) "
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


def _format_settings_view(
    cfg: Dict[str, Any],
    dcfg: Dict[str, Any],
    bans: List[str],
    allowed: List[str],
) -> Tuple[str, InlineKeyboardMarkup]:
    side_ru = {"BOTH": "Обе (BID + ASK)", "BID": "Только BID (Лонг)", "ASK": "Только ASK (Шорт)"}[cfg["side_mode"]]
    mkt_ru = {"BOTH": "Фьючерсы + Спот + Alpha", "FUTURES": "Только Фьючерсы", "SPOT": "Только Спот + Alpha"}[cfg["market_mode"]]
    dc_url = resolve_discord_webhook(dcfg)
    dc_st = "✅ Подключён" if dc_url else "❌ Не задан"
    watch_txt = ", ".join(cfg["watch_coins"]) if cfg["watch_coins"] else "Весь рынок"
    allow_txt = ", ".join(allowed) if allowed else "нет"
    ban_txt = ", ".join(bans) if bans else "нет"

    text = (
        "⚙️ <b>Настройки скринера плотностей</b>\n\n"
        f"• Статус алертов: <b>{'🟢 ВКЛ' if cfg['enabled'] else '🔴 ВЫКЛ'}</b>\n"
        f"• Рынки: <b>{mkt_ru}</b> (<code>/market both|futures|spot</code>)\n"
        f"• Мин. плотность <b>СПОТ / Alpha</b>: <b>${cfg['min_wall_spot_usd']:,.0f}</b> (<code>/minspot 250000</code>)\n"
        f"• Мин. плотность <b>ФЬЮЧЕРСЫ</b>: <b>${cfg['min_wall_futures_usd']:,.0f}</b> (<code>/minfut 500000</code>)\n"
        f"• Диапазон от спреда: <b>±{cfg['max_distance_pct']:g}%</b> (лонг/шорт, <code>/dist 10</code>)\n"
        f"• Мин. аномальность: <b>x{cfg['min_anomaly_x']:g}</b> к стакану (<code>/anom 5</code>)\n"
        f"• Задержка (жизнь стены): <b>{cfg['min_lifetime_sec']:g} сек</b> (0 = сразу, <code>/life 0</code>)\n"
        f"• Мин. оборот 24ч: <b>${cfg['min_vol24h']:,.0f}</b> (<code>/minvol 500000</code>)\n"
        f"• Кулдаун повтора: <b>{cfg['cooldown_sec']:g} сек</b> (<code>/cooldown 300</code>)\n"
        f"• Сторона: <b>{side_ru}</b> (<code>/side both|bid|ask</code>)\n"
        f"• Биржи (8): <b>{', '.join(cfg['exchanges'])}</b>\n"
        f"• ⏱ Полный обход всех монет: <b>раз в 3 минуты</b> + WS реалтайм\n"
        f"• 👁 Отслеживание монет: <b>{watch_txt}</b> (<code>/watch SOL</code>)\n"
        f"• 🔓 Разблокированы из ЧС: <b>{allow_txt}</b> (<code>/allow BTC</code>)\n"
        f"• 🚫 Личный ЧС: <b>{ban_txt}</b> (<code>/ban WIF</code>)\n"
        f"• 💬 Discord: <b>{dc_st}</b> (<code>/discord URL</code>)"
    )

    rows: List[List[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(text="🔴 Выкл" if cfg["enabled"] else "🟢 Вкл", callback_data="toggle_en"),
            InlineKeyboardButton(text=f"📊 {cfg['market_mode']}", callback_data="cycle_market"),
            InlineKeyboardButton(text=f"🎯 {cfg['side_mode']}", callback_data="cycle_side"),
        ],
        [
            InlineKeyboardButton(text="⚡ Скальп (100/200K)", callback_data="preset:scalp"),
            InlineKeyboardButton(text="⚖️ Станд. (250/500K)", callback_data="preset:standard"),
            InlineKeyboardButton(text="🐋 Кит (500K/1M)", callback_data="preset:whale"),
        ],
        [
            InlineKeyboardButton(text="🔓 Добавить из ЧС", callback_data="unblock_menu"),
            InlineKeyboardButton(text="💬 Тест Discord", callback_data="dc_test"),
        ],
    ]
    if WEBAPP_URL:
        rows.append([InlineKeyboardButton(text="🧱 Открыть Mini App", web_app=WebAppInfo(url=WEBAPP_URL))])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    if not message.from_user:
        return
    await db_execute("INSERT INTO users (user_id) VALUES (?) ON CONFLICT(user_id) DO NOTHING", (message.from_user.id,))
    invalidate_users_cache()
    text = (
        "🧱 <b>Скринер аномальных плотностей (8 бирж · Фьючерсы + Спот + Binance Alpha)</b>\n\n"
        "Биржи: <b>Binance, BinanceAlpha, Bybit, OKX, Bitget, Gate, KuCoin, Aster</b>.\n"
        "• Полный обход <b>всех монет каждые 3 минуты</b> + WebSocket реалтайм.\n"
        "• Диапазон от спреда: <b>10% лонг (BID) и 10% шорт (ASK)</b>.\n"
        "• Алерт приходит <b>сразу</b> при нахождении плотности (Спот/Alpha от $250K, Фьючерсы от $500K).\n\n"
        "<b>Команды настройки прямо в боте:</b>\n"
        "/settings — панель настроек, пресеты и разблокировка монет из ЧС\n"
        "/active — активные аномальные плотности прямо сейчас\n"
        "/find SOL [сумма] — мгновенно найти плотности по отдельной монете (8 бирж)\n"
        "/watch SOL WIF — отслеживать только выбранные монеты (/watch clear — весь рынок)\n"
        "/unwatch SOL — убрать монету из фильтра отслеживания\n"
        "/market both|futures|spot — Фьючерсы + Спот, только Фьючерсы или только Спот\n"
        "/minspot 250000 — мин. плотность на СПОТЕ и Binance Alpha ($)\n"
        "/minfut 500000 — мин. плотность на ФЬЮЧЕРСАХ ($)\n"
        "/minwall 250000 500000 — задать Спот и Фьючерсы одной командой\n"
        "/dist 10 — диапазон от спреда в % (лонг и шорт)\n"
        "/anom 5 — мин. кратность к медианному уровню (x)\n"
        "/life 0 — задержка жизни стены в сек (0 = слать сразу)\n"
        "/minvol 500000 — мин. суточный оборот ($)\n"
        "/cooldown 300 — пауза повтора (сек)\n"
        "/side both|bid|ask — фильтр стороны\n"
        "/exchanges all — включить все 8 бирж (или списком через запятую)\n"
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
    text, kb = _format_settings_view(cfg, dcfg, bans, allowed)
    await message.answer(text, parse_mode="HTML", reply_markup=kb)


@dp.message(Command("active"))
async def cmd_active(message: types.Message) -> None:
    walls = density_engine.get_active_walls()
    if not walls:
        await message.answer("🧱 Активных аномальных плотностей прямо сейчас нет.", reply_markup=_build_main_keyboard())
        return
    lines = ["🧱 <b>Активные плотности сейчас:</b>\n"]
    for ev in walls[:15]:
        em = "🟢" if ev.side == WallSide.BID else "🔴"
        ml = "Alpha" if ev.exchange == "BinanceAlpha" else ("Спот" if ev.market_type == "SPOT" else "Фьюч")
        lines.append(
            f"• {em} <b>{ev.exchange} ({ml})</b> | <code>{ev.symbol}</code> — "
            f"<b>${ev.wall_usd:,.0f}</b> (x{ev.anomaly_x:g}) @ <code>{ev.wall_price:g}</code> ({ev.distance_pct:.2f}%)"
        )
    await message.answer("\n".join(lines), parse_mode="HTML", reply_markup=_build_main_keyboard())


@dp.message(Command("find"))
async def cmd_find(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split()
    if len(parts) < 2:
        await message.answer(
            "Использование:\n"
            "• <code>/find SOL</code> — по текущим фильтрам (Спот/Фьючерс)\n"
            "• <code>/find SOL 150000</code> — временный порог $150K для обоих рынков\n"
            "• <code>/find SOL 100000 300000</code> — Спот $100K, Фьючерс $300K",
            parse_mode="HTML",
        )
        return

    coin = parts[1]
    ov_spot: Optional[float] = None
    ov_fut: Optional[float] = None
    if len(parts) >= 3:
        v1 = _safe_float(parts[2], 0.0)
        if v1 > 0:
            ov_spot = v1
            ov_fut = v1
    if len(parts) >= 4:
        v2 = _safe_float(parts[3], 0.0)
        if v2 > 0:
            ov_fut = v2

    cfg, _, _, _ = await get_user_record(message.from_user.id)
    wait_msg = await message.answer(
        f"🔎 Сканирую живые стаканы <code>{extract_base_asset(coin)}</code> на 8 биржах...",
        parse_mode="HTML",
    )
    res = await live_search_coin_densities(
        _http_session,
        coin,
        cfg,
        override_min_spot=ov_spot,
        override_min_fut=ov_fut,
    )
    matches = res.get("matches") or []
    top_seen = res.get("top_seen") or []
    base = res.get("coin") or coin.upper()
    lines = [
        f"🔎 <b>Живой скан: {base}</b> (проверено стаканов: {res.get('checked_books', 0)})",
        f"Пороги: Спот/Alpha ≥ <b>${res['min_wall_spot_usd']:,.0f}</b> · Фьюч ≥ <b>${res['min_wall_futures_usd']:,.0f}</b> · ±{res['max_distance_pct']:g}%\n",
    ]
    if matches:
        for m in matches[:15]:
            em = "🟢" if m["side"] == "BID" else "🔴"
            ml = "Alpha" if m["exchange"] == "BinanceAlpha" else ("Спот" if m["market_type"] == "SPOT" else "Фьюч")
            lines.append(
                f"• {em} <b>{m['exchange']} ({ml})</b> {m['side']} — <b>${m['wall_usd']:,.0f}</b> "
                f"(x{m['anomaly_x']:g}) @ <code>{m['wall_price']:g}</code> ({m['distance_pct']:.2f}%)"
            )
    else:
        lines.append("По заданным порогам плотностей не найдено.")
        if top_seen:
            lines.append("\n<b>Крупнейшие лимитки в стаканах сейчас:</b>")
            for m in top_seen[:6]:
                em = "🟢" if m["side"] == "BID" else "🔴"
                ml = "Alpha" if m["exchange"] == "BinanceAlpha" else ("Спот" if m["market_type"] == "SPOT" else "Фьюч")
                lines.append(
                    f"• {em} {m['exchange']} ({ml}) {m['side']}: <b>${m['wall_usd']:,.0f}</b> "
                    f"(x{m['anomaly_x']:g}) @ <code>{m['wall_price']:g}</code> ({m['distance_pct']:.2f}%)"
                )
    try:
        await wait_msg.edit_text("\n".join(lines), parse_mode="HTML")
    except Exception:
        await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("watch"))
async def cmd_watch(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split(maxsplit=1)
    cfg, _, _, allowed = await get_user_record(message.from_user.id)
    if len(parts) < 2:
        cur = ", ".join(cfg["watch_coins"]) if cfg["watch_coins"] else "Весь рынок"
        await message.answer(
            f"👁 Отслеживаемые монеты: <b>{cur}</b>\n"
            "Задать: <code>/watch SOL WIF PEPE</code>\n"
            "Сбросить на весь рынок: <code>/watch clear</code>",
            parse_mode="HTML",
        )
        return
    arg = parts[1].strip()
    if arg.lower() in ("clear", "all", "reset", "сброс", "все"):
        cfg["watch_coins"] = []
        await save_user_record(message.from_user.id, cfg=cfg)
        await message.answer("✅ Режим отслеживания сброшен — сканируется <b>весь рынок</b>.", parse_mode="HTML")
        return
    coins = parse_coin_list(arg)
    if not coins:
        await message.answer("Укажите тикеры, например: <code>/watch SOL WIF</code>", parse_mode="HTML")
        return
    cfg["watch_coins"] = coins
    merged_allowed = list(dict.fromkeys(allowed + coins))
    await save_user_record(message.from_user.id, cfg=cfg, allowed=merged_allowed)
    await message.answer(
        f"👁 Алерты переключены строго на монеты: <b>{', '.join(coins)}</b>\n"
        "(Если они были в ЧС — автоматически разблокированы. Сброс: <code>/watch clear</code>)",
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
    rem = set(parse_coin_list(parts[1]))
    cfg, _, _, _ = await get_user_record(message.from_user.id)
    cfg["watch_coins"] = [c for c in cfg["watch_coins"] if c not in rem]
    await save_user_record(message.from_user.id, cfg=cfg)
    cur = ", ".join(cfg["watch_coins"]) if cfg["watch_coins"] else "Весь рынок"
    await message.answer(f"✅ Обновлено. Отслеживаются: <b>{cur}</b>", parse_mode="HTML")


@dp.message(Command("market"))
async def cmd_market(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split()
    arg = parts[1].strip().upper() if len(parts) > 1 else ""
    if arg not in ("BOTH", "FUTURES", "SPOT"):
        await message.answer("Использование: <code>/market both</code> | <code>/market futures</code> | <code>/market spot</code>", parse_mode="HTML")
        return
    cfg, _, _, _ = await get_user_record(message.from_user.id)
    cfg["market_mode"] = arg
    await save_user_record(message.from_user.id, cfg=cfg)
    await message.answer(f"✅ Режим рынков: <b>{arg}</b>", parse_mode="HTML")


@dp.message(Command("minspot"))
async def cmd_minspot(message: types.Message) -> None:
    if not message.from_user:
        return
    try:
        val = float((message.text or "").split()[1])
        cfg, _, _, _ = await get_user_record(message.from_user.id)
        cfg["min_wall_spot_usd"] = val
        cfg, _, _, _ = await save_user_record(message.from_user.id, cfg=cfg)
        await message.answer(f"✅ Мин. плотность на СПОТЕ и Binance Alpha: <b>${cfg['min_wall_spot_usd']:,.0f}</b>", parse_mode="HTML")
    except Exception:
        await message.answer("Пример: <code>/minspot 250000</code>", parse_mode="HTML")


@dp.message(Command("minfut"))
async def cmd_minfut(message: types.Message) -> None:
    if not message.from_user:
        return
    try:
        val = float((message.text or "").split()[1])
        cfg, _, _, _ = await get_user_record(message.from_user.id)
        cfg["min_wall_futures_usd"] = val
        cfg, _, _, _ = await save_user_record(message.from_user.id, cfg=cfg)
        await message.answer(f"✅ Мин. плотность на ФЬЮЧЕРСАХ: <b>${cfg['min_wall_futures_usd']:,.0f}</b>", parse_mode="HTML")
    except Exception:
        await message.answer("Пример: <code>/minfut 500000</code>", parse_mode="HTML")


@dp.message(Command("minwall"))
async def cmd_minwall(message: types.Message) -> None:
    if not message.from_user:
        return
    try:
        parts = (message.text or "").split()
        v1 = float(parts[1])
        v2 = float(parts[2]) if len(parts) > 2 else v1
        cfg, _, _, _ = await get_user_record(message.from_user.id)
        cfg["min_wall_spot_usd"] = v1
        cfg["min_wall_futures_usd"] = v2
        cfg, _, _, _ = await save_user_record(message.from_user.id, cfg=cfg)
        await message.answer(
            f"✅ Мин. плотность обновлена:\n• Спот / Alpha: <b>${cfg['min_wall_spot_usd']:,.0f}</b>\n"
            f"• Фьючерсы: <b>${cfg['min_wall_futures_usd']:,.0f}</b>",
            parse_mode="HTML",
        )
    except Exception:
        await message.answer("Пример: <code>/minwall 250000 500000</code>", parse_mode="HTML")


@dp.message(Command("dist"))
async def cmd_dist(message: types.Message) -> None:
    if not message.from_user:
        return
    try:
        val = float((message.text or "").split()[1])
        cfg, _, _, _ = await get_user_record(message.from_user.id)
        cfg["max_distance_pct"] = val
        cfg, _, _, _ = await save_user_record(message.from_user.id, cfg=cfg)
        await message.answer(f"✅ Диапазон от спреда (лонг/шорт): <b>±{cfg['max_distance_pct']:g}%</b>", parse_mode="HTML")
    except Exception:
        await message.answer("Пример: <code>/dist 10</code>", parse_mode="HTML")


@dp.message(Command("anom"))
async def cmd_anom(message: types.Message) -> None:
    if not message.from_user:
        return
    try:
        val = float((message.text or "").split()[1])
        cfg, _, _, _ = await get_user_record(message.from_user.id)
        cfg["min_anomaly_x"] = val
        cfg, _, _, _ = await save_user_record(message.from_user.id, cfg=cfg)
        await message.answer(f"✅ Мин. аномальность: <b>x{cfg['min_anomaly_x']:g}</b> к медиане стакана", parse_mode="HTML")
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
        extra = " (мгновенно)" if cfg["min_lifetime_sec"] <= 0 else ""
        await message.answer(f"✅ Мин. время удержания: <b>{cfg['min_lifetime_sec']:g} сек</b>{extra}", parse_mode="HTML")
    except Exception:
        await message.answer("Пример: <code>/life 0</code> (0 = слать сразу)", parse_mode="HTML")


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
        await message.answer("Пример: <code>/side both</code> | <code>/side bid</code> | <code>/side ask</code>", parse_mode="HTML")
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
            "Пример: <code>/exchanges all</code> или <code>/exchanges Binance,BinanceAlpha,Bybit,OKX,Bitget,Gate,KuCoin,Aster</code>",
            parse_mode="HTML",
        )
        return
    raw_arg = parts[1].strip()
    if raw_arg.lower() in ("all", "все"):
        chosen = list(ALL_EXCHANGES)
    else:
        raw_items = [x.strip() for x in re.split(r"[,\s]+", raw_arg) if x.strip()]
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


@dp.message(Command("allow"))
async def cmd_allow(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split(maxsplit=1)
    _, _, bans, allowed = await get_user_record(message.from_user.id)
    if len(parts) < 2:
        cur = ", ".join(allowed) if allowed else "нет"
        await message.answer(
            f"🔓 Разблокированные из ЧС: <b>{cur}</b>\nПример: <code>/allow BTC SOL XAU</code>",
            parse_mode="HTML",
        )
        return
    coins = parse_coin_list(parts[1])
    if not coins:
        await message.answer("Пример: <code>/allow BTC SOL</code>", parse_mode="HTML")
        return
    new_allowed = list(dict.fromkeys(allowed + coins))
    new_bans = [b for b in bans if b not in coins]
    await save_user_record(message.from_user.id, bans=new_bans, allowed=new_allowed)
    await message.answer(f"🔓 Разблокированы из ЧС: <b>{', '.join(coins)}</b>", parse_mode="HTML")


@dp.message(Command("unallow"))
async def cmd_unallow(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Пример: <code>/unallow BTC</code>", parse_mode="HTML")
        return
    rem = set(parse_coin_list(parts[1]))
    _, _, _, allowed = await get_user_record(message.from_user.id)
    new_allowed = [c for c in allowed if c not in rem]
    await save_user_record(message.from_user.id, allowed=new_allowed)
    await message.answer(f"🔒 Возвращены в чёрный список: <b>{', '.join(sorted(rem))}</b>", parse_mode="HTML")


@dp.message(Command("ban"))
async def cmd_ban(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Пример: <code>/ban WIF</code>", parse_mode="HTML")
        return
    coins = parse_coin_list(parts[1])
    if not coins:
        return
    _, _, bans, allowed = await get_user_record(message.from_user.id)
    new_bans = list(dict.fromkeys(bans + coins))
    new_allowed = [c for c in allowed if c not in coins]
    await save_user_record(message.from_user.id, bans=new_bans, allowed=new_allowed)
    await message.answer(f"🚫 Скрыты: <b>{', '.join(coins)}</b>", parse_mode="HTML")


@dp.message(Command("unban"))
async def cmd_unban(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Пример: <code>/unban WIF</code>", parse_mode="HTML")
        return
    rem = set(parse_coin_list(parts[1]))
    _, _, bans, _ = await get_user_record(message.from_user.id)
    new_bans = [c for c in bans if c not in rem]
    await save_user_record(message.from_user.id, bans=new_bans)
    await message.answer(f"✅ Возвращены: <b>{', '.join(sorted(rem))}</b>", parse_mode="HTML")


@dp.message(Command("discord"))
async def cmd_discord(message: types.Message) -> None:
    if not message.from_user:
        return
    parts = (message.text or "").split(maxsplit=1)
    _, dcfg, _, _ = await get_user_record(message.from_user.id)
    if len(parts) < 2:
        st = "✅ Подключён" if resolve_discord_webhook(dcfg) else "❌ Не задан"
        await message.answer(
            f"💬 Discord: <b>{st}</b>\n\n"
            "• Подключить: <code>/discord https://discord.com/api/webhooks/...</code>\n"
            "• Тест: <code>/discord test</code>\n"
            "• Вкл/Выкл: <code>/discord on</code> | <code>/discord off</code>",
            parse_mode="HTML",
        )
        return
    arg = parts[1].strip()
    low = arg.lower()
    if low in ("on", "off"):
        dcfg["enabled"] = low == "on"
        await save_user_record(message.from_user.id, dcfg=dcfg)
        await message.answer(f"💬 Discord: <b>{'ВКЛЮЧЁН' if dcfg['enabled'] else 'ВЫКЛЮЧЕН'}</b>", parse_mode="HTML")
        return
    if low == "test":
        url = resolve_discord_webhook(dcfg)
        if not url:
            await message.answer("❌ Сначала укажите вебхук: <code>/discord https://discord.com/api/webhooks/...</code>", parse_mode="HTML")
            return
        ok, err = await send_test_discord(url, session=_http_session)
        await message.answer("✅ Тестовое сообщение отправлено в Discord!" if ok else f"❌ Ошибка Discord: {err}")
        return
    if not is_valid_discord_webhook(arg):
        await message.answer("❌ Неверный формат ссылки. Нужна ссылка вида <code>https://discord.com/api/webhooks/...</code>", parse_mode="HTML")
        return
    dcfg["webhook_url"] = arg
    dcfg["enabled"] = True
    await save_user_record(message.from_user.id, dcfg=dcfg)
    ok, err = await send_test_discord(arg, session=_http_session)
    if ok:
        await message.answer("✅ Discord-вебхук сохранён и протестирован! Проверьте канал.")
    else:
        await message.answer(f"⚠️ Вебхук сохранён, но тест вернул ошибку: {err}")


@dp.callback_query(F.data)
async def handle_callbacks(query: types.CallbackQuery) -> None:
    if not query.from_user or not query.data:
        return
    uid = query.from_user.id
    data = query.data
    cfg, dcfg, bans, allowed = await get_user_record(uid)

    if data == "toggle_en":
        cfg["enabled"] = not cfg["enabled"]
        cfg, dcfg, bans, allowed = await save_user_record(uid, cfg=cfg)
        await query.answer("Алерты включены" if cfg["enabled"] else "Алерты выключены")
    elif data == "cycle_market":
        order = ["BOTH", "FUTURES", "SPOT"]
        cfg["market_mode"] = order[(order.index(cfg["market_mode"]) + 1) % len(order)]
        cfg, dcfg, bans, allowed = await save_user_record(uid, cfg=cfg)
        await query.answer(f"Рынок: {cfg['market_mode']}")
    elif data == "cycle_side":
        order = ["BOTH", "BID", "ASK"]
        cfg["side_mode"] = order[(order.index(cfg["side_mode"]) + 1) % len(order)]
        cfg, dcfg, bans, allowed = await save_user_record(uid, cfg=cfg)
        await query.answer(f"Сторона: {cfg['side_mode']}")
    elif data.startswith("preset:"):
        p = data.split(":", 1)[1]
        presets = {
            "scalp": {"min_wall_spot_usd": 100_000.0, "min_wall_futures_usd": 200_000.0, "max_distance_pct": 5.0, "min_anomaly_x": 4.0, "min_lifetime_sec": 0.0, "cooldown_sec": 180.0},
            "standard": {"min_wall_spot_usd": 250_000.0, "min_wall_futures_usd": 500_000.0, "max_distance_pct": 10.0, "min_anomaly_x": 5.0, "min_lifetime_sec": 0.0, "cooldown_sec": 300.0},
            "whale": {"min_wall_spot_usd": 500_000.0, "min_wall_futures_usd": 1_000_000.0, "max_distance_pct": 10.0, "min_anomaly_x": 7.0, "min_lifetime_sec": 0.0, "cooldown_sec": 600.0},
        }
        if p in presets:
            cfg.update(presets[p])
            cfg, dcfg, bans, allowed = await save_user_record(uid, cfg=cfg)
            await query.answer(f"Пресет применён: {p}")
    elif data == "unblock_menu":
        rows: List[List[InlineKeyboardButton]] = []
        row: List[InlineKeyboardButton] = []
        for coin in QUICK_UNBLOCK_COINS:
            mark = "🔓 " if coin in allowed else "🔒 "
            row.append(InlineKeyboardButton(text=f"{mark}{coin}", callback_data=f"allow_coin:{coin}"))
            if len(row) == 3:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        rows.append([InlineKeyboardButton(text="⬅️ Назад к настройкам", callback_data="back_settings")])
        if query.message:
            await query.message.edit_text(
                "🔓 <b>Разблокировка монет из чёрного списка</b>\n"
                "Нажмите на монету, чтобы разрешить или снова заблокировать её.\n"
                "Любой другой тикер: <code>/allow ТИКЕР</code>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
            )
        await query.answer()
        return
    elif data.startswith("allow_coin:"):
        coin = extract_base_asset(data.split(":", 1)[1])
        if coin:
            if coin in allowed:
                allowed = [c for c in allowed if c != coin]
                msg_ans = f"🔒 {coin} снова в ЧС"
            else:
                allowed = list(dict.fromkeys(allowed + [coin]))
                bans = [b for b in bans if b != coin]
                msg_ans = f"🔓 {coin} разблокирована"
            _, _, bans, allowed = await save_user_record(uid, bans=bans, allowed=allowed)
            await query.answer(msg_ans)
        rows = []
        row = []
        for c in QUICK_UNBLOCK_COINS:
            mark = "🔓 " if c in allowed else "🔒 "
            row.append(InlineKeyboardButton(text=f"{mark}{c}", callback_data=f"allow_coin:{c}"))
            if len(row) == 3:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        rows.append([InlineKeyboardButton(text="⬅️ Назад к настройкам", callback_data="back_settings")])
        if query.message:
            await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
        return
    elif data == "dc_test":
        url = resolve_discord_webhook(dcfg)
        if not url:
            await query.answer("Сначала задайте вебхук командой /discord URL", show_alert=True)
            return
        ok, err = await send_test_discord(url, session=_http_session)
        await query.answer("✅ Тест отправлен в Discord!" if ok else f"Ошибка: {err}", show_alert=True)
        return
    elif data == "back_settings":
        await query.answer()

    text, kb = _format_settings_view(cfg, dcfg, bans, allowed)
    if query.message:
        try:
            await query.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
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
            --bg: #0c0f16; --card: #151a24; --card-2: #1c2433;
            --text: #eef2f8; --hint: #8b95a7; --border: rgba(255,255,255,0.08);
            --bid: #16c784; --ask: #ea3943; --gold: #f5b942; --accent: #3b82f6;
        }
        * { box-sizing: border-box; -webkit-tap-highlight-color: transparent; }
        body {
            margin: 0; padding: 12px 12px 84px; background: var(--bg); color: var(--text);
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; font-size: 14px;
        }
        .topbar { display: flex; align-items: center; justify-content: space-between; gap: 8px; margin-bottom: 10px; }
        .brand { font-size: 15px; font-weight: 800; display: flex; align-items: center; gap: 6px; }
        .chip {
            border: 1px solid var(--border); background: var(--card); color: var(--text);
            padding: 6px 10px; border-radius: 10px; font-size: 12px; font-weight: 700; cursor: pointer;
        }
        .chip.on { border-color: rgba(22,199,132,0.5); background: rgba(22,199,132,0.14); color: var(--bid); }
        .tabs {
            display: grid; grid-template-columns: repeat(3, 1fr); gap: 6px; background: var(--card);
            padding: 4px; border-radius: 12px; border: 1px solid var(--border); margin-bottom: 12px;
        }
        .tab-btn {
            border: none; background: transparent; color: var(--hint);
            padding: 8px 6px; border-radius: 9px; font-size: 12px; font-weight: 700; cursor: pointer;
        }
        .tab-btn.active { background: var(--accent); color: #fff; }
        .panel { display: none; }
        .panel.active { display: block; }
        .box {
            background: var(--card); border: 1px solid var(--border);
            border-radius: 12px; padding: 12px; margin-bottom: 10px;
        }
        .box-title { font-size: 13px; font-weight: 800; margin-bottom: 8px; display: block; }
        .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 8px; }
        .fg { display: flex; flex-direction: column; gap: 4px; }
        .fl { font-size: 11px; color: var(--hint); font-weight: 600; }
        input[type="number"], input[type="text"], select {
            width: 100%; background: var(--card-2); color: var(--text); border: 1px solid var(--border);
            border-radius: 9px; padding: 8px 10px; font-size: 13px; outline: none;
        }
        .btn {
            border: none; background: var(--accent); color: #fff; padding: 9px 12px;
            border-radius: 9px; font-size: 13px; font-weight: 700; cursor: pointer;
        }
        .btn.sec { background: var(--card-2); color: var(--text); border: 1px solid var(--border); }
        .card {
            background: var(--card); border: 1px solid var(--border); border-radius: 12px;
            padding: 10px 12px; margin-bottom: 8px; border-left: 3px solid var(--accent); cursor: pointer;
        }
        .card.bid { border-left-color: var(--bid); }
        .card.ask { border-left-color: var(--ask); }
        .card-top { display: flex; justify-content: space-between; align-items: center; gap: 6px; margin-bottom: 5px; }
        .sym { font-weight: 800; font-size: 14px; display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }
        .tag { font-size: 10px; font-weight: 700; padding: 2px 6px; border-radius: 6px; background: rgba(255,255,255,0.08); }
        .tag.spot { background: rgba(59,130,246,0.2); color: #93c5fd; }
        .tag.alpha { background: rgba(245,185,66,0.22); color: var(--gold); }
        .badge { font-size: 11px; font-weight: 800; padding: 2px 7px; border-radius: 6px; }
        .badge.bid { background: rgba(22,199,132,0.18); color: var(--bid); }
        .badge.ask { background: rgba(234,57,67,0.18); color: var(--ask); }
        .badge.gold { background: rgba(245,185,66,0.18); color: var(--gold); }
        .meta { font-size: 12px; color: var(--hint); display: flex; justify-content: space-between; flex-wrap: wrap; gap: 6px; }
        .meta b { color: var(--text); }
        .ex-wrap { display: flex; flex-wrap: wrap; gap: 6px; margin: 6px 0 10px; }
        .ex-btn {
            padding: 6px 9px; border-radius: 9px; border: 1px solid var(--border);
            background: rgba(255,255,255,0.03); color: var(--hint); font-size: 12px; font-weight: 700; cursor: pointer;
        }
        .ex-btn.on { border-color: rgba(22,199,132,0.55); background: rgba(22,199,132,0.15); color: var(--text); }
        .pill-tag {
            display: inline-flex; align-items: center; gap: 6px; padding: 5px 9px; border-radius: 8px;
            background: rgba(22,199,132,0.14); border: 1px solid rgba(22,199,132,0.35); font-size: 12px; font-weight: 700;
        }
        .pill-tag.ban { background: rgba(234,57,67,0.14); border-color: rgba(234,57,67,0.35); }
        .pill-tag button { border: none; background: transparent; color: inherit; font-weight: 800; cursor: pointer; }
        .empty { text-align: center; color: var(--hint); font-size: 12px; padding: 14px 4px; }
        .toggle-row { display: flex; align-items: center; justify-content: space-between; padding: 6px 0; font-size: 13px; }
        .toggle-row input { width: 18px; height: 18px; accent-color: var(--bid); }
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
        <div class="brand">🧱 Density Screener <span style="font-size:11px;color:var(--bid)">8 EX · ALPHA · ±10%</span></div>
        <div style="display:flex;gap:6px">
            <button id="tiger-btn" class="chip" onclick="connectTiger(true)">🐯 Tiger</button>
        </div>
    </div>

    <div class="tabs">
        <button class="tab-btn active" id="tb-live" onclick="switchTab('live')">🧱 Лайв / Поиск</button>
        <button class="tab-btn" id="tb-hist" onclick="switchTab('hist')">📜 История</button>
        <button class="tab-btn" id="tb-set" onclick="switchTab('set')">⚙️ Настройки</button>
    </div>

    <div id="p-live" class="panel active">
        <div class="box">
            <span class="box-title">🔎 Живой поиск плотностей по монете (8 бирж)</span>
            <div style="display:flex;gap:6px;margin-bottom:8px">
                <input type="text" id="s-coin" placeholder="Тикер, напр. SOL, RIVER, WIF">
                <button class="btn" onclick="searchCoin()">🔎 Стаканы</button>
            </div>
            <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px">
                <div class="fg"><span class="fl">Спот ($)</span><input type="number" id="s-spot" value="250000" step="25000"></div>
                <div class="fg"><span class="fl">Фьючерс ($)</span><input type="number" id="s-fut" value="500000" step="50000"></div>
                <div class="fg"><span class="fl">Дист (%)</span><input type="number" id="s-dist" value="10" step="0.5"></div>
            </div>
            <div id="search-res" style="margin-top:8px"></div>
        </div>

        <div class="fl" style="margin:6px 2px">АКТИВНЫЕ ПЛОТНОСТИ СЕЙЧАС (<span id="live-cnt">0</span>)</div>
        <div id="live-list"></div>
    </div>

    <div id="p-hist" class="panel">
        <div class="fl" style="margin:6px 2px">ИСТОРИЯ ЗА СЕГОДНЯ (UTC)</div>
        <div id="hist-list"></div>
    </div>

    <div id="p-set" class="panel">
        <div class="box">
            <span class="box-title">⚙️ Основные фильтры плотностей</span>
            <div class="toggle-row">
                <span>Включить уведомления</span>
                <input type="checkbox" id="c-enabled" checked>
            </div>
            <div class="grid-2">
                <div class="fg">
                    <span class="fl">Рынки</span>
                    <select id="c-market">
                        <option value="BOTH">Фьючерсы + Спот + Alpha</option>
                        <option value="FUTURES">Только Фьючерсы</option>
                        <option value="SPOT">Только Спот + Alpha</option>
                    </select>
                </div>
                <div class="fg">
                    <span class="fl">Сторона</span>
                    <select id="c-side">
                        <option value="BOTH">Обе (BID + ASK)</option>
                        <option value="BID">Только BID (Лонг)</option>
                        <option value="ASK">Только ASK (Шорт)</option>
                    </select>
                </div>
            </div>
            <div class="grid-2">
                <div class="fg"><span class="fl">Мин. плотность СПОТ / Alpha ($)</span><input type="number" id="c-wall-spot" value="250000" step="25000"></div>
                <div class="fg"><span class="fl">Мин. плотность ФЬЮЧЕРС ($)</span><input type="number" id="c-wall-fut" value="500000" step="50000"></div>
            </div>
            <div class="grid-2">
                <div class="fg"><span class="fl">Диапазон от спреда (±%)</span><input type="number" id="c-dist" value="10" step="0.5"></div>
                <div class="fg"><span class="fl">Аномальность (x к стакану)</span><input type="number" id="c-anom" value="5" step="0.5"></div>
            </div>
            <div class="grid-2">
                <div class="fg"><span class="fl">Мин. время жизни (сек · 0=сразу)</span><input type="number" id="c-life" value="0" step="5"></div>
                <div class="fg"><span class="fl">Мин. оборот 24ч ($)</span><input type="number" id="c-vol" value="500000" step="100000"></div>
            </div>
            <div class="fg" style="margin-bottom:8px">
                <span class="fl">Кулдаун повтора (сек)</span><input type="number" id="c-cd" value="300" step="30">
            </div>
            <span class="fl">Биржи (8):</span>
            <div class="ex-wrap" id="ex-checks"></div>
            <button class="btn" style="width:100%" onclick="saveSettings()">💾 Сохранить фильтры</button>
        </div>

        <div class="box">
            <span class="box-title">💬 Discord Webhook</span>
            <div class="toggle-row">
                <span>Отправлять алерты в Discord</span>
                <input type="checkbox" id="dc-en" checked>
            </div>
            <div class="fg" style="margin-bottom:8px">
                <span class="fl">Webhook URL</span>
                <input type="text" id="dc-url" placeholder="https://discord.com/api/webhooks/...">
            </div>
            <div class="grid-2">
                <button class="btn" onclick="saveSettings()">💾 Сохранить</button>
                <button class="btn sec" onclick="testDiscord()">🧪 Тест Discord</button>
            </div>
        </div>

        <div class="box">
            <span class="box-title">👁 Отслеживать только выбранные монеты (Watchlist)</span>
            <div style="display:flex;gap:6px;margin-bottom:6px">
                <input type="text" id="w-inp" placeholder="Напр. SOL, WIF (пусто = весь рынок)">
                <button class="btn" onclick="addWatch()">+</button>
            </div>
            <div class="ex-wrap" id="watch-list"></div>
        </div>

        <div class="box">
            <span class="box-title">🔓 Разблокировать монеты из чёрного списка</span>
            <div class="ex-wrap" id="quick-allow"></div>
            <div style="display:flex;gap:6px;margin-bottom:6px">
                <input type="text" id="a-inp" placeholder="Любой тикер, напр. NVDA">
                <button class="btn" onclick="addAllow()">Разблокировать</button>
            </div>
            <div class="ex-wrap" id="allow-list"></div>
        </div>

        <div class="box">
            <span class="box-title">🚫 Личный чёрный список</span>
            <div style="display:flex;gap:6px;margin-bottom:6px">
                <input type="text" id="b-inp" placeholder="Скрыть тикер">
                <button class="btn" onclick="addBan()">Скрыть</button>
            </div>
            <div class="ex-wrap" id="ban-list"></div>
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
        const QUICK_UNBLOCK = ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'SUI', 'PEPE', 'WIF', 'XAU', 'XAG', 'TSLA', 'NVDA'];
        let selectedEx = [...ALL_EX];
        let allowedCoins = [];
        let bannedCoins = [];
        let watchCoins = [];
        let inited = false;
        let tigerWs = null;

        function toast(m) {
            const t = document.getElementById('toast');
            t.textContent = m; t.classList.add('show');
            clearTimeout(window._tt); window._tt = setTimeout(() => t.classList.remove('show'), 2200);
        }
        function fmtUsd(v) {
            const n = Number(v || 0);
            if (n >= 1e6) return '$' + (n / 1e6).toFixed(2) + 'M';
            if (n >= 1e3) return '$' + Math.round(n / 1e3) + 'K';
            return '$' + Math.round(n);
        }
        function switchTab(t) {
            ['live', 'hist', 'set'].forEach(x => {
                document.getElementById('p-' + x).classList.toggle('active', x === t);
                document.getElementById('tb-' + x).classList.toggle('active', x === t);
            });
        }
        function connectTiger(manual) {
            const btn = document.getElementById('tiger-btn');
            try {
                if (tigerWs && tigerWs.readyState === 1) { if (manual) toast('🐯 Tiger подключён'); return; }
                tigerWs = new WebSocket('ws://127.0.0.1:21310');
                tigerWs.onopen = () => { btn.className = 'chip on'; btn.textContent = '🐯 ON'; if (manual) toast('🐯 Подключено'); };
                tigerWs.onclose = () => { btn.className = 'chip'; btn.textContent = '🐯 Tiger'; };
                tigerWs.onerror = () => { btn.className = 'chip'; btn.textContent = '🐯 Tiger'; };
            } catch {}
        }
        function openTiger(ex, sym, mkt) {
            if (!tigerWs || tigerWs.readyState !== 1) return;
            const futMap = { Binance: 'BINANCEF', BinanceAlpha: 'BINANCE', Bybit: 'BYBITF', OKX: 'OKXF', Bitget: 'BITGETF', Gate: 'GATEIOF', KuCoin: 'KUCOINF', Aster: 'BINANCEF' };
            const spotMap = { Binance: 'BINANCE', BinanceAlpha: 'BINANCE', Bybit: 'BYBIT', OKX: 'OKX', Bitget: 'BITGET', Gate: 'GATEIO', KuCoin: 'KUCOIN', Aster: 'BINANCE' };
            const exCode = mkt === 'SPOT' ? (spotMap[ex] || 'BINANCE') : (futMap[ex] || 'BINANCEF');
            try {
                tigerWs.send(JSON.stringify({ action: 'SetTicker', exchange: exCode, ticker: sym, group: 1 }));
                toast('🐯 ' + sym);
            } catch {}
        }
        function renderEx() {
            document.getElementById('ex-checks').innerHTML = ALL_EX.map(e =>
                `<button type="button" class="ex-btn ${selectedEx.includes(e) ? 'on' : ''}" onclick="toggleEx('${e}')">${e}</button>`
            ).join('');
        }
        function toggleEx(e) {
            if (selectedEx.includes(e)) {
                if (selectedEx.length <= 1) return toast('Оставьте хотя бы 1 биржу');
                selectedEx = selectedEx.filter(x => x !== e);
            } else selectedEx.push(e);
            renderEx();
        }
        function renderLists() {
            document.getElementById('quick-allow').innerHTML = QUICK_UNBLOCK.map(c =>
                `<button type="button" class="ex-btn ${allowedCoins.includes(c) ? 'on' : ''}" onclick="toggleAllow('${c}')">${allowedCoins.includes(c) ? '🔓' : '🔒'} ${c}</button>`
            ).join('');
            document.getElementById('allow-list').innerHTML = allowedCoins.length
                ? allowedCoins.map(c => `<span class="pill-tag">${c}<button onclick="updateAllow('${c}','remove')">✕</button></span>`).join('')
                : '<span class="fl">Нет разблокированных монет</span>';
            document.getElementById('ban-list').innerHTML = bannedCoins.length
                ? bannedCoins.map(c => `<span class="pill-tag ban">${c}<button onclick="updateBan('${c}','unban')">✕</button></span>`).join('')
                : '<span class="fl">Список пуст</span>';
            document.getElementById('watch-list').innerHTML = watchCoins.length
                ? watchCoins.map(c => `<span class="pill-tag">${c}<button onclick="updateWatch('${c}','remove')">✕</button></span>`).join('')
                : '<span class="fl">Сканируется весь рынок</span>';
        }
        function cardHtml(x) {
            const isB = x.side === 'BID';
            const mTag = x.exchange === 'BinanceAlpha'
                ? '<span class="tag alpha">Alpha</span>'
                : (x.market_type === 'SPOT' ? '<span class="tag spot">Спот</span>' : '<span class="tag">Фьюч</span>');
            return `<div class="card ${isB ? 'bid' : 'ask'}" onclick="openTiger('${x.exchange}','${x.symbol}','${x.market_type}')">
                <div class="card-top">
                    <div class="sym"><span>${x.symbol}</span><span class="tag">${x.exchange}</span>${mTag}</div>
                    <div style="display:flex;gap:5px;align-items:center">
                        <span class="badge gold">${fmtUsd(x.wall_usd)}</span>
                        <span class="badge ${isB ? 'bid' : 'ask'}">${x.side} · x${x.anomaly_x}</span>
                    </div>
                </div>
                <div class="meta">
                    <span>Стена: <b>${x.wall_price}</b> (${x.distance_pct}% от спреда)</span>
                    <span>${x.eat_minutes ? ('~' + x.eat_minutes + 'м · ') : ''}${x.utc_time || ''}</span>
                </div>
            </div>`;
        }
        async function searchCoin() {
            const coin = (document.getElementById('s-coin').value || '').trim();
            if (!coin) return toast('Введите тикер монеты');
            const box = document.getElementById('search-res');
            box.innerHTML = '<div class="empty">⏳ Сканирую стаканы на 8 биржах (Спот + Фьючерсы + Alpha)...</div>';
            const sp = Number(document.getElementById('s-spot').value || 250000);
            const ft = Number(document.getElementById('s-fut').value || 500000);
            const ds = Number(document.getElementById('s-dist').value || 10);
            try {
                const res = await fetch(`/api/search?user_id=${userId}&coin=${encodeURIComponent(coin)}&min_spot=${sp}&min_fut=${ft}&max_dist=${ds}`);
                const d = await res.json();
                const m = d.matches || [];
                const top = d.top_seen || [];
                if (m.length) {
                    box.innerHTML = `<div class="fl" style="margin-bottom:6px">Найдено: ${m.length} (стаканов: ${d.checked_books})</div>` + m.map(cardHtml).join('');
                } else {
                    box.innerHTML = `<div class="empty">По порогам не найдено (стаканов: ${d.checked_books})</div>` +
                        (top.length ? '<div class="fl" style="margin:4px 0">Крупнейшие лимитки сейчас:</div>' + top.slice(0, 4).map(cardHtml).join('') : '');
                }
            } catch { box.innerHTML = '<div class="empty">Ошибка поиска</div>'; }
        }
        async function saveSettings() {
            const cfg = {
                enabled: document.getElementById('c-enabled').checked,
                market_mode: document.getElementById('c-market').value,
                side_mode: document.getElementById('c-side').value,
                min_wall_spot_usd: Number(document.getElementById('c-wall-spot').value || 250000),
                min_wall_futures_usd: Number(document.getElementById('c-wall-fut').value || 500000),
                max_distance_pct: Number(document.getElementById('c-dist').value || 10),
                min_anomaly_x: Number(document.getElementById('c-anom').value || 5),
                min_lifetime_sec: Number(document.getElementById('c-life').value ?? 0),
                min_vol24h: Number(document.getElementById('c-vol').value || 500000),
                cooldown_sec: Number(document.getElementById('c-cd').value || 300),
                exchanges: selectedEx,
                watch_coins: watchCoins
            };
            const dcfg = {
                enabled: document.getElementById('dc-en').checked,
                webhook_url: (document.getElementById('dc-url').value || '').trim()
            };
            const res = await fetch('/api/settings', {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ user_id: userId, config: cfg, discord_config: dcfg })
            });
            if (res.ok) { toast('✅ Сохранено'); loadData(); } else toast('❌ Ошибка');
        }
        async function testDiscord() {
            const res = await fetch('/api/discord/test', {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ user_id: userId, webhook_url: (document.getElementById('dc-url').value || '').trim() })
            });
            const d = await res.json();
            toast(d.ok ? '✅ Тест отправлен!' : ('❌ ' + (d.detail || 'Ошибка')));
        }
        async function toggleAllow(c) { await updateAllow(c, allowedCoins.includes(c) ? 'remove' : 'add'); }
        async function addAllow() {
            const el = document.getElementById('a-inp');
            if (!el.value.trim()) return;
            await updateAllow(el.value.trim(), 'add'); el.value = '';
        }
        async function updateAllow(coin, action) {
            const res = await fetch('/api/allow', {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ user_id: userId, coin, action })
            });
            if (res.ok) { const d = await res.json(); allowedCoins = d.allowed_coins || []; bannedCoins = d.banned_coins || []; renderLists(); toast('✅ Обновлено'); }
        }
        async function addBan() {
            const el = document.getElementById('b-inp');
            if (!el.value.trim()) return;
            await updateBan(el.value.trim(), 'ban'); el.value = '';
        }
        async function updateBan(coin, action) {
            const res = await fetch('/api/ban', {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ user_id: userId, coin, action })
            });
            if (res.ok) { const d = await res.json(); bannedCoins = d.banned_coins || []; allowedCoins = d.allowed_coins || []; renderLists(); toast('✅ Обновлено'); }
        }
        async function addWatch() {
            const el = document.getElementById('w-inp');
            if (!el.value.trim()) return;
            await updateWatch(el.value.trim(), 'add'); el.value = '';
        }
        async function updateWatch(coin, action) {
            const res = await fetch('/api/watch', {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ user_id: userId, coin, action })
            });
            if (res.ok) { const d = await res.json(); watchCoins = d.watch_coins || []; allowedCoins = d.allowed_coins || []; renderLists(); toast('✅ Watchlist обновлён'); }
        }
        async function loadData() {
            try {
                const res = await fetch('/api/data?user_id=' + encodeURIComponent(userId));
                if (!res.ok) return;
                const d = await res.json();
                if (!inited) {
                    const c = d.config || {};
                    const dc = d.discord_config || {};
                    document.getElementById('c-enabled').checked = c.enabled !== false;
                    document.getElementById('c-market').value = c.market_mode || 'BOTH';
                    document.getElementById('c-side').value = c.side_mode || 'BOTH';
                    document.getElementById('c-wall-spot').value = c.min_wall_spot_usd ?? 250000;
                    document.getElementById('c-wall-fut').value = c.min_wall_futures_usd ?? 500000;
                    document.getElementById('c-dist').value = c.max_distance_pct ?? 10;
                    document.getElementById('s-spot').value = c.min_wall_spot_usd ?? 250000;
                    document.getElementById('s-fut').value = c.min_wall_futures_usd ?? 500000;
                    document.getElementById('s-dist').value = c.max_distance_pct ?? 10;
                    document.getElementById('c-anom').value = c.min_anomaly_x ?? 5;
                    document.getElementById('c-life').value = c.min_lifetime_sec ?? 0;
                    document.getElementById('c-vol').value = c.min_vol24h ?? 500000;
                    document.getElementById('c-cd').value = c.cooldown_sec ?? 300;
                    if (Array.isArray(c.exchanges) && c.exchanges.length) selectedEx = c.exchanges;
                    document.getElementById('dc-en').checked = dc.enabled !== false;
                    document.getElementById('dc-url').value = dc.webhook_url || '';
                    allowedCoins = d.allowed_coins || [];
                    bannedCoins = d.banned_coins || [];
                    watchCoins = c.watch_coins || [];
                    renderEx(); renderLists();
                    inited = true;
                }
                const act = d.active || [];
                const hist = d.history || [];
                document.getElementById('live-cnt').textContent = act.length;
                document.getElementById('live-list').innerHTML = act.length ? act.map(cardHtml).join('') : '<div class="empty">Сейчас активных плотностей нет</div>';
                document.getElementById('hist-list').innerHTML = hist.length ? hist.map(cardHtml).join('') : '<div class="empty">За сегодня (UTC) сигналов пока не было</div>';
            } catch {}
        }
        renderEx(); connectTiger(false); loadData(); setInterval(loadData, 2000);
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
    coin: str
    action: str = "add"


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
    await sync_runtime_allowed_bases()
    users = await get_cached_users(force=True)
    if users:
        density_engine.update_config(normalize_config(users[-1].get("config")))

    ws_session = aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=120, ttl_dns_cache=300),
        headers={"User-Agent": "Mozilla/5.0"},
    )
    _http_session = ws_session
    await refresh_tradfi_metadata(ws_session)
    await refresh_binance_alpha_tokens(ws_session, force=True)

    tasks: List[asyncio.Task[Any]] = [
        asyncio.create_task(_run_telegram_polling()),
        asyncio.create_task(_tradfi_refresher_loop(ws_session)),
        # === ПОЛНЫЙ ОБХОД ВСЕХ МОНЕТ КАЖДЫЕ 3 МИНУТЫ (±10% ОТ СПРЕДА, 8 БИРЖ) ===
        asyncio.create_task(full_market_scan_loop(ws_session, density_engine, "Binance", "FUTURES")),
        asyncio.create_task(full_market_scan_loop(ws_session, density_engine, "Binance", "SPOT")),
        asyncio.create_task(full_market_scan_loop(ws_session, density_engine, "BinanceAlpha", "SPOT")),
        asyncio.create_task(full_market_scan_loop(ws_session, density_engine, "Bybit", "FUTURES")),
        asyncio.create_task(full_market_scan_loop(ws_session, density_engine, "Bybit", "SPOT")),
        asyncio.create_task(full_market_scan_loop(ws_session, density_engine, "OKX", "FUTURES")),
        asyncio.create_task(full_market_scan_loop(ws_session, density_engine, "OKX", "SPOT")),
        asyncio.create_task(full_market_scan_loop(ws_session, density_engine, "Bitget", "FUTURES")),
        asyncio.create_task(full_market_scan_loop(ws_session, density_engine, "Bitget", "SPOT")),
        asyncio.create_task(full_market_scan_loop(ws_session, density_engine, "Gate", "FUTURES")),
        asyncio.create_task(full_market_scan_loop(ws_session, density_engine, "Gate", "SPOT")),
        asyncio.create_task(full_market_scan_loop(ws_session, density_engine, "KuCoin", "FUTURES")),
        asyncio.create_task(full_market_scan_loop(ws_session, density_engine, "KuCoin", "SPOT")),
        asyncio.create_task(full_market_scan_loop(ws_session, density_engine, "Aster", "FUTURES")),
        asyncio.create_task(full_market_scan_loop(ws_session, density_engine, "Aster", "SPOT")),
        # === WEBSOCKET РЕАЛТАЙМ ДЛЯ ТОП-ПАР ===
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
        asyncio.create_task(bybit_depth_ws(ws_session, density_engine, "FUTURES")),
        asyncio.create_task(bybit_depth_ws(ws_session, density_engine, "SPOT")),
        asyncio.create_task(okx_depth_ws(ws_session, density_engine, "FUTURES")),
        asyncio.create_task(okx_depth_ws(ws_session, density_engine, "SPOT")),
        asyncio.create_task(bitget_depth_ws(ws_session, density_engine, "FUTURES")),
        asyncio.create_task(bitget_depth_ws(ws_session, density_engine, "SPOT")),
        asyncio.create_task(gate_depth_ws(ws_session, density_engine, "FUTURES")),
        asyncio.create_task(gate_depth_ws(ws_session, density_engine, "SPOT")),
        asyncio.create_task(kucoin_depth_ws(ws_session, density_engine, "FUTURES")),
        asyncio.create_task(kucoin_depth_ws(ws_session, density_engine, "SPOT")),
    ]
    logger.info("Запущен скринер аномальных плотностей (8 бирж + BinanceAlpha · ±10% от спреда · полный скан раз в 3 мин).")
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
    return JSONResponse({"status": "ok", "service": "density-wall-screener", "exchanges": 8, "markets": ["FUTURES", "SPOT", "ALPHA"]})


@app.api_route("/ping", methods=["GET", "HEAD"])
async def ping() -> JSONResponse:
    return JSONResponse({"status": "ok", "exchanges": 8, "markets": 3})


@app.get("/webapp", response_class=HTMLResponse)
async def webapp() -> HTMLResponse:
    return HTMLResponse(WEBAPP_HTML)


@app.get("/api/data")
async def api_data(user_id: int = Query(default=0)) -> JSONResponse:
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now = time.time()
    if _api_cache["utc_date"] != today_utc or (now - _api_cache["ts"]) >= 2.0:
        _api_cache["history"] = await db_fetchall(
            "SELECT id, utc_time, exchange, market_type, symbol, side, wall_price, current_price, "
            "distance_pct, wall_usd, anomaly_x, eat_minutes, volume_24h_usd "
            "FROM densities WHERE utc_date = ? ORDER BY id DESC LIMIT 120",
            (today_utc,),
        )
        _api_cache["utc_date"] = today_utc
        _api_cache["ts"] = now

    if user_id:
        cfg, dcfg, bans, allowed = await get_user_record(user_id)
    else:
        cfg, dcfg, bans, allowed = dict(density_engine.config), dict(DEFAULT_DISCORD_CONFIG), [], sorted(_runtime_allowed_bases)

    ban_set = set(bans)
    allow_set = set(allowed) | set(cfg.get("watch_coins") or [])

    active_out = [
        {
            "exchange": ev.exchange,
            "market_type": ev.market_type,
            "symbol": ev.symbol,
            "side": ev.side.value,
            "wall_price": ev.wall_price,
            "current_price": ev.current_price,
            "distance_pct": ev.distance_pct,
            "wall_usd": ev.wall_usd,
            "anomaly_x": ev.anomaly_x,
            "eat_minutes": ev.eat_minutes,
            "utc_time": ev.timestamp_utc,
        }
        for ev in density_engine.get_active_walls()
        if extract_base_asset(ev.symbol) not in ban_set
        and not is_blacklisted_symbol(ev.symbol, ev.exchange, extra_allowed=allow_set)
        and wall_matches_user_config(
            ev.exchange, ev.market_type, ev.symbol, ev.side.value,
            ev.wall_usd, ev.distance_pct, ev.anomaly_x, ev.lifetime_sec, ev.volume_24h_usd, cfg,
        )
    ]

    hist_out = [
        h
        for h in _api_cache["history"]
        if extract_base_asset(str(h.get("symbol") or "")) not in ban_set
        and not is_blacklisted_symbol(str(h.get("symbol") or ""), str(h.get("exchange") or ""), extra_allowed=allow_set)
        and wall_matches_user_config(
            str(h.get("exchange") or ""),
            str(h.get("market_type") or "FUTURES"),
            str(h.get("symbol") or ""),
            str(h.get("side") or ""),
            float(h.get("wall_usd") or 0),
            float(h.get("distance_pct") or 0),
            float(h.get("anomaly_x") or 0),
            float(cfg.get("min_lifetime_sec", 0.0)),
            float(h.get("volume_24h_usd") or 0),
            cfg,
        )
    ]

    return JSONResponse(
        {
            "utc_date": today_utc,
            "active": active_out,
            "history": hist_out,
            "config": cfg,
            "discord_config": dcfg,
            "banned_coins": bans,
            "allowed_coins": allowed,
        }
    )


@app.get("/api/search")
async def api_search(
    coin: str = Query(default=""),
    user_id: int = Query(default=0),
    min_spot: Optional[float] = Query(default=None),
    min_fut: Optional[float] = Query(default=None),
    max_dist: Optional[float] = Query(default=None),
) -> JSONResponse:
    cfg = (await get_user_record(user_id))[0] if user_id else dict(density_engine.config)
    res = await live_search_coin_densities(
        _http_session,
        coin,
        cfg,
        override_min_spot=min_spot,
        override_min_fut=min_fut,
        override_max_dist=max_dist,
    )
    return JSONResponse(res)


@app.post("/api/settings")
async def api_settings(payload: SettingsPayload) -> JSONResponse:
    if payload.user_id > 0:
        cfg, dcfg, _, _ = await save_user_record(payload.user_id, cfg=payload.config, dcfg=payload.discord_config)
    else:
        cfg = normalize_config(payload.config)
        dcfg = normalize_discord_config(payload.discord_config)
        density_engine.update_config(cfg)
    return JSONResponse({"status": "ok", "config": cfg, "discord_config": dcfg})


@app.post("/api/discord/test")
async def api_discord_test(payload: DiscordTestPayload) -> JSONResponse:
    url = payload.webhook_url.strip()
    if not url and payload.user_id > 0:
        _, dcfg, _, _ = await get_user_record(payload.user_id)
        url = resolve_discord_webhook(dcfg)
    if not url:
        url = ENV_DISCORD_WEBHOOK
    ok, err = await send_test_discord(url, session=_http_session)
    return JSONResponse({"ok": ok, "detail": err}, status_code=200 if ok else 400)


@app.post("/api/allow")
async def api_allow(payload: CoinActionPayload) -> JSONResponse:
    coins = parse_coin_list(payload.coin)
    if not coins:
        return JSONResponse({"status": "error"}, status_code=400)
    if payload.user_id > 0:
        _, _, bans, allowed = await get_user_record(payload.user_id)
        if payload.action == "remove":
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
        _, _, bans, allowed = await get_user_record(payload.user_id)
        if payload.action == "unban":
            bans = [c for c in bans if c not in coins]
        else:
            bans = list(dict.fromkeys(bans + coins))
            allowed = [a for a in allowed if a not in coins]
        _, _, bans, allowed = await save_user_record(payload.user_id, bans=bans, allowed=allowed)
        return JSONResponse({"status": "ok", "banned_coins": bans, "allowed_coins": allowed})
    return JSONResponse({"status": "ok", "banned_coins": coins, "allowed_coins": []})


@app.post("/api/watch")
async def api_watch(payload: CoinActionPayload) -> JSONResponse:
    coins = parse_coin_list(payload.coin)
    if not coins:
        return JSONResponse({"status": "error"}, status_code=400)
    if payload.user_id > 0:
        cfg, _, _, allowed = await get_user_record(payload.user_id)
        if payload.action == "remove":
            cfg["watch_coins"] = [c for c in cfg["watch_coins"] if c not in coins]
        else:
            cfg["watch_coins"] = list(dict.fromkeys(cfg["watch_coins"] + coins))
            allowed = list(dict.fromkeys(allowed + coins))
        cfg, _, _, allowed = await save_user_record(payload.user_id, cfg=cfg, allowed=allowed)
        return JSONResponse({"status": "ok", "watch_coins": cfg["watch_coins"], "allowed_coins": allowed})
    return JSONResponse({"status": "ok", "watch_coins": coins, "allowed_coins": coins})


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)