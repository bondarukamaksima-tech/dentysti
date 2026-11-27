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
# ... existing code ...
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
# ... existing code ...
# Глобальный белый список монет, разблокированных из ЧС или добавленных в отслеживание (/allow, /watch)
_runtime_allowed_bases: Set[str] = set()
_whitelist_version: int = 0

# Кэш токенов Binance Alpha: display_symbol (напр. BUBBUSDT) -> (alpha_pair, base_ticker, vol24h_usd)
_alpha_markets: Dict[str, Tuple[str, str, float]] = {}
_alpha_by_base: Dict[str, Tuple[str, str, float]] = {}
_alpha_last_refresh: float = 0.0
# ... existing code ...
    if new_pairs or new_bases:
        _dynamic_tradfi_pairs.update(new_pairs)
        _dynamic_tradfi_bases.update(new_bases)
        logger.info(
            "[TradFi] Заблокировано %s контрактов (%s базовых тикеров) из API 7 бирж.",
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

    # Маппинг alphaId (напр. ALPHA_175) -> (trade_pair, quoteAsset)
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
        # Предпочитаем USDT-пару, если есть обе (USDT и USDC)
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
            # Если get-exchange-info временно недоступен, используем стандартную пару ALPHA_xxxUSDT
            if pair_by_alpha_id:
                continue
            trade_pair, quote = f"{alpha_id}USDT", "USDT"
        else:
            trade_pair, quote = pair_info

        vol24 = _safe_float(item.get("volume24h"))
        display_sym = f"{raw_sym}{quote}"
        # При совпадении тикера у разных сетей берём более ликвидный
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
# ... existing code ...
    cfg["min_wall_spot_usd"] = round(max(10_000.0, _safe_float(data.get("min_wall_spot_usd"), spot_default)), 2)
    cfg["min_wall_futures_usd"] = round(max(10_000.0, _safe_float(data.get("min_wall_futures_usd"), fut_default)), 2)
    cfg["max_distance_pct"] = round(min(50.0, max(0.1, _safe_float(data.get("max_distance_pct"), cfg["max_distance_pct"]))), 2)
    cfg["min_anomaly_x"] = round(min(100.0, max(1.5, _safe_float(data.get("min_anomaly_x"), cfg["min_anomaly_x"]))), 1)
# ... existing code ...
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
# ... existing code ...
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
# ... existing code ...
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

        # Берём все уровни в диапазоне ±max_distance_pct от спреда (до 500 уровней на сторону)
        top_bids = [(px, px * qty) for px, qty in bids[:500] if px > 0 and qty > 0 and px >= min_bid_bound]
        top_asks = [(px, px * qty) for px, qty in asks[:500] if px > 0 and qty > 0 and px <= max_ask_bound]
        if len(top_bids) < 2 or len(top_asks) < 2:
            return

        # Медиану считаем по уровням внутри диапазона (до 200 на сторону), чтобы точнее отражать норму стакана
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

        # Порог поддержания отслеживания (75% от min_wall)
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
                # Дистанция строго от спреда: для BID (лонг) от best_bid, для ASK (шорт) от best_ask
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

        # Удаляем только те исчезнувшие плотности, которые попадали в глубину текущего снимка стакана,
        # чтобы мелкие WS-обновления (depth20) не стирали глубокие плотности (3–10%) из 3-минутного скана
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
        # Храним активные плотности 3.5 минуты (до следующего 3-минутного полного обхода)
        expired = [k for k, (_, ts) in self._active.items() if now - ts > 210.0]
        for k in expired:
            self._active.pop(k, None)
        events = [ev for ev, _ in self._active.values()]
        events.sort(key=lambda e: e.wall_usd, reverse=True)
        return events


# ==================== ЖИВОЙ ПОИСК ПЛОТНОСТЕЙ ПО ОТДЕЛЬНОЙ МОНЕТЕ (8 БИРЖ · ФЬЮЧЕРСЫ + СПОТ + ALPHA) ====================

async def live_search_coin_densities(
    session: Optional[aiohttp.ClientSession],
    coin_raw: str,
    cfg: Dict[str, Any],
    override_min_spot: Optional[float] = None,
    override_min_fut: Optional[float] = None,
    override_max_dist: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Делает параллельный живой REST-снимок глубоких стаканов выбранной монеты по 8 биржам
    (Фьючерсы, Спот и Binance Alpha) и возвращает плотности в диапазоне ±max_distance_pct от спреда.
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
# ... existing code ...
async def _tradfi_refresher_loop(session: aiohttp.ClientSession) -> None:
    while True:
        await asyncio.sleep(1800)
        try:
            await refresh_tradfi_metadata(session)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass


# ==================== ПОЛНЫЙ ОБХОД ВСЕХ МОНЕТ НА ВСЕХ 8 БИРЖАХ РАЗ В 3 МИНУТЫ (±10% ОТ СПРЕДА) ====================

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
    # Небольшой сдвиг старта, чтобы биржи не делали залп в одну секунду
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

            # Сортируем от более ликвидных к менее ликвидным и проходим ВСЕ подходящие монеты
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
# ... existing code ...
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
# ... existing code ...
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
# ... existing code ...
    cfg, _, _, _ = await get_user_record(message.from_user.id)
    wait_msg = await message.answer(f"🔎 Сканирую живые стаканы <code>{extract_base_asset(coin)}</code> на 8 биржах...", parse_mode="HTML")
    res = await live_search_coin_densities(
# ... existing code ...
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
# ... existing code ...
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
# ... existing code ...
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
# ... existing code ...
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
# ... existing code ...
    <div class="topbar">
        <div class="brand">🧱 Density Screener <span style="font-size:11px;color:var(--bid)">8 EX · ALPHA · ±10%</span></div>
        <div style="display:flex;gap:6px">
# ... existing code ...
            <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px">
                <div class="fg"><span class="fl">Спот ($)</span><input type="number" id="s-spot" value="250000" step="25000"></div>
                <div class="fg"><span class="fl">Фьючерс ($)</span><input type="number" id="s-fut" value="500000" step="50000"></div>
                <div class="fg"><span class="fl">Дист (%)</span><input type="number" id="s-dist" value="10" step="0.5"></div>
            </div>
# ... existing code ...
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
# ... existing code ...
        const ALL_EX = ['Binance', 'BinanceAlpha', 'Bybit', 'OKX', 'Bitget', 'Gate', 'KuCoin', 'Aster'];
        const QUICK_UNBLOCK = ['BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'SUI', 'PEPE', 'WIF', 'XAU', 'XAG', 'TSLA', 'NVDA'];
# ... existing code ...
        function openTiger(ex, sym, mkt) {
            if (!tigerWs || tigerWs.readyState !== 1) return;
            const futMap = { Binance: 'BINANCEF', BinanceAlpha: 'BINANCE', Bybit: 'BYBITF', OKX: 'OKXF', Bitget: 'BITGETF', Gate: 'GATEIOF', KuCoin: 'KUCOINF', Aster: 'BINANCEF' };
            const spotMap = { Binance: 'BINANCE', BinanceAlpha: 'BINANCE', Bybit: 'BYBIT', OKX: 'OKX', Bitget: 'BITGET', Gate: 'GATEIO', KuCoin: 'KUCOIN', Aster: 'BINANCE' };
            const exCode = mkt === 'SPOT' ? (spotMap[ex] || 'BINANCE') : (futMap[ex] || 'BINANCEF');
# ... existing code ...
            box.innerHTML = '<div class="empty">⏳ Сканирую стаканы на 8 биржах (Спот + Фьючерсы + Alpha)...</div>';
            const sp = Number(document.getElementById('s-spot').value || 250000);
            const ft = Number(document.getElementById('s-fut').value || 500000);
            const ds = Number(document.getElementById('s-dist').value || 10);
# ... existing code ...
                min_wall_spot_usd: Number(document.getElementById('c-wall-spot').value || 250000),
                min_wall_futures_usd: Number(document.getElementById('c-wall-fut').value || 500000),
                max_distance_pct: Number(document.getElementById('c-dist').value || 10),
                min_anomaly_x: Number(document.getElementById('c-anom').value || 5),
                min_lifetime_sec: Number(document.getElementById('c-life').value ?? 0),
# ... existing code ...
                    document.getElementById('c-wall-spot').value = c.min_wall_spot_usd ?? 250000;
                    document.getElementById('c-wall-fut').value = c.min_wall_futures_usd ?? 500000;
                    document.getElementById('c-dist').value = c.max_distance_pct ?? 10;
                    document.getElementById('s-spot').value = c.min_wall_spot_usd ?? 250000;
                    document.getElementById('s-fut').value = c.min_wall_futures_usd ?? 500000;
                    document.getElementById('s-dist').value = c.max_distance_pct ?? 10;
                    document.getElementById('c-anom').value = c.min_anomaly_x ?? 5;
                    document.getElementById('c-life').value = c.min_lifetime_sec ?? 0;
# ... existing code ...
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
# ... existing code ...
if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)