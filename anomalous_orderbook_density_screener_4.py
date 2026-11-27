# ... existing code ...
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
    """Формирует интерактивное меню управления личным чёрным списком монет."""
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


async def _broadcast_messages(
# ... existing code ...
    for wurl in discord_urls:
        asyncio.create_task(send_density_discord(wurl, ev, session=_http_session))
    if target_ids:
        asyncio.create_task(_broadcast_messages(target_ids, text, _build_alert_keyboard(base)))

    await db_execute(
# ... existing code ...
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
# ... existing code ...
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
# ... existing code ...
        if query.message:
            await query.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
        return
    elif data.startswith("ban_coin:"):
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
    elif data.startswith("unban_coin:"):
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
    elif data == "ban_menu":
        text, kb = _format_ban_menu(bans)
        if query.message:
            try:
                await query.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
            except Exception:
                pass
        await query.answer()
        return
    elif data.startswith("ban_rm:"):
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
    elif data == "ban_clear":
        _, _, new_bans, _ = await save_user_record(uid, bans=[])
        await query.answer("✅ Чёрный список очищен")
        text, kb = _format_ban_menu(new_bans)
        if query.message:
            try:
                await query.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
            except Exception:
                pass
        return
    elif data == "dc_test":
# ... existing code ...
        .badge.bid { background: rgba(22,199,132,0.18); color: var(--bid); }
        .badge.ask { background: rgba(234,57,67,0.18); color: var(--ask); }
        .badge.gold { background: rgba(245,185,66,0.18); color: var(--gold); }
        .ban-x {
            border: none; background: rgba(234,57,67,0.16); color: var(--ask);
            width: 24px; height: 24px; border-radius: 6px; font-size: 12px; font-weight: 800;
            cursor: pointer; display: inline-flex; align-items: center; justify-content: center;
        }
        .meta { font-size: 12px; color: var(--hint); display: flex; justify-content: space-between; flex-wrap: wrap; gap: 6px; }
# ... existing code ...
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
# ... existing code ...
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
# ... existing code ...
        async function updateBan(coin, action) {
            const res = await fetch('/api/ban', {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ user_id: userId, coin, action })
            });
            if (res.ok) {
                const d = await res.json();
                bannedCoins = d.banned_coins || [];
                allowedCoins = d.allowed_coins || [];
                if (Array.isArray(d.watch_coins)) watchCoins = d.watch_coins;
                renderLists();
                toast(action === 'ban' ? ('🚫 ' + coin + ' заблокирована') : ('✅ ' + coin + ' разблокирована'));
                loadData();
            }
        }
# ... existing code ...
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
# ... existing code ...