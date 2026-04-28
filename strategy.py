"""
strategy.py — логика расчётов: Fair Price, спред, PnL, форматирование.
"""
 
from datetime import datetime, timezone
from typing import Optional
 
 
def calculate_fair_price(data: dict) -> Optional[float]:
    prices = []
    for key in ("binance", "okx", "coingecko"):
        entry = data.get(key, {})
        if entry.get("ok") and entry.get("price") is not None:
            prices.append(entry["price"])
    if not prices:
        return None
    return sum(prices) / len(prices)
 
 
def calculate_spread(bybit_price: float, fair_price: float) -> float:
    if fair_price == 0:
        return 0.0
    return (bybit_price - fair_price) / fair_price * 100
 
 
def calculate_pnl(entry_price: float, current_price: float, side: str, leverage: int) -> float:
    if entry_price == 0:
        return 0.0
    if side.lower() == "long":
        raw = (current_price - entry_price) / entry_price
    else:
        raw = (entry_price - current_price) / entry_price
    return raw * leverage * 100
 
 
def format_duration(start_time: datetime) -> str:
    now = datetime.now(timezone.utc)
    delta = now - start_time
    total_seconds = int(delta.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
 
 
def fmt_price(p) -> str:
    """Умное форматирование цены: больше знаков для маленьких цен."""
    if p is None:
        return "N/A"
    if p >= 1000:
        return f"{p:,.2f}$"
    elif p >= 1:
        return f"{p:,.4f}$"
    elif p >= 0.01:
        return f"{p:,.6f}$"
    else:
        return f"{p:,.8f}$"
 
 
def fmt_fr(fr) -> str:
    if fr is None:
        return "N/A"
    pct = fr * 100
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.4f}%"
 
 
def build_analysis_message(symbol: str, data: dict) -> str:
    bybit = data["bybit"]
    bybit_price = bybit.get("price")
    fair_price = calculate_fair_price(data)
    fetched_at = bybit.get("fetched_at", "—")
 
    if bybit_price and fair_price:
        spread = calculate_spread(bybit_price, fair_price)
        spread_sign = "+" if spread >= 0 else ""
        spread_str = f"{spread_sign}{spread:.4f}%"
        if spread > 0.01:
            price_signal = "🔴 Цена <b>выше</b> справедливой — сигнал на <b>Short</b>"
        elif spread < -0.01:
            price_signal = "🟢 Цена <b>ниже</b> справедливой — сигнал на <b>Long</b>"
        else:
            price_signal = "⚖️ Цена близка к справедливой"
    else:
        spread_str = "N/A"
        price_signal = "⚖️ Недостаточно данных"
 
    bybit_fr = bybit.get("funding_rate")
    bybit_interval = bybit.get("funding_interval", "8ч")
 
    lines = [
        f"🚀 <b>Анализ: {symbol}</b>",
        f"🕐 <i>Данные получены: {fetched_at}</i>",
        "",
        f"📊 <b>Bybit:</b> <code>{fmt_price(bybit_price)}</code>",
        f"⚖️ <b>Fair Price:</b> <code>{fmt_price(fair_price)}</code> (<b>{spread_str}</b> от Bybit)" if fair_price else "⚖️ <b>Fair Price:</b> N/A",
        f"📉 <b>Фандинг Bybit:</b> <code>{fmt_fr(bybit_fr)}</code> [{bybit_interval}]" if bybit_fr is not None else "📉 <b>Фандинг Bybit:</b> N/A",
        "",
        "━━━━━━━━━━━━━━━━━━━━",
        "📈 <b>Данные по биржам:</b>",
        "",
    ]
 
    for key, label, emoji in [("binance", "Binance", "🟡"), ("okx", "OKX", "🔵"), ("coingecko", "CoinGecko", "🌍")]:
        entry = data.get(key, {})
        ex_time = entry.get("fetched_at", "—")
        if not entry.get("ok"):
            err = entry.get("error", "Недоступна")
            lines.append(f"{emoji} <b>{label}:</b> ⚠️ {err}")
            lines.append("")
            continue
 
        price = entry.get("price")
        fr = entry.get("funding_rate")
        interval = entry.get("funding_interval", "—")
 
        if price and fair_price:
            diff = (price - fair_price) / fair_price * 100
            diff_sign = "+" if diff >= 0 else ""
            diff_str = f"{diff_sign}{diff:.4f}%"
            diff_emoji = "🔴" if diff > 0.01 else ("🟢" if diff < -0.01 else "⚪")
        else:
            diff_str = "N/A"
            diff_emoji = "⚪"
 
        lines.append(f"{emoji} <b>{label}</b>  <i>({ex_time})</i>")
        lines.append(f"  💵 Текущая цена: <code>{fmt_price(price)}</code>")
        lines.append(f"  ⚖️ Fair Price:   <code>{fmt_price(fair_price)}</code>")
        lines.append(f"  📊 Разница:      {diff_emoji} <code>{diff_str}</code>")
        if key != "coingecko":
            lines.append(f"  📉 Фандинг:      <code>{fmt_fr(fr)}</code> [{interval}]" if fr is not None else "  📉 Фандинг:      N/A")
        lines.append("")
 
    lines += [
        "━━━━━━━━━━━━━━━━━━━━",
        f"💡 <b>Рекомендация:</b> {price_signal}",
    ]
    return "\n".join(lines)
 
 
def build_single_exchange_message(symbol: str, exchange_name: str, ex_data: dict, fair_price: Optional[float]) -> str:
    """Сообщение для команд типа /btcbybit."""
    price = ex_data.get("price")
    fr = ex_data.get("funding_rate")
    interval = ex_data.get("funding_interval", "—")
    fetched_at = ex_data.get("fetched_at", "—")
 
    if not ex_data.get("ok"):
        return f"❌ <b>{exchange_name}</b> недоступна\nОшибка: {ex_data.get('error', '—')}"
 
    if price and fair_price:
        spread = (price - fair_price) / fair_price * 100
        spread_sign = "+" if spread >= 0 else ""
        spread_str = f"{spread_sign}{spread:.4f}%"
        if spread > 0.01:
            signal = "🔴 Цена выше Fair Price — сигнал на <b>Short</b>"
        elif spread < -0.01:
            signal = "🟢 Цена ниже Fair Price — сигнал на <b>Long</b>"
        else:
            signal = "⚖️ Цена близка к справедливой — ждём"
    else:
        spread_str = "N/A"
        signal = "⚖️ Недостаточно данных"
 
    emoji_map = {"bybit": "📊", "binance": "🟡", "okx": "🔵", "coingecko": "🌍"}
    emoji = emoji_map.get(exchange_name.lower(), "📊")
 
    lines = [
        f"{emoji} <b>{exchange_name.upper()} — {symbol}</b>",
        f"🕐 <i>{fetched_at}</i>",
        "",
        f"💵 <b>Текущая цена:</b>  <code>{fmt_price(price)}</code>",
        f"⚖️ <b>Fair Price:</b>    <code>{fmt_price(fair_price)}</code>",
        f"📊 <b>Спред:</b>         <code>{spread_str}</code>",
    ]
    if fr is not None:
        lines.append(f"📉 <b>Фандинг:</b>        <code>{fmt_fr(fr)}</code> [{interval}]")
    lines += ["", f"💡 {signal}"]
    return "\n".join(lines)
 
 
def build_check_message(trade: dict, current_data: dict) -> str:
    symbol = trade["symbol"]
    side = trade["side"]
    leverage = trade["leverage"]
    entry_price = trade["entry_price"]
    entry_time = trade["entry_time"]
 
    bybit_price = current_data["bybit"].get("price")
    fair_price = calculate_fair_price(current_data)
    bybit_fr = current_data["bybit"].get("funding_rate")
    bybit_interval = current_data["bybit"].get("funding_interval", "8ч")
    fetched_at = current_data["bybit"].get("fetched_at", "—")
 
    pnl = calculate_pnl(entry_price, bybit_price, side, leverage) if bybit_price else None
    spread = calculate_spread(bybit_price, fair_price) if (bybit_price and fair_price) else None
    duration = format_duration(entry_time)
 
    side_emoji = "📈" if side.lower() == "long" else "📉"
    pnl_emoji = "💰" if (pnl and pnl >= 0) else "🔴"
    pnl_str = f"{pnl:+.2f}%" if pnl is not None else "N/A"
 
    spread_comment = ""
    if spread is not None:
        spread_abs = abs(spread)
        if spread_abs < 0.05:
            spread_comment = "🟢 Сближаемся"
        elif spread_abs < 0.2:
            spread_comment = "🟡 Умеренный спред"
        else:
            spread_comment = "🔴 Большой спред"
 
    lines = [
        f"🔄 <b>Статус: {symbol} ({side_emoji} {side})</b>",
        f"🕐 <i>Обновлено: {fetched_at}</i>",
        "",
        f"⏱ <b>Удерживаю:</b> <code>{duration}</code>",
        f"💵 <b>Вход:</b> <code>{fmt_price(entry_price)}</code> | <b>Текущая:</b> <code>{fmt_price(bybit_price)}</code>" if bybit_price else f"💵 <b>Вход:</b> <code>{fmt_price(entry_price)}</code>",
        f"⚖️ <b>Fair Price:</b> <code>{fmt_price(fair_price)}</code>" if fair_price else "⚖️ <b>Fair Price:</b> N/A",
        f"📊 <b>Спред:</b> <code>{spread:.4f}%</code> {spread_comment}" if spread is not None else "📊 <b>Спред:</b> N/A",
        f"{pnl_emoji} <b>PnL:</b> <code>{pnl_str}</code> (с плечом x{leverage})",
        f"📉 <b>Фандинг (Bybit):</b> <code>{fmt_fr(bybit_fr)}</code> [{bybit_interval}]" if bybit_fr is not None else "📉 <b>Фандинг (Bybit):</b> N/A",
    ]
    return "\n".join(lines)
 
 
def build_close_message(trade: dict, current_data: dict, reason: str = "Спред выровнялся") -> str:
    symbol = trade["symbol"]
    side = trade["side"]
    leverage = trade["leverage"]
    entry_price = trade["entry_price"]
    entry_time = trade["entry_time"]
 
    bybit_price = current_data["bybit"].get("price")
    fair_price = calculate_fair_price(current_data)
    spread = calculate_spread(bybit_price, fair_price) if (bybit_price and fair_price) else None
    pnl = calculate_pnl(entry_price, bybit_price, side, leverage) if bybit_price else None
    duration = format_duration(entry_time)
    fetched_at = current_data["bybit"].get("fetched_at", "—")
 
    pnl_str = f"{pnl:+.2f}%" if pnl is not None else "N/A"
    spread_str = f"{spread:.4f}%" if spread is not None else "N/A"
 
    lines = [
        "✅ <b>Сделка закрыта!</b>",
        f"🕐 <i>{fetched_at}</i>",
        "",
        f"📌 <b>Тикер:</b> {symbol}",
        f"⏱ <b>Время удержания:</b> <code>{duration}</code>",
        f"🎯 <b>Вход:</b> <code>{fmt_price(entry_price)}</code> | <b>Выход:</b> <code>{fmt_price(bybit_price)}</code>" if bybit_price else f"🎯 <b>Вход:</b> <code>{fmt_price(entry_price)}</code>",
        f"📊 <b>Итоговый PnL:</b> <code>{pnl_str}</code> (x{leverage})",
        f"⚖️ <b>Статус:</b> {reason} до {spread_str}",
    ]
    return "\n".join(lines)
