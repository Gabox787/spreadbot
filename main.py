"""
main.py — точка входа: хэндлеры aiogram 3.x, FSM, мониторинг.
"""
 
import asyncio
import logging
import os
import re
from datetime import datetime, timezone
 
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)
from dotenv import load_dotenv
 
from exchanges import fetch_all_data, fetch_single_exchange
from strategy import (
    build_analysis_message, build_check_message, build_close_message,
    build_single_exchange_message, calculate_fair_price, calculate_spread,
)
from utils import CLOSE_SPREAD_THRESHOLD, normalize_symbol, setup_logging, start_keep_alive_server
 
load_dotenv()
setup_logging()
logger = logging.getLogger(__name__)
 
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не установлен в .env!")
 
# Опциональная защита по user_id
ALLOWED_USER_ID = os.getenv("ALLOWED_USER_ID")
 
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher(storage=MemoryStorage())
 
active_trades: dict[int, dict] = {}
 
# Паттерн для команд типа /btcbybit /ethbinance /solocx
EXCHANGE_CMD_RE = re.compile(
    r"^/([a-zA-Z0-9]+)(bybit|binance|okx|coingecko)$", re.IGNORECASE
)
 
# ─── FSM ───────────────────────────────────────────────────────────────────────
 
class TradeStates(StatesGroup):
    waiting_for_leverage = State()
 
# ─── Auth middleware ────────────────────────────────────────────────────────────
 
def is_allowed(user_id: int) -> bool:
    if not ALLOWED_USER_ID:
        return True
    return str(user_id) == str(ALLOWED_USER_ID)
 
# ─── Keyboards ─────────────────────────────────────────────────────────────────
 
def analysis_keyboard(symbol: str) -> InlineKeyboardMarkup:
    enc = symbol.replace("/", "_")
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📉 Открыть Short", callback_data=f"open_short:{enc}"),
        InlineKeyboardButton(text="📈 Открыть Long",  callback_data=f"open_long:{enc}"),
    ]])
 
def check_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🛑 Закрыть вручную", callback_data="close_trade"),
        InlineKeyboardButton(text="🔄 Обновить статус", callback_data="refresh_check"),
    ]])
 
def confirm_keyboard(symbol: str, side: str) -> InlineKeyboardMarkup:
    enc = symbol.replace("/", "_")
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="❌ Отменить", callback_data=f"cancel_trade:{enc}:{side}")
    ]])
 
# ─── Handlers ──────────────────────────────────────────────────────────────────
 
@dp.message(CommandStart())
async def cmd_start(message: Message):
    if not is_allowed(message.from_user.id):
        return
    await message.answer(
        "👋 <b>Привет!</b> Я бот для анализа торговых возможностей.\n\n"
        "📌 <b>Как использовать:</b>\n"
        "• Отправь тикер: <code>BTC</code> или <code>ETH/USDT</code>\n"
        "• <code>/check</code> — статус активной сделки\n"
        "• <code>/close</code> — закрыть сделку вручную\n\n"
        "📡 <b>Быстрые команды по бирже:</b>\n"
        "• <code>/btcbybit</code> — BTC только с Bybit\n"
        "• <code>/ethbinance</code> — ETH только с Binance\n"
        "• <code>/solocx</code> — SOL только с OKX\n"
        "<i>(формат: /[тикер][биржа])</i>\n\n"
        "⚡ Авто-закрытие при спреде &lt; 0.03%"
    )
 
 
@dp.message(Command("check"))
async def cmd_check(message: Message):
    if not is_allowed(message.from_user.id):
        return
    trade = active_trades.get(message.from_user.id)
    if not trade:
        await message.answer("❌ Нет активной сделки. Отправь тикер для анализа.")
        return
    await message.answer("⏳ Обновляю данные...")
    data = await fetch_all_data(trade["symbol"])
    await message.answer(build_check_message(trade, data), reply_markup=check_keyboard())
 
 
@dp.message(Command("close"))
async def cmd_close(message: Message, state: FSMContext):
    if not is_allowed(message.from_user.id):
        return
    trade = active_trades.pop(message.from_user.id, None)
    if not trade:
        await message.answer("❌ Нет активной сделки.")
        return
    await state.clear()
    data = await fetch_all_data(trade["symbol"])
    await message.answer(build_close_message(trade, data, reason="Закрыто вручную"))
 
 
@dp.message(TradeStates.waiting_for_leverage)
async def process_leverage(message: Message, state: FSMContext):
    if not is_allowed(message.from_user.id):
        return
    try:
        leverage = int(message.text.strip())
        if not (1 <= leverage <= 200):
            raise ValueError
    except ValueError:
        await message.answer("⚠️ Введите корректное плечо (от 1 до 200):")
        return
 
    fsm_data = await state.get_data()
    symbol, side = fsm_data["symbol"], fsm_data["side"]
 
    await message.answer("⏳ Фиксирую цену входа...")
    data = await fetch_all_data(symbol)
    entry_price = data["bybit"].get("price")
 
    if not entry_price:
        await message.answer("❌ Не удалось получить цену Bybit. Попробуйте ещё раз.")
        await state.clear()
        return
 
    active_trades[message.from_user.id] = {
        "symbol": symbol, "side": side, "leverage": leverage,
        "entry_price": entry_price,
        "entry_time": datetime.now(timezone.utc),
        "chat_id": message.chat.id,
    }
    await state.clear()
 
    side_emoji = "📈" if side.lower() == "long" else "📉"
    await message.answer(
        f"✅ <b>Сделка открыта!</b>\n\n"
        f"{side_emoji} <b>{side}</b> | {symbol}\n"
        f"💵 <b>Цена входа:</b> <code>{entry_price:,.4f}$</code>\n"
        f"⚡ <b>Плечо:</b> x{leverage}\n\n"
        f"🤖 Авто-мониторинг активен. /check для статуса.",
        reply_markup=check_keyboard(),
    )
 
 
@dp.message(F.text & ~F.text.startswith("/"))
async def handle_ticker(message: Message, state: FSMContext):
    if not is_allowed(message.from_user.id):
        return
    current_state = await state.get_state()
    if current_state == TradeStates.waiting_for_leverage:
        await process_leverage(message, state)
        return
 
    symbol = normalize_symbol(message.text.strip())
    msg = await message.answer(f"🔍 Анализирую <b>{symbol}</b>... ⏳")
    data = await fetch_all_data(symbol)
    text = build_analysis_message(symbol, data)
    await msg.edit_text(text, reply_markup=analysis_keyboard(symbol))
 
 
@dp.message(F.text.regexp(r"^/[a-zA-Z0-9]+(bybit|binance|okx|coingecko)$"))
async def handle_exchange_command(message: Message):
    """Обработчик команд типа /btcbybit /ethbinance /solocx"""
    if not is_allowed(message.from_user.id):
        return
    match = EXCHANGE_CMD_RE.match(message.text)
    if not match:
        return
 
    ticker_raw = match.group(1).upper()
    exchange_name = match.group(2).lower()
    symbol = normalize_symbol(ticker_raw)
 
    msg = await message.answer(f"🔍 Получаю данные <b>{symbol}</b> с <b>{exchange_name.capitalize()}</b>... ⏳")
 
    # Получаем данные нужной биржи + параллельно fair price с остальных
    ex_data, all_data = await asyncio.gather(
        fetch_single_exchange(symbol, exchange_name),
        fetch_all_data(symbol),
    )
    fair_price = calculate_fair_price(all_data)
 
    text = build_single_exchange_message(symbol, exchange_name.capitalize(), ex_data, fair_price)
    await msg.edit_text(text)
 
 
# ─── Callbacks ─────────────────────────────────────────────────────────────────
 
@dp.callback_query(F.data.startswith("open_short:") | F.data.startswith("open_long:"))
async def cb_open_trade(callback: CallbackQuery, state: FSMContext):
    if not is_allowed(callback.from_user.id):
        return
    if callback.from_user.id in active_trades:
        await callback.answer("⚠️ Уже есть активная сделка! Сначала закройте её /close", show_alert=True)
        return
    parts = callback.data.split(":")
    side = "Short" if parts[0] == "open_short" else "Long"
    symbol = parts[1].replace("_", "/")
    await state.set_state(TradeStates.waiting_for_leverage)
    await state.update_data(symbol=symbol, side=side)
    side_emoji = "📈" if side == "Long" else "📉"
    await callback.message.answer(
        f"⚠️ <b>Подтверждение входа</b>\n\n"
        f"📌 <b>Тикер:</b> {symbol} | {side_emoji} <b>Сторона:</b> {side}\n\n"
        f"Введите плечо (например: <code>10</code>, <code>20</code>, <code>50</code>):",
        reply_markup=confirm_keyboard(symbol, side),
    )
    await callback.answer()
 
 
@dp.callback_query(F.data == "refresh_check")
async def cb_refresh_check(callback: CallbackQuery):
    if not is_allowed(callback.from_user.id):
        return
    trade = active_trades.get(callback.from_user.id)
    if not trade:
        await callback.answer("❌ Нет активной сделки.", show_alert=True)
        return
    await callback.answer("⏳ Обновляю...")
    data = await fetch_all_data(trade["symbol"])
    await callback.message.edit_text(build_check_message(trade, data), reply_markup=check_keyboard())
 
 
@dp.callback_query(F.data == "close_trade")
async def cb_close_trade(callback: CallbackQuery):
    if not is_allowed(callback.from_user.id):
        return
    trade = active_trades.pop(callback.from_user.id, None)
    if not trade:
        await callback.answer("❌ Нет активной сделки.", show_alert=True)
        return
    await callback.answer("🛑 Закрываю...")
    data = await fetch_all_data(trade["symbol"])
    await callback.message.answer(build_close_message(trade, data, reason="Закрыто вручную"))
 
 
@dp.callback_query(F.data.startswith("cancel_trade:"))
async def cb_cancel_trade(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("❌ Вход отменён.")
    await callback.answer()
 
 
# ─── Monitor ───────────────────────────────────────────────────────────────────
 
async def monitor_trades():
    logger.info("Фоновый мониторинг запущен.")
    while True:
        await asyncio.sleep(30)
        for user_id, trade in list(active_trades.items()):
            try:
                data = await fetch_all_data(trade["symbol"])
                bybit_price = data["bybit"].get("price")
                fair_price = calculate_fair_price(data)
                if not bybit_price or not fair_price:
                    continue
                spread_abs = abs(calculate_spread(bybit_price, fair_price))
                logger.info(f"[Monitor] {trade['symbol']} user={user_id} spread={spread_abs:.4f}%")
                if spread_abs < CLOSE_SPREAD_THRESHOLD:
                    del active_trades[user_id]
                    text = build_close_message(trade, data, reason=f"Спред выровнялся до {spread_abs:.4f}%")
                    await bot.send_message(trade["chat_id"], "🔔 <b>Авто-закрытие!</b>\n\n" + text)
            except Exception as e:
                logger.error(f"Monitor error user={user_id}: {e}")
 
 
# ─── Main ──────────────────────────────────────────────────────────────────────
 
async def main():
    await start_keep_alive_server()
    asyncio.create_task(monitor_trades())
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
 
 
if __name__ == "__main__":
    asyncio.run(main())
