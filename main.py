"""
main.py — точка входа: регистрация хэндлеров aiogram 3.x, FSM, фоновый мониторинг.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from dotenv import load_dotenv

from exchanges import fetch_all_data
from strategy import (
    build_analysis_message,
    build_check_message,
    build_close_message,
    calculate_fair_price,
    calculate_spread,
)
from utils import CLOSE_SPREAD_THRESHOLD, normalize_symbol, setup_logging, start_keep_alive_server

load_dotenv()
setup_logging()

logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не установлен в файле .env!")

bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(storage=MemoryStorage())

# Глобальное хранилище активных сделок: {user_id: trade_dict}
active_trades: dict[int, dict] = {}

# Фоновый таск мониторинга
_monitor_task: asyncio.Task | None = None


# ─── FSM States ────────────────────────────────────────────────────────────────

class TradeStates(StatesGroup):
    waiting_for_leverage = State()


# ─── Keyboards ─────────────────────────────────────────────────────────────────

def analysis_keyboard(symbol: str, spread: float | None) -> InlineKeyboardMarkup:
    """Кнопки после анализа: Short / Long."""
    enc = symbol.replace("/", "_")
    buttons = [
        [
            InlineKeyboardButton(text="📉 Открыть Short", callback_data=f"open_short:{enc}"),
            InlineKeyboardButton(text="📈 Открыть Long", callback_data=f"open_long:{enc}"),
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def check_keyboard() -> InlineKeyboardMarkup:
    """Кнопки статуса сделки."""
    buttons = [
        [
            InlineKeyboardButton(text="🛑 Закрыть вручную", callback_data="close_trade"),
            InlineKeyboardButton(text="🔄 Обновить статус", callback_data="refresh_check"),
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def confirm_keyboard(symbol: str, side: str) -> InlineKeyboardMarkup:
    enc = symbol.replace("/", "_")
    buttons = [
        [InlineKeyboardButton(text="❌ Отменить", callback_data=f"cancel_trade:{enc}:{side}")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ─── Handlers ──────────────────────────────────────────────────────────────────

@dp.message(CommandStart())
async def cmd_start(message: Message):
    await message.answer(
        "👋 <b>Привет!</b> Я бот для анализа торговых возможностей.\n\n"
        "📌 <b>Как использовать:</b>\n"
        "• Отправь тикер (например: <code>BTC</code> или <code>ETH/USDT</code>)\n"
        "• Получи анализ и открой сделку через кнопки\n"
        "• <code>/check</code> — статус активной сделки\n"
        "• <code>/close</code> — закрыть сделку вручную\n\n"
        "⚡ Автоматическое закрытие при спреде &lt; 0.03%"
    )


@dp.message(Command("check"))
async def cmd_check(message: Message):
    user_id = message.from_user.id
    trade = active_trades.get(user_id)
    if not trade:
        await message.answer("❌ Нет активной сделки. Отправь тикер, чтобы начать анализ.")
        return

    await message.answer("⏳ Обновляю данные...")
    data = await fetch_all_data(trade["symbol"])
    text = build_check_message(trade, data)
    await message.answer(text, reply_markup=check_keyboard())


@dp.message(Command("close"))
async def cmd_close(message: Message, state: FSMContext):
    user_id = message.from_user.id
    trade = active_trades.pop(user_id, None)
    if not trade:
        await message.answer("❌ Нет активной сделки.")
        return

    await state.clear()
    data = await fetch_all_data(trade["symbol"])
    text = build_close_message(trade, data, reason="Закрыто вручную")
    await message.answer(text)


@dp.message(TradeStates.waiting_for_leverage)
async def process_leverage(message: Message, state: FSMContext):
    """Обработка введённого плеча."""
    user_id = message.from_user.id
    try:
        leverage = int(message.text.strip())
        if not (1 <= leverage <= 200):
            raise ValueError
    except ValueError:
        await message.answer("⚠️ Введите корректное плечо (целое число от 1 до 200):")
        return

    fsm_data = await state.get_data()
    symbol = fsm_data["symbol"]
    side = fsm_data["side"]

    await message.answer("⏳ Фиксирую цену входа...")
    data = await fetch_all_data(symbol)
    entry_price = data["bybit"].get("price")

    if not entry_price:
        await message.answer("❌ Не удалось получить цену Bybit. Попробуйте ещё раз.")
        await state.clear()
        return

    active_trades[user_id] = {
        "symbol": symbol,
        "side": side,
        "leverage": leverage,
        "entry_price": entry_price,
        "entry_time": datetime.now(timezone.utc),
        "chat_id": message.chat.id,
    }

    await state.clear()

    side_emoji = "📈" if side.lower() == "long" else "📉"
    await message.answer(
        f"✅ <b>Сделка открыта!</b>\n\n"
        f"{side_emoji} <b>{side}</b> | {symbol}\n"
        f"💵 <b>Цена входа:</b> {entry_price:,.2f}$\n"
        f"⚡ <b>Плечо:</b> x{leverage}\n\n"
        f"🤖 Авто-мониторинг активен. Используй /check для статуса.",
        reply_markup=check_keyboard(),
    )


@dp.message(F.text & ~F.text.startswith("/"))
async def handle_ticker(message: Message, state: FSMContext):
    """Обработчик тикера — главный анализ."""
    current_state = await state.get_state()
    if current_state == TradeStates.waiting_for_leverage:
        await process_leverage(message, state)
        return

    symbol = normalize_symbol(message.text.strip())
    await message.answer(f"🔍 Анализирую <b>{symbol}</b>... Подождите ⏳")

    data = await fetch_all_data(symbol)

    bybit_price = data["bybit"].get("price")
    fair_price = calculate_fair_price(data)
    spread = calculate_spread(bybit_price, fair_price) if (bybit_price and fair_price) else None

    text = build_analysis_message(symbol, data)
    await message.answer(text, reply_markup=analysis_keyboard(symbol, spread))


# ─── Callback Handlers ─────────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("open_short:") | F.data.startswith("open_long:"))
async def cb_open_trade(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id

    if user_id in active_trades:
        await callback.answer("⚠️ У вас уже есть активная сделка! Сначала закройте её (/close).", show_alert=True)
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
    user_id = callback.from_user.id
    trade = active_trades.get(user_id)
    if not trade:
        await callback.answer("❌ Нет активной сделки.", show_alert=True)
        return

    await callback.answer("⏳ Обновляю...")
    data = await fetch_all_data(trade["symbol"])
    text = build_check_message(trade, data)
    await callback.message.edit_text(text, reply_markup=check_keyboard())


@dp.callback_query(F.data == "close_trade")
async def cb_close_trade(callback: CallbackQuery):
    user_id = callback.from_user.id
    trade = active_trades.pop(user_id, None)
    if not trade:
        await callback.answer("❌ Нет активной сделки.", show_alert=True)
        return

    await callback.answer("🛑 Закрываю сделку...")
    data = await fetch_all_data(trade["symbol"])
    text = build_close_message(trade, data, reason="Закрыто вручную")
    await callback.message.answer(text)


@dp.callback_query(F.data.startswith("cancel_trade:"))
async def cb_cancel_trade(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("❌ Вход отменён.")
    await callback.answer()


# ─── Background Monitor ────────────────────────────────────────────────────────

async def monitor_trades():
    """
    Фоновый таск: каждые 30 секунд проверяет спред для всех активных сделок.
    Если спред < CLOSE_SPREAD_THRESHOLD% — отправляет уведомление и закрывает сделку.
    """
    logger.info("Фоновый мониторинг запущен.")
    while True:
        await asyncio.sleep(30)
        if not active_trades:
            continue

        for user_id, trade in list(active_trades.items()):
            try:
                data = await fetch_all_data(trade["symbol"])
                bybit_price = data["bybit"].get("price")
                fair_price = calculate_fair_price(data)

                if bybit_price is None or fair_price is None:
                    continue

                spread = calculate_spread(bybit_price, fair_price)
                spread_abs = abs(spread)

                logger.info(
                    f"[Monitor] {trade['symbol']} user={user_id} spread={spread_abs:.4f}% threshold={CLOSE_SPREAD_THRESHOLD}%"
                )

                if spread_abs < CLOSE_SPREAD_THRESHOLD:
                    del active_trades[user_id]
                    text = build_close_message(
                        trade, data,
                        reason=f"Спред выровнялся до {spread_abs:.4f}%"
                    )
                    await bot.send_message(
                        trade["chat_id"],
                        "🔔 <b>Авто-закрытие сделки!</b>\n\n" + text,
                    )
                    logger.info(f"Сделка закрыта автоматически для user={user_id}")
            except Exception as e:
                logger.error(f"Ошибка мониторинга для user={user_id}: {e}")


# ─── Startup ───────────────────────────────────────────────────────────────────

async def main():
    global _monitor_task

    logger.info("Запуск бота...")

    # Keep-Alive сервер для Render
    await start_keep_alive_server()

    # Фоновый мониторинг сделок
    _monitor_task = asyncio.create_task(monitor_trades())

    # Запуск polling
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())
