"""
utils.py — утилиты: Keep-Alive HTTP-сервер для Render, обработка ошибок.
"""

import logging
import os
from aiohttp import web

logger = logging.getLogger(__name__)

CLOSE_SPREAD_THRESHOLD = 0.03  # % — порог автозакрытия


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


async def health_handler(request: web.Request) -> web.Response:
    return web.Response(text="OK", status=200)


async def start_keep_alive_server():
    """
    Запускает минимальный HTTP-сервер на порту PORT (из env, default 8080).
    Render использует его для проверки жизнеспособности сервиса.
    """
    port = int(os.getenv("PORT", 8080))
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Keep-Alive сервер запущен на порту {port}")


def normalize_symbol(symbol: str) -> str:
    """Нормализует тикер: 'btc' -> 'BTC/USDT', 'BTC/USDT' -> 'BTC/USDT'."""
    symbol = symbol.upper().strip()
    if "/" not in symbol:
        symbol = f"{symbol}/USDT"
    return symbol


def safe_float(value, default=None):
    """Безопасное приведение к float."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
