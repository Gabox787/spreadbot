"""
exchanges.py — сбор данных через публичные REST API (без ccxt где возможно).
Используем прямые HTTP запросы — быстрее и меньше зависимостей.
"""
 
import asyncio
import logging
from datetime import datetime, timezone
import aiohttp
 
logger = logging.getLogger(__name__)
 
# Таймаут на каждый запрос
TIMEOUT = aiohttp.ClientTimeout(total=10, connect=5)
 
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}
 
 
def _symbol_to_coingecko_id(symbol: str) -> str:
    base = symbol.split("/")[0].lower()
    mapping = {
        "btc": "bitcoin", "eth": "ethereum", "sol": "solana",
        "bnb": "binancecoin", "xrp": "ripple", "ada": "cardano",
        "avax": "avalanche-2", "dot": "polkadot", "link": "chainlink",
        "matic": "matic-network", "ltc": "litecoin", "doge": "dogecoin",
        "uni": "uniswap", "atom": "cosmos", "near": "near",
        "arb": "arbitrum", "op": "optimism", "trx": "tron",
        "ton": "the-open-network", "sui": "sui", "apt": "aptos",
        "pepe": "pepe", "shib": "shiba-inu", "wif": "dogwifcoin",
        "bonk": "bonk", "jup": "jupiter-exchange-solana",
        "ena": "ethena", "tia": "celestia", "sei": "sei-network",
        "bch": "bitcoin-cash", "etc": "ethereum-classic",
        "fil": "filecoin", "aave": "aave", "xlm": "stellar",
        "hype": "hyperliquid", "trump": "official-trump",
    }
    return mapping.get(base, base)
 
 
def _now_str() -> str:
    return datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M:%S UTC")
 
 
def _fmt_symbol_bybit(symbol: str) -> str:
    """BTC/USDT -> BTCUSDT"""
    return symbol.replace("/", "")
 
 
def _fmt_symbol_binance(symbol: str) -> str:
    """BTC/USDT -> BTCUSDT"""
    return symbol.replace("/", "")
 
 
def _fmt_symbol_okx(symbol: str) -> str:
    """BTC/USDT -> BTC-USDT-SWAP"""
    base, quote = symbol.split("/")
    return f"{base}-{quote}-SWAP"
 
 
async def fetch_bybit_data(symbol: str) -> dict:
    """Прямой запрос к Bybit V5 API."""
    sym = _fmt_symbol_bybit(symbol)
    price_url = f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={sym}"
    fr_url = f"https://api.bybit.com/v5/market/funding/history?category=linear&symbol={sym}&limit=1"
 
    try:
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            # Цена и фандинг параллельно
            price_resp, fr_resp = await asyncio.gather(
                session.get(price_url, timeout=TIMEOUT),
                session.get(fr_url, timeout=TIMEOUT),
                return_exceptions=True
            )
 
            price = None
            if not isinstance(price_resp, Exception):
                async with price_resp:
                    if price_resp.status == 200:
                        data = await price_resp.json()
                        items = data.get("result", {}).get("list", [])
                        if items:
                            price = float(items[0].get("lastPrice", 0)) or None
 
            funding = None
            interval = "8ч (каждые 8 часов)"
            if not isinstance(fr_resp, Exception):
                async with fr_resp:
                    if fr_resp.status == 200:
                        data = await fr_resp.json()
                        items = data.get("result", {}).get("list", [])
                        if items:
                            funding = float(items[0].get("fundingRate", 0))
 
        if price is None:
            return {"exchange": "Bybit", "price": None, "funding_rate": None,
                    "funding_interval": "—", "ok": False, "error": "Нет данных", "fetched_at": _now_str()}
 
        return {"exchange": "Bybit", "price": price, "funding_rate": funding,
                "funding_interval": interval, "ok": True, "fetched_at": _now_str()}
 
    except Exception as e:
        logger.error(f"Bybit error: {e}")
        return {"exchange": "Bybit", "price": None, "funding_rate": None,
                "funding_interval": "—", "ok": False, "error": str(e)[:80], "fetched_at": _now_str()}
 
 
async def fetch_binance_data(symbol: str) -> dict:
    """Прямой запрос к Binance Futures API (USDT-M)."""
    sym = _fmt_symbol_binance(symbol)
    price_url = f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={sym}"
    fr_url = f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={sym}&limit=1"
    fr_info_url = f"https://fapi.binance.com/fapi/v1/fundingInfo"
 
    try:
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            price_resp, fr_resp = await asyncio.gather(
                session.get(price_url, timeout=TIMEOUT),
                session.get(fr_url, timeout=TIMEOUT),
                return_exceptions=True
            )
 
            price = None
            if not isinstance(price_resp, Exception):
                async with price_resp:
                    if price_resp.status == 200:
                        data = await price_resp.json()
                        price = float(data.get("price", 0)) or None
 
            funding = None
            interval = "8ч (каждые 8 часов)"
            if not isinstance(fr_resp, Exception):
                async with fr_resp:
                    if fr_resp.status == 200:
                        data = await fr_resp.json()
                        if data and isinstance(data, list):
                            funding = float(data[0].get("fundingRate", 0))
 
        if price is None:
            return {"exchange": "Binance", "price": None, "funding_rate": None,
                    "funding_interval": "—", "ok": False, "error": "Нет данных", "fetched_at": _now_str()}
 
        return {"exchange": "Binance", "price": price, "funding_rate": funding,
                "funding_interval": interval, "ok": True, "fetched_at": _now_str()}
 
    except Exception as e:
        logger.error(f"Binance error: {e}")
        return {"exchange": "Binance", "price": None, "funding_rate": None,
                "funding_interval": "—", "ok": False, "error": str(e)[:80], "fetched_at": _now_str()}
 
 
async def fetch_okx_data(symbol: str) -> dict:
    """Прямой запрос к OKX API."""
    sym = _fmt_symbol_okx(symbol)
    price_url = f"https://www.okx.com/api/v5/market/ticker?instId={sym}"
    fr_url = f"https://www.okx.com/api/v5/public/funding-rate?instId={sym}"
 
    try:
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            price_resp, fr_resp = await asyncio.gather(
                session.get(price_url, timeout=TIMEOUT),
                session.get(fr_url, timeout=TIMEOUT),
                return_exceptions=True
            )
 
            price = None
            if not isinstance(price_resp, Exception):
                async with price_resp:
                    if price_resp.status == 200:
                        data = await price_resp.json()
                        items = data.get("data", [])
                        if items:
                            price = float(items[0].get("last", 0)) or None
 
            funding = None
            interval = "8ч (каждые 8 часов)"
            if not isinstance(fr_resp, Exception):
                async with fr_resp:
                    if fr_resp.status == 200:
                        data = await fr_resp.json()
                        items = data.get("data", [])
                        if items:
                            funding = float(items[0].get("fundingRate", 0))
                            # OKX даёт fundingTime и nextFundingTime
                            try:
                                cur = int(items[0].get("fundingTime", 0))
                                nxt = int(items[0].get("nextFundingTime", 0))
                                if cur and nxt:
                                    diff_h = round((nxt - cur) / 3_600_000)
                                    if diff_h in (1, 4, 8):
                                        label = {1: "каждый час", 4: "каждые 4 часа", 8: "каждые 8 часов"}[diff_h]
                                        interval = f"{diff_h}ч ({label})"
                            except Exception:
                                pass
 
        if price is None:
            return {"exchange": "OKX", "price": None, "funding_rate": None,
                    "funding_interval": "—", "ok": False, "error": "Нет данных", "fetched_at": _now_str()}
 
        return {"exchange": "OKX", "price": price, "funding_rate": funding,
                "funding_interval": interval, "ok": True, "fetched_at": _now_str()}
 
    except Exception as e:
        logger.error(f"OKX error: {e}")
        return {"exchange": "OKX", "price": None, "funding_rate": None,
                "funding_interval": "—", "ok": False, "error": str(e)[:80], "fetched_at": _now_str()}
 
 
async def fetch_coingecko_price(symbol: str) -> dict:
    """CoinGecko простой price endpoint."""
    coin_id = _symbol_to_coingecko_id(symbol)
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd"
    for attempt in range(2):
        try:
            async with aiohttp.ClientSession(headers=HEADERS) as session:
                async with session.get(url, timeout=TIMEOUT) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        price = data.get(coin_id, {}).get("usd")
                        if price:
                            return {"exchange": "CoinGecko", "price": float(price),
                                    "funding_rate": None, "funding_interval": "—",
                                    "ok": True, "fetched_at": _now_str()}
                        return {"exchange": "CoinGecko", "price": None, "funding_rate": None,
                                "funding_interval": "—", "ok": False,
                                "error": f"'{coin_id}' не найден — добавь в mapping",
                                "fetched_at": _now_str()}
                    elif resp.status == 429:
                        await asyncio.sleep(1)
                    else:
                        return {"exchange": "CoinGecko", "price": None, "funding_rate": None,
                                "funding_interval": "—", "ok": False,
                                "error": f"HTTP {resp.status}", "fetched_at": _now_str()}
        except Exception as e:
            logger.error(f"CoinGecko attempt {attempt+1}: {e}")
            await asyncio.sleep(0.5)
 
    return {"exchange": "CoinGecko", "price": None, "funding_rate": None,
            "funding_interval": "—", "ok": False, "error": "Недоступна", "fetched_at": _now_str()}
 
 
async def fetch_all_data(symbol: str) -> dict:
    """Параллельный сбор данных, общий таймаут 12 сек."""
    try:
        results = await asyncio.wait_for(
            asyncio.gather(
                fetch_bybit_data(symbol),
                fetch_binance_data(symbol),
                fetch_okx_data(symbol),
                fetch_coingecko_price(symbol),
            ),
            timeout=12.0
        )
    except asyncio.TimeoutError:
        logger.error("fetch_all_data: общий таймаут")
        empty = {"price": None, "funding_rate": None, "funding_interval": "—",
                 "ok": False, "error": "Таймаут", "fetched_at": _now_str()}
        return {
            "bybit":     {**empty, "exchange": "Bybit"},
            "binance":   {**empty, "exchange": "Binance"},
            "okx":       {**empty, "exchange": "OKX"},
            "coingecko": {**empty, "exchange": "CoinGecko"},
        }
 
    data = {
        "bybit": results[0], "binance": results[1],
        "okx": results[2], "coingecko": results[3],
    }
    for k, v in data.items():
        logger.info(f"{'✅' if v.get('ok') else '❌'} {k}: price={v.get('price')}")
    return data
 
 
async def fetch_single_exchange(symbol: str, exchange_name: str) -> dict:
    fn = {
        "bybit": fetch_bybit_data,
        "binance": fetch_binance_data,
        "okx": fetch_okx_data,
        "coingecko": fetch_coingecko_price,
    }.get(exchange_name.lower())
    if not fn:
        return {"ok": False, "error": f"Неизвестная биржа: {exchange_name}"}
    return await fn(symbol)
