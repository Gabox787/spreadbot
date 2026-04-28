"""
exchanges.py — сбор данных с Bybit, Binance, OKX и CoinGecko.
Оптимизировано: без load_markets() где возможно, жёсткие таймауты.
"""
 
import asyncio
import logging
from datetime import datetime, timezone
import aiohttp
import ccxt.async_support as ccxt
 
logger = logging.getLogger(__name__)
 
# Кэш рынков чтобы не грузить каждый раз
_markets_cache: dict = {}
 
 
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
    }
    return mapping.get(base, base)
 
 
def _get_funding_interval(fr_data: dict, default_hours: int = 8) -> str:
    hours = None
    raw = fr_data.get("fundingIntervalHours") or fr_data.get("info", {}).get("fundingIntervalHours")
    if raw:
        try:
            hours = int(float(raw))
        except Exception:
            pass
    if not hours:
        try:
            nxt = fr_data.get("nextFundingTimestamp")
            cur = fr_data.get("fundingTimestamp")
            if nxt and cur and isinstance(nxt, (int, float)) and isinstance(cur, (int, float)):
                diff = round((nxt - cur) / 3_600_000)
                if diff in (1, 2, 4, 8):
                    hours = diff
        except Exception:
            pass
    if not hours:
        hours = default_hours
    label = {1: "каждый час", 4: "каждые 4 часа", 8: "каждые 8 часов"}.get(hours, f"каждые {hours}ч")
    return f"{hours}ч ({label})"
 
 
def _now_str() -> str:
    """Текущее время UTC в читаемом формате."""
    return datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M:%S UTC")
 
 
async def fetch_bybit_data(symbol: str) -> dict:
    exchange = ccxt.bybit({
        "enableRateLimit": False,
        "options": {"defaultType": "linear"},
        "timeout": 8000,
    })
    try:
        # Быстрый путь: пробуем сразу без load_markets
        base, quote = symbol.split("/")
        bybit_symbol = f"{base}/{quote}:{quote}"
 
        try:
            ticker = await exchange.fetch_ticker(bybit_symbol)
        except Exception:
            # Fallback: грузим рынки и ищем
            await exchange.load_markets()
            bybit_symbol = None
            for candidate in [symbol, f"{base}/{quote}:{quote}"]:
                if candidate in exchange.markets:
                    bybit_symbol = candidate
                    break
            if not bybit_symbol:
                for mid, m in exchange.markets.items():
                    if m.get("base") == base and m.get("quote") == quote and m.get("linear") and m.get("active"):
                        bybit_symbol = mid
                        break
            if not bybit_symbol:
                return {"exchange": "Bybit", "price": None, "funding_rate": None,
                        "funding_interval": "—", "ok": False, "error": "Символ не найден", "fetched_at": _now_str()}
            ticker = await exchange.fetch_ticker(bybit_symbol)
 
        price = ticker.get("last") or ticker.get("close")
        funding, interval = None, "8ч (каждые 8 часов)"
        try:
            fr = await exchange.fetch_funding_rate(bybit_symbol)
            funding = fr.get("fundingRate")
            interval = _get_funding_interval(fr, default_hours=8)
        except Exception as e:
            logger.warning(f"Bybit FR: {e}")
 
        return {"exchange": "Bybit", "price": price, "funding_rate": funding,
                "funding_interval": interval, "ok": True, "fetched_at": _now_str()}
    except Exception as e:
        logger.error(f"Bybit error: {e}")
        return {"exchange": "Bybit", "price": None, "funding_rate": None,
                "funding_interval": "—", "ok": False, "error": str(e), "fetched_at": _now_str()}
    finally:
        await exchange.close()
 
 
async def fetch_binance_data(symbol: str) -> dict:
    exchange = ccxt.binance({
        "enableRateLimit": False,
        "options": {"defaultType": "future"},
        "timeout": 8000,
    })
    try:
        ticker = await exchange.fetch_ticker(symbol)
        price = ticker.get("last") or ticker.get("close")
        funding, interval = None, "8ч (каждые 8 часов)"
        try:
            fr = await exchange.fetch_funding_rate(symbol)
            funding = fr.get("fundingRate")
            interval = _get_funding_interval(fr, default_hours=8)
        except Exception as e:
            logger.warning(f"Binance FR: {e}")
        return {"exchange": "Binance", "price": price, "funding_rate": funding,
                "funding_interval": interval, "ok": True, "fetched_at": _now_str()}
    except Exception as e:
        logger.error(f"Binance error: {e}")
        return {"exchange": "Binance", "price": None, "funding_rate": None,
                "funding_interval": "—", "ok": False, "error": str(e), "fetched_at": _now_str()}
    finally:
        await exchange.close()
 
 
async def fetch_okx_data(symbol: str) -> dict:
    exchange = ccxt.okx({
        "enableRateLimit": False,
        "options": {"defaultType": "swap"},
        "timeout": 8000,
    })
    try:
        base, quote = symbol.split("/")
        okx_symbol = f"{base}/{quote}:{quote}"
        ticker = await exchange.fetch_ticker(okx_symbol)
        price = ticker.get("last") or ticker.get("close")
        funding, interval = None, "8ч (каждые 8 часов)"
        try:
            fr = await exchange.fetch_funding_rate(okx_symbol)
            funding = fr.get("fundingRate")
            interval = _get_funding_interval(fr, default_hours=8)
        except Exception as e:
            logger.warning(f"OKX FR: {e}")
        return {"exchange": "OKX", "price": price, "funding_rate": funding,
                "funding_interval": interval, "ok": True, "fetched_at": _now_str()}
    except Exception as e:
        logger.error(f"OKX error: {e}")
        return {"exchange": "OKX", "price": None, "funding_rate": None,
                "funding_interval": "—", "ok": False, "error": str(e), "fetched_at": _now_str()}
    finally:
        await exchange.close()
 
 
async def fetch_coingecko_price(symbol: str) -> dict:
    coin_id = _symbol_to_coingecko_id(symbol)
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    for attempt in range(2):
        try:
            url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        price = data.get(coin_id, {}).get("usd")
                        if price:
                            return {"exchange": "CoinGecko", "price": price, "funding_rate": None,
                                    "funding_interval": "—", "ok": True, "fetched_at": _now_str()}
                        return {"exchange": "CoinGecko", "price": None, "funding_rate": None,
                                "funding_interval": "—", "ok": False,
                                "error": f"'{coin_id}' не найден — добавь в mapping", "fetched_at": _now_str()}
                    elif resp.status == 429:
                        await asyncio.sleep(1)
                    else:
                        return {"exchange": "CoinGecko", "price": None, "funding_rate": None,
                                "funding_interval": "—", "ok": False,
                                "error": f"HTTP {resp.status}", "fetched_at": _now_str()}
        except Exception as e:
            logger.error(f"CoinGecko (попытка {attempt+1}): {e}")
    return {"exchange": "CoinGecko", "price": None, "funding_rate": None,
            "funding_interval": "—", "ok": False, "error": "Таймаут", "fetched_at": _now_str()}
 
 
async def fetch_all_data(symbol: str) -> dict:
    """Параллельно собирает данные со всех источников с общим таймаутом 12 сек."""
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
        logger.error("fetch_all_data: общий таймаут 12 сек")
        empty = {"price": None, "funding_rate": None, "funding_interval": "—",
                 "ok": False, "error": "Таймаут", "fetched_at": _now_str()}
        return {"bybit": {**empty, "exchange": "Bybit"},
                "binance": {**empty, "exchange": "Binance"},
                "okx": {**empty, "exchange": "OKX"},
                "coingecko": {**empty, "exchange": "CoinGecko"}}
 
    data = {"bybit": results[0], "binance": results[1], "okx": results[2], "coingecko": results[3]}
    for k, v in data.items():
        logger.info(f"{'✅' if v.get('ok') else '❌'} {k}: price={v.get('price')}, interval={v.get('funding_interval')}")
    return data
 
 
async def fetch_single_exchange(symbol: str, exchange_name: str) -> dict:
    """Получает данные только с одной биржи (для команд /btcbybit и т.д.)"""
    fn = {
        "bybit": fetch_bybit_data,
        "binance": fetch_binance_data,
        "okx": fetch_okx_data,
        "coingecko": fetch_coingecko_price,
    }.get(exchange_name.lower())
    if not fn:
        return {"ok": False, "error": f"Неизвестная биржа: {exchange_name}"}
    return await fn(symbol)
