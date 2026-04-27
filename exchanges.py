"""
exchanges.py — сбор данных с Bybit, Binance, OKX и CoinGecko.
Все запросы асинхронные через ccxt.async_support.
"""
 
import asyncio
import logging
from typing import Optional
import aiohttp
import ccxt.async_support as ccxt
 
logger = logging.getLogger(__name__)
 
 
def _symbol_to_coingecko_id(symbol: str) -> str:
    """Конвертирует тикер (BTC/USDT) в CoinGecko coin_id."""
    base = symbol.split("/")[0].lower()
    mapping = {
        "btc": "bitcoin", "eth": "ethereum", "sol": "solana",
        "bnb": "binancecoin", "xrp": "ripple", "ada": "cardano",
        "avax": "avalanche-2", "dot": "polkadot", "link": "chainlink",
        "matic": "matic-network", "ltc": "litecoin", "doge": "dogecoin",
        "uni": "uniswap", "atom": "cosmos", "near": "near",
        "arb": "arbitrum", "op": "optimism", "trx": "tron",
    }
    return mapping.get(base, base)
 
 
async def fetch_bybit_data(symbol: str) -> dict:
    """Получает цену и funding rate с Bybit (линейные фьючерсы)."""
    exchange = ccxt.bybit({
        "enableRateLimit": True,
        "options": {"defaultType": "linear"},
    })
    try:
        await exchange.load_markets()
 
        base, quote = symbol.split("/")
        bybit_symbol = None
        candidates = [symbol, f"{base}/{quote}:{quote}", f"{base}USDT"]
 
        for candidate in candidates:
            if candidate in exchange.markets:
                bybit_symbol = candidate
                break
 
        if bybit_symbol is None:
            for market_id, market in exchange.markets.items():
                if (market.get("base") == base and
                        market.get("quote") == quote and
                        market.get("linear") and
                        market.get("active")):
                    bybit_symbol = market_id
                    break
 
        if bybit_symbol is None:
            return {"exchange": "Bybit", "price": None, "funding_rate": None, "ok": False, "error": "Symbol not found"}
 
        ticker = await exchange.fetch_ticker(bybit_symbol)
        price = ticker.get("last") or ticker.get("close")
 
        funding = None
        try:
            fr_data = await exchange.fetch_funding_rate(bybit_symbol)
            funding = fr_data.get("fundingRate")
        except Exception as e:
            logger.warning(f"Bybit funding rate error: {e}")
 
        return {"exchange": "Bybit", "price": price, "funding_rate": funding, "ok": True}
    except Exception as e:
        logger.error(f"Bybit fetch error: {e}")
        return {"exchange": "Bybit", "price": None, "funding_rate": None, "ok": False, "error": str(e)}
    finally:
        await exchange.close()
 
 
async def fetch_binance_data(symbol: str) -> dict:
    """Получает цену и funding rate с Binance."""
    exchange = ccxt.binance({
        "enableRateLimit": True,
        "options": {"defaultType": "future"},
    })
    try:
        await exchange.load_markets()
        ticker = await exchange.fetch_ticker(symbol)
        price = ticker.get("last") or ticker.get("close")
 
        funding = None
        try:
            fr_data = await exchange.fetch_funding_rate(symbol)
            funding = fr_data.get("fundingRate")
        except Exception as e:
            logger.warning(f"Binance funding rate error: {e}")
 
        return {"exchange": "Binance", "price": price, "funding_rate": funding, "ok": True}
    except Exception as e:
        logger.error(f"Binance fetch error: {e}")
        return {"exchange": "Binance", "price": None, "funding_rate": None, "ok": False, "error": str(e)}
    finally:
        await exchange.close()
 
 
async def fetch_okx_data(symbol: str) -> dict:
    """Получает цену и funding rate с OKX."""
    exchange = ccxt.okx({
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    })
    try:
        await exchange.load_markets()
        # OKX perpetual symbol format: BTC/USDT:USDT
        okx_symbol = symbol
        base, quote = symbol.split("/")
        okx_symbol = f"{base}/{quote}:{quote}"
 
        ticker = await exchange.fetch_ticker(okx_symbol)
        price = ticker.get("last") or ticker.get("close")
 
        funding = None
        try:
            fr_data = await exchange.fetch_funding_rate(okx_symbol)
            funding = fr_data.get("fundingRate")
        except Exception as e:
            logger.warning(f"OKX funding rate error: {e}")
 
        return {"exchange": "OKX", "price": price, "funding_rate": funding, "ok": True}
    except Exception as e:
        logger.error(f"OKX fetch error: {e}")
        return {"exchange": "OKX", "price": None, "funding_rate": None, "ok": False, "error": str(e)}
    finally:
        await exchange.close()
 
 
async def fetch_coingecko_price(symbol: str) -> dict:
    """Получает рыночную цену с CoinGecko с несколькими попытками."""
    coin_id = _symbol_to_coingecko_id(symbol)
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
 
    for attempt in range(3):
        try:
            url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        price = data.get(coin_id, {}).get("usd")
                        if price:
                            return {"exchange": "CoinGecko", "price": price, "funding_rate": None, "ok": True}
                        return {"exchange": "CoinGecko", "price": None, "funding_rate": None, "ok": False, "error": f"coin_id '{coin_id}' not found"}
                    elif resp.status == 429:
                        await asyncio.sleep(2 * (attempt + 1))
                        continue
                    else:
                        return {"exchange": "CoinGecko", "price": None, "funding_rate": None, "ok": False, "error": f"HTTP {resp.status}"}
        except Exception as e:
            logger.error(f"CoinGecko error (попытка {attempt + 1}): {e}")
            if attempt < 2:
                await asyncio.sleep(1)
 
    return {"exchange": "CoinGecko", "price": None, "funding_rate": None, "ok": False, "error": "Недоступна"}
 
 
async def fetch_all_data(symbol: str) -> dict:
    """
    Параллельно собирает данные со всех источников.
    Возвращает словарь с данными по каждой бирже.
    """
    results = await asyncio.gather(
        fetch_bybit_data(symbol),
        fetch_binance_data(symbol),
        fetch_okx_data(symbol),
        fetch_coingecko_price(symbol),
        return_exceptions=False,
    )
 
    return {
        "bybit": results[0],
        "binance": results[1],
        "okx": results[2],
        "coingecko": results[3],
    }
