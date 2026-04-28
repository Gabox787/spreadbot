"""
exchanges.py — сбор данных с Bybit, Binance, OKX и CoinGecko.
"""
 
import asyncio
import logging
import aiohttp
import ccxt.async_support as ccxt
 
logger = logging.getLogger(__name__)
 
 
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
        "pepe": "pepe", "shib": "shiba-inu",
    }
    return mapping.get(base, base)
 
 
def _get_funding_interval(fr_data: dict, default_hours: int = 8) -> str:
    """Определяет интервал фандинга и возвращает строку типа '8ч (каждые 8 часов)'."""
    hours = None
 
    # Пробуем поле fundingIntervalHours
    raw = fr_data.get("fundingIntervalHours") or fr_data.get("info", {}).get("fundingIntervalHours")
    if raw:
        try:
            hours = int(float(raw))
        except Exception:
            pass
 
    # Считаем по разнице timestamp'ов
    if not hours:
        try:
            nxt = fr_data.get("nextFundingTimestamp") or fr_data.get("nextFundingDatetime")
            cur = fr_data.get("fundingTimestamp") or fr_data.get("fundingDatetime")
            if nxt and cur:
                if isinstance(nxt, str):
                    from datetime import datetime, timezone
                    nxt = datetime.fromisoformat(nxt.replace("Z", "+00:00")).timestamp() * 1000
                    cur = datetime.fromisoformat(cur.replace("Z", "+00:00")).timestamp() * 1000
                diff = round((nxt - cur) / 3_600_000)
                if diff in (1, 2, 4, 8):
                    hours = diff
        except Exception:
            pass
 
    if not hours:
        hours = default_hours
 
    label = {1: "каждый час", 4: "каждые 4 часа", 8: "каждые 8 часов"}.get(hours, f"каждые {hours}ч")
    return f"{hours}ч ({label})"
 
 
async def fetch_bybit_data(symbol: str) -> dict:
    exchange = ccxt.bybit({"enableRateLimit": True, "options": {"defaultType": "linear"}})
    try:
        await exchange.load_markets()
        base, quote = symbol.split("/")
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
            return {"exchange": "Bybit", "price": None, "funding_rate": None, "funding_interval": "—", "ok": False}
 
        ticker = await exchange.fetch_ticker(bybit_symbol)
        price = ticker.get("last") or ticker.get("close")
        funding, interval = None, "8ч (каждые 8 часов)"
        try:
            fr = await exchange.fetch_funding_rate(bybit_symbol)
            funding = fr.get("fundingRate")
            interval = _get_funding_interval(fr, default_hours=8)
        except Exception as e:
            logger.warning(f"Bybit FR error: {e}")
        return {"exchange": "Bybit", "price": price, "funding_rate": funding, "funding_interval": interval, "ok": True}
    except Exception as e:
        logger.error(f"Bybit error: {e}")
        return {"exchange": "Bybit", "price": None, "funding_rate": None, "funding_interval": "—", "ok": False}
    finally:
        await exchange.close()
 
 
async def fetch_binance_data(symbol: str) -> dict:
    exchange = ccxt.binance({"enableRateLimit": True, "options": {"defaultType": "future"}})
    try:
        await exchange.load_markets()
        ticker = await exchange.fetch_ticker(symbol)
        price = ticker.get("last") or ticker.get("close")
        funding, interval = None, "8ч (каждые 8 часов)"
        try:
            fr = await exchange.fetch_funding_rate(symbol)
            funding = fr.get("fundingRate")
            interval = _get_funding_interval(fr, default_hours=8)
        except Exception as e:
            logger.warning(f"Binance FR error: {e}")
        return {"exchange": "Binance", "price": price, "funding_rate": funding, "funding_interval": interval, "ok": True}
    except Exception as e:
        logger.error(f"Binance error: {e}")
        return {"exchange": "Binance", "price": None, "funding_rate": None, "funding_interval": "—", "ok": False}
    finally:
        await exchange.close()
 
 
async def fetch_okx_data(symbol: str) -> dict:
    exchange = ccxt.okx({"enableRateLimit": True, "options": {"defaultType": "swap"}})
    try:
        await exchange.load_markets()
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
            logger.warning(f"OKX FR error: {e}")
        return {"exchange": "OKX", "price": price, "funding_rate": funding, "funding_interval": interval, "ok": True}
    except Exception as e:
        logger.error(f"OKX error: {e}")
        return {"exchange": "OKX", "price": None, "funding_rate": None, "funding_interval": "—", "ok": False}
    finally:
        await exchange.close()
 
 
async def fetch_coingecko_price(symbol: str) -> dict:
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
                            return {"exchange": "CoinGecko", "price": price, "funding_rate": None, "funding_interval": "—", "ok": True}
                        return {"exchange": "CoinGecko", "price": None, "funding_rate": None, "funding_interval": "—", "ok": False, "error": f"'{coin_id}' не найден"}
                    elif resp.status == 429:
                        await asyncio.sleep(2 * (attempt + 1))
                    else:
                        return {"exchange": "CoinGecko", "price": None, "funding_rate": None, "funding_interval": "—", "ok": False}
        except Exception as e:
            logger.error(f"CoinGecko error (попытка {attempt+1}): {e}")
            await asyncio.sleep(1)
    return {"exchange": "CoinGecko", "price": None, "funding_rate": None, "funding_interval": "—", "ok": False}
 
 
async def fetch_all_data(symbol: str) -> dict:
    results = await asyncio.gather(
        fetch_bybit_data(symbol),
        fetch_binance_data(symbol),
        fetch_okx_data(symbol),
        fetch_coingecko_price(symbol),
    )
    data = {"bybit": results[0], "binance": results[1], "okx": results[2], "coingecko": results[3]}
    for k, v in data.items():
        logger.info(f"{'✅' if v.get('ok') else '❌'} {k}: price={v.get('price')}, fr={v.get('funding_rate')}, interval={v.get('funding_interval')}")
    return data
