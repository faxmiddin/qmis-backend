"""
QMIS Backend — 14 birja real-time aggregator
FastAPI + WebSocket + asyncio
Deploy: Render.com (bepul)
"""

import asyncio
import json
import logging
import time
from typing import Dict, Set, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import websockets
import aiohttp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("QMIS")

app = FastAPI(title="QMIS Backend", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── GLOBAL STATE ────────────────────────────────────────────
prices: Dict[str, Dict] = {}      # sym -> {venue -> price, unified, vol}
clients: Set[WebSocket] = set()    # connected frontends
venue_status: Dict[str, str] = {}  # venue -> connected/error
stats = {"updates": 0, "clients": 0, "uptime": time.time()}

# ─── TOP 400 COINS ───────────────────────────────────────────
SYMS = [
    "BTC","ETH","BNB","SOL","XRP","DOGE","ADA","AVAX","LINK","DOT",
    "MATIC","UNI","ATOM","LTC","ETC","XLM","ALGO","VET","FIL","THETA",
    "AAVE","MKR","COMP","SNX","YFI","SUSHI","CRV","BAL","ZRX","REN",
    "OP","ARB","IMX","SAND","MANA","AXS","GALA","ENJ","CHZ","FLOW",
    "ICP","NEAR","FTM","ONE","HBAR","EGLD","TRX","SUI","APT","SEI",
    "INJ","TIA","JUP","WIF","BONK","PEPE","FLOKI","SHIB","BOME","POPCAT",
    "GMX","DYDX","GNS","RDNT","WLD","CELO","ROSE","KAVA","BAND","API3",
    "UMA","BAT","GRT","LPT","NMR","OCEAN","FET","AGIX","RNDR","TAO",
    "ARKM","AIOZ","CLV","CTSI","DENT","GNO","ICX","JST","KSM","MTL",
    "OGN","POLS","QNT","REEF","SKL","TOMO","UTK","YGG","ZEN","ACH",
    "ALICE","ARPA","BADGER","BICO","CHESS","COMBO","DASH","DOCK","DREP",
    "DUSK","ELF","FRONT","GHST","GLM","HFT","HIGH","ILV","JASMY","KEEP",
    "MAGIC","MASK","MBOX","NFP","NKN","OMG","PHB","PLA","PROM","PYR",
    "RUNE","SC","SPELL","STG","STORJ","SXP","SYN","TRU","TVK","UNFI",
    "VOXEL","WRX","XVS","ZIL","ACE","ADS","ADX","AMP","ANKR","ANT",
    "APE","AR","ASTR","BLUR","BNX","BOBA","BTT","CAKE","CATI","CKB",
    "CORE","COTI","CTK","CVC","CVX","CYBER","DAR","DGB","DODO","DOGS",
    "FLR","FLUX","GAL","GARI","GAS","GEM","GMT","GOG","GTC","HAI",
    "HARD","HOOK","HOT","HT","IAG","IBAT","IDEX","IOTA","IOTX","JTO",
    "LOOM","LUNC","MEW","MYRO","MANTA","ALT","DYM","STRK","ZK","ONDO",
    "PENDLE","ETHFI","REZ","OMNI","SAFE","SAGA","PORTAL","PIXEL","W","COW",
    "PYTH","FRAX","STRAX","TURBO","TOSHI","DEGEN","BRETT",
    "WOJAK","LADYS","MONG","SNEK","NEIRO","MOG","SLERF","PUPS","BOOK",
    "BABYDOGE","KISHU","ELON","SAMO","COQ","MEME","TAMA","ELMO",
    "DOGELON","LEASH","BONE","MNGO","COPE","STEP","ATLAS","POLIS",
]

# 14 VENUE DEFINITION
VENUES = {
    "binance":     {"name": "Binance",     "type": "CEX", "weight": 5},
    "bybit":       {"name": "Bybit",       "type": "CEX", "weight": 4},
    "okx":         {"name": "OKX",         "type": "CEX", "weight": 4},
    "mexc":        {"name": "MEXC",        "type": "CEX", "weight": 3},
    "kucoin":      {"name": "KuCoin",      "type": "CEX", "weight": 3},
    "kraken":      {"name": "Kraken",      "type": "CEX", "weight": 3},
    "htx":         {"name": "HTX",         "type": "CEX", "weight": 2},
    "bitget":      {"name": "Bitget",      "type": "CEX", "weight": 2},
    "coinbase":    {"name": "Coinbase",    "type": "CEX", "weight": 4},
    "bingx":       {"name": "BingX",       "type": "CEX", "weight": 1},
    "upbit":       {"name": "Upbit",       "type": "CEX", "weight": 2},
    "hyperliquid": {"name": "Hyperliquid", "type": "DEX", "weight": 3},
    "dydx":        {"name": "dYdX",        "type": "DEX", "weight": 2},
    "weex":        {"name": "WEEX",        "type": "DEX", "weight": 1},
}

def init_sym(sym: str):
    if sym not in prices:
        prices[sym] = {
            "venues": {},
            "unified": 0,
            "vol": 0,
            "ch": 0,
            "hi": 0,
            "lo": 0,
            "fund": 0,
            "ts": 0,
        }

def update_price(sym: str, venue: str, price: float,
                 vol: float = 0, ch: float = 0,
                 hi: float = 0, lo: float = 0):
    if not price or price <= 0:
        return
    init_sym(sym)
    prices[sym]["venues"][venue] = price
    if vol:  prices[sym]["vol"] = vol
    if ch:   prices[sym]["ch"] = ch
    if hi:   prices[sym]["hi"] = hi
    if lo:   prices[sym]["lo"] = lo
    prices[sym]["ts"] = time.time()
    # Axelar weighted median
    compute_unified(sym)
    stats["updates"] += 1

def compute_unified(sym: str):
    """Axelar: 14 venue weighted median"""
    v = prices[sym]["venues"]
    if not v:
        return
    weighted = []
    for venue_id, price in v.items():
        w = VENUES.get(venue_id, {}).get("weight", 1)
        for _ in range(w):
            weighted.append(price)
    weighted.sort()
    if weighted:
        prices[sym]["unified"] = weighted[len(weighted) // 2]

# ═══════════════════════════════════════════════════════════════
# VENUE WEBSOCKET CONNECTORS
# ═══════════════════════════════════════════════════════════════

# ── BINANCE (batch, 400 coin) ──────────────────────────────────
async def connect_binance():
    batch_size = 180
    batches = [SYMS[i:i+batch_size] for i in range(0, len(SYMS), batch_size)]
    tasks = [_binance_batch(batch, i) for i, batch in enumerate(batches)]
    await asyncio.gather(*tasks)

async def _binance_batch(syms, batch_id):
    streams = "/".join(f"{s.lower()}usdt@miniTicker" for s in syms)
    url = f"wss://stream.binance.com:9443/stream?streams={streams}"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                venue_status["binance"] = "live"
                logger.info(f"Binance batch {batch_id} connected ({len(syms)} coins)")
                async for msg in ws:
                    data = json.loads(msg)
                    d = data.get("data", {})
                    if not d.get("s"):
                        continue
                    sym = d["s"].replace("USDT", "")
                    update_price(
                        sym, "binance",
                        float(d.get("c", 0)),
                        float(d.get("v", 0)) * float(d.get("c", 0)),
                        float(d.get("P", 0)),
                        float(d.get("h", 0)),
                        float(d.get("l", 0)),
                    )
        except Exception as e:
            venue_status["binance"] = "error"
            logger.warning(f"Binance batch {batch_id} error: {e}")
            await asyncio.sleep(3)

# ── BINANCE FUNDING ───────────────────────────────────────────
async def connect_binance_funding():
    syms_50 = SYMS[:50]
    streams = "/".join(f"{s.lower()}usdt@markPrice" for s in syms_50)
    url = f"wss://fstream.binance.com/stream?streams={streams}"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                async for msg in ws:
                    data = json.loads(msg)
                    d = data.get("data", {})
                    if not d.get("s"):
                        continue
                    sym = d["s"].replace("USDT", "")
                    if sym in prices:
                        prices[sym]["fund"] = float(d.get("r", 0))
        except Exception as e:
            logger.warning(f"Binance funding error: {e}")
            await asyncio.sleep(5)

# ── BYBIT ─────────────────────────────────────────────────────
async def connect_bybit():
    url = "wss://stream.bybit.com/v5/public/spot"
    top_syms = SYMS[:50]  # Bybit top 50
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                # Subscribe
                topics = [f"tickers.{s}USDT" for s in top_syms]
                # Bybit: max 10 per message
                for i in range(0, len(topics), 10):
                    sub = {"op": "subscribe", "args": topics[i:i+10]}
                    await ws.send(json.dumps(sub))
                venue_status["bybit"] = "live"
                logger.info("Bybit connected")
                async for msg in ws:
                    data = json.loads(msg)
                    d = data.get("data", {})
                    if not d:
                        continue
                    sym_raw = d.get("symbol", "")
                    if not sym_raw.endswith("USDT"):
                        continue
                    sym = sym_raw.replace("USDT", "")
                    p = float(d.get("lastPrice", 0) or 0)
                    if p > 0:
                        update_price(sym, "bybit", p,
                            float(d.get("volume24h", 0) or 0) * p,
                            float(d.get("price24hPcnt", 0) or 0) * 100,
                            float(d.get("highPrice24h", 0) or 0),
                            float(d.get("lowPrice24h", 0) or 0))
        except Exception as e:
            venue_status["bybit"] = "error"
            logger.warning(f"Bybit error: {e}")
            await asyncio.sleep(5)

# ── OKX ───────────────────────────────────────────────────────
async def connect_okx():
    url = "wss://ws.okx.com:8443/ws/v5/public"
    top_syms = SYMS[:40]
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                args = [{"channel": "tickers", "instId": f"{s}-USDT"} for s in top_syms]
                sub = {"op": "subscribe", "args": args}
                await ws.send(json.dumps(sub))
                venue_status["okx"] = "live"
                logger.info("OKX connected")
                async for msg in ws:
                    data = json.loads(msg)
                    if data.get("event"):
                        continue
                    for d in data.get("data", []):
                        inst = d.get("instId", "")
                        if not inst.endswith("-USDT"):
                            continue
                        sym = inst.replace("-USDT", "")
                        p = float(d.get("last", 0) or 0)
                        if p > 0:
                            update_price(sym, "okx", p,
                                float(d.get("volCcy24h", 0) or 0),
                                float(d.get("sodUtc8", 0) or 0),
                                float(d.get("high24h", 0) or 0),
                                float(d.get("low24h", 0) or 0))
        except Exception as e:
            venue_status["okx"] = "error"
            logger.warning(f"OKX error: {e}")
            await asyncio.sleep(5)

# ── MEXC ──────────────────────────────────────────────────────
async def connect_mexc():
    url = "wss://wbs.mexc.com/ws"
    top_syms = SYMS[:30]
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                for s in top_syms:
                    sub = {"method": "SUBSCRIPTION",
                           "params": [f"spot@public.miniTickers.v3.api@{s}USDT"]}
                    await ws.send(json.dumps(sub))
                venue_status["mexc"] = "live"
                logger.info("MEXC connected")
                async for msg in ws:
                    data = json.loads(msg)
                    d = data.get("d", {})
                    if not d:
                        continue
                    sym_raw = data.get("s", "")
                    if not sym_raw.endswith("USDT"):
                        continue
                    sym = sym_raw.replace("USDT", "")
                    p = float(d.get("c", 0) or 0)
                    if p > 0:
                        update_price(sym, "mexc", p,
                            float(d.get("v", 0) or 0) * p,
                            float(d.get("P", 0) or 0))
        except Exception as e:
            venue_status["mexc"] = "error"
            logger.warning(f"MEXC error: {e}")
            await asyncio.sleep(5)

# ── KUCOIN ────────────────────────────────────────────────────
async def connect_kucoin():
    """KuCoin: avval token olish kerak, keyin WS"""
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post("https://api.kucoin.com/api/v1/bullet-public") as resp:
                    data = await resp.json()
                    token = data["data"]["token"]
                    endpoint = data["data"]["instanceServers"][0]["endpoint"]
            url = f"{endpoint}?token={token}"
            async with websockets.connect(url, ping_interval=18) as ws:
                # Subscribe top 20
                top_syms = SYMS[:20]
                topics = ",".join(f"{s}-USDT" for s in top_syms)
                sub = {
                    "id": "1",
                    "type": "subscribe",
                    "topic": f"/market/ticker:{topics}",
                    "privateChannel": False,
                    "response": True
                }
                await ws.send(json.dumps(sub))
                venue_status["kucoin"] = "live"
                logger.info("KuCoin connected")
                async for msg in ws:
                    data = json.loads(msg)
                    if data.get("type") != "message":
                        continue
                    d = data.get("data", {})
                    subject = data.get("subject", "")
                    if not subject or "-USDT" not in subject:
                        continue
                    sym = subject.replace("-USDT", "")
                    p = float(d.get("price", 0) or 0)
                    if p > 0:
                        update_price(sym, "kucoin", p,
                            float(d.get("vol", 0) or 0) * p)
        except Exception as e:
            venue_status["kucoin"] = "error"
            logger.warning(f"KuCoin error: {e}")
            await asyncio.sleep(10)

# ── KRAKEN ────────────────────────────────────────────────────
async def connect_kraken():
    url = "wss://ws.kraken.com"
    top_syms = SYMS[:15]
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                pairs = [f"{s}/USDT" for s in top_syms]
                sub = {"event": "subscribe", "pair": pairs,
                       "subscription": {"name": "ticker"}}
                await ws.send(json.dumps(sub))
                venue_status["kraken"] = "live"
                logger.info("Kraken connected")
                async for msg in ws:
                    data = json.loads(msg)
                    if not isinstance(data, list) or len(data) < 4:
                        continue
                    d = data[1]
                    pair = data[3]
                    if not isinstance(d, dict):
                        continue
                    sym = pair.split("/")[0]
                    p = float(d.get("c", [0])[0] or 0)
                    if p > 0:
                        update_price(sym, "kraken", p,
                            float(d.get("v", [0,0])[1] or 0) * p,
                            float(d.get("p", [0,0])[1] or 0))
        except Exception as e:
            venue_status["kraken"] = "error"
            logger.warning(f"Kraken error: {e}")
            await asyncio.sleep(5)

# ── HTX (Huobi) ───────────────────────────────────────────────
async def connect_htx():
    import gzip
    url = "wss://api.huobi.pro/ws"
    top_syms = SYMS[:30]
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                for s in top_syms:
                    sub = {"sub": f"market.{s.lower()}usdt.ticker", "id": s}
                    await ws.send(json.dumps(sub))
                venue_status["htx"] = "live"
                logger.info("HTX connected")
                async for msg in ws:
                    try:
                        data = json.loads(gzip.decompress(msg))
                    except Exception:
                        continue
                    if "ping" in data:
                        await ws.send(json.dumps({"pong": data["ping"]}))
                        continue
                    d = data.get("tick", {})
                    ch = data.get("ch", "")
                    if not d or not ch:
                        continue
                    # market.btcusdt.ticker
                    parts = ch.split(".")
                    if len(parts) < 2:
                        continue
                    sym_raw = parts[1].replace("usdt", "").upper()
                    p = float(d.get("close", 0) or 0)
                    if p > 0:
                        update_price(sym_raw, "htx", p,
                            float(d.get("vol", 0) or 0),
                            float(d.get("open", 0) or 0),
                            float(d.get("high", 0) or 0),
                            float(d.get("low", 0) or 0))
        except Exception as e:
            venue_status["htx"] = "error"
            logger.warning(f"HTX error: {e}")
            await asyncio.sleep(5)

# ── BITGET ────────────────────────────────────────────────────
async def connect_bitget():
    url = "wss://ws.bitget.com/v2/ws/public"
    top_syms = SYMS[:30]
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                args = [{"instType": "SPOT", "channel": "ticker",
                         "instId": f"{s}USDT"} for s in top_syms]
                sub = {"op": "subscribe", "args": args}
                await ws.send(json.dumps(sub))
                venue_status["bitget"] = "live"
                logger.info("Bitget connected")
                async for msg in ws:
                    data = json.loads(msg)
                    if data.get("event"):
                        continue
                    for d in data.get("data", []):
                        inst = d.get("instId", "")
                        if not inst.endswith("USDT"):
                            continue
                        sym = inst.replace("USDT", "")
                        p = float(d.get("lastPr", 0) or 0)
                        if p > 0:
                            update_price(sym, "bitget", p,
                                float(d.get("baseVolume", 0) or 0) * p,
                                float(d.get("change24h", 0) or 0) * 100,
                                float(d.get("high24h", 0) or 0),
                                float(d.get("low24h", 0) or 0))
        except Exception as e:
            venue_status["bitget"] = "error"
            logger.warning(f"Bitget error: {e}")
            await asyncio.sleep(5)

# ── COINBASE ──────────────────────────────────────────────────
async def connect_coinbase():
    url = "wss://advanced-trade-ws.coinbase.com"
    top_syms = SYMS[:20]
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                product_ids = [f"{s}-USDT" for s in top_syms]
                sub = {
                    "type": "subscribe",
                    "product_ids": product_ids,
                    "channel": "ticker"
                }
                await ws.send(json.dumps(sub))
                venue_status["coinbase"] = "live"
                logger.info("Coinbase connected")
                async for msg in ws:
                    data = json.loads(msg)
                    if data.get("channel") != "ticker":
                        continue
                    for event in data.get("events", []):
                        for t in event.get("tickers", []):
                            pid = t.get("product_id", "")
                            if not pid.endswith("-USDT"):
                                continue
                            sym = pid.replace("-USDT", "")
                            p = float(t.get("price", 0) or 0)
                            if p > 0:
                                update_price(sym, "coinbase", p,
                                    float(t.get("volume_24_h", 0) or 0) * p,
                                    float(t.get("price_percent_chg_24_h", 0) or 0))
        except Exception as e:
            venue_status["coinbase"] = "error"
            logger.warning(f"Coinbase error: {e}")
            await asyncio.sleep(5)

# ── HYPERLIQUID (DEX) ─────────────────────────────────────────
async def connect_hyperliquid():
    url = "wss://api.hyperliquid.xyz/ws"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                sub = {"method": "subscribe", "subscription": {"type": "allMids"}}
                await ws.send(json.dumps(sub))
                venue_status["hyperliquid"] = "live"
                logger.info("Hyperliquid connected")
                async for msg in ws:
                    data = json.loads(msg)
                    if data.get("channel") != "allMids":
                        continue
                    mids = data.get("data", {}).get("mids", {})
                    for sym, price_str in mids.items():
                        p = float(price_str or 0)
                        if p > 0 and sym in [s for s in SYMS]:
                            update_price(sym, "hyperliquid", p)
        except Exception as e:
            venue_status["hyperliquid"] = "error"
            logger.warning(f"Hyperliquid error: {e}")
            await asyncio.sleep(5)

# ── dYdX (DEX) ────────────────────────────────────────────────
async def connect_dydx():
    url = "wss://indexer.dydx.trade/v4/ws"
    top_syms = SYMS[:20]
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                for s in top_syms:
                    sub = {"type": "subscribe", "channel": "v4_trades",
                           "id": f"{s}-USD"}
                    await ws.send(json.dumps(sub))
                venue_status["dydx"] = "live"
                logger.info("dYdX connected")
                async for msg in ws:
                    data = json.loads(msg)
                    if data.get("type") not in ("subscribed", "channel_data"):
                        continue
                    contents = data.get("contents", {})
                    trades = contents.get("trades", [])
                    if trades:
                        market = data.get("id", "").replace("-USD", "")
                        p = float(trades[0].get("price", 0) or 0)
                        if p > 0:
                            update_price(market, "dydx", p)
        except Exception as e:
            venue_status["dydx"] = "error"
            logger.warning(f"dYdX error: {e}")
            await asyncio.sleep(5)

# ── BINGX ─────────────────────────────────────────────────────
async def connect_bingx():
    url = "wss://open-api-ws.bingx.com/market"
    top_syms = SYMS[:20]
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                for s in top_syms:
                    sub = {"id": s, "reqType": "sub",
                           "dataType": f"{s}-USDT@ticker"}
                    await ws.send(json.dumps(sub))
                venue_status["bingx"] = "live"
                logger.info("BingX connected")
                async for msg in ws:
                    try:
                        import gzip
                        text = gzip.decompress(msg).decode()
                    except Exception:
                        text = msg if isinstance(msg, str) else msg.decode()
                    data = json.loads(text)
                    if "Ping" in data:
                        await ws.send(json.dumps({"Pong": data["Ping"]}))
                        continue
                    d = data.get("data", {})
                    sym_raw = data.get("dataType", "").split("@")[0]
                    if not sym_raw or "-USDT" not in sym_raw:
                        continue
                    sym = sym_raw.replace("-USDT", "")
                    p = float(d.get("c", 0) or 0)
                    if p > 0:
                        update_price(sym, "bingx", p,
                            float(d.get("v", 0) or 0) * p,
                            float(d.get("P", 0) or 0))
        except Exception as e:
            venue_status["bingx"] = "error"
            logger.warning(f"BingX error: {e}")
            await asyncio.sleep(5)

# ── UPBIT ─────────────────────────────────────────────────────
async def connect_upbit():
    url = "wss://api.upbit.com/websocket/v1"
    top_syms = SYMS[:15]
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                markets = [f"USDT-{s}" for s in top_syms]
                sub = [{"ticket": "QMIS"},
                       {"type": "ticker", "codes": markets}]
                await ws.send(json.dumps(sub))
                venue_status["upbit"] = "live"
                logger.info("Upbit connected")
                async for msg in ws:
                    data = json.loads(msg)
                    if data.get("type") != "ticker":
                        continue
                    code = data.get("code", "")
                    if not code.startswith("USDT-"):
                        continue
                    sym = code.replace("USDT-", "")
                    p = float(data.get("trade_price", 0) or 0)
                    if p > 0:
                        update_price(sym, "upbit", p,
                            float(data.get("acc_trade_price_24h", 0) or 0),
                            float(data.get("signed_change_rate", 0) or 0) * 100,
                            float(data.get("high_price", 0) or 0),
                            float(data.get("low_price", 0) or 0))
        except Exception as e:
            venue_status["upbit"] = "error"
            logger.warning(f"Upbit error: {e}")
            await asyncio.sleep(5)

# ── WEEX (REST fallback) ──────────────────────────────────────
async def poll_weex():
    url = "https://api.weex.com/api/spot/instruments"
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    data = await resp.json()
                    venue_status["weex"] = "live"
                    for item in data.get("data", []):
                        sym_raw = item.get("instId", "")
                        if not sym_raw.endswith("USDT"):
                            continue
                        sym = sym_raw.replace("USDT", "").replace("-","")
                        p = float(item.get("last", 0) or 0)
                        if p > 0:
                            update_price(sym, "weex", p)
        except Exception as e:
            venue_status["weex"] = "error"
            logger.warning(f"WEEX error: {e}")
        await asyncio.sleep(5)  # REST: 5s polling

# ═══════════════════════════════════════════════════════════════
# FRONTEND WEBSOCKET SERVER
# ═══════════════════════════════════════════════════════════════

@app.websocket("/ws")
async def frontend_ws(websocket: WebSocket):
    await websocket.accept()
    clients.add(websocket)
    stats["clients"] = len(clients)
    logger.info(f"Frontend connected. Total: {len(clients)}")
    try:
        # Send initial status
        await websocket.send_json({
            "type": "init",
            "venues": list(VENUES.keys()),
            "coins": len(SYMS),
        })
        # Keep alive
        while True:
            await asyncio.sleep(30)
            await websocket.send_json({"type": "ping"})
    except WebSocketDisconnect:
        pass
    finally:
        clients.discard(websocket)
        stats["clients"] = len(clients)

# Push updates to all frontend clients
async def broadcast_loop():
    """Har 100ms da yangi narxlarni frontendga yuboradi"""
    last_sent = {}
    while True:
        await asyncio.sleep(0.1)  # 100ms
        if not clients:
            continue
        updates = {}
        for sym, data in prices.items():
            if not data.get("unified"):
                continue
            # Faqat o'zgargan narxlarni yuboramiz
            key = f"{sym}:{data['unified']:.6f}"
            if last_sent.get(sym) == key:
                continue
            last_sent[sym] = key
            updates[sym] = {
                "p": round(data["unified"], 8),      # unified price
                "ch": round(data.get("ch", 0), 4),
                "v": round(data.get("vol", 0), 2),
                "hi": round(data.get("hi", 0), 8),
                "lo": round(data.get("lo", 0), 8),
                "f": round(data.get("fund", 0), 6),  # funding
                "vs": {k: round(v, 8) for k, v in  # venue prices
                       data.get("venues", {}).items()},
            }
        if updates:
            msg = json.dumps({"type": "prices", "data": updates})
            dead = set()
            for client in clients.copy():
                try:
                    await client.send_text(msg)
                except Exception:
                    dead.add(client)
            clients -= dead

# ── REST ENDPOINTS ────────────────────────────────────────────
@app.get("/")
async def root():
    uptime = int(time.time() - stats["uptime"])
    return {
        "name": "QMIS Backend",
        "version": "1.0",
        "uptime_sec": uptime,
        "coins_tracked": len(prices),
        "clients": len(clients),
        "updates": stats["updates"],
        "venues": {v: venue_status.get(v, "connecting") for v in VENUES},
    }

@app.get("/prices")
async def get_prices():
    return {"data": prices, "ts": time.time()}

@app.get("/prices/{sym}")
async def get_price(sym: str):
    sym = sym.upper()
    if sym not in prices:
        return {"error": "Not found"}
    return {"sym": sym, **prices[sym]}

@app.get("/venues")
async def get_venues():
    return {
        "venues": VENUES,
        "status": venue_status,
        "live": sum(1 for v in venue_status.values() if v == "live"),
        "total": len(VENUES),
    }

# ── STARTUP ───────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    logger.info("QMIS Backend starting...")
    connectors = [
        connect_binance(),
        connect_binance_funding(),
        connect_bybit(),
        connect_okx(),
        connect_mexc(),
        connect_kucoin(),
        connect_kraken(),
        connect_htx(),
        connect_bitget(),
        connect_coinbase(),
        connect_hyperliquid(),
        connect_dydx(),
        connect_bingx(),
        connect_upbit(),
        poll_weex(),
        broadcast_loop(),
    ]
    for coro in connectors:
        asyncio.create_task(coro)
    logger.info(f"Started {len(connectors)} tasks")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)

# ═══════════════════════════════════════════════════════════════
# CROSS-VENUE ORDER BOOK DEPTH — 14 BIRJA
# Har birjadan real bid/ask depth olib, unified OBI hisoblaymiz
# ═══════════════════════════════════════════════════════════════

from collections import defaultdict

# Order book storage: sym -> venue -> {bids:[], asks:[]}
order_books: Dict[str, Dict] = defaultdict(lambda: defaultdict(lambda: {"bids": [], "asks": [], "ts": 0}))

# Venue weights for OBI aggregation (liquidity-based)
OBI_WEIGHTS = {
    "binance":      5.0,
    "bybit":        4.0,
    "okx":          4.0,
    "coinbase":     3.5,
    "kraken":       3.0,
    "mexc":         2.5,
    "kucoin":       2.5,
    "htx":          2.0,
    "bitget":       2.0,
    "hyperliquid":  3.0,  # DEX liquidity growing
    "dydx":         2.0,
    "bingx":        1.0,
    "upbit":        1.5,
    "weex":         0.5,
}

def calc_obi_single(bids: list, asks: list, levels: int = 20) -> float:
    """Bitta birja OBI = (BidUSD - AskUSD) / TotalUSD"""
    if not bids or not asks:
        return 0.0
    bid_vol = sum(float(p) * float(q) for p, q in bids[:levels])
    ask_vol = sum(float(p) * float(q) for p, q in asks[:levels])
    total = bid_vol + ask_vol
    return (bid_vol - ask_vol) / total if total > 0 else 0.0

def calc_weighted_obi(bids: list, asks: list, levels: int = 20) -> float:
    """Weighted OBI: yaqin darajalar ko'proq og'irlik (1/i)"""
    if not bids or not asks:
        return 0.0
    bid_w = sum(float(p) * float(q) * (1.0 / (i + 1))
                for i, (p, q) in enumerate(bids[:levels]))
    ask_w = sum(float(p) * float(q) * (1.0 / (i + 1))
                for i, (p, q) in enumerate(asks[:levels]))
    total = bid_w + ask_w
    return (bid_w - ask_w) / total if total > 0 else 0.0

def calc_cross_venue_obi(sym: str) -> dict:
    """
    14 birjadan weighted OBI hisoblash
    Returns: {unified_obi, weighted_obi, venue_obis, active_venues}
    """
    venue_obis = {}
    venue_wobis = {}
    total_weight = 0.0
    weighted_sum = 0.0
    w_weighted_sum = 0.0

    for venue, ob in order_books[sym].items():
        bids = ob.get("bids", [])
        asks = ob.get("asks", [])
        age = time.time() - ob.get("ts", 0)

        # Stale data ni o'tkazib yuborish (30s dan eski)
        if age > 30 or not bids or not asks:
            continue

        obi = calc_obi_single(bids, asks)
        wobi = calc_weighted_obi(bids, asks)
        w = OBI_WEIGHTS.get(venue, 1.0)

        venue_obis[venue] = round(obi, 4)
        venue_wobis[venue] = round(wobi, 4)
        weighted_sum += obi * w
        w_weighted_sum += wobi * w
        total_weight += w

    if total_weight == 0:
        return {
            "unified_obi": 0.0,
            "weighted_obi": 0.0,
            "venue_obis": {},
            "active_venues": 0,
        }

    unified = weighted_sum / total_weight
    w_unified = w_weighted_sum / total_weight

    # Interpretation
    if unified > 0.3:
        label = "BID_DOMINANT"
        signal = "ORGANIC"
    elif unified < -0.3:
        label = "ASK_DOMINANT"
        signal = "PUMP_WARNING"
    elif unified > 0.1:
        label = "SLIGHT_BID"
        signal = "NEUTRAL_BULLISH"
    elif unified < -0.1:
        label = "SLIGHT_ASK"
        signal = "CAUTION"
    else:
        label = "BALANCED"
        signal = "NEUTRAL"

    return {
        "unified_obi":  round(unified, 4),
        "weighted_obi": round(w_unified, 4),
        "venue_obis":   venue_obis,
        "venue_wobis":  venue_wobis,
        "active_venues": len(venue_obis),
        "label": label,
        "signal": signal,
    }

def update_orderbook(sym: str, venue: str, bids: list, asks: list):
    """Order book ni yangilash va OBI ni qayta hisoblash"""
    ob = order_books[sym][venue]
    ob["bids"] = sorted(bids, key=lambda x: -float(x[0]))[:20]  # top 20 bids (desc)
    ob["asks"] = sorted(asks, key=lambda x: float(x[0]))[:20]   # top 20 asks (asc)
    ob["ts"] = time.time()

    # Cross-venue OBI ni prices ga saqlash
    obi_data = calc_cross_venue_obi(sym)
    if sym not in prices:
        init_sym(sym)
    prices[sym]["obi"] = obi_data

# ─── BINANCE DEPTH STREAM ─────────────────────────────────────
async def connect_binance_depth(sym: str):
    """Bitta coin uchun Binance @depth20@100ms"""
    url = f"wss://stream.binance.com:9443/ws/{sym.lower()}usdt@depth20@100ms"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                async for msg in ws:
                    d = json.loads(msg)
                    bids = [[float(b[0]), float(b[1])] for b in d.get("bids", []) if float(b[1]) > 0]
                    asks = [[float(a[0]), float(a[1])] for a in d.get("asks", []) if float(a[1]) > 0]
                    update_orderbook(sym, "binance", bids, asks)
        except Exception:
            await asyncio.sleep(3)

# ─── BYBIT DEPTH STREAM ───────────────────────────────────────
async def connect_bybit_depth(sym: str):
    url = "wss://stream.bybit.com/v5/public/spot"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                sub = {"op": "subscribe", "args": [f"orderbook.20.{sym}USDT"]}
                await ws.send(json.dumps(sub))
                async for msg in ws:
                    data = json.loads(msg)
                    if data.get("topic", "").startswith("orderbook"):
                        d = data.get("data", {})
                        bids = [[float(b[0]), float(b[1])] for b in d.get("b", []) if float(b[1]) > 0]
                        asks = [[float(a[0]), float(a[1])] for a in d.get("a", []) if float(a[1]) > 0]
                        if bids or asks:
                            update_orderbook(sym, "bybit", bids, asks)
        except Exception:
            await asyncio.sleep(3)

# ─── OKX DEPTH STREAM ────────────────────────────────────────
async def connect_okx_depth(sym: str):
    url = "wss://ws.okx.com:8443/ws/v5/public"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                sub = {"op": "subscribe", "args": [{"channel": "books5", "instId": f"{sym}-USDT"}]}
                await ws.send(json.dumps(sub))
                async for msg in ws:
                    data = json.loads(msg)
                    if data.get("event"):
                        continue
                    for d in data.get("data", []):
                        bids = [[float(b[0]), float(b[1])] for b in d.get("bids", []) if float(b[1]) > 0]
                        asks = [[float(a[0]), float(a[1])] for a in d.get("asks", []) if float(a[1]) > 0]
                        if bids or asks:
                            update_orderbook(sym, "okx", bids, asks)
        except Exception:
            await asyncio.sleep(3)

# ─── COINBASE DEPTH ───────────────────────────────────────────
async def connect_coinbase_depth(sym: str):
    url = "wss://advanced-trade-ws.coinbase.com"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                sub = {"type": "subscribe", "product_ids": [f"{sym}-USDT"],
                       "channel": "level2"}
                await ws.send(json.dumps(sub))
                ob_bids: Dict[float, float] = {}
                ob_asks: Dict[float, float] = {}
                async for msg in ws:
                    data = json.loads(msg)
                    if data.get("channel") != "l2_data":
                        continue
                    for event in data.get("events", []):
                        for upd in event.get("updates", []):
                            side = upd.get("side")
                            price = float(upd.get("price_level", 0))
                            qty = float(upd.get("new_quantity", 0))
                            if side == "bid":
                                if qty > 0:
                                    ob_bids[price] = qty
                                else:
                                    ob_bids.pop(price, None)
                            elif side == "offer":
                                if qty > 0:
                                    ob_asks[price] = qty
                                else:
                                    ob_asks.pop(price, None)
                    bids = sorted([[p, q] for p, q in ob_bids.items()], key=lambda x: -x[0])[:20]
                    asks = sorted([[p, q] for p, q in ob_asks.items()], key=lambda x: x[0])[:20]
                    if bids or asks:
                        update_orderbook(sym, "coinbase", bids, asks)
        except Exception:
            await asyncio.sleep(5)

# ─── KRAKEN DEPTH ─────────────────────────────────────────────
async def connect_kraken_depth(sym: str):
    url = "wss://ws.kraken.com"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                sub = {"event": "subscribe", "pair": [f"{sym}/USDT"],
                       "subscription": {"name": "book", "depth": 20}}
                await ws.send(json.dumps(sub))
                ob_bids: Dict[float, float] = {}
                ob_asks: Dict[float, float] = {}
                async for msg in ws:
                    data = json.loads(msg)
                    if not isinstance(data, list):
                        continue
                    for item in data[1:3]:
                        if isinstance(item, dict):
                            for p, q, _ in item.get("bs", item.get("b", [])):
                                price, qty = float(p), float(q)
                                if qty > 0:
                                    ob_bids[price] = qty
                                else:
                                    ob_bids.pop(price, None)
                            for p, q, _ in item.get("as", item.get("a", [])):
                                price, qty = float(p), float(q)
                                if qty > 0:
                                    ob_asks[price] = qty
                                else:
                                    ob_asks.pop(price, None)
                    bids = sorted([[p, q] for p, q in ob_bids.items()], key=lambda x: -x[0])[:20]
                    asks = sorted([[p, q] for p, q in ob_asks.items()], key=lambda x: x[0])[:20]
                    if bids or asks:
                        update_orderbook(sym, "kraken", bids, asks)
        except Exception:
            await asyncio.sleep(5)

# ─── HYPERLIQUID DEPTH (DEX) ──────────────────────────────────
async def connect_hyperliquid_depth(sym: str):
    url = "wss://api.hyperliquid.xyz/ws"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                sub = {"method": "subscribe",
                       "subscription": {"type": "l2Book", "coin": sym}}
                await ws.send(json.dumps(sub))
                async for msg in ws:
                    data = json.loads(msg)
                    if data.get("channel") != "l2Book":
                        continue
                    book = data.get("data", {})
                    bids = [[float(l["px"]), float(l["sz"])]
                            for l in book.get("levels", [[]])[0] if float(l.get("sz", 0)) > 0]
                    asks = [[float(l["px"]), float(l["sz"])]
                            for l in book.get("levels", [[], []])[1] if float(l.get("sz", 0)) > 0]
                    if bids or asks:
                        update_orderbook(sym, "hyperliquid", bids, asks)
        except Exception:
            await asyncio.sleep(5)

# ─── dYdX DEPTH (DEX) ─────────────────────────────────────────
async def connect_dydx_depth(sym: str):
    url = "wss://indexer.dydx.trade/v4/ws"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                sub = {"type": "subscribe", "channel": "v4_orderbook",
                       "id": f"{sym}-USD"}
                await ws.send(json.dumps(sub))
                ob_bids: Dict[float, float] = {}
                ob_asks: Dict[float, float] = {}
                async for msg in ws:
                    data = json.loads(msg)
                    contents = data.get("contents", {})
                    # Initial snapshot
                    for b in contents.get("bids", []):
                        p, q = float(b.get("price", 0)), float(b.get("size", 0))
                        if q > 0:
                            ob_bids[p] = q
                        else:
                            ob_bids.pop(p, None)
                    for a in contents.get("asks", []):
                        p, q = float(a.get("price", 0)), float(a.get("size", 0))
                        if q > 0:
                            ob_asks[p] = q
                        else:
                            ob_asks.pop(p, None)
                    bids = sorted([[p, q] for p, q in ob_bids.items()], key=lambda x: -x[0])[:20]
                    asks = sorted([[p, q] for p, q in ob_asks.items()], key=lambda x: x[0])[:20]
                    if bids or asks:
                        update_orderbook(sym, "dydx", bids, asks)
        except Exception:
            await asyncio.sleep(5)

# ─── MEXC DEPTH ───────────────────────────────────────────────
async def connect_mexc_depth(sym: str):
    url = "wss://wbs.mexc.com/ws"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                sub = {"method": "SUBSCRIPTION",
                       "params": [f"spot@public.limit.depth.v3.api@{sym}USDT@20"]}
                await ws.send(json.dumps(sub))
                async for msg in ws:
                    data = json.loads(msg)
                    d = data.get("d", {})
                    if not d:
                        continue
                    bids = [[float(b[0]), float(b[1])] for b in d.get("bids", []) if float(b[1]) > 0]
                    asks = [[float(a[0]), float(a[1])] for a in d.get("asks", []) if float(a[1]) > 0]
                    if bids or asks:
                        update_orderbook(sym, "mexc", bids, asks)
        except Exception:
            await asyncio.sleep(3)

# ─── KUCOIN DEPTH ─────────────────────────────────────────────
async def connect_kucoin_depth(sym: str):
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post("https://api.kucoin.com/api/v1/bullet-public") as resp:
                    tkdata = await resp.json()
                    token = tkdata["data"]["token"]
                    endpoint = tkdata["data"]["instanceServers"][0]["endpoint"]
            url = f"{endpoint}?token={token}"
            async with websockets.connect(url, ping_interval=18) as ws:
                sub = {"id": "depth", "type": "subscribe",
                       "topic": f"/spotMarket/level2Depth20:{sym}-USDT",
                       "privateChannel": False, "response": True}
                await ws.send(json.dumps(sub))
                async for msg in ws:
                    data = json.loads(msg)
                    if data.get("type") != "message":
                        continue
                    d = data.get("data", {})
                    bids = [[float(b[0]), float(b[1])] for b in d.get("bids", []) if float(b[1]) > 0]
                    asks = [[float(a[0]), float(a[1])] for a in d.get("asks", []) if float(a[1]) > 0]
                    if bids or asks:
                        update_orderbook(sym, "kucoin", bids, asks)
        except Exception:
            await asyncio.sleep(10)

# ─── HTX DEPTH ────────────────────────────────────────────────
async def connect_htx_depth(sym: str):
    import gzip
    url = "wss://api.huobi.pro/ws"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                sub = {"sub": f"market.{sym.lower()}usdt.depth.step0", "id": sym}
                await ws.send(json.dumps(sub))
                async for msg in ws:
                    try:
                        data = json.loads(gzip.decompress(msg))
                    except Exception:
                        continue
                    if "ping" in data:
                        await ws.send(json.dumps({"pong": data["ping"]}))
                        continue
                    tick = data.get("tick", {})
                    if not tick:
                        continue
                    bids = [[float(b[0]), float(b[1])] for b in tick.get("bids", [])[:20] if float(b[1]) > 0]
                    asks = [[float(a[0]), float(a[1])] for a in tick.get("asks", [])[:20] if float(a[1]) > 0]
                    if bids or asks:
                        update_orderbook(sym, "htx", bids, asks)
        except Exception:
            await asyncio.sleep(5)

# ─── BITGET DEPTH ─────────────────────────────────────────────
async def connect_bitget_depth(sym: str):
    url = "wss://ws.bitget.com/v2/ws/public"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                sub = {"op": "subscribe",
                       "args": [{"instType": "SPOT", "channel": "books5",
                                 "instId": f"{sym}USDT"}]}
                await ws.send(json.dumps(sub))
                async for msg in ws:
                    data = json.loads(msg)
                    if data.get("event"):
                        continue
                    for d in data.get("data", []):
                        bids = [[float(b[0]), float(b[1])] for b in d.get("bids", []) if float(b[1]) > 0]
                        asks = [[float(a[0]), float(a[1])] for a in d.get("asks", []) if float(a[1]) > 0]
                        if bids or asks:
                            update_orderbook(sym, "bitget", bids, asks)
        except Exception:
            await asyncio.sleep(5)

# ─── BINGX DEPTH ──────────────────────────────────────────────
async def connect_bingx_depth(sym: str):
    url = "wss://open-api-ws.bingx.com/market"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                sub = {"id": f"d{sym}", "reqType": "sub",
                       "dataType": f"{sym}-USDT@depth20"}
                await ws.send(json.dumps(sub))
                async for msg in ws:
                    try:
                        import gzip as gz
                        text = gz.decompress(msg).decode()
                    except Exception:
                        text = msg if isinstance(msg, str) else msg.decode()
                    data = json.loads(text)
                    if "Ping" in data:
                        await ws.send(json.dumps({"Pong": data["Ping"]}))
                        continue
                    d = data.get("data", {})
                    bids = [[float(b[0]), float(b[1])] for b in d.get("bids", []) if float(b[1]) > 0]
                    asks = [[float(a[0]), float(a[1])] for a in d.get("asks", []) if float(a[1]) > 0]
                    if bids or asks:
                        update_orderbook(sym, "bingx", bids, asks)
        except Exception:
            await asyncio.sleep(5)

# ─── UPBIT DEPTH ──────────────────────────────────────────────
async def connect_upbit_depth(sym: str):
    url = "wss://api.upbit.com/websocket/v1"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                sub = [{"ticket": f"ob{sym}"},
                       {"type": "orderbook", "codes": [f"USDT-{sym}"]}]
                await ws.send(json.dumps(sub))
                async for msg in ws:
                    data = json.loads(msg)
                    if data.get("type") != "orderbook":
                        continue
                    units = data.get("orderbook_units", [])
                    bids = [[float(u["bid_price"]), float(u["bid_size"])]
                            for u in units if float(u.get("bid_size", 0)) > 0]
                    asks = [[float(u["ask_price"]), float(u["ask_size"])]
                            for u in units if float(u.get("ask_size", 0)) > 0]
                    if bids or asks:
                        update_orderbook(sym, "upbit", bids, asks)
        except Exception:
            await asyncio.sleep(5)

# ─── DEPTH CONNECTIONS MANAGER ────────────────────────────────
# TOP 20 coin uchun barcha 14 birja depth stream
DEPTH_SYMS = [
    "BTC", "ETH", "BNB", "SOL", "XRP",
    "DOGE", "ADA", "AVAX", "LINK", "DOT",
    "UNI", "ATOM", "LTC", "AAVE", "ARB",
    "OP", "INJ", "WIF", "PEPE", "TIA",
]

DEPTH_CONNECTORS = {
    "binance":      connect_binance_depth,
    "bybit":        connect_bybit_depth,
    "okx":          connect_okx_depth,
    "coinbase":     connect_coinbase_depth,
    "kraken":       connect_kraken_depth,
    "mexc":         connect_mexc_depth,
    "kucoin":       connect_kucoin_depth,
    "htx":          connect_htx_depth,
    "bitget":       connect_bitget_depth,
    "hyperliquid":  connect_hyperliquid_depth,
    "dydx":         connect_dydx_depth,
    "bingx":        connect_bingx_depth,
    "upbit":        connect_upbit_depth,
    # WEEX: depth WS yo'q, REST fallback
}

async def connect_all_depths():
    """TOP 20 coin x 13 venue = 260 WS connection"""
    tasks = []
    for sym in DEPTH_SYMS:
        for venue, connector in DEPTH_CONNECTORS.items():
            tasks.append(connector(sym))
    logger.info(f"Starting {len(tasks)} depth streams "
                f"({len(DEPTH_SYMS)} coins x {len(DEPTH_CONNECTORS)} venues)")
    await asyncio.gather(*tasks, return_exceptions=True)

# ─── OBI BROADCAST (enhanced) ─────────────────────────────────
async def obi_broadcast_loop():
    """Har 500ms da cross-venue OBI ni frontendga yuboradi"""
    while True:
        await asyncio.sleep(0.5)
        if not clients:
            continue
        obi_updates = {}
        for sym in DEPTH_SYMS:
            obi_data = calc_cross_venue_obi(sym)
            if obi_data["active_venues"] > 0:
                obi_updates[sym] = obi_data
        if obi_updates:
            msg = json.dumps({"type": "obi", "data": obi_updates})
            dead = set()
            for client in clients.copy():
                try:
                    await client.send_text(msg)
                except Exception:
                    dead.add(client)
            clients -= dead

# ─── REST: OBI endpoint ───────────────────────────────────────
@app.get("/obi/{sym}")
async def get_obi(sym: str):
    sym = sym.upper()
    obi = calc_cross_venue_obi(sym)
    # Add individual venue details
    details = {}
    for venue, ob in order_books[sym].items():
        bids = ob.get("bids", [])
        asks = ob.get("asks", [])
        age = time.time() - ob.get("ts", 0)
        details[venue] = {
            "obi":    round(calc_obi_single(bids, asks), 4),
            "w_obi":  round(calc_weighted_obi(bids, asks), 4),
            "bid_levels": len(bids),
            "ask_levels": len(asks),
            "best_bid": bids[0][0] if bids else 0,
            "best_ask": asks[0][0] if asks else 0,
            "bid_usd": round(sum(p*q for p,q in bids), 2),
            "ask_usd": round(sum(p*q for p,q in asks), 2),
            "age_sec": round(age, 1),
            "stale":   age > 30,
        }
    return {
        "sym": sym,
        "cross_venue": obi,
        "venues": details,
        "formula": "(BidUSD - AskUSD) / TotalUSD per venue, then weighted median",
        "weights": OBI_WEIGHTS,
    }

@app.get("/obi")
async def get_all_obi():
    result = {}
    for sym in DEPTH_SYMS:
        result[sym] = calc_cross_venue_obi(sym)
    return {"data": result, "coins": len(DEPTH_SYMS), "ts": time.time()}

# ─── UPDATE STARTUP EVENT ─────────────────────────────────────
@app.on_event("startup")
async def startup_with_depth():
    logger.info("QMIS Backend v2 starting (14-venue OBI)...")
    tasks = [
        # Price streams (400 coin)
        connect_binance(),
        connect_binance_funding(),
        connect_bybit(),
        connect_okx(),
        connect_mexc(),
        connect_kucoin(),
        connect_kraken(),
        connect_htx(),
        connect_bitget(),
        connect_coinbase(),
        connect_hyperliquid(),
        connect_dydx(),
        connect_bingx(),
        connect_upbit(),
        poll_weex(),
        # OBI depth streams (top 20 coin x 13 venue)
        connect_all_depths(),
        # Broadcast loops
        broadcast_loop(),
        obi_broadcast_loop(),
        venue_broadcast_loop(),
    ]
    for coro in tasks:
        asyncio.create_task(coro)
    logger.info(f"Started {len(tasks)} task groups")
    logger.info(f"Depth streams: {len(DEPTH_SYMS)} coins x {len(DEPTH_CONNECTORS)} venues")

# ─── VENUE STATUS BROADCAST ───────────────────────────────────
async def venue_broadcast_loop():
    """Har 5s da venue statuslarni frontendga yuboradi"""
    while True:
        await asyncio.sleep(5)
        if not clients:
            continue
        msg = json.dumps({
            "type": "venues",
            "data": {v: venue_status.get(v, "connecting") for v in VENUES}
        })
        dead = set()
        for client in clients.copy():
            try:
                await client.send_text(msg)
            except Exception:
                dead.add(client)
        clients -= dead
