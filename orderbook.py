import asyncio
import json
import time
import os
import httpx
import websockets
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# --- متغيرات التخزين المؤقت ---
orderbook_cache = {"data": None, "last_update": 0}
klines_cache = {}
live_orderbook = {"bids": [], "asks": [], "last_update": 0}
live_hyperliquid = {"bid": 0, "ask": 0, "last_update": 0}

# --- دوال Binance ---
async def binance_orderbook_listener():
    global live_orderbook
    url = "wss://fstream.binance.com/ws/xauusdt@depth20@100ms"
    while True:
        try:
            async with websockets.connect(url) as websocket:
                print("Binance OrderBook Connected!")
                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    if "bids" in data and "asks" in data:
                        live_orderbook["bids"] = data["bids"]
                        live_orderbook["asks"] = data["asks"]
                        live_orderbook["last_update"] = time.time()
        except Exception as e:
            print(f"Binance WS Error: {e}. Reconnecting...")
            await asyncio.sleep(5)

# --- دالة Hyperliquid ---
async def hyperliquid_listener():
    global live_hyperliquid
    url = "wss://api.hyperliquid.xyz/ws"
    subscribe_msg = {"method": "subscribe", "subscription": {"type": "l2Book", "coin": "GOLD"}}
    
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as websocket:
                print("Hyperliquid WebSocket Connected!")
                await websocket.send(json.dumps(subscribe_msg))
                
                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    
                    if data.get("channel") == "l2Book":
                        levels = data["data"]["levels"]
                        if levels and len(levels) == 2:
                            bids = levels[0]
                            asks = levels[1]
                            if bids and asks:
                                live_hyperliquid["bid"] = float(bids[0]["px"])
                                live_hyperliquid["ask"] = float(asks[0]["px"])
                                live_hyperliquid["last_update"] = time.time()
        except Exception as e:
            print(f"Hyperliquid WS Error: {e}. Reconnecting...")
            await asyncio.sleep(5)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(binance_orderbook_listener())
    asyncio.create_task(hyperliquid_listener())

# --- واجهة المستخدم (HTML) بعد مسح التداولات ---
HTML_PAGE = """
<!DOCTYPE html>
<html>
<head>
    <title>XAUUSDT Pro - Binance vs Hyperliquid</title>
    <style>
        body { background-color: #050505; color: #e0e0e0; font-family: 'Consolas', monospace; text-align: center; margin: 0; padding: 15px; }
        h1 { color: #00ffcc; text-shadow: 0 0 15px rgba(0,255,204,0.6); font-size: 22px; }
        .panel { width: 320px; background: #111; padding: 10px; border-radius: 8px; border: 1px solid #222; }
        .container { display: flex; justify-content: center; gap: 15px; flex-wrap: wrap; margin-top: 15px; }
        .row { display: flex; justify-content: space-between; padding: 3px 5px; border-bottom: 1px solid #1a1a1a; font-size: 13px; position: relative; }
        .bg-bar { position: absolute; top: 0; bottom: 0; right: 0; z-index: 1; opacity: 0.15; }
        .text-content { z-index: 2; display: flex; justify-content: space-between; width: 100%; }
        .asks-color { color: #ff3333; } .bids-color { color: #33ff33; }
        .spread-center { margin: 5px 0; padding: 8px; background: #1a1a1a; border-left: 2px solid #00ffcc; font-size: 14px; }
        #chart-container { width: 80%; max-width: 800px; height: 250px; margin: 15px auto; background: #111; padding: 10px; border-radius: 8px; }
        .hl-panel { background: #111; border: 1px solid #ffaa00; padding: 10px; border-radius: 8px; margin: 10px auto; max-width: 500px; }
        .hl-panel h3 { color: #ffaa00; margin: 0 0 5px 0; }
        .btn { background: #1a1a1a; color: #fff; border: 1px solid #00ffcc; padding: 5px 10px; cursor: pointer; border-radius: 4px; }
        .btn.active { background: #00ffcc; color: #000; }
    </style>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
    <h1>XAUUSDT Pro (Binance vs Hyperliquid)</h1>
    
    <div class="hl-panel">
        <h3>Hyperliquid (GOLD)</h3>
        <div style="display: flex; justify-content: space-around;">
            <div>Bid: <span id="hl-bid" style="color:#33ff33; font-weight:bold;">--</span></div>
            <div>Ask: <span id="hl-ask" style="color:#ff3333; font-weight:bold;">--</span></div>
        </div>
        <div style="margin-top:5px; font-size:12px; color:#aaa;">Spread: <span id="hl-spread">--</span></div>
    </div>

    <div style="margin-bottom: 10px;">
        <button onclick="changeTimeframe('5m', this)" class="btn active">5m</button>
        <button onclick="changeTimeframe('1h', this)" class="btn">1h</button>
        <button onclick="changeTimeframe('4h', this)" class="btn">4h</button>
    </div>

    <div id="chart-container"><canvas id="priceChart"></canvas></div>

    <div class="container">
        <div class="panel"><h3>Bids (Binance)</h3><div id="bids"></div></div>
        <div style="display: flex; flex-direction: column; justify-content: center;">
            <div class="spread-center" id="spread-indicator">Spread: --</div>
            <div class="spread-center" id="volume-delta" style="border-left-color: #ffaa00; margin-top: 5px; font-size: 11px;">Delta: --</div>
        </div>
        <div class="panel"><h3>Asks (Binance)</h3><div id="asks"></div></div>
    </div>

    <script>
        let chartInstance = null;

        async function fetchOrderBook() {
            try {
                let response = await fetch("/api/orderbook");
                let data = await response.json();
                if(!data.bids || !data.asks) return;
                
                let totalBids = 0, totalAsks = 0;
                data.bids.forEach(b => totalBids += parseFloat(b[1]));
                data.asks.forEach(a => totalAsks += parseFloat(a[1]));
                document.getElementById("volume-delta").innerHTML = `Delta: <span style="color:${totalBids>totalAsks?'#33ff33':'#ff3333'}">${(totalBids-totalAsks).toFixed(2)}</span>`;

                const bestBid = parseFloat(data.bids[0][0]);
                const bestAsk = parseFloat(data.asks[0][0]);
                document.getElementById("spread-indicator").innerHTML = `Spread<br>${(bestAsk-bestBid).toFixed(3)}`;

                let maxBidQty = Math.max(...data.bids.map(b => parseFloat(b[1])));
                let maxAskQty = Math.max(...data.asks.map(a => parseFloat(a[1])));
                let maxQty = Math.max(maxBidQty, maxAskQty);

                let bidsHtml = "", asksHtml = "";
                data.bids.forEach(bid => {
                    let w = Math.min((parseFloat(bid[1])/maxQty)*100, 100);
                    bidsHtml += `<div class="row"><div class="bg-bar" style="width:${w}%;background:#33ff33;"></div><div class="text-content bids-color"><span>${parseFloat(bid[0]).toFixed(2)}</span><span>${parseFloat(bid[1]).toFixed(3)}</span></div></div>`;
                });
                data.asks.forEach(ask => {
                    let w = Math.min((parseFloat(ask[1])/maxQty)*100, 100);
                    asksHtml += `<div class="row"><div class="bg-bar" style="width:${w}%;background:#ff3333;"></div><div class="text-content asks-color"><span>${parseFloat(ask[0]).toFixed(2)}</span><span>${parseFloat(ask[1]).toFixed(3)}</span></div></div>`;
                });
                document.getElementById("bids").innerHTML = bidsHtml;
                document.getElementById("asks").innerHTML = asksHtml;
            } catch (e) {}
        }

        async function fetchHyperliquid() {
            try {
                let response = await fetch("/api/hyperliquid");
                let data = await response.json();
                if(data.bid && data.ask) {
                    document.getElementById("hl-bid").innerText = data.bid.toFixed(2);
                    document.getElementById("hl-ask").innerText = data.ask.toFixed(2);
                    document.getElementById("hl-spread").innerText = (data.ask - data.bid).toFixed(3);
                }
            } catch (e) {}
        }

        async function fetchKlines(timeframe) {
            try {
                let response = await fetch(`/api/klines?interval=${timeframe}`);
                let data = await response.json();
                renderChart(data);
            } catch (e) {}
        }

        function renderChart(data) {
            const ctx = document.getElementById('priceChart').getContext('2d');
            const labels = data.map(k => new Date(k[0]).toLocaleTimeString());
            const prices = data.map(k => parseFloat(k[4]));
            if (chartInstance) chartInstance.destroy();
            chartInstance = new Chart(ctx, { type: 'line', data: { labels: labels, datasets: [{ label: 'السعر', data: prices, borderColor: '#00ffcc', backgroundColor: 'rgba(0,255,204,0.1)', borderWidth: 2, fill: true, pointRadius: 0 }] }, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { ticks: { color: '#666' } }, y: { ticks: { color: '#666' } } } } });
        }

        function changeTimeframe(tf, btn) {
            document.querySelectorAll('.btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active'); fetchKlines(tf);
        }

        fetchKlines('5m'); fetchOrderBook(); fetchHyperliquid();
        setInterval(fetchOrderBook, 1000);
        setInterval(fetchHyperliquid, 1000);
    </script>
</body>
</html>
"""

@app.get("/")
async def get(): return HTMLResponse(HTML_PAGE)

@app.get("/api/orderbook")
async def get_orderbook():
    global orderbook_cache
    current_time = time.time()
    if current_time - live_orderbook["last_update"] < 2.0 and live_orderbook["bids"]:
        return {"bids": live_orderbook["bids"], "asks": live_orderbook["asks"]}
    if current_time - orderbook_cache["last_update"] > 0.5 or not orderbook_cache["data"]:
        url = "https://fapi.binance.com/fapi/v1/depth?symbol=XAUUSDT&limit=20"
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, timeout=5.0)
                data = response.json()
                orderbook_cache["data"] = {"bids": data.get("bids", []), "asks": data.get("asks", [])}
                orderbook_cache["last_update"] = current_time
            except Exception as e:
                if orderbook_cache["data"]: return orderbook_cache["data"]
    return orderbook_cache["data"]

@app.get("/api/hyperliquid")
async def get_hyperliquid():
    return live_hyperliquid

@app.get("/api/klines")
async def get_klines(interval: str = Query("5m", regex="^(5m|1h|4h)$")):
    global klines_cache
    current_time = time.time()
    if interval not in klines_cache or current_time - klines_cache[interval]["last_update"] > 10:
        url = f"https://fapi.binance.com/fapi/v1/klines?symbol=XAUUSDT&interval={interval}&limit=50"
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, timeout=5.0)
                klines_cache[interval] = {"data": response.json(), "last_update": current_time}
            except Exception as e:
                if interval in klines_cache: return klines_cache[interval]["data"]
                return []
    return klines_cache[interval]["data"]

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
