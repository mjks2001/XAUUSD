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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- متغيرات التخزين المؤقت ---
orderbook_cache = {"data": None, "last_update": 0}
klines_cache = {}  
live_orderbook = {"bids": [], "asks": [], "last_update": 0}
live_trades = [] 
last_walls = {"bids": {}, "asks": {}} 

# --- دوال الويب سوكيت ---
async def binance_orderbook_listener():
    global live_orderbook
    url = "wss://fstream.binance.com/ws/xauusdt@depth20@100ms"
    while True:
        try:
            async with websockets.connect(url) as websocket:
                print("OrderBook WebSocket Connected!")
                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    if "bids" in data and "asks" in data:
                        live_orderbook["bids"] = data["bids"]
                        live_orderbook["asks"] = data["asks"]
                        live_orderbook["last_update"] = time.time()
        except Exception as e:
            print(f"OrderBook WS Error: {e}. Reconnecting...")
            await asyncio.sleep(5)

async def binance_trades_listener():
    global live_trades
    url = "wss://fstream.binance.com/ws/xauusdt@aggTrade" 
    while True:
        try:
            async with websockets.connect(url) as websocket:
                print("Trades WebSocket Connected!")
                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    trade = {
                        "price": float(data["p"]),
                        "qty": float(data["q"]),
                        "isBuyerMaker": data["m"], 
                        "time": data["T"] / 1000
                    }
                    live_trades.insert(0, trade)
                    if len(live_trades) > 30:
                        live_trades.pop()
        except Exception as e:
            print(f"Trades WS Error: {e}. Reconnecting...")
            await asyncio.sleep(5)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(binance_orderbook_listener())
    asyncio.create_task(binance_trades_listener())

# --- واجهة المستخدم (HTML) ---
HTML_PAGE = """
<!DOCTYPE html>
<html>
<head>
    <title>XAUUSDT Pro Dashboard - The Ultimate</title>
    <style>
        :root { --bg-main: #050505; --bg-panel: #111; --text-main: #e0e0e0; --accent: #00ffcc; --red: #ff3333; --green: #33ff33; }
        body { background-color: var(--bg-main); color: var(--text-main); font-family: 'Consolas', 'Courier New', monospace; text-align: center; margin: 0; padding: 15px; transition: background-color 0.3s; }
        h1 { color: var(--accent); text-shadow: 0 0 15px rgba(0, 255, 204, 0.6); margin-bottom: 5px; font-size: 22px; }
        
        /* وميض الشاشة */
        .screen-flash { background-color: #330000 !important; }
        
        /* لوحة التحكم */
        .top-controls { display: flex; justify-content: center; align-items: center; gap: 10px; margin-bottom: 15px; flex-wrap: wrap; }
        .btn { background: #1a1a1a; color: var(--text-main); border: 1px solid var(--accent); padding: 5px 10px; cursor: pointer; border-radius: 4px; font-weight: bold; transition: all 0.2s; font-size: 12px; }
        .btn:hover, .btn.active { background: var(--accent); color: #000; box-shadow: 0 0 10px var(--accent); }
        .btn-danger { border-color: var(--red); }
        .btn-danger:hover { background: var(--red); color: #fff; box-shadow: 0 0 10px var(--red); }
        
        /* الإعدادات */
        .settings-box { display: inline-flex; gap: 10px; background: #111; padding: 5px 10px; border-radius: 4px; border: 1px solid #333; font-size: 12px; align-items: center; margin-bottom: 15px; }
        .settings-box input { width: 50px; background: #222; color: var(--accent); border: 1px solid #444; padding: 2px 4px; border-radius: 3px; text-align: center; }

        /* الحاوية الرئيسية */
        .main-layout { display: flex; justify-content: center; gap: 15px; flex-wrap: wrap; }
        .panel { width: 300px; background: var(--bg-panel); padding: 10px; border-radius: 8px; border: 1px solid #222; box-shadow: 0 5px 15px rgba(0,0,0,0.8); }
        h3 { margin: 0 0 10px 0; font-size: 15px; color: #fff; border-bottom: 1px solid #333; padding-bottom: 5px; }
        
        /* دفتر الأوامر */
        .row { display: flex; justify-content: space-between; padding: 3px 5px; border-bottom: 1px solid #1a1a1a; font-size: 12px; position: relative; cursor: pointer; }
        .row:hover { background: #222; }
        .bg-bar { position: absolute; top: 0; bottom: 0; right: 0; z-index: 1; opacity: 0.15; }
        .text-content { z-index: 2; display: flex; justify-content: space-between; width: 100%; }
        .asks-color { color: var(--red); }
        .bids-color { color: var(--green); }
        .spread-center { margin: 5px 0; padding: 5px; background: #1a1a1a; border-left: 2px solid var(--accent); font-size: 13px; font-weight: bold; color: #fff; }

        /* التداولات */
        .trade-row { display: flex; justify-content: space-between; padding: 2px 5px; font-size: 11px; border-bottom: 1px solid #1a1a1a; }
        .trade-buy { color: var(--green); }
        .trade-sell { color: var(--red); }
        
        /* الوميض */
        .flash-green { animation: flashGreen 0.5s; }
        .flash-red { animation: flashRed 0.5s; }
        @keyframes flashGreen { 0% { background-color: rgba(51, 255, 51, 0.4); } 100% { background-color: transparent; } }
        @keyframes flashRed { 0% { background-color: rgba(255, 51, 51, 0.4); } 100% { background-color: transparent; } }

        /* الشارت */
        #chart-container { width: 90%; max-width: 900px; height: 250px; margin: 0 auto 15px auto; background: var(--bg-panel); padding: 10px; border-radius: 8px; border: 1px solid #222; position: relative; }
        
        /* الحاسبة */
        #calc-panel { display: none; position: fixed; bottom: 20px; right: 20px; background: #111; border: 2px solid var(--accent); padding: 15px; border-radius: 8px; box-shadow: 0 0 20px rgba(0,255,204,0.3); z-index: 100; font-size: 13px; text-align: right; }
        #calc-panel h4 { margin: 0 0 10px 0; color: var(--accent); border-bottom: 1px solid #333; padding-bottom: 5px;}
        .close-calc { position: absolute; top: 5px; left: 5px; color: var(--red); cursor: pointer; font-weight: bold; }
    </style>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body id="main-body">
    <h1>XAUUSDT Ultimate - Algo Trader Edition</h1>
    
    <div class="settings-box">
        <span>حجم الجدار (أونصة):</span>
        <input type="number" id="whale-input" value="40" min="10" step="10">
        <span>|</span>
        <span>الهدف (ربح $):</span>
        <input type="number" id="profit-input" value="5" step="0.5">
        <span>|</span>
        <span>الوقف (خسارة $):</span>
        <input type="number" id="stop-input" value="3" step="0.5">
    </div>

    <div class="top-controls">
        <button id="pause-btn" class="btn" onclick="togglePause()">⏸ إيقاف</button>
        <button class="btn" onclick="enableAudio()" id="audio-btn">🔔 الصوت</button>
        <button class="btn active" onclick="changeTimeframe('5m', this)">5m</button>
        <button class="btn" onclick="changeTimeframe('1h', this)">1h</button>
        <button class="btn" onclick="changeTimeframe('4h', this)">4h</button>
    </div>

    <div id="chart-container">
        <canvas id="priceChart"></canvas>
    </div>

    <div class="main-layout">
        <div class="panel">
            <h3>Bids (Buyers)</h3>
            <div id="bids"></div>
        </div>
        
        <div style="display: flex; flex-direction: column; justify-content: center;">
            <div class="spread-center" id="spread-indicator">Spread: --</div>
            <div class="spread-center" id="volume-delta" style="border-left-color: #ffaa00; margin-top: 5px; font-size: 11px;">Delta: --</div>
        </div>

        <div class="panel">
            <h3>Asks (Sellers)</h3>
            <div id="asks"></div>
        </div>

        <div class="panel">
            <h3>Recent Trades</h3>
            <div id="trades"></div>
        </div>
    </div>

    <!-- حاسبة الصفقة -->
    <div id="calc-panel">
        <span class="close-calc" onclick="document.getElementById('calc-panel').style.display='none'">✖</span>
        <h4>حاسبة (الدخول: <span id="calc-entry-price" style="color:#fff"></span>)</h4>
        <div style="margin-bottom: 5px;">الهدف (TP): <span id="calc-profit" style="color: var(--green); font-weight:bold;"></span></div>
        <div>وقف الخسارة (SL): <span id="calc-stop" style="color: var(--red); font-weight:bold;"></span></div>
    </div>

    <script>
        let chartInstance = null;
        let lastTradePrice = 0;
        let isPaused = false;
        let audioCtx = null;
        let lastBestBid = 0;
        let lastBestAsk = 0;
        
        let previousBids = {};
        let previousAsks = {};

        function flashScreen() {
            const body = document.getElementById('main-body');
            body.classList.add('screen-flash');
            setTimeout(() => body.classList.remove('screen-flash'), 300);
        }

        function playSound(type) {
            if (!audioCtx) return;
            const osc = audioCtx.createOscillator();
            const gain = audioCtx.createGain();
            osc.connect(gain); gain.connect(audioCtx.destination);
            if (type === 'buy') { osc.frequency.setValueAtTime(800, audioCtx.currentTime); osc.frequency.exponentialRampToValueAtTime(1200, audioCtx.currentTime + 0.1); } 
            else { osc.frequency.setValueAtTime(400, audioCtx.currentTime); osc.frequency.exponentialRampToValueAtTime(200, audioCtx.currentTime + 0.1); }
            gain.gain.setValueAtTime(0.1, audioCtx.currentTime);
            gain.gain.exponentialRampToValueAtTime(0.01, audioCtx.currentTime + 0.2);
            osc.start(); osc.stop(audioCtx.currentTime + 0.2);
        }

        function enableAudio() {
            if (!audioCtx) {
                audioCtx = new (window.AudioContext || window.webkitAudioContext)();
                document.getElementById('audio-btn').innerText = "🔔 مفعل";
                document.getElementById('audio-btn').classList.add('btn-danger');
                playSound('buy');
            }
        }

        function togglePause() {
            isPaused = !isPaused;
            document.getElementById('pause-btn').innerText = isPaused ? "▶ استئناف" : "⏸ إيقاف";
            document.getElementById('pause-btn').classList.toggle('btn-danger');
        }

        function showCalculator(price) {
            const p = parseFloat(price);
            const profitVal = parseFloat(document.getElementById('profit-input').value);
            const stopVal = parseFloat(document.getElementById('stop-input').value);
            
            document.getElementById('calc-entry-price').innerText = p.toFixed(2);
            document.getElementById('calc-profit').innerText = (p + profitVal).toFixed(2) + ` (+$${profitVal})`;
            document.getElementById('calc-stop').innerText = (p - stopVal).toFixed(2) + ` (-$${stopVal})`;
            document.getElementById('calc-panel').style.display = 'block';
        }

        function createRow(price, qty, maxQty, isBid) {
            const qtyNum = parseFloat(qty);
            const widthPercent = Math.min((qtyNum / maxQty) * 100, 100);
            const colorClass = isBid ? 'bids-color' : 'asks-color';
            const bgColor = isBid ? '#33ff33' : '#ff3333';
            return `
                <div class="row" onclick="showCalculator('${price}')">
                    <div class="bg-bar" style="width: ${widthPercent}%; background-color: ${bgColor};"></div>
                    <div class="text-content ${colorClass}">
                        <span>${parseFloat(price).toFixed(2)}</span>
                        <span>${qtyNum.toFixed(3)}</span>
                    </div>
                </div>`;
        }

        async function fetchOrderBook() {
            if (isPaused) return;
            try {
                let response = await fetch("/api/orderbook");
                let data = await response.json();
                if(!data.bids || !data.asks) return;

                let whaleThreshold = parseFloat(document.getElementById('whale-input').value);

                // رادار الحيتان
                let currentBids = {}; data.bids.forEach(b => currentBids[b[0]] = parseFloat(b[1]));
                let currentAsks = {}; data.asks.forEach(a => currentAsks[a[0]] = parseFloat(a[1]));

                for (let price in previousBids) {
                    if (previousBids[price] > whaleThreshold && (!currentBids[price] || currentBids[price] < previousBids[price] * 0.5)) { flashScreen(); break; }
                }
                for (let price in previousAsks) {
                    if (previousAsks[price] > whaleThreshold && (!currentAsks[price] || currentAsks[price] < previousAsks[price] * 0.5)) { flashScreen(); break; }
                }
                previousBids = currentBids; previousAsks = currentAsks;

                if (audioCtx && data.bids.length > 0 && data.asks.length > 0) {
                    let cBid = parseFloat(data.bids[0][0]); let cAsk = parseFloat(data.asks[0][0]);
                    if (lastBestAsk !== 0 && cAsk > lastBestAsk && parseFloat(data.asks[0][1]) > whaleThreshold) playSound('buy');
                    if (lastBestBid !== 0 && cBid < lastBestBid && parseFloat(data.bids[0][1]) > whaleThreshold) playSound('sell');
                    lastBestBid = cBid; lastBestAsk = cAsk;
                }

                // إحصائيات
                let totalBids = 0, totalAsks = 0;
                data.bids.forEach(b => totalBids += parseFloat(b[1]));
                data.asks.forEach(a => totalAsks += parseFloat(a[1]));
                
                let delta = totalBids - totalAsks;
                let deltaColor = delta > 0 ? 'var(--green)' : 'var(--red)';
                document.getElementById("volume-delta").innerHTML = `Delta: <span style="color:${deltaColor}">${delta.toFixed(2)}</span>`;

                const bestBid = parseFloat(data.bids[0][0]);
                const bestAsk = parseFloat(data.asks[0][0]);
                const spread = (bestAsk - bestBid).toFixed(3);
                const spreadPercent = ((spread / bestAsk) * 100).toFixed(3);
                document.getElementById("spread-indicator").innerHTML = `Spread<br>${spread} (${spreadPercent}%)`;

                let maxBidQty = Math.max(...data.bids.map(b => parseFloat(b[1])));
                let maxAskQty = Math.max(...data.asks.map(a => parseFloat(a[1])));
                let maxQty = Math.max(maxBidQty, maxAskQty);

                let bidsHtml = "", asksHtml = "";
                data.bids.forEach(bid => bidsHtml += createRow(bid[0], bid[1], maxQty, true));
                data.asks.forEach(ask => asksHtml += createRow(ask[0], ask[1], maxQty, false));
                
                document.getElementById("bids").innerHTML = bidsHtml;
                document.getElementById("asks").innerHTML = asksHtml;
            } catch (e) { console.error("OrderBook Error:", e); }
        }

        async function fetchTrades() {
            if (isPaused) return;
            try {
                let response = await fetch("/api/trades");
                let trades = await response.json();
                let html = "";
                
                trades.forEach((t, index) => {
                    const isBuy = !t.isBuyerMaker;
                    const colorClass = isBuy ? 'trade-buy' : 'trade-sell';
                    const price = parseFloat(t.price); const qty = parseFloat(t.qty);
                    const timeStr = new Date(t.time * 1000).toLocaleTimeString('en-US', { hour12: false });

                    let flashClass = "";
                    if (index === 0 && lastTradePrice !== 0) {
                        flashClass = price > lastTradePrice ? 'flash-green' : (price < lastTradePrice ? 'flash-red' : '');
                        lastTradePrice = price;
                    } else if (index === 0) { lastTradePrice = price; }

                    html += `<div class="trade-row ${colorClass} ${flashClass}"><span>${price.toFixed(2)}</span><span>${qty.toFixed(3)}</span><span style="color:#666;">${timeStr}</span></div>`;
                });
                document.getElementById("trades").innerHTML = html;
            } catch (e) { console.error("Trades Error:", e); }
        }

        async function fetchKlines(timeframe) {
            if (isPaused) return;
            try {
                let response = await fetch(`/api/klines?interval=${timeframe}`);
                let data = await response.json();
                renderChart(data, timeframe);
            } catch (e) { console.error("Klines Error:", e); }
        }

        function renderChart(data, timeframe) {
            const ctx = document.getElementById('priceChart').getContext('2d');
            const labels = data.map(k => new Date(k[0]).toLocaleTimeString());
            const prices = data.map(k => parseFloat(k[4]));
            if (chartInstance) chartInstance.destroy();
            chartInstance = new Chart(ctx, {
                type: 'line',
                data: { labels: labels, datasets: [{ label: `السعر`, data: prices, borderColor: '#00ffcc', backgroundColor: 'rgba(0, 255, 204, 0.05)', borderWidth: 2, fill: true, tension: 0.1, pointRadius: 0 }] },
                options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { ticks: { color: '#666', maxTicksLimit: 6 } }, y: { ticks: { color: '#666' } } } }
            });
        }

        function changeTimeframe(tf, btn) {
            document.querySelectorAll('.top-controls .btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active'); fetchKlines(tf);
        }

        fetchKlines('5m'); fetchOrderBook(); fetchTrades();
        setInterval(fetchOrderBook, 1000);
        setInterval(fetchTrades, 1000); 
    </script>
</body>
</html>
"""

@app.get("/")
async def get():
    return HTMLResponse(HTML_PAGE)

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

@app.get("/api/trades")
async def get_trades():
    global live_trades
    if len(live_trades) > 0:
        return live_trades
    url = "https://fapi.binance.com/fapi/v1/trades?symbol=XAUUSDT&limit=25"
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, timeout=5.0)
            data = response.json()
            formatted_trades = []
            for t in data:
                formatted_trades.append({"price": float(t["price"]), "qty": float(t["qty"]), "isBuyerMaker": t["isBuyerMaker"], "time": t["time"] / 1000})
            return formatted_trades[::-1]
        except Exception as e:
            return []

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
