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
    <title>XAUUSDT Pro Dashboard</title>
    <style>
        body { background-color: #121212; color: #e0e0e0; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; text-align: center; margin: 0; padding: 15px; }
        h1 { color: #00ffcc; margin: 0 0 10px 0; font-size: 24px; }
        h3 { margin: 5px 0; }
        
        /* تحكم علوي */
        .top-controls { display: flex; justify-content: center; align-items: center; gap: 15px; margin-bottom: 15px; }
        .btn { background: #333; color: #fff; border: 1px solid #00ffcc; padding: 6px 12px; cursor: pointer; border-radius: 4px; font-weight: bold;}
        .btn:hover { background: #00ffcc; color: #000; }
        .btn-danger { border-color: #ff5555; }
        .btn-danger:hover { background: #ff5555; color: #fff; }
        
        /* شريط الإحصائيات */
        .stats-bar { background: #1e1e1e; padding: 8px; border-radius: 6px; display: inline-flex; gap: 20px; font-size: 13px; font-weight: bold; border: 1px solid #333; margin-bottom: 15px;}
        .stat-buy { color: #55ff55; }
        .stat-sell { color: #ff5555; }

        /* الشارت */
        #chart-container { width: 70%; max-width: 700px; height: 250px; margin: 0 auto 15px auto; background: #1e1e1e; padding: 10px; border-radius: 8px; }
        
        /* الحاوية الرئيسية */
        .container { display: flex; justify-content: center; gap: 15px; flex-wrap: wrap; margin-top: 10px; }
        .panel { width: 280px; background: #1e1e1e; padding: 10px; border-radius: 8px; }
        
        /* تنسيق الصفوف */
        .row { display: flex; justify-content: space-between; padding: 4px 8px; border-bottom: 1px solid #2a2a2a; font-size: 13px; position: relative; }
        .bg-bar { position: absolute; top: 0; bottom: 0; right: 0; z-index: 1; opacity: 0.15; transition: width 0.2s; }
        .text-content { z-index: 2; display: flex; justify-content: space-between; width: 100%; }
        .asks-color { color: #ff5555; }
        .bids-color { color: #55ff55; }

        /* الفارق */
        .spread-center { margin: 5px 0; padding: 8px; background: #222; border-left: 3px solid #00ffcc; border-right: 3px solid #00ffcc; font-weight: bold; font-size: 14px; color: #fff; }

        /* لوحة التداولات */
        .trade-row { display: flex; justify-content: space-between; padding: 3px 5px; font-size: 12px; border-bottom: 1px solid #2a2a2a; }
        .trade-buy { color: #55ff55; }
        .trade-sell { color: #ff5555; }
        
        /* تأثير وميض السعر */
        .flash-green { animation: flashGreen 0.5s; }
        .flash-red { animation: flashRed 0.5s; }
        @keyframes flashGreen { 0% { background-color: rgba(85, 255, 85, 0.5); } 100% { background-color: transparent; } }
        @keyframes flashRed { 0% { background-color: rgba(255, 85, 85, 0.5); } 100% { background-color: transparent; } }
    </style>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
    <h1>XAUUSDT Pro Dashboard</h1>
    
    <div class="top-controls">
        <button id="pause-btn" class="btn" onclick="togglePause()">⏸ إيقاف التحديث</button>
        <button class="btn" onclick="enableAudio()" id="audio-btn">🔔 تفعيل التنبيهات الصوتية</button>
    </div>

    <div class="stats-bar" id="stats-bar">
        <span>إجمالي الشراء: <span class="stat-buy" id="total-bids">0</span></span>
        <span>إجمالي البيع: <span class="stat-sell" id="total-asks">0</span></span>
    </div>

    <div style="margin-bottom: 10px;">
        <button onclick="changeTimeframe('5m', this)" class="btn active">5 دقائق</button>
        <button onclick="changeTimeframe('1h', this)" class="btn">ساعة</button>
        <button onclick="changeTimeframe('4h', this)" class="btn">4 ساعات</button>
    </div>

    <div id="chart-container">
        <canvas id="priceChart"></canvas>
    </div>

    <div class="container">
        <div class="panel">
            <h3>Bids (Buyers)</h3>
            <div id="bids"></div>
        </div>
        
        <div style="display: flex; flex-direction: column; justify-content: center;">
            <div class="spread-center" id="spread-indicator">Spread: --</div>
        </div>

        <div class="panel">
            <h3>Asks (Sellers)</h3>
            <div id="asks"></div>
        </div>

        <div class="panel trades-panel">
            <h3>Recent Trades</h3>
            <div id="trades"></div>
        </div>
    </div>

    <script>
        let chartInstance = null;
        let lastTradePrice = 0;
        let isPaused = false;
        let audioCtx = null;
        
        let lastBestBid = 0;
        let lastBestAsk = 0;
        let wallThreshold = 50;

        function playSound(type) {
            if (!audioCtx) return;
            const osc = audioCtx.createOscillator();
            const gain = audioCtx.createGain();
            osc.connect(gain);
            gain.connect(audioCtx.destination);
            
            if (type === 'buy') {
                osc.frequency.setValueAtTime(600, audioCtx.currentTime);
                osc.frequency.exponentialRampToValueAtTime(900, audioCtx.currentTime + 0.1);
            } else if (type === 'sell') {
                osc.frequency.setValueAtTime(600, audioCtx.currentTime);
                osc.frequency.exponentialRampToValueAtTime(300, audioCtx.currentTime + 0.1);
            }
            gain.gain.setValueAtTime(0.1, audioCtx.currentTime);
            gain.gain.exponentialRampToValueAtTime(0.01, audioCtx.currentTime + 0.2);
            osc.start();
            osc.stop(audioCtx.currentTime + 0.2);
        }

        function enableAudio() {
            if (!audioCtx) {
                audioCtx = new (window.AudioContext || window.webkitAudioContext)();
                document.getElementById('audio-btn').innerText = "🔔 التنبيهات مفعلة";
                document.getElementById('audio-btn').classList.add('btn-danger');
                playSound('buy');
            }
        }

        function togglePause() {
            isPaused = !isPaused;
            const btn = document.getElementById('pause-btn');
            btn.innerText = isPaused ? "▶ استئناف التحديث" : "⏸ إيقاف التحديث";
            btn.classList.toggle('btn-danger');
        }

        function createRow(price, qty, maxQty, isBid) {
            const qtyNum = parseFloat(qty);
            const widthPercent = Math.min((qtyNum / maxQty) * 100, 100);
            const colorClass = isBid ? 'bids-color' : 'asks-color';
            const bgColor = isBid ? '#55ff55' : '#ff5555';
            return `
                <div class="row">
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

                if (audioCtx && data.bids.length > 0 && data.asks.length > 0) {
                    let currentBestBid = parseFloat(data.bids[0][0]);
                    let currentBestAsk = parseFloat(data.asks[0][0]);
                    
                    if (lastBestAsk !== 0 && currentBestAsk > lastBestAsk) {
                        let askVolume = parseFloat(data.asks[0][1]);
                        if (askVolume > wallThreshold) playSound('buy');
                    }
                    if (lastBestBid !== 0 && currentBestBid < lastBestBid) {
                        let bidVolume = parseFloat(data.bids[0][1]);
                        if (bidVolume > wallThreshold) playSound('sell');
                    }
                    lastBestBid = currentBestBid;
                    lastBestAsk = currentBestAsk;
                }

                let totalBids = 0, totalAsks = 0;
                data.bids.forEach(b => totalBids += parseFloat(b[1]));
                data.asks.forEach(a => totalAsks += parseFloat(a[1]));
                document.getElementById("total-bids").innerText = totalBids.toFixed(2);
                document.getElementById("total-asks").innerText = totalAsks.toFixed(2);

                const bestBid = parseFloat(data.bids[0][0]);
                const bestAsk = parseFloat(data.asks[0][0]);
                const spread = (bestAsk - bestBid).toFixed(3);
                const spreadPercent = ((spread / bestAsk) * 100).toFixed(3);
                document.getElementById("spread-indicator").innerHTML = `Spread<br>${spread}<br>(${spreadPercent}%)`;

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
                    const price = parseFloat(t.price);
                    const qty = parseFloat(t.qty);
                    const timeStr = new Date(t.time * 1000).toLocaleTimeString('en-US', { hour12: false });

                    let flashClass = "";
                    if (index === 0 && lastTradePrice !== 0) {
                        flashClass = price > lastTradePrice ? 'flash-green' : (price < lastTradePrice ? 'flash-red' : '');
                        lastTradePrice = price;
                    } else if (index === 0) {
                        lastTradePrice = price;
                    }

                    html += `
                        <div class="trade-row ${colorClass} ${flashClass}">
                            <span>${price.toFixed(2)}</span>
                            <span>${qty.toFixed(3)}</span>
                            <span style="color:#888;">${timeStr}</span>
                        </div>`;
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
                data: { labels: labels, datasets: [{ label: `السعر (${timeframe})`, data: prices, borderColor: '#00ffcc', backgroundColor: 'rgba(0, 255, 204, 0.1)', borderWidth: 2, fill: true, tension: 0.1, pointRadius: 0 }] },
                options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { ticks: { color: '#aaa', maxTicksLimit: 6 } }, y: { ticks: { color: '#aaa' } } } }
            });
        }

        function changeTimeframe(tf, btn) {
            document.querySelectorAll('.btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            fetchKlines(tf);
        }

        fetchKlines('5m');
        fetchOrderBook();
        fetchTrades();
        
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
                formatted_trades.append({
                    "price": float(t["price"]),
                    "qty": float(t["qty"]),
                    "isBuyerMaker": t["isBuyerMaker"],
                    "time": t["time"] / 1000
                })
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
