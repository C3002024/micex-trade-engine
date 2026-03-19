// ═══════════════════════════════════════════════════════════
// MICEX Trade Engine v2 — Deploy trên Railway
// File: index.js
// FIX: Robust price fetching with 3 sources + retry + pre-warm
// ═══════════════════════════════════════════════════════════

const express = require('express');
const app = express();
app.use(express.json());

// ── ENV VARS (set trong Railway Dashboard) ──
const BASE44_FUNCTION_URL = process.env.BASE44_FUNCTION_URL;
const RAILWAY_SECRET = process.env.RAILWAY_CLOSE_SECRET;
const PORT = process.env.PORT || 3000;

// ── IN-MEMORY ──
const tradeQueue = new Map();    // trade_id → { closeHandle, preWarmHandle }
const priceCache = new Map();    // symbol → { price, time }
const CACHE_TTL = 3000;          // 3s cache

// ── FETCH PRICE — 3 sources with timeout + retry + cache ──
async function fetchFromUrl(url, parseFn) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), 4000);
  try {
    const r = await fetch(url, { signal: controller.signal });
    clearTimeout(t);
    if (!r.ok) return 0;
    const d = await r.json();
    const p = parseFn(d);
    return (p > 0 && isFinite(p)) ? p : 0;
  } catch (_) { clearTimeout(t); return 0; }
}

async function fetchPrice(symbol, retries = 2) {
  const cached = priceCache.get(symbol);
  if (cached && Date.now() - cached.time < CACHE_TTL) return cached.price;

  const sources = [
    { url: `https://api.binance.com/api/v3/ticker/price?symbol=${symbol}USDT`, parse: d => parseFloat(d?.price) },
    { url: `https://api.bybit.com/v5/market/tickers?category=linear&symbol=${symbol}USDT`, parse: d => parseFloat(d?.result?.list?.[0]?.lastPrice) },
    { url: `https://fapi.binance.com/fapi/v1/ticker/price?symbol=${symbol}USDT`, parse: d => parseFloat(d?.price) },
  ];

  for (const src of sources) {
    const price = await fetchFromUrl(src.url, src.parse);
    if (price > 0) {
      priceCache.set(symbol, { price, time: Date.now() });
      console.log(`[PRICE] ${symbol} = ${price} (from ${src.url.includes('bybit') ? 'Bybit' : src.url.includes('fapi') ? 'Binance Futures' : 'Binance'})`);
      return price;
    }
  }

  if (retries > 0) {
    await new Promise(r => setTimeout(r, 1000));
    console.log(`[PRICE] Retry ${retries} for ${symbol}...`);
    return fetchPrice(symbol, retries - 1);
  }
  console.error(`[PRICE] FAILED to get price for ${symbol} after all retries`);
  return 0;
}

// ── CLOSE TRADE VIA BASE44 ──
async function closeTrade(tradeId, symbol) {
  try {
    const closePrice = await fetchPrice(symbol);
    console.log(`[CLOSE] Closing trade ${tradeId} (${symbol}) at price ${closePrice}`);

    const res = await fetch(BASE44_FUNCTION_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        secret: RAILWAY_SECRET,
        action: 'close',
        trade_id: tradeId,
        close_price: closePrice,
      }),
    });
    const data = await res.json();
    console.log(`[CLOSE] Result: ${JSON.stringify(data)}`);
    tradeQueue.delete(tradeId);
    return data;
  } catch (err) {
    console.error(`[CLOSE ERROR] ${tradeId}: ${err.message}`);
    setTimeout(() => closeTrade(tradeId, symbol).catch(() => {}), 3000);
  }
}

// ── SCHEDULE TRADE CLOSE with pre-warm ──
function scheduleTrade(trade) {
  // Clear old timers
  const old = tradeQueue.get(trade.id);
  if (old) {
    clearTimeout(old.closeHandle);
    if (old.preWarmHandle) clearTimeout(old.preWarmHandle);
  }

  const closeMs = new Date(trade.close_time).getTime();
  const delay = Math.max(closeMs - Date.now(), 500);

  console.log(`[SCHEDULE] Trade ${trade.id} (${trade.symbol}) closes in ${(delay/1000).toFixed(1)}s`);

  // Pre-warm: fetch price 5s before close to warm cache
  const preWarmDelay = Math.max(delay - 5000, 100);
  const preWarmHandle = setTimeout(() => {
    fetchPrice(trade.symbol).then(p => {
      if (p > 0) console.log(`[PREWARM] ${trade.symbol} = ${p}`);
    }).catch(() => {});
  }, preWarmDelay);

  const closeHandle = setTimeout(() => {
    closeTrade(trade.id, trade.symbol);
  }, delay);

  tradeQueue.set(trade.id, { closeHandle, preWarmHandle });
}

// ── SYNC: Load all open trades from Base44 ──
async function syncOpenTrades() {
  try {
    console.log('[SYNC] Fetching open trades...');
    const res = await fetch(BASE44_FUNCTION_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ secret: RAILWAY_SECRET, action: 'get_open_trades' }),
    });
    const data = await res.json();
    const trades = data.trades || [];
    
    // Clear old queue
    for (const [id, entry] of tradeQueue) {
      clearTimeout(entry.closeHandle || entry);
      if (entry.preWarmHandle) clearTimeout(entry.preWarmHandle);
    }
    tradeQueue.clear();
    
    // Schedule all
    let scheduled = 0;
    for (const trade of trades) {
      if (trade.close_time) {
        scheduleTrade(trade);
        scheduled++;
      }
    }
    console.log(`[SYNC] Scheduled ${scheduled} trades (total open: ${trades.length})`);
  } catch (err) {
    console.error(`[SYNC ERROR] ${err.message}`);
  }
}

// ── API: Notify new trade (called by Base44 tradeProxy) ──
app.post('/notify-new-trade', (req, res) => {
  const { secret, trade } = req.body;
  if (secret !== RAILWAY_SECRET) return res.status(401).json({ error: 'Unauthorized' });
  if (!trade || !trade.id) return res.status(400).json({ error: 'Missing trade' });
  
  scheduleTrade(trade);
  res.json({ ok: true, trade_id: trade.id, queue_size: tradeQueue.size });
});

// ── API: Health check ──
app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    queue_size: tradeQueue.size,
    uptime: process.uptime(),
    time: new Date().toISOString(),
  });
});

// ── API: Force sync ──
app.post('/sync', async (req, res) => {
  const { secret } = req.body;
  if (secret !== RAILWAY_SECRET) return res.status(401).json({ error: 'Unauthorized' });
  await syncOpenTrades();
  res.json({ ok: true, queue_size: tradeQueue.size });
});

// ── START ──
app.listen(PORT, () => {
  console.log(`[RAILWAY] Trade Engine running on port ${PORT}`);
  // Initial sync
  syncOpenTrades();
  // Re-sync every 20 seconds as safety net
  setInterval(syncOpenTrades, 20000);
  // Keep price cache warm for common symbols
  setInterval(() => {
    ['BTC', 'ETH', 'BNB', 'SOL', 'XRP'].forEach(s => fetchPrice(s).catch(() => {}));
  }, 10000);
});
