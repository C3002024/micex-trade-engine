const express = require('express');
const app = express();
app.use(express.json());

const BASE44_FUNCTION_URL = process.env.BASE44_FUNCTION_URL;
const RAILWAY_SECRET = process.env.RAILWAY_CLOSE_SECRET;
const PORT = process.env.PORT || 3000;

const tradeQueue = new Map();

async function fetchPrice(symbol) {
  try {
    const r = await fetch(`https://api.bybit.com/v5/market/tickers?category=linear&symbol=${symbol}USDT`);
    if (r.ok) { const d = await r.json(); const p = parseFloat(d?.result?.list?.[0]?.lastPrice); if (p > 0) return p; }
  } catch (_) {}
  try {
    const r = await fetch(`https://api.binance.com/api/v3/ticker/price?symbol=${symbol}USDT`);
    if (r.ok) { const d = await r.json(); const p = parseFloat(d?.price); if (p > 0) return p; }
  } catch (_) {}
  return 0;
}

async function closeTrade(tradeId, symbol) {
  try {
    const closePrice = await fetchPrice(symbol);
    console.log(`[CLOSE] Closing trade ${tradeId} at price ${closePrice}`);
    const res = await fetch(BASE44_FUNCTION_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ secret: RAILWAY_SECRET, action: 'close', trade_id: tradeId, close_price: closePrice }),
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

function scheduleTrade(trade) {
  if (tradeQueue.has(trade.id)) clearTimeout(tradeQueue.get(trade.id));
  const delay = Math.max(new Date(trade.close_time).getTime() - Date.now(), 500);
  console.log(`[SCHEDULE] Trade ${trade.id} (${trade.symbol}) closes in ${(delay/1000).toFixed(1)}s`);
  const handle = setTimeout(() => closeTrade(trade.id, trade.symbol), delay);
  tradeQueue.set(trade.id, handle);
}

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
    for (const [id, handle] of tradeQueue) clearTimeout(handle);
    tradeQueue.clear();
    let scheduled = 0;
    for (const trade of trades) {
      if (trade.close_time) { scheduleTrade(trade); scheduled++; }
    }
    console.log(`[SYNC] Scheduled ${scheduled} trades (total open: ${trades.length})`);
  } catch (err) {
    console.error(`[SYNC ERROR] ${err.message}`);
  }
}

app.post('/notify-new-trade', (req, res) => {
  const { secret, trade } = req.body;
  if (secret !== RAILWAY_SECRET) return res.status(401).json({ error: 'Unauthorized' });
  if (!trade || !trade.id) return res.status(400).json({ error: 'Missing trade' });
  scheduleTrade(trade);
  res.json({ ok: true, trade_id: trade.id, queue_size: tradeQueue.size });
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok', queue_size: tradeQueue.size, uptime: process.uptime(), time: new Date().toISOString() });
});

app.post('/sync', async (req, res) => {
  const { secret } = req.body;
  if (secret !== RAILWAY_SECRET) return res.status(401).json({ error: 'Unauthorized' });
  await syncOpenTrades();
  res.json({ ok: true, queue_size: tradeQueue.size });
});

app.listen(PORT, () => {
  console.log(`[RAILWAY] Trade Engine running on port ${PORT}`);
  syncOpenTrades();
  setInterval(syncOpenTrades, 120000);
});
