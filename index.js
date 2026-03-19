// ═══════════════════════════════════════════════════════════
// MICEX Trade Engine v3 — Deploy trên Railway
// File: index.js
// FEATURES: 8 price sources parallel, liquidation detection 500ms,
//           instant close, pre-warm cache
// ═══════════════════════════════════════════════════════════

const express = require('express');
const app = express();
app.use(express.json());

// ── ENV VARS (set trong Railway Dashboard) ──
const BASE44_FUNCTION_URL = process.env.BASE44_FUNCTION_URL;
const RAILWAY_SECRET = process.env.RAILWAY_CLOSE_SECRET;
const PORT = process.env.PORT || 3000;

// ── IN-MEMORY ──
const tradeQueue = new Map();    // trade_id → { trade, closeHandle, preWarmHandle }
const priceCache = new Map();    // symbol → { price, time }
const closingSet = new Set();    // trade_ids currently being closed
const CACHE_TTL = 1500;          // 1.5s cache for liquidation accuracy

// ── CoinGecko ID mapping ──
const CG_IDS = {
  BTC:'bitcoin',ETH:'ethereum',BNB:'binancecoin',SOL:'solana',
  XRP:'ripple',DOGE:'dogecoin',ADA:'cardano',AVAX:'avalanche-2',
  DOT:'polkadot',LINK:'chainlink',LTC:'litecoin',APT:'aptos',
  ARB:'arbitrum',OP:'optimism',SUI:'sui',NEAR:'near',TRX:'tron',
  PEPE:'pepe',SHIB:'shiba-inu',UNI:'uniswap',ATOM:'cosmos',
  FIL:'filecoin',INJ:'injective-protocol',SEI:'sei-network',
};

// ── FETCH PRICE — 8 sources PARALLEL, first valid wins ──
async function fetchFromUrl(url, parseFn, timeoutMs = 4000) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const r = await fetch(url, { signal: controller.signal });
    clearTimeout(t);
    if (!r.ok) return 0;
    const d = await r.json();
    const p = parseFn(d);
    return (p > 0 && isFinite(p)) ? p : 0;
  } catch (_) { clearTimeout(t); return 0; }
}

async function fetchPrice(symbol) {
  const cached = priceCache.get(symbol);
  if (cached && Date.now() - cached.time < CACHE_TTL) return cached.price;

  const cgId = CG_IDS[symbol];
  const sources = [
    { url: `https://api.binance.com/api/v3/ticker/price?symbol=${symbol}USDT`, parse: d => parseFloat(d?.price) },
    { url: `https://api.bybit.com/v5/market/tickers?category=linear&symbol=${symbol}USDT`, parse: d => parseFloat(d?.result?.list?.[0]?.lastPrice) },
    { url: `https://fapi.binance.com/fapi/v1/ticker/price?symbol=${symbol}USDT`, parse: d => parseFloat(d?.price) },
    { url: `https://www.okx.com/api/v5/market/ticker?instId=${symbol}-USDT`, parse: d => parseFloat(d?.data?.[0]?.last) },
    { url: `https://api.kraken.com/0/public/Ticker?pair=${symbol}USDT`, parse: d => { const r=d?.result; if(!r) return 0; const k=Object.keys(r)[0]; return parseFloat(r[k]?.c?.[0]); } },
    { url: `https://api.mexc.com/api/v3/ticker/price?symbol=${symbol}USDT`, parse: d => parseFloat(d?.price) },
    { url: `https://api.gateio.ws/api/v4/spot/tickers?currency_pair=${symbol}_USDT`, parse: d => Array.isArray(d)&&d.length>0 ? parseFloat(d[0]?.last) : 0 },
    ...(cgId ? [{ url: `https://api.coingecko.com/api/v3/simple/price?ids=${cgId}&vs_currencies=usd`, parse: d => parseFloat(d?.[cgId]?.usd) }] : []),
  ];

  // Fire ALL in parallel — first valid price wins
  const results = await Promise.allSettled(
    sources.map(src => fetchFromUrl(src.url, src.parse))
  );

  for (const r of results) {
    if (r.status === 'fulfilled' && r.value > 0) {
      priceCache.set(symbol, { price: r.value, time: Date.now() });
      return r.value;
    }
  }
  return 0;
}

// ── PNL CALCULATION (for liquidation detection) ──
function calcPnl(side, entryPrice, currentPrice, positionSize) {
  const qty = positionSize / entryPrice;
  return side === 'long'
    ? (currentPrice - entryPrice) * qty
    : (entryPrice - currentPrice) * qty;
}

function isLiquidated(trade, currentPrice) {
  if (trade.wallet_type === 'demo') return false;
  const margin = trade.position_size / trade.leverage;
  const turbo = trade.turbo_multiplier || 1;
  const basePnl = calcPnl(trade.side, trade.entry_price, currentPrice, trade.position_size);
  const rawPnl = basePnl * turbo;
  // Check PnL-based liquidation
  if (rawPnl <= -margin * 0.8) return true;
  // Check price-based liquidation
  const liqPrice = trade.liquidation_price || 0;
  if (liqPrice > 0) {
    if (trade.side === 'long' && currentPrice <= liqPrice) return true;
    if (trade.side === 'short' && currentPrice >= liqPrice) return true;
  }
  return false;
}

// ── CLOSE TRADE VIA BASE44 ──
async function closeTrade(tradeId, symbol, reason = 'time_expired') {
  if (closingSet.has(tradeId)) return;
  closingSet.add(tradeId);

  try {
    const closePrice = await fetchPrice(symbol);
    console.log(`[CLOSE] ${reason.toUpperCase()} trade ${tradeId} (${symbol}) at ${closePrice}`);

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

    // Remove from queue
    const entry = tradeQueue.get(tradeId);
    if (entry) {
      if (entry.closeHandle) clearTimeout(entry.closeHandle);
      if (entry.preWarmHandle) clearTimeout(entry.preWarmHandle);
      tradeQueue.delete(tradeId);
    }
    closingSet.delete(tradeId);
    return data;
  } catch (err) {
    console.error(`[CLOSE ERROR] ${tradeId}: ${err.message}`);
    closingSet.delete(tradeId);
    // Retry once after 2s
    setTimeout(() => {
      closingSet.delete(tradeId);
      closeTrade(tradeId, symbol, reason).catch(() => {});
    }, 2000);
  }
}

// ── SCHEDULE TRADE CLOSE with pre-warm ──
function scheduleTrade(trade) {
  const old = tradeQueue.get(trade.id);
  if (old) {
    if (old.closeHandle) clearTimeout(old.closeHandle);
    if (old.preWarmHandle) clearTimeout(old.preWarmHandle);
  }

  const closeMs = new Date(trade.close_time).getTime();
  const delay = Math.max(closeMs - Date.now(), 500);

  // Pre-warm: fetch price 3s before close
  const preWarmDelay = Math.max(delay - 3000, 100);
  const preWarmHandle = setTimeout(() => {
    fetchPrice(trade.symbol).catch(() => {});
  }, preWarmDelay);

  const closeHandle = setTimeout(() => {
    closeTrade(trade.id, trade.symbol, 'time_expired');
  }, delay);

  // Store trade data for liquidation checking
  tradeQueue.set(trade.id, { trade, closeHandle, preWarmHandle });
}

// ═══════════════════════════════════════════════════════════
// LIQUIDATION DETECTION LOOP — runs every 500ms
// Checks all open non-demo trades against current prices
// ═══════════════════════════════════════════════════════════
let liqLoopRunning = false;

async function liquidationLoop() {
  if (liqLoopRunning) return;
  liqLoopRunning = true;

  try {
    // Collect unique symbols from active trades (non-demo only)
    const symbolsToCheck = new Set();
    for (const [id, entry] of tradeQueue) {
      if (entry.trade && entry.trade.wallet_type !== 'demo') {
        symbolsToCheck.add(entry.trade.symbol);
      }
    }
    if (symbolsToCheck.size === 0) { liqLoopRunning = false; return; }

    // Fetch all needed prices in parallel
    const priceMap = {};
    const priceResults = await Promise.allSettled(
      [...symbolsToCheck].map(async (sym) => {
        const p = await fetchPrice(sym);
        if (p > 0) priceMap[sym] = p;
      })
    );

    // Check each trade for liquidation
    for (const [tradeId, entry] of tradeQueue) {
      const trade = entry.trade;
      if (!trade || trade.wallet_type === 'demo') continue;
      if (closingSet.has(tradeId)) continue;

      const price = priceMap[trade.symbol];
      if (!price || price <= 0) continue;

      if (isLiquidated(trade, price)) {
        console.log(`[LIQUIDATION] Trade ${tradeId} ${trade.symbol} ${trade.side} at ${price} (entry: ${trade.entry_price}, liq: ${trade.liquidation_price || 'N/A'})`);
        closeTrade(tradeId, trade.symbol, 'liquidation');
      }
    }
  } catch (err) {
    console.error(`[LIQ-LOOP ERROR] ${err.message}`);
  }

  liqLoopRunning = false;
}

// ── SYNC: Load all open trades from Base44 ──
async function syncOpenTrades() {
  try {
    const res = await fetch(BASE44_FUNCTION_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ secret: RAILWAY_SECRET, action: 'get_open_trades' }),
    });
    const data = await res.json();
    const trades = data.trades || [];
    
    // Clear old queue (preserve closing trades)
    for (const [id, entry] of tradeQueue) {
      if (!closingSet.has(id)) {
        if (entry.closeHandle) clearTimeout(entry.closeHandle);
        if (entry.preWarmHandle) clearTimeout(entry.preWarmHandle);
      }
    }
    tradeQueue.clear();
    
    let scheduled = 0;
    for (const trade of trades) {
      if (trade.close_time && !closingSet.has(trade.id)) {
        scheduleTrade(trade);
        scheduled++;
      }
    }
    console.log(`[SYNC] ${scheduled} trades scheduled (queue: ${tradeQueue.size})`);
  } catch (err) {
    console.error(`[SYNC ERROR] ${err.message}`);
  }
}

// ── API: Notify new trade ──
app.post('/notify-new-trade', (req, res) => {
  const { secret, trade } = req.body;
  if (secret !== RAILWAY_SECRET) return res.status(401).json({ error: 'Unauthorized' });
  if (!trade || !trade.id) return res.status(400).json({ error: 'Missing trade' });
  
  scheduleTrade(trade);
  res.json({ ok: true, trade_id: trade.id, queue_size: tradeQueue.size });
});

// ── API: Health check ──
app.get('/health', (req, res) => {
  const nonDemo = [...tradeQueue.values()].filter(e => e.trade?.wallet_type !== 'demo').length;
  res.json({
    status: 'ok',
    queue_size: tradeQueue.size,
    non_demo_trades: nonDemo,
    closing: closingSet.size,
    uptime: Math.floor(process.uptime()),
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
  console.log(`[RAILWAY] Trade Engine v3 running on port ${PORT}`);
  console.log('[RAILWAY] Features: 8 price sources, liquidation detection 500ms');
  
  // Initial sync
  syncOpenTrades();
  
  // Re-sync every 15 seconds
  setInterval(syncOpenTrades, 15000);
  
  // LIQUIDATION LOOP — check every 500ms for instant detection
  setInterval(liquidationLoop, 500);
  
  // Keep price cache warm for top coins every 5s
  setInterval(() => {
    ['BTC','ETH','BNB','SOL','XRP','DOGE','ADA','AVAX'].forEach(s =>
      fetchPrice(s).catch(() => {})
    );
  }, 5000);
});
