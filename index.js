// index.js — MICEX Trade Engine V3 (Railway)
// Architecture: Railway = scheduler + price feed + TP/SL/Liq detection
// Closing trades: calls railwayCloseTrade Base44 function (NOT direct entity API)
// Price feed: REST polling from multiple sources (Binance WS geo-blocked on Railway)
//
// Railway Variables needed:
//   REDIS_URL              — auto from Railway Redis addon
//   BASE44_FUNCTION_URL    — https://xxx.base44.app/api/functions/railwayCloseTrade
//   RAILWAY_CLOSE_SECRET   — shared secret (same as Base44 secret)
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const Redis = require('ioredis');
const { Queue, Worker } = require('bullmq');
const axios = require('axios');

// ═══════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════
const PORT = process.env.PORT || 4000;
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const FUNCTION_URL = process.env.BASE44_FUNCTION_URL || '';
const SECRET = process.env.RAILWAY_CLOSE_SECRET || '';

if (!FUNCTION_URL) { console.error('[FATAL] BASE44_FUNCTION_URL not set!'); process.exit(1); }
if (!SECRET) { console.error('[FATAL] RAILWAY_CLOSE_SECRET not set!'); process.exit(1); }
console.log('[CONFIG] Function URL:', FUNCTION_URL);

// ═══════════════════════════════════════
// REDIS
// ═══════════════════════════════════════
const redis = new Redis(REDIS_URL, { maxRetriesPerRequest: null, enableReadyCheck: false });
redis.on('connect', () => console.log('[REDIS] Connected'));
redis.on('error', (err) => console.error('[REDIS] Error:', err.message));

// ═══════════════════════════════════════
// BASE44 FUNCTION CLIENT
// All DB operations go through railwayCloseTrade function
// ═══════════════════════════════════════
async function callBase44(action, payload = {}) {
  const res = await axios.post(FUNCTION_URL, { secret: SECRET, action, ...payload }, { timeout: 15000 });
  return res.data;
}

// ═══════════════════════════════════════
// PRICE SERVICE — REST polling (WS geo-blocked on Railway)
// ═══════════════════════════════════════
const SYMBOLS = ['BTC','ETH','BNB','SOL','XRP','ADA','DOGE','AVAX','DOT','LINK','MATIC','UNI','ATOM','FIL','LTC','APT','ARB','OP','NEAR','SUI'];
const prices = {};

async function fetchPrices() {
  // Try multiple sources in parallel
  const sources = [
    // Binance spot (may be geo-blocked)
    axios.get('https://api.binance.com/api/v3/ticker/price', { timeout: 5000 })
      .then(r => { for (const item of r.data) { const sym = item.symbol.replace('USDT',''); if (SYMBOLS.includes(sym) && parseFloat(item.price) > 0) prices[sym] = parseFloat(item.price); } return 'binance'; })
      .catch(() => null),
    // Bybit
    axios.get('https://api.bybit.com/v5/market/tickers?category=linear', { timeout: 5000 })
      .then(r => { for (const item of (r.data?.result?.list || [])) { const sym = item.symbol.replace('USDT',''); if (SYMBOLS.includes(sym) && parseFloat(item.lastPrice) > 0 && !prices[sym]) prices[sym] = parseFloat(item.lastPrice); } return 'bybit'; })
      .catch(() => null),
    // OKX
    axios.get('https://www.okx.com/api/v5/market/tickers?instType=SPOT', { timeout: 5000 })
      .then(r => { for (const item of (r.data?.data || [])) { const sym = item.instId?.replace('-USDT',''); if (sym && SYMBOLS.includes(sym) && parseFloat(item.last) > 0 && !prices[sym]) prices[sym] = parseFloat(item.last); } return 'okx'; })
      .catch(() => null),
  ];
  const results = await Promise.allSettled(sources);
  const loaded = results.filter(r => r.status === 'fulfilled' && r.value).map(r => r.value);
  
  // Cache in Redis
  for (const [sym, price] of Object.entries(prices)) {
    redis.set(`price:${sym}`, JSON.stringify({ symbol: sym, price, ts: Date.now() }), 'EX', 30).catch(() => {});
  }
  
  console.log(`[PRICE] ${Object.keys(prices).length} prices from [${loaded.join(',')}]`);
}

function getPrice(symbol) { return prices[symbol] || 0; }

// Poll prices every 2s
function startPricePolling() {
  fetchPrices();
  setInterval(fetchPrices, 2000);
  console.log('[PRICE] REST polling started (2s interval)');
}

// ═══════════════════════════════════════
// TRADE QUEUE (BullMQ)
// ═══════════════════════════════════════
const tradeQueue = new Queue('trade-close', {
  connection: redis,
  defaultJobOptions: { removeOnComplete: true, removeOnFail: 50, attempts: 3, backoff: { type: 'exponential', delay: 2000 } },
});

async function scheduleClose(tradeId, delayMs) {
  const jobId = `close-${tradeId}`;
  try { const existing = await tradeQueue.getJob(jobId); if (existing) await existing.remove(); } catch (_) {}
  await tradeQueue.add('close-trade', { tradeId }, { delay: Math.max(0, delayMs), jobId });
  console.log(`[QUEUE] Scheduled ${tradeId} in ${(delayMs/1000).toFixed(1)}s`);
}

async function cancelClose(tradeId) {
  try { const job = await tradeQueue.getJob(`close-${tradeId}`); if (job) { await job.remove(); } } catch (_) {}
}

// ═══════════════════════════════════════
// CLOSE TRADE — delegates to railwayCloseTrade function
// ═══════════════════════════════════════
async function closeTradeById(tradeId) {
  const lockKey = `lock:close:${tradeId}`;
  const locked = await redis.set(lockKey, '1', 'NX', 'EX', 30);
  if (!locked) return { success: false, reason: 'lock_held' };
  try {
    const closePrice = getPrice('BTC'); // dummy — function fetches its own price
    const result = await callBase44('close', { trade_id: tradeId, close_price: closePrice });
    if (result.skipped) return { success: false, reason: result.reason || 'skipped' };
    console.log(`[CLOSE] ${tradeId} pnl=${result.pnl_final?.toFixed(2)} reason=${result.close_reason}`);
    return { success: true, ...result };
  } catch (e) {
    console.error(`[CLOSE ERROR] ${tradeId}: ${e.message}`);
    return { success: false, reason: e.message };
  } finally { await redis.del(lockKey); }
}

async function closeTradeByTpSl(tradeId, closePrice, reason) {
  const lockKey = `lock:close:${tradeId}`;
  const locked = await redis.set(lockKey, '1', 'NX', 'EX', 30);
  if (!locked) return { success: false, reason: 'lock_held' };
  try {
    const result = await callBase44('close', { trade_id: tradeId, close_price: closePrice, close_reason: reason });
    if (result.skipped) return { success: false, reason: result.reason || 'skipped' };
    console.log(`[TPSL] ${tradeId} ${reason} pnl=${result.pnl_final?.toFixed(2)}`);
    await cancelClose(tradeId);
    return { success: true, ...result };
  } catch (e) {
    console.error(`[TPSL ERROR] ${tradeId}: ${e.message}`);
    return { success: false, reason: e.message };
  } finally { await redis.del(lockKey); }
}

// ═══════════════════════════════════════
// TP/SL + LIQUIDATION SERVICE (3s interval)
// Fetches open trades from Base44, checks against live prices
// ═══════════════════════════════════════
let openTradesCache = [];
let lastTradesFetch = 0;

async function refreshOpenTrades() {
  try {
    const result = await callBase44('get_open_trades');
    openTradesCache = result.trades || [];
    lastTradesFetch = Date.now();
  } catch (e) {
    console.error('[TRADES] Refresh failed:', e.message);
  }
}

function startMonitorService() {
  // Refresh open trades every 5s
  setInterval(refreshOpenTrades, 5000);
  
  // Check TP/SL/Liquidation every 3s
  setInterval(async () => {
    if (openTradesCache.length === 0) return;
    
    for (const trade of openTradesCache) {
      const price = getPrice(trade.symbol);
      if (!price || price <= 0) continue;
      
      const isDemo = trade.wallet_type === 'demo';
      const turbo = trade.turbo_multiplier || 1;
      
      // TP check
      if (trade.take_profit > 0) {
        const tpHit = trade.side === 'long' ? price >= trade.take_profit : price <= trade.take_profit;
        if (tpHit) {
          console.log(`[TP] ${trade.symbol} price=${price} tp=${trade.take_profit}`);
          await closeTradeByTpSl(trade.id, price, 'take_profit');
          openTradesCache = openTradesCache.filter(t => t.id !== trade.id);
          continue;
        }
      }
      
      // SL check
      if (trade.stop_loss > 0) {
        const slHit = trade.side === 'long' ? price <= trade.stop_loss : price >= trade.stop_loss;
        if (slHit) {
          console.log(`[SL] ${trade.symbol} price=${price} sl=${trade.stop_loss}`);
          await closeTradeByTpSl(trade.id, price, 'stop_loss');
          openTradesCache = openTradesCache.filter(t => t.id !== trade.id);
          continue;
        }
      }
      
      // Liquidation check (skip demo)
      if (isDemo) continue;
      const liqFactor = turbo >= 5 ? 0.55 : turbo >= 3 ? 0.65 : 0.8;
      const liq = trade.liquidation_price || (trade.side === 'long'
        ? trade.entry_price * (1 - liqFactor / trade.leverage)
        : trade.entry_price * (1 + liqFactor / trade.leverage));
      const isLiq = trade.side === 'long' ? price <= liq : price >= liq;
      if (isLiq) {
        console.log(`[LIQ] ${trade.symbol} price=${price} liq=${liq.toFixed(2)} turbo=x${turbo}`);
        await closeTradeByTpSl(trade.id, price, 'liquidation');
        openTradesCache = openTradesCache.filter(t => t.id !== trade.id);
      }
    }
  }, 3000);
  
  console.log('[MONITOR] TP/SL + Liquidation service started (3s)');
}

// ═══════════════════════════════════════
// LIMIT ORDER CHECK (piggyback on price polling)
// ═══════════════════════════════════════
function startLimitOrderCheck() {
  setInterval(async () => {
    if (Object.keys(prices).length === 0) return;
    try {
      const result = await callBase44('check_limits', { prices });
      if (result.filled > 0 || result.expired > 0) {
        console.log(`[LIMITS] checked=${result.checked} filled=${result.filled} expired=${result.expired}`);
        // Refresh open trades cache since new trades may have been created
        await refreshOpenTrades();
      }
    } catch (e) {
      if (!e.message?.includes('timeout')) console.error('[LIMITS]', e.message);
    }
  }, 5000);
  console.log('[LIMITS] Limit order checker started (5s)');
}

// ═══════════════════════════════════════
// RECOVERY SERVICE — schedule close jobs for open trades
// ═══════════════════════════════════════
async function recoverOpenTrades() {
  console.log('[RECOVERY] Scanning...');
  try {
    await refreshOpenTrades();
    const now = Date.now();
    let scheduled = 0, immediate = 0;
    for (const trade of openTradesCache) {
      const delay = new Date(trade.close_time).getTime() - now;
      if (delay <= 0) { await scheduleClose(trade.id, 0); immediate++; }
      else { await scheduleClose(trade.id, delay); scheduled++; }
    }
    console.log(`[RECOVERY] ${openTradesCache.length} trades: ${scheduled} scheduled, ${immediate} immediate`);
  } catch (e) { console.error('[RECOVERY]', e.message); }
}

// ═══════════════════════════════════════
// QUEUE WORKER — processes scheduled close jobs
// ═══════════════════════════════════════
function startWorker() {
  const worker = new Worker('trade-close', async (job) => {
    const { tradeId } = job.data;
    console.log(`[QUEUE] Processing: ${tradeId}`);
    const result = await closeTradeById(tradeId);
    if (!result.success && result.reason === 'no_price') throw new Error('No price — will retry');
    return result;
  }, { connection: redis, concurrency: 10, limiter: { max: 50, duration: 1000 } });

  worker.on('completed', (job, result) => { if (result?.success) console.log(`[QUEUE] ✓ ${job.data.tradeId}`); });
  worker.on('failed', (job, err) => { console.error(`[QUEUE] ✗ ${job?.data?.tradeId}: ${err.message}`); });
  console.log('[QUEUE] Worker started');
}

// ═══════════════════════════════════════
// EXPRESS SERVER
// ═══════════════════════════════════════
const app = express();
app.use(cors());
app.use(express.json());

// Notify endpoint — Base44 tradeProxy calls this after creating a trade
app.post('/notify-new-trade', async (req, res) => {
  try {
    const { secret, trade } = req.body;
    if (secret !== SECRET) return res.status(403).json({ error: 'Forbidden' });
    if (!trade?.id || !trade?.close_time) return res.status(400).json({ error: 'Missing data' });
    const delay = Math.max(0, new Date(trade.close_time).getTime() - Date.now());
    await scheduleClose(trade.id, delay);
    // Also add to local cache for immediate TP/SL monitoring
    if (!openTradesCache.find(t => t.id === trade.id)) {
      openTradesCache.push(trade);
    }
    console.log(`[NOTIFY] ${trade.id} (${trade.symbol} ${trade.side} mode=${trade.trade_mode||'quick'}) close in ${(delay/1000).toFixed(1)}s`);
    return res.json({ success: true, trade_id: trade.id, delay_ms: delay });
  } catch (e) { return res.status(500).json({ error: e.message }); }
});

app.get('/health', (req, res) => {
  res.json({
    status: 'ok', uptime: process.uptime(),
    prices: Object.keys(prices).length,
    open_trades: openTradesCache.length,
    timestamp: Date.now(),
  });
});

// ═══════════════════════════════════════
// START
// ═══════════════════════════════════════
app.listen(PORT, async () => {
  console.log(`[SERVER] Running on port ${PORT}`);
  
  // Test connection to Base44 function
  try {
    const health = await callBase44('health');
    console.log('[API] Base44 function OK ✓', health);
  } catch (e) {
    console.error('[API] Base44 function FAILED:', e.response?.status || e.message);
    console.error('[API] Check BASE44_FUNCTION_URL and RAILWAY_CLOSE_SECRET');
  }
  
  startPricePolling();
  startWorker();
  await recoverOpenTrades();
  startMonitorService();
  startLimitOrderCheck();
  console.log('[SERVER] All services ready ✓');
});
