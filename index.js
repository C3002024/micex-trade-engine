// index.js — MICEX Trade Engine V5 (Railway) — CROSS DETECTION
// Architecture: Railway = scheduler + price feed + TP/SL/Liq detection
// Closing trades: calls railwayCloseTrade Base44 function (NOT direct entity API)
// Price feed: REST polling 500ms from multiple sources
// TP/SL/Liq check: 100ms loop (RAM-only, zero network)
// Limit order check: 500ms loop with CROSS DETECTION (prev_prices tracking)
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
const BASE44_SERVICE_TOKEN = process.env.BASE44_SERVICE_TOKEN || '';

// Timing config (ms)
const PRICE_POLL_MS = 500;       // REST price fetch interval
const MONITOR_MS = 100;          // TP/SL/Liquidation check (RAM-only)
const LIMIT_CHECK_MS = 2000;     // Limit order check with cross detection
const TRADES_REFRESH_MS = 3000;  // Open trades refresh from DB

if (!FUNCTION_URL) { console.error('[FATAL] BASE44_FUNCTION_URL not set!'); process.exit(1); }
if (!SECRET) { console.error('[FATAL] RAILWAY_CLOSE_SECRET not set!'); process.exit(1); }
if (!BASE44_SERVICE_TOKEN) { console.error('[FATAL] BASE44_SERVICE_TOKEN not set!'); process.exit(1); }
console.log('[CONFIG] Function URL:', FUNCTION_URL);
console.log('[CONFIG] Intervals: price=' + PRICE_POLL_MS + 'ms monitor=' + MONITOR_MS + 'ms limits=' + LIMIT_CHECK_MS + 'ms');

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
  const res = await axios.post(FUNCTION_URL, { secret: SECRET, action, ...payload }, {
    timeout: 15000,
    headers: { 'Authorization': `Bearer ${BASE44_SERVICE_TOKEN}` },
  });
  return res.data;
}

// ═══════════════════════════════════════
// PRICE SERVICE — REST polling 500ms
// ═══════════════════════════════════════
const SYMBOLS = ['BTC','ETH','BNB','SOL','XRP','ADA','DOGE','AVAX','DOT','LINK','MATIC','UNI','ATOM','FIL','LTC','APT','ARB','OP','NEAR','SUI'];
const prices = {};
const prevPrices = {};  // Previous tick prices for cross detection
let priceLogCounter = 0;
let priceFetching = false;

async function fetchPrices() {
  if (priceFetching) return; // skip if previous fetch still running
  priceFetching = true;
  try {
    // Save previous prices BEFORE updating (for cross detection)
    for (const sym of SYMBOLS) {
      if (prices[sym] > 0) prevPrices[sym] = prices[sym];
    }
    
    const newPrices = {};
    const sources = [
      axios.get('https://api.binance.com/api/v3/ticker/price', { timeout: 3000 })
        .then(r => { for (const item of r.data) { const sym = item.symbol.replace('USDT',''); if (SYMBOLS.includes(sym) && parseFloat(item.price) > 0) newPrices[sym] = parseFloat(item.price); } return 'binance'; })
        .catch(() => null),
      axios.get('https://api.bybit.com/v5/market/tickers?category=linear', { timeout: 3000 })
        .then(r => { for (const item of (r.data?.result?.list || [])) { const sym = item.symbol.replace('USDT',''); if (SYMBOLS.includes(sym) && parseFloat(item.lastPrice) > 0 && !newPrices[sym]) newPrices[sym] = parseFloat(item.lastPrice); } return 'bybit'; })
        .catch(() => null),
      axios.get('https://www.okx.com/api/v5/market/tickers?instType=SPOT', { timeout: 3000 })
        .then(r => { for (const item of (r.data?.data || [])) { const sym = item.instId?.replace('-USDT',''); if (sym && SYMBOLS.includes(sym) && parseFloat(item.last) > 0 && !newPrices[sym]) newPrices[sym] = parseFloat(item.last); } return 'okx'; })
        .catch(() => null),
    ];
    await Promise.allSettled(sources);
    // Update prices atomically
    for (const [sym, price] of Object.entries(newPrices)) { prices[sym] = price; }
    // Log only every 20th fetch (~10s) to avoid spam
    if (++priceLogCounter % 20 === 0) {
      console.log(`[PRICE] ${Object.keys(prices).length} symbols | BTC=${prices.BTC || '?'} ETH=${prices.ETH || '?'}`);
    }
  } finally { priceFetching = false; }
}

function getPrice(symbol) { return prices[symbol] || 0; }

function startPricePolling() {
  fetchPrices();
  setInterval(fetchPrices, PRICE_POLL_MS);
  console.log('[PRICE] REST polling started (' + PRICE_POLL_MS + 'ms interval)');
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
    if (result.skipped) {
      // If trade not expired yet, re-schedule with correct delay
      if (result.reason === 'not_expired_yet' && result.remaining_seconds > 0) {
        const reDelay = result.remaining_seconds * 1000;
        console.log(`[CLOSE] ${tradeId} not expired — re-scheduling in ${result.remaining_seconds}s`);
        await scheduleClose(tradeId, reDelay);
      }
      return { success: false, reason: result.reason || 'skipped' };
    }
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
// TP/SL + LIQUIDATION SERVICE (100ms interval — RAM only)
// Uses CROSS DETECTION: tracks prev price per trade to catch wicks
// SL has priority over TP when both triggered simultaneously
// ═══════════════════════════════════════
let openTradesCache = [];
let lastTradesFetch = 0;
const closingSet = new Set(); // prevent duplicate close calls
const tradePrevPrices = {};   // per-trade previous prices for cross detection

async function refreshOpenTrades() {
  try {
    const result = await callBase44('get_open_trades');
    openTradesCache = result.trades || [];
    lastTradesFetch = Date.now();
    // Clean up closingSet and tradePrevPrices — remove IDs no longer in open trades
    const activeIds = new Set(openTradesCache.map(t => t.id));
    for (const id of closingSet) { if (!activeIds.has(id)) closingSet.delete(id); }
    for (const id of Object.keys(tradePrevPrices)) { if (!activeIds.has(id)) delete tradePrevPrices[id]; }
  } catch (e) {
    console.error('[TRADES] Refresh failed:', e.message);
  }
}

function startMonitorService() {
  // Refresh open trades from DB
  setInterval(refreshOpenTrades, TRADES_REFRESH_MS);
  
  // High-frequency TP/SL/Liquidation check — 100ms (pure RAM, no network)
  // CROSS DETECTION: compare prev price vs current price per trade
  setInterval(() => {
    if (openTradesCache.length === 0) return;
    
    for (const trade of openTradesCache) {
      if (closingSet.has(trade.id)) continue;
      const price = getPrice(trade.symbol);
      if (!price || price <= 0) continue;
      
      const prev = tradePrevPrices[trade.id] || 0;
      tradePrevPrices[trade.id] = price; // save for next tick
      if (prev <= 0) continue; // need at least 2 ticks
      
      const isDemo = trade.wallet_type === 'demo';
      const turbo = trade.turbo_multiplier || 1;
      let closeReason = null;
      let closePrice = 0;
      
      // SL check FIRST (priority over TP)
      if (trade.stop_loss > 0) {
        const slCross = trade.side === 'long'
          ? (prev > trade.stop_loss && price <= trade.stop_loss)   // crossed down
          : (prev < trade.stop_loss && price >= trade.stop_loss);  // crossed up
        const slStd = trade.side === 'long' ? price <= trade.stop_loss : price >= trade.stop_loss;
        if (slCross || slStd) {
          closeReason = 'stop_loss';
          closePrice = trade.stop_loss; // close at exact SL price
        }
      }
      
      // TP check (only if SL not triggered)
      if (!closeReason && trade.take_profit > 0) {
        const tpCross = trade.side === 'long'
          ? (prev < trade.take_profit && price >= trade.take_profit)  // crossed up
          : (prev > trade.take_profit && price <= trade.take_profit); // crossed down
        const tpStd = trade.side === 'long' ? price >= trade.take_profit : price <= trade.take_profit;
        if (tpCross || tpStd) {
          closeReason = 'take_profit';
          closePrice = trade.take_profit; // close at exact TP price
        }
      }
      
      // Liquidation check (skip demo, only if no TP/SL triggered)
      if (!closeReason && !isDemo) {
        const liqFactor = turbo >= 5 ? 0.55 : turbo >= 3 ? 0.65 : 0.8;
        const liq = trade.liquidation_price || (trade.side === 'long'
          ? trade.entry_price * (1 - liqFactor / trade.leverage)
          : trade.entry_price * (1 + liqFactor / trade.leverage));
        const liqCross = trade.side === 'long'
          ? (prev > liq && price <= liq)
          : (prev < liq && price >= liq);
        const liqStd = trade.side === 'long' ? price <= liq : price >= liq;
        if (liqCross || liqStd) {
          closeReason = 'liquidation';
          closePrice = price; // liquidation uses market price
        }
      }
      
      if (closeReason) {
        closingSet.add(trade.id);
        console.log(`[${closeReason.toUpperCase()}] ${trade.symbol} prev=${prev} price=${price} close_at=${closePrice} ${trade.side} tp=${trade.take_profit||'-'} sl=${trade.stop_loss||'-'}`);
        closeTradeByTpSl(trade.id, closePrice, closeReason)
          .then(() => { openTradesCache = openTradesCache.filter(t => t.id !== trade.id); delete tradePrevPrices[trade.id]; })
          .catch(() => { closingSet.delete(trade.id); });
      }
    }
  }, MONITOR_MS);
  
  console.log('[MONITOR] TP/SL + Liquidation service started (' + MONITOR_MS + 'ms) with cross detection');
}

// ═══════════════════════════════════════
// LIMIT ORDER CHECK (2s interval with cross detection + error backoff)
// Sends both current prices AND previous prices to Base44
// so the function can detect price crossing through trigger levels
// ═══════════════════════════════════════
let limitChecking = false;
let limitErrorCount = 0;
function startLimitOrderCheck() {
  setInterval(async () => {
    if (limitChecking) return; // skip if previous check still running
    
    // Backoff on repeated errors: skip cycles to avoid hammering API
    if (limitErrorCount > 0) {
      const skipCycles = Math.min(limitErrorCount * 2, 10); // max 20s backoff
      limitErrorCount--;
      return;
    }
    
    limitChecking = true;
    try {
      // Send both current and previous prices for cross detection
      const result = await callBase44('check_limits', { prices, prev_prices: prevPrices });
      limitErrorCount = 0; // reset on success
      if (result.filled > 0 || result.expired > 0 || result.failed > 0) {
        console.log(`[LIMITS] checked=${result.checked} filled=${result.filled} expired=${result.expired} failed=${result.failed || 0}`);
        await refreshOpenTrades();
      }
    } catch (e) {
      limitErrorCount = Math.min(limitErrorCount + 3, 15); // backoff faster on error
      if (!e.message?.includes('timeout')) console.error('[LIMITS] Error:', e.response?.status || e.message, e.response?.data?.error || '');
    } finally { limitChecking = false; }
  }, LIMIT_CHECK_MS);
  console.log('[LIMITS] Limit order checker started (' + LIMIT_CHECK_MS + 'ms) with cross detection');
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
    // not_expired_yet is handled inside closeTradeById (re-schedules automatically)
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

// TP/SL realtime update — frontend pushes changes instantly (no 3s wait)
app.post('/notify-tpsl-update', (req, res) => {
  try {
    const { secret, trade_id, take_profit, stop_loss } = req.body;
    if (secret !== SECRET) return res.status(403).json({ error: 'Forbidden' });
    if (!trade_id) return res.status(400).json({ error: 'Missing trade_id' });
    const t = openTradesCache.find(t => t.id === trade_id);
    if (t) {
      t.take_profit = take_profit || null;
      t.stop_loss = stop_loss || null;
      // Reset prev price so cross detection starts fresh with new TP/SL
      delete tradePrevPrices[trade_id];
      console.log(`[TPSL-UPDATE] ${trade_id} tp=${take_profit||'-'} sl=${stop_loss||'-'}`);
    }
    return res.json({ success: true, found: !!t });
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
