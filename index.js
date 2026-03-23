// index.js — MICEX Trade Engine V3 (Futures Pro + TP/SL + Turbo)
// All-in-one for Railway deployment
// Railway Variables needed:
//   REDIS_URL (auto from Railway Redis addon)
//   BASE44_FUNCTION_URL (e.g. https://xxx.base44.app/api/functions/railwayCloseTrade)
//   RAILWAY_CLOSE_SECRET (shared secret)
//   TELEGRAM_TRADE_BOT_TOKEN (optional)
//   TELEGRAM_GROUP_CHAT_ID (optional)
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const Redis = require('ioredis');
const { Queue, Worker } = require('bullmq');
const WebSocket = require('ws');
const axios = require('axios');

// ═══════════════════════════════════════
// CONFIG — auto-detect Base44 API from FUNCTION_URL
// ═══════════════════════════════════════
const PORT = process.env.PORT || 4000;
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const TRADE_API_SECRET = process.env.RAILWAY_CLOSE_SECRET || process.env.TRADE_API_SECRET;

// Derive Base44 API from BASE44_FUNCTION_URL
// e.g. https://task-flow-pro-furry.base44.app/api/functions/railwayCloseTrade
//   → baseURL = https://task-flow-pro-furry.base44.app/api
const FUNCTION_URL = process.env.BASE44_FUNCTION_URL || '';
const BASE44_BASE = FUNCTION_URL.replace(/\/functions\/.*$/, '') || process.env.BASE44_API_URL;
const BASE44_SERVICE_TOKEN = process.env.BASE44_SERVICE_TOKEN || process.env.BASE44_SERVICE_ROLE_KEY;

const CLOSE_FEE_RATE = parseFloat(process.env.CLOSE_FEE_RATE || '0.0002');
const MAX_PROFIT = parseFloat(process.env.MAX_PROFIT_PER_TRADE || '50');
const MAX_OPEN = parseInt(process.env.MAX_OPEN_TRADES || '10');
const MAX_EXPOSURE = parseInt(process.env.MAX_EXPOSURE || '100000');
const FEE_RATE = 0.0002;

// ═══════════════════════════════════════
// REDIS
// ═══════════════════════════════════════
const redis = new Redis(REDIS_URL, { maxRetriesPerRequest: null, enableReadyCheck: false });
redis.on('connect', () => console.log('[REDIS] Connected'));
redis.on('error', (err) => console.error('[REDIS] Error:', err.message));

// ═══════════════════════════════════════
// BASE44 CLIENT
// ═══════════════════════════════════════
if (!BASE44_BASE) { console.error('[FATAL] BASE44_FUNCTION_URL not set!'); process.exit(1); }
if (!BASE44_SERVICE_TOKEN) { console.error('[FATAL] BASE44_SERVICE_TOKEN / BASE44_SERVICE_ROLE_KEY not set!'); process.exit(1); }
console.log('[CONFIG] Base44 API:', BASE44_BASE);

const b44 = axios.create({
  baseURL: BASE44_BASE,
  headers: { 'Authorization': `Bearer ${BASE44_SERVICE_TOKEN}`, 'Content-Type': 'application/json' },
  timeout: 10000,
});

const entities = {
  async get(name, id) { return (await b44.get(`/entities/${name}/${id}`)).data; },
  async filter(name, query = {}, sort = '', limit = 50) {
    return (await b44.post(`/entities/${name}/filter`, { query, sort, limit })).data;
  },
  async create(name, data) { return (await b44.post(`/entities/${name}`, data)).data; },
  async update(name, id, data) { return (await b44.put(`/entities/${name}/${id}`, data)).data; },
};

// Quick test: verify API connectivity on startup
async function testApiConnection() {
  try {
    await entities.filter('Trade', { status: 'open' }, '', 1);
    console.log('[API] Base44 connection OK ✓');
  } catch (e) {
    console.error('[API] Base44 connection FAILED:', e.response?.status, e.message);
    console.error('[API] Check BASE44_FUNCTION_URL and BASE44_SERVICE_TOKEN');
  }
}

// ═══════════════════════════════════════
// PRICE SERVICE
// ═══════════════════════════════════════
const SYMBOLS = [
  'BTCUSDT','ETHUSDT','BNBUSDT','SOLUSDT','XRPUSDT',
  'ADAUSDT','DOGEUSDT','AVAXUSDT','DOTUSDT','LINKUSDT',
  'MATICUSDT','UNIUSDT','ATOMUSDT','FILUSDT','LTCUSDT',
  'APTUSDT','ARBUSDT','OPUSDT','NEARUSDT','SUIUSDT',
];
const prices = {};
let priceWs = null;

function connectPriceWs() {
  const streams = SYMBOLS.map(s => `${s.toLowerCase()}@ticker`).join('/');
  priceWs = new WebSocket(`wss://stream.binance.com:9443/stream?streams=${streams}`);
  priceWs.on('open', () => console.log('[PRICE] Binance WS connected'));
  priceWs.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      if (msg.data?.s && msg.data?.c) {
        const sym = msg.data.s.replace('USDT', '');
        const p = parseFloat(msg.data.c);
        if (p > 0) { prices[sym] = p; redis.set(`price:${sym}`, JSON.stringify({ symbol: sym, price: p, ts: Date.now() }), 'EX', 30); }
      }
    } catch (_) {}
  });
  priceWs.on('close', () => { console.warn('[PRICE] WS closed, reconnecting...'); setTimeout(connectPriceWs, 3000); });
  priceWs.on('error', (e) => { console.error('[PRICE] WS error:', e.message); try { priceWs.close(); } catch(_){} });
}

async function fetchRestPrices() {
  try {
    const res = await axios.get('https://api.binance.com/api/v3/ticker/price', { timeout: 5000 });
    for (const item of res.data) {
      const sym = item.symbol.replace('USDT', '');
      if (SYMBOLS.includes(item.symbol) && parseFloat(item.price) > 0) prices[sym] = parseFloat(item.price);
    }
    console.log(`[PRICE] REST: ${Object.keys(prices).length} prices loaded`);
  } catch (e) { console.error('[PRICE] REST failed:', e.message); }
}

function getPrice(symbol) { return prices[symbol] || 0; }

// ═══════════════════════════════════════
// SPREAD CONFIG
// ═══════════════════════════════════════
const SPREAD_CONFIG = {
  BTC:[2,5,15], ETH:[0.2,0.5,2], BNB:[0.1,0.3,1], SOL:[0.05,0.15,0.5],
  XRP:[0.001,0.003,0.01], ADA:[0.001,0.003,0.01], DOGE:[0.0002,0.0005,0.002],
  AVAX:[0.02,0.06,0.2], DOT:[0.005,0.015,0.05], LINK:[0.01,0.03,0.1],
  MATIC:[0.001,0.003,0.01], UNI:[0.005,0.015,0.05], ATOM:[0.005,0.015,0.05],
  FIL:[0.005,0.015,0.05], LTC:[0.05,0.15,0.5], APT:[0.005,0.015,0.05],
  ARB:[0.001,0.003,0.01], OP:[0.001,0.003,0.01], NEAR:[0.005,0.015,0.05],
  SUI:[0.001,0.003,0.01],
};

function calcSpread(symbol, margin) {
  const c = SPREAD_CONFIG[symbol] || [2,5,15];
  const sf = margin > 1000 ? 2.5 : margin > 500 ? 2.0 : margin > 200 ? 1.6 : margin > 50 ? 1.3 : 1.0;
  return Math.min((c[0] + Math.random() * (c[1] - c[0])) * sf, c[2]);
}

let coinConfigCache = null, coinConfigTime = 0;
async function getCoinConfig() {
  if (coinConfigCache && Date.now() - coinConfigTime < 300000) return coinConfigCache;
  try {
    const list = await entities.filter('PlatformConfig', { key: 'COIN_CONFIG' });
    if (list.length > 0 && list[0].value) { coinConfigCache = JSON.parse(list[0].value); coinConfigTime = Date.now(); }
  } catch (_) {}
  return coinConfigCache || [];
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
  try { const job = await tradeQueue.getJob(`close-${tradeId}`); if (job) { await job.remove(); console.log(`[QUEUE] Cancelled ${tradeId}`); } } catch (_) {}
}

// ═══════════════════════════════════════
// OPEN TRADE (supports Pro mode)
// ═══════════════════════════════════════
async function openTrade({ user_email, user_name, symbol, side, leverage, margin_amount, wallet_type, client_price, trade_mode, duration_minutes, turbo_multiplier, take_profit, stop_loss }) {
  const isDemo = wallet_type === 'demo';
  const isBonus = wallet_type === 'bonus';
  const wt = isDemo ? 'demo' : isBonus ? 'bonus' : wallet_type === 'fiat' ? 'fiat' : 'futures';

  const isPro = trade_mode === 'pro';
  const durationMin = isPro ? (duration_minutes || 60) : 1;
  const turbo = isPro ? 1 : (turbo_multiplier || 1);

  const lockKey = `lock:open:${user_email}`;
  const locked = await redis.set(lockKey, '1', 'NX', 'EX', 5);
  if (!locked) throw new Error('Đang xử lý lệnh trước đó');

  try {
    let balanceData, openTrades;
    if (isDemo) {
      [balanceData, openTrades] = await Promise.all([
        entities.filter('UserStats', { user_email }),
        entities.filter('Trade', { user_email, status: 'open', wallet_type: 'demo' }),
      ]);
    } else if (isBonus) {
      [balanceData, openTrades] = await Promise.all([
        entities.filter('BonusWallet', { user_email, status: 'active' }, '-created_date', 1),
        entities.filter('Trade', { user_email, status: 'open', wallet_type: 'bonus' }),
      ]);
    } else if (wt === 'fiat') {
      [balanceData, openTrades] = await Promise.all([
        entities.filter('FiatWallet', { user_email, status: 'active' }, '-created_date', 1),
        entities.filter('Trade', { user_email, status: 'open', wallet_type: 'fiat' }),
      ]);
    } else {
      [balanceData, openTrades] = await Promise.all([
        entities.filter('UserStats', { user_email }),
        entities.filter('Trade', { user_email, status: 'open', wallet_type: 'futures' }),
      ]);
    }

    if (openTrades.length >= MAX_OPEN) throw new Error(`Tối đa ${MAX_OPEN} lệnh`);

    const marketPrice = client_price > 0 ? client_price : getPrice(symbol);
    if (marketPrice <= 0) throw new Error('Không có giá');

    const coinConfigs = await getCoinConfig();
    const coinEntry = coinConfigs.find(c => c.symbol === symbol);
    if (coinEntry && !coinEntry.enabled) throw new Error(`${symbol} tạm ngừng`);
    const configMaxLev = coinEntry?.max_leverage || 0;
    const priceLev = marketPrice >= 1000 ? 200 : marketPrice >= 10 ? 150 : marketPrice >= 1 ? 100 : 50;
    let maxLev = configMaxLev > 0 ? configMaxLev : priceLev;
    if (isBonus && balanceData.length > 0 && (balanceData[0].max_leverage || 0) > 0)
      maxLev = Math.min(maxLev, balanceData[0].max_leverage);
    if (leverage > maxLev) throw new Error(`Leverage tối đa ${maxLev}x`);

    let entryPrice = marketPrice, spreadTick = 0;
    if (!isDemo) {
      spreadTick = calcSpread(symbol, margin_amount);
      entryPrice = side === 'long' ? marketPrice + spreadTick : marketPrice - spreadTick;
    }

    const positionSize = margin_amount * leverage;
    const openFee = positionSize * FEE_RATE;
    const totalCost = margin_amount + openFee;

    const liqFactor = turbo >= 5 ? 0.55 : turbo >= 3 ? 0.65 : 0.8;
    const liquidationPrice = isDemo ? 0 : (
      side === 'long' ? entryPrice * (1 - liqFactor / leverage) : entryPrice * (1 + liqFactor / leverage)
    );

    if (!isDemo && !isBonus && wt !== 'fiat') {
      const stats = balanceData[0];
      const balance = stats?.deposit_amount || 0;
      const currentExposure = openTrades.reduce((s, t) => s + (t.position_size || 0), 0);
      if (currentExposure + positionSize > MAX_EXPOSURE) throw new Error('Vượt exposure');
      if (balance > 0 && margin_amount > balance * 0.30) throw new Error('Vượt 30% số dư');
    }

    let newBalance = 0;
    if (isDemo) {
      const stats = balanceData[0];
      const demoBalance = typeof stats?.demo_balance === 'number' ? stats.demo_balance : 1000;
      if (demoBalance < totalCost) throw new Error('Demo không đủ');
      newBalance = demoBalance - totalCost;
      await entities.update('UserStats', stats.id, { demo_balance: newBalance });
    } else if (isBonus) {
      if (balanceData.length === 0) throw new Error('Không có ví Bonus');
      const bw = balanceData[0];
      if (bw.expiry_date && new Date(bw.expiry_date) < new Date()) {
        await entities.update('BonusWallet', bw.id, { status: 'expired' });
        throw new Error('Bonus hết hạn');
      }
      if ((bw.balance || 0) < totalCost) throw new Error('Bonus không đủ');
      newBalance = (bw.balance || 0) - totalCost;
      const today = new Date().toISOString().slice(0, 10);
      let tradingDays = []; try { tradingDays = JSON.parse(bw.trading_days || '[]'); } catch(_) {}
      if (!tradingDays.includes(today)) tradingDays.push(today);
      await entities.update('BonusWallet', bw.id, {
        balance: newBalance, trading_volume_progress: (bw.trading_volume_progress||0)+positionSize,
        trade_count: (bw.trade_count||0)+1, trading_days: JSON.stringify(tradingDays),
      });
    } else if (wt === 'fiat') {
      if (balanceData.length === 0) throw new Error('Không có ví Fiat');
      const fw = balanceData[0];
      if ((fw.balance || 0) < totalCost) throw new Error('Fiat không đủ');
      newBalance = (fw.balance || 0) - totalCost;
      await entities.update('FiatWallet', fw.id, { balance: newBalance });
    } else {
      const stats = balanceData[0];
      if (!stats) throw new Error('Không tìm thấy tài khoản');
      const currentBalance = stats.deposit_amount || 0;
      if (currentBalance < totalCost) throw new Error(`Không đủ. Cần ${totalCost.toFixed(2)}`);
      newBalance = currentBalance - totalCost;
      await entities.update('UserStats', stats.id, { deposit_amount: newBalance, trading_volume: (stats.trading_volume||0)+positionSize });
    }

    const now = new Date();
    const expireTime = new Date(now.getTime() + durationMin * 60000);
    const tradeData = {
      user_email, user_name: user_name || '', symbol, side,
      entry_price: entryPrice, position_size: positionSize, leverage,
      liquidation_price: liquidationPrice, open_time: now.toISOString(),
      close_time: expireTime.toISOString(), open_fee: openFee, status: 'open', wallet_type: wt,
      turbo_multiplier: turbo, trade_mode: isPro ? 'pro' : 'quick', duration_minutes: durationMin,
    };
    if (isPro && take_profit > 0) tradeData.take_profit = take_profit;
    if (isPro && stop_loss > 0) tradeData.stop_loss = stop_loss;
    if (isBonus && balanceData[0]) tradeData.bonus_wallet_id = balanceData[0].id;

    const trade = await entities.create('Trade', tradeData);
    await scheduleClose(trade.id, expireTime.getTime() - Date.now());

    console.log(`[OPEN] ${user_email} ${symbol} ${side} ${leverage}x margin=${margin_amount} mode=${tradeData.trade_mode} dur=${durationMin}min id=${trade.id}`);
    return { success: true, trade, new_balance: newBalance, market_price: marketPrice, entry_price: entryPrice, spread_tick: spreadTick };
  } finally { await redis.del(lockKey); }
}

// ═══════════════════════════════════════
// PNL CALCULATION (supports turbo)
// ═══════════════════════════════════════
function calcPnl(trade, closePrice, defaultReason) {
  const ps = trade.position_size || 0, lev = trade.leverage || 1, margin = ps / lev;
  const openFee = trade.open_fee || 0, entry = trade.entry_price;
  const isDemo = trade.wallet_type === 'demo', isBonus = trade.wallet_type === 'bonus';
  const turbo = trade.turbo_multiplier || 1;
  const qty = ps / entry;
  const basePnl = trade.side === 'long' ? (closePrice - entry) * qty : (entry - closePrice) * qty;
  const rawPnl = basePnl * turbo;

  let pnl, closeFee, totalFee, pnlFinal, returnAmount, roi;
  let closeReason = defaultReason || 'time_expired';
  const liqThreshold = turbo >= 5 ? 0.55 : turbo >= 3 ? 0.65 : 0.8;

  if (!isDemo && rawPnl <= -margin * liqThreshold) {
    closeReason = 'liquidation';
    pnl = -margin * liqThreshold; closeFee = 0; totalFee = openFee;
    pnlFinal = pnl; returnAmount = margin + pnlFinal;
    roi = margin > 0 ? (pnl / margin) * 100 : 0;
  } else {
    pnl = rawPnl; closeFee = ps * CLOSE_FEE_RATE; totalFee = openFee + closeFee;
    pnlFinal = pnl - totalFee; returnAmount = margin + pnl - closeFee;
    roi = margin > 0 ? (pnlFinal / margin) * 100 : 0;
  }

  if (!isDemo && !isBonus && pnlFinal > MAX_PROFIT) {
    pnlFinal = MAX_PROFIT; pnl = MAX_PROFIT + totalFee;
    roi = margin > 0 ? (pnlFinal / margin) * 100 : 0;
    returnAmount = margin + pnl - closeFee;
  }
  if (returnAmount < 0) returnAmount = 0;
  return { pnl, closeFee, totalFee, pnlFinal, returnAmount, roi, closeReason, margin };
}

// ═══════════════════════════════════════
// WALLET BALANCE UPDATE
// ═══════════════════════════════════════
async function updateWalletBalance(trade, returnAmount, margin) {
  const isDemo = trade.wallet_type === 'demo';
  const isBonus = trade.wallet_type === 'bonus';
  const isFiat = trade.wallet_type === 'fiat';
  let newBalance = null;

  if (isBonus) {
    let bw = null;
    if (trade.bonus_wallet_id) { try { bw = await entities.get('BonusWallet', trade.bonus_wallet_id); } catch(_){} }
    if (!bw) { const bws = await entities.filter('BonusWallet', { user_email: trade.user_email, status: 'active' }, '-created_date', 1); if (bws.length > 0) bw = bws[0]; }
    if (bw) { const f = await entities.get('BonusWallet', bw.id); newBalance = Math.max(0, (f.balance||0)+returnAmount); await entities.update('BonusWallet', bw.id, { balance: newBalance }); }
  } else if (isFiat) {
    const fws = await entities.filter('FiatWallet', { user_email: trade.user_email, status: 'active' }, '-created_date', 1);
    if (fws.length > 0) { const f = await entities.get('FiatWallet', fws[0].id); newBalance = Math.max(0, (f.balance||0)+returnAmount); await entities.update('FiatWallet', fws[0].id, { balance: newBalance }); }
  } else if (!isDemo) {
    const stats = await entities.filter('UserStats', { user_email: trade.user_email });
    if (stats.length > 0) { const f = await entities.get('UserStats', stats[0].id); newBalance = Math.max(0, (f.deposit_amount||0)+returnAmount); await entities.update('UserStats', stats[0].id, { deposit_amount: newBalance }); }
    entities.filter('EventProgress', { user_email: trade.user_email }).then(list => {
      list.filter(ep => !ep.is_claimed).forEach(ep => {
        const nv = (ep.current_volume||0) + margin;
        entities.update('EventProgress', ep.id, { current_volume: nv, is_completed: nv >= (ep.required_volume||500) }).catch(()=>{});
      });
    }).catch(()=>{});
  } else {
    const stats = await entities.filter('UserStats', { user_email: trade.user_email });
    if (stats.length > 0) { const d = typeof stats[0].demo_balance === 'number' ? stats[0].demo_balance : 1000; newBalance = Math.max(0, d+returnAmount); await entities.update('UserStats', stats[0].id, { demo_balance: newBalance }); }
  }
  return newBalance;
}

// ═══════════════════════════════════════
// CLOSE TRADE BY ID (time expired)
// ═══════════════════════════════════════
async function closeTradeById(tradeId) {
  const lockKey = `lock:close:${tradeId}`;
  const locked = await redis.set(lockKey, '1', 'NX', 'EX', 30);
  if (!locked) return { success: false, reason: 'lock_held' };

  try {
    let trade;
    try { trade = await entities.get('Trade', tradeId); } catch (_) { return { success: false, reason: 'not_found' }; }
    if (trade.status === 'closed') return { success: false, reason: 'already_closed' };

    await entities.update('Trade', trade.id, { status: 'pending_close' });
    const fresh = await entities.get('Trade', trade.id);
    if (fresh.status === 'closed') return { success: false, reason: 'already_closed' };
    if (fresh.status !== 'pending_close') return { success: false, reason: 'status_changed' };

    let closePrice = fresh.close_price_snapshot || 0;
    if (closePrice <= 0) closePrice = getPrice(trade.symbol);
    if (closePrice <= 0) {
      try {
        const r = await axios.get(`https://api.binance.com/api/v3/ticker/price?symbol=${trade.symbol}USDT`, { timeout: 3000 });
        const p = parseFloat(r.data?.price);
        if (p > 0) closePrice = p;
      } catch (_) {}
    }
    if (closePrice <= 0) { await entities.update('Trade', trade.id, { status: 'open' }); return { success: false, reason: 'no_price' }; }

    const calc = calcPnl(trade, closePrice, 'time_expired');
    await entities.update('Trade', trade.id, {
      status: 'closed', closed_at: new Date().toISOString(),
      close_price: closePrice, close_price_snapshot: closePrice,
      close_reason: calc.closeReason, pnl: calc.pnl, roi: calc.roi,
      close_fee: calc.closeFee, total_fee: calc.totalFee, pnl_final: calc.pnlFinal,
    });
    const newBal = await updateWalletBalance(trade, calc.returnAmount, calc.margin);
    if (newBal !== null) await entities.update('Trade', trade.id, { balance_after_close: newBal });
    console.log(`[CLOSE] ${trade.user_email} ${trade.symbol} mode=${trade.trade_mode||'quick'} pnl=${calc.pnlFinal.toFixed(2)}`);
    return { success: true, close_price: closePrice, close_reason: calc.closeReason, pnl: calc.pnl, pnl_final: calc.pnlFinal, roi: calc.roi, return_amount: calc.returnAmount };
  } finally { await redis.del(lockKey); }
}

// ═══════════════════════════════════════
// CLOSE BY TP/SL
// ═══════════════════════════════════════
async function closeTradeByTpSl(tradeId, closePrice, reason) {
  const lockKey = `lock:close:${tradeId}`;
  const locked = await redis.set(lockKey, '1', 'NX', 'EX', 30);
  if (!locked) return { success: false, reason: 'lock_held' };

  try {
    let trade;
    try { trade = await entities.get('Trade', tradeId); } catch (_) { return { success: false, reason: 'not_found' }; }
    if (trade.status !== 'open') return { success: false, reason: 'not_open' };
    await entities.update('Trade', trade.id, { status: 'pending_close' });

    const calc = calcPnl(trade, closePrice, reason);
    await entities.update('Trade', trade.id, {
      status: 'closed', closed_at: new Date().toISOString(),
      close_price: closePrice, close_price_snapshot: closePrice,
      close_reason: reason, pnl: calc.pnl, roi: calc.roi,
      close_fee: calc.closeFee, total_fee: calc.totalFee, pnl_final: calc.pnlFinal,
    });
    await cancelClose(tradeId);
    const newBal = await updateWalletBalance(trade, calc.returnAmount, calc.margin);
    if (newBal !== null) await entities.update('Trade', trade.id, { balance_after_close: newBal });
    console.log(`[TPSL] ${trade.user_email} ${trade.symbol} ${reason} price=${closePrice} pnl=${calc.pnlFinal.toFixed(2)}`);
    return { success: true, pnl_final: calc.pnlFinal };
  } finally { await redis.del(lockKey); }
}

// ═══════════════════════════════════════
// CLOSE BY LIQUIDATION
// ═══════════════════════════════════════
async function closeTradeLiquidation(tradeId, closePrice) {
  const lockKey = `lock:close:${tradeId}`;
  const locked = await redis.set(lockKey, '1', 'NX', 'EX', 30);
  if (!locked) return { success: false, reason: 'lock_held' };
  try {
    let trade;
    try { trade = await entities.get('Trade', tradeId); } catch (_) { return { success: false, reason: 'not_found' }; }
    if (trade.status !== 'open') return { success: false, reason: 'not_open' };
    await entities.update('Trade', trade.id, { status: 'pending_close' });

    const ps = trade.position_size || 0, lev = trade.leverage || 1, margin = ps / lev, openFee = trade.open_fee || 0;
    const turbo = trade.turbo_multiplier || 1;
    const liqThreshold = turbo >= 5 ? 0.55 : turbo >= 3 ? 0.65 : 0.8;
    const pnl = -margin * liqThreshold, pnlFinal = pnl, returnAmount = Math.max(0, margin + pnlFinal);

    await entities.update('Trade', trade.id, {
      status: 'closed', closed_at: new Date().toISOString(), close_price: closePrice, close_price_snapshot: closePrice,
      close_reason: 'liquidation', pnl, roi: margin > 0 ? (pnl/margin)*100 : 0, close_fee: 0, total_fee: openFee, pnl_final: pnlFinal,
    });
    await cancelClose(tradeId);
    if (returnAmount > 0) await updateWalletBalance(trade, returnAmount, margin);
    console.log(`[LIQ] ${trade.user_email} ${trade.symbol} turbo=x${turbo} pnl=${pnlFinal.toFixed(2)}`);
    return { success: true, pnl_final: pnlFinal };
  } finally { await redis.del(lockKey); }
}

// ═══════════════════════════════════════
// LIQUIDATION + TP/SL SERVICE (2s interval)
// ═══════════════════════════════════════
let liqInterval = null;

function startLiquidationService() {
  liqInterval = setInterval(async () => {
    try {
      const openTrades = await entities.filter('Trade', { status: 'open' });
      for (const trade of openTrades) {
        const price = getPrice(trade.symbol);
        if (!price) continue;
        const isDemo = trade.wallet_type === 'demo';
        const turbo = trade.turbo_multiplier || 1;

        // TP/SL check
        if (trade.take_profit > 0) {
          const tpHit = trade.side === 'long' ? price >= trade.take_profit : price <= trade.take_profit;
          if (tpHit) { console.log(`[TP] ${trade.user_email} ${trade.symbol} price=${price} tp=${trade.take_profit}`); await closeTradeByTpSl(trade.id, price, 'take_profit'); continue; }
        }
        if (trade.stop_loss > 0) {
          const slHit = trade.side === 'long' ? price <= trade.stop_loss : price >= trade.stop_loss;
          if (slHit) { console.log(`[SL] ${trade.user_email} ${trade.symbol} price=${price} sl=${trade.stop_loss}`); await closeTradeByTpSl(trade.id, price, 'stop_loss'); continue; }
        }

        // Liquidation check (skip demo)
        if (isDemo) continue;
        const liqFactor = turbo >= 5 ? 0.55 : turbo >= 3 ? 0.65 : 0.8;
        const liq = trade.liquidation_price || (trade.side === 'long'
          ? trade.entry_price * (1 - liqFactor / trade.leverage)
          : trade.entry_price * (1 + liqFactor / trade.leverage));
        const isLiq = trade.side === 'long' ? price <= liq : price >= liq;
        if (isLiq) { console.log(`[LIQ] Trigger: ${trade.user_email} ${trade.symbol} price=${price} liq=${liq} turbo=x${turbo}`); await closeTradeLiquidation(trade.id, price); }
      }
    } catch (e) { if (!e.message?.includes('404')) console.error('[LIQ]', e.message); }
  }, 2000);
  console.log('[LIQ] Started (2s interval) — with TP/SL support');
}

// ═══════════════════════════════════════
// RECOVERY SERVICE
// ═══════════════════════════════════════
async function recoverOpenTrades() {
  console.log('[RECOVERY] Scanning open trades...');
  try {
    const [openTrades, pendingTrades] = await Promise.all([
      entities.filter('Trade', { status: 'open' }),
      entities.filter('Trade', { status: 'pending_close' }),
    ]);
    const now = Date.now();
    for (const t of pendingTrades) {
      if (now - new Date(t.updated_date).getTime() > 120000) {
        await entities.update('Trade', t.id, { status: 'open' });
        openTrades.push(t);
        console.log(`[RECOVERY] Reset stuck ${t.id}`);
      }
    }
    let scheduled = 0, immediate = 0;
    for (const trade of openTrades) {
      const delay = new Date(trade.close_time).getTime() - now;
      if (delay <= 0) { await scheduleClose(trade.id, 0); immediate++; }
      else { await scheduleClose(trade.id, delay); scheduled++; }
    }
    console.log(`[RECOVERY] Done: ${scheduled} scheduled, ${immediate} immediate`);
  } catch (e) { console.error('[RECOVERY]', e.message); }
}

// ═══════════════════════════════════════
// QUEUE WORKER
// ═══════════════════════════════════════
function startWorker() {
  const worker = new Worker('trade-close', async (job) => {
    const { tradeId } = job.data;
    console.log(`[QUEUE] Processing close: ${tradeId}`);
    const result = await closeTradeById(tradeId);
    if (!result.success && result.reason === 'no_price') throw new Error('No price — will retry');
    return result;
  }, { connection: redis, concurrency: 20, limiter: { max: 100, duration: 1000 } });

  worker.on('completed', (job, result) => { if (result?.success) console.log(`[QUEUE] ✓ Closed ${job.data.tradeId} pnl=${result.pnl_final?.toFixed(2)}`); });
  worker.on('failed', (job, err) => { console.error(`[QUEUE] ✗ Failed ${job?.data?.tradeId}: ${err.message}`); });
  console.log('[QUEUE] Worker started');
}

// ═══════════════════════════════════════
// EXPRESS SERVER
// ═══════════════════════════════════════
const app = express();
app.use(cors());
app.use(express.json());

// Notify endpoint (Base44 backend calls this)
app.post('/notify-new-trade', async (req, res) => {
  try {
    const { secret, trade } = req.body;
    if (secret !== TRADE_API_SECRET) return res.status(403).json({ error: 'Forbidden' });
    if (!trade?.id || !trade?.close_time) return res.status(400).json({ error: 'Missing trade data' });
    const delay = Math.max(0, new Date(trade.close_time).getTime() - Date.now());
    await scheduleClose(trade.id, delay);
    const mode = trade.trade_mode || 'quick';
    const dur = trade.duration_minutes || 1;
    console.log(`[NOTIFY] Scheduled ${trade.id} (${trade.symbol} ${trade.side} mode=${mode} dur=${dur}min) close in ${(delay/1000).toFixed(1)}s`);
    return res.json({ success: true, trade_id: trade.id, delay_ms: delay });
  } catch (e) { console.error(`[NOTIFY] Error: ${e.message}`); return res.status(500).json({ error: e.message }); }
});

// Auth middleware for /trade/*
app.use('/trade', (req, res, next) => {
  if (req.headers['x-trade-secret'] !== TRADE_API_SECRET) return res.status(403).json({ error: 'Forbidden' });
  next();
});

app.post('/trade/open', async (req, res) => {
  try {
    const { user_email, user_name, symbol, side, leverage, margin_amount, wallet_type, client_price, trade_mode, duration_minutes, turbo_multiplier, take_profit, stop_loss } = req.body;
    if (!user_email || !symbol || !side || !leverage || !margin_amount) return res.status(400).json({ error: 'Missing fields' });
    const result = await openTrade({
      user_email, user_name, symbol, side,
      leverage: Math.floor(Number(leverage)), margin_amount: Number(margin_amount),
      wallet_type: wallet_type || 'futures', client_price: Number(client_price) || 0,
      trade_mode: trade_mode || 'quick', duration_minutes: Number(duration_minutes) || 1,
      turbo_multiplier: Number(turbo_multiplier) || 1,
      take_profit: Number(take_profit) || 0, stop_loss: Number(stop_loss) || 0,
    });
    return res.json(result);
  } catch (e) { return res.status(400).json({ error: e.message }); }
});

app.post('/trade/close', async (req, res) => {
  try {
    const { trade_id, close_reason, close_price } = req.body;
    if (!trade_id) return res.status(400).json({ error: 'trade_id required' });
    if (close_reason === 'take_profit' || close_reason === 'stop_loss') {
      return res.json(await closeTradeByTpSl(trade_id, Number(close_price) || 0, close_reason));
    }
    return res.json(await closeTradeById(trade_id));
  } catch (e) { return res.status(500).json({ error: e.message }); }
});

app.get('/trade/positions', async (req, res) => {
  try {
    const { user_email, wallet_type } = req.query;
    if (!user_email) return res.status(400).json({ error: 'user_email required' });
    const query = { user_email, status: 'open' };
    if (wallet_type) query.wallet_type = wallet_type;
    return res.json({ positions: await entities.filter('Trade', query) });
  } catch (e) { return res.status(500).json({ error: e.message }); }
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok', uptime: process.uptime(), prices: Object.keys(prices).length, timestamp: Date.now() });
});

// ═══════════════════════════════════════
// START
// ═══════════════════════════════════════
app.listen(PORT, async () => {
  console.log(`[SERVER] Running on port ${PORT}`);
  await testApiConnection();
  connectPriceWs();
  await fetchRestPrices();
  startWorker();
  await recoverOpenTrades();
  startLiquidationService();
  console.log('[SERVER] All services ready ✓');
});
