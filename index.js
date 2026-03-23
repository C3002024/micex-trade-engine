// ═══════════════════════════════════════════════════════════
// MICEX Trade Engine v3 + Signal Bot + Limit Orders — Deploy trên Railway
// File: index.js (THAY THẾ TOÀN BỘ file index.js cũ)
// ═══════════════════════════════════════════════════════════

const express = require('express');
const app = express();
app.use(express.json());

// ── ENV VARS (set trong Railway Dashboard) ──
const BASE44_FUNCTION_URL = process.env.BASE44_FUNCTION_URL;
const RAILWAY_SECRET = process.env.RAILWAY_CLOSE_SECRET;
const PORT = process.env.PORT || 3000;

// ── LIMIT ORDER CONFIG ──
const LIMIT_ORDER_FUNCTION_URL = process.env.LIMIT_ORDER_FUNCTION_URL || '';
// ^ Set trong Railway Variables, ví dụ: https://app.base44.com/api/v1/apps/YOUR_APP_ID/functions/checkLimitOrders

// ── TELEGRAM SIGNAL BOT CONFIG ──
const TG_BOT_TOKEN = process.env.TELEGRAM_TRADE_BOT_TOKEN;
const TG_CHAT_ID = process.env.TELEGRAM_GROUP_CHAT_ID || process.env.TELEGRAM_PRIVATE_CHAT_ID;
const SIGNAL_INTERVAL_MS = 60 * 1000; // 60 giây

// ── IN-MEMORY ──
const tradeQueue = new Map();
const priceCache = new Map();
const closingSet = new Set();
const CACHE_TTL = 1500;

// ── CoinGecko ID mapping ──
const CG_IDS = {
  BTC:'bitcoin',ETH:'ethereum',BNB:'binancecoin',SOL:'solana',
  XRP:'ripple',DOGE:'dogecoin',ADA:'cardano',AVAX:'avalanche-2',
  DOT:'polkadot',LINK:'chainlink',LTC:'litecoin',APT:'aptos',
  ARB:'arbitrum',OP:'optimism',SUI:'sui',NEAR:'near',TRX:'tron',
  PEPE:'pepe',SHIB:'shiba-inu',UNI:'uniswap',ATOM:'cosmos',
  FIL:'filecoin',INJ:'injective-protocol',SEI:'sei-network',
};

// ═══════════════════════════════════════════════════════════
// PRICE ENGINE — 8 sources PARALLEL + User-Agent bypass
// ═══════════════════════════════════════════════════════════
const FETCH_HEADERS = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', 'Accept': 'application/json' };

async function fetchFromUrl(url, parseFn, timeoutMs = 5000) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const r = await fetch(url, { signal: controller.signal, headers: FETCH_HEADERS });
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
  const ppId = symbol === 'BTC' ? 'btc-bitcoin' : symbol === 'ETH' ? 'eth-ethereum' : symbol === 'BNB' ? 'bnb-binance-coin' : null;
  const sources = [
    ...(ppId ? [{ url: `https://api.coinpaprika.com/v1/tickers/${ppId}`, parse: d => parseFloat(d?.quotes?.USD?.price) }] : []),
    ...(cgId ? [{ url: `https://api.coincap.io/v2/assets/${cgId}`, parse: d => parseFloat(d?.data?.priceUsd) }] : []),
    { url: `https://api.kraken.com/0/public/Ticker?pair=${symbol}USDT`, parse: d => { const r=d?.result; if(!r) return 0; const k=Object.keys(r)[0]; return parseFloat(r[k]?.c?.[0]); } },
    { url: `https://min-api.cryptocompare.com/data/price?fsym=${symbol}&tsyms=USD`, parse: d => parseFloat(d?.USD) },
    { url: `https://api.binance.com/api/v3/ticker/price?symbol=${symbol}USDT`, parse: d => parseFloat(d?.price) },
    { url: `https://api.bybit.com/v5/market/tickers?category=linear&symbol=${symbol}USDT`, parse: d => parseFloat(d?.result?.list?.[0]?.lastPrice) },
    { url: `https://fapi.binance.com/fapi/v1/ticker/price?symbol=${symbol}USDT`, parse: d => parseFloat(d?.price) },
    { url: `https://www.okx.com/api/v5/market/ticker?instId=${symbol}-USDT`, parse: d => parseFloat(d?.data?.[0]?.last) },
    { url: `https://api.mexc.com/api/v3/ticker/price?symbol=${symbol}USDT`, parse: d => parseFloat(d?.price) },
    { url: `https://api.gateio.ws/api/v4/spot/tickers?currency_pair=${symbol}_USDT`, parse: d => Array.isArray(d)&&d.length>0 ? parseFloat(d[0]?.last) : 0 },
    ...(cgId ? [{ url: `https://api.coingecko.com/api/v3/simple/price?ids=${cgId}&vs_currencies=usd`, parse: d => parseFloat(d?.[cgId]?.usd) }] : []),
  ];

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

// ═══════════════════════════════════════════════════════════
// TRADE ENGINE — PnL + Liquidation
// ═══════════════════════════════════════════════════════════
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
  if (rawPnl <= -margin * 0.8) return true;
  const liqPrice = trade.liquidation_price || 0;
  if (liqPrice > 0) {
    if (trade.side === 'long' && currentPrice <= liqPrice) return true;
    if (trade.side === 'short' && currentPrice >= liqPrice) return true;
  }
  return false;
}

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
    setTimeout(() => {
      closingSet.delete(tradeId);
      closeTrade(tradeId, symbol, reason).catch(() => {});
    }, 2000);
  }
}

function scheduleTrade(trade) {
  const old = tradeQueue.get(trade.id);
  if (old) {
    if (old.closeHandle) clearTimeout(old.closeHandle);
    if (old.preWarmHandle) clearTimeout(old.preWarmHandle);
  }

  const closeMs = new Date(trade.close_time).getTime();
  const delay = Math.max(closeMs - Date.now(), 500);

  const preWarmDelay = Math.max(delay - 3000, 100);
  const preWarmHandle = setTimeout(() => {
    fetchPrice(trade.symbol).catch(() => {});
  }, preWarmDelay);

  const closeHandle = setTimeout(() => {
    closeTrade(trade.id, trade.symbol, 'time_expired');
  }, delay);

  tradeQueue.set(trade.id, { trade, closeHandle, preWarmHandle });
}

// ═══════════════════════════════════════════════════════════
// LIQUIDATION LOOP — 500ms
// ═══════════════════════════════════════════════════════════
let liqLoopRunning = false;

async function liquidationLoop() {
  if (liqLoopRunning) return;
  liqLoopRunning = true;

  try {
    const symbolsToCheck = new Set();
    for (const [id, entry] of tradeQueue) {
      if (entry.trade && entry.trade.wallet_type !== 'demo') {
        symbolsToCheck.add(entry.trade.symbol);
      }
    }
    if (symbolsToCheck.size === 0) { liqLoopRunning = false; return; }

    const priceMap = {};
    await Promise.allSettled(
      [...symbolsToCheck].map(async (sym) => {
        const p = await fetchPrice(sym);
        if (p > 0) priceMap[sym] = p;
      })
    );

    for (const [tradeId, entry] of tradeQueue) {
      const trade = entry.trade;
      if (!trade || trade.wallet_type === 'demo') continue;
      if (closingSet.has(tradeId)) continue;

      const price = priceMap[trade.symbol];
      if (!price || price <= 0) continue;

      if (isLiquidated(trade, price)) {
        console.log(`[LIQUIDATION] Trade ${tradeId} ${trade.symbol} ${trade.side} at ${price}`);
        closeTrade(tradeId, trade.symbol, 'liquidation');
      }
    }
  } catch (err) {
    console.error(`[LIQ-LOOP ERROR] ${err.message}`);
  }

  liqLoopRunning = false;
}

// ═══════════════════════════════════════════════════════════
// SYNC — Load open trades from Base44
// ═══════════════════════════════════════════════════════════
async function syncOpenTrades() {
  try {
    const res = await fetch(BASE44_FUNCTION_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ secret: RAILWAY_SECRET, action: 'get_open_trades' }),
    });
    const data = await res.json();
    const trades = data.trades || [];

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

// ═══════════════════════════════════════════════════════════
// SIGNAL BOT — Gửi tín hiệu BTC/USDT vào Telegram
// ═══════════════════════════════════════════════════════════
async function sendTelegram(text) {
  if (!TG_BOT_TOKEN || !TG_CHAT_ID) return;
  try {
    await fetch(`https://api.telegram.org/bot${TG_BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: TG_CHAT_ID, text, parse_mode: 'HTML', disable_web_page_preview: true }),
    });
  } catch (_) {}
}

async function getBTCData() {
  const sources = [
    { url: 'https://api.coinpaprika.com/v1/tickers/btc-bitcoin', parse: d => parseFloat(d?.quotes?.USD?.price) },
    { url: 'https://api.coincap.io/v2/assets/bitcoin', parse: d => parseFloat(d?.data?.priceUsd) },
    { url: 'https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT', parse: d => parseFloat(d?.price) },
    { url: 'https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT', parse: d => parseFloat(d?.result?.list?.[0]?.lastPrice) },
    { url: 'https://fapi.binance.com/fapi/v1/ticker/price?symbol=BTCUSDT', parse: d => parseFloat(d?.price) },
    { url: 'https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT', parse: d => parseFloat(d?.data?.[0]?.last) },
    { url: 'https://api.kraken.com/0/public/Ticker?pair=BTCUSDT', parse: d => { const r=d?.result; if(!r) return 0; const k=Object.keys(r)[0]; return parseFloat(r[k]?.c?.[0]); } },
    { url: 'https://min-api.cryptocompare.com/data/price?fsym=BTC&tsyms=USD', parse: d => parseFloat(d?.USD) },
    { url: 'https://api.mexc.com/api/v3/ticker/price?symbol=BTCUSDT', parse: d => parseFloat(d?.price) },
    { url: 'https://api.gateio.ws/api/v4/spot/tickers?currency_pair=BTC_USDT', parse: d => Array.isArray(d)&&d.length>0 ? parseFloat(d[0]?.last) : 0 },
    { url: 'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd', parse: d => parseFloat(d?.bitcoin?.usd) },
  ];

  const results = await Promise.allSettled(
    sources.map(src => fetchFromUrl(src.url, src.parse))
  );

  const prices = results.filter(r => r.status === 'fulfilled' && r.value > 0).map(r => r.value);
  if (prices.length === 0) return null;

  prices.sort((a, b) => a - b);
  const median = prices[Math.floor(prices.length / 2)];
  return { price: median, sources: prices.length };
}

let lastBTCPrice = null;
let signalCount = 0;
const TREND_HISTORY = [];
const TREND_MAX = 10;

async function sendSignal() {
  try {
    const data = await getBTCData();
    if (!data) return;

    const { price, sources } = data;
    const changeAbs = lastBTCPrice ? price - lastBTCPrice : 0;
    const changePct = lastBTCPrice ? ((changeAbs / lastBTCPrice) * 100).toFixed(2) : '0.00';
    const arrow = changeAbs > 0 ? '🟢 ▲' : changeAbs < 0 ? '🔴 ▼' : '⚪ ━';

    TREND_HISTORY.push(price);
    if (TREND_HISTORY.length > TREND_MAX) TREND_HISTORY.shift();

    let trendEmoji = '━━';
    if (TREND_HISTORY.length >= 3) {
      const recent = TREND_HISTORY.slice(-3);
      if (recent[2] > recent[1] && recent[1] > recent[0]) trendEmoji = '📈 Uptrend';
      else if (recent[2] < recent[1] && recent[1] < recent[0]) trendEmoji = '📉 Downtrend';
      else trendEmoji = '📊 Sideway';
    }

    signalCount++;
    lastBTCPrice = price;

    const now = new Date();
    const timeStr = now.toLocaleString('vi-VN', { timeZone: 'Asia/Ho_Chi_Minh', hour: '2-digit', minute: '2-digit', second: '2-digit' });

    const text = `${arrow} <b>BTC/USDT Signal #${signalCount}</b>\n\n`
      + `💰 Giá: <b>$${price.toLocaleString('en-US', { minimumFractionDigits: 2 })}</b>\n`
      + `📊 Thay đổi: <b>${changePct}%</b> (${changeAbs >= 0 ? '+' : ''}${changeAbs.toFixed(2)})\n`
      + `🔍 Xu hướng: <b>${trendEmoji}</b>\n`
      + `📡 Nguồn: ${sources}/11 APIs\n`
      + `🕐 ${timeStr} (GMT+7)\n\n`
      + `<i>⚡ MICEX Signal Bot — Realtime</i>`;

    await sendTelegram(text);
  } catch (_) {}
}

// ═══════════════════════════════════════════════════════════
// LIMIT ORDER CHECK — Gọi checkLimitOrders mỗi 5 giây
// ═══════════════════════════════════════════════════════════
let limitCheckRunning = false;

async function checkLimitOrders() {
  if (limitCheckRunning) return;
  limitCheckRunning = true;
  try {
    // Lấy giá tất cả symbol từ priceCache
    const prices = {};
    for (const [sym, data] of priceCache) {
      if (data.price > 0) prices[sym] = data.price;
    }

    // Nếu chưa có giá thì fetch các coin chính
    if (Object.keys(prices).length === 0) {
      for (const sym of ['BTC', 'ETH', 'BNB', 'SOL', 'XRP']) {
        const p = await fetchPrice(sym);
        if (p > 0) prices[sym] = p;
      }
    }

    if (Object.keys(prices).length === 0) { limitCheckRunning = false; return; }

    // Xác định URL function checkLimitOrders
    let limitUrl = LIMIT_ORDER_FUNCTION_URL;
    if (!limitUrl && BASE44_FUNCTION_URL) {
      // Tự suy ra từ BASE44_FUNCTION_URL (thay railwayCloseTrade → checkLimitOrders)
      limitUrl = BASE44_FUNCTION_URL.replace(/\/railwayCloseTrade\/?$/, '/checkLimitOrders');
      // Nếu không thay được (URL không chứa railwayCloseTrade), thử thay phần cuối
      if (limitUrl === BASE44_FUNCTION_URL) {
        limitUrl = BASE44_FUNCTION_URL.replace(/\/[^/]+\/?$/, '/checkLimitOrders');
      }
    }

    if (!limitUrl) { limitCheckRunning = false; return; }

    const res = await fetch(limitUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ secret: RAILWAY_SECRET, prices }),
    });
    const data = await res.json();
    if (data.filled > 0) {
      console.log(`[LIMIT] Filled ${data.filled} limit orders`);
    }
    if (data.expired > 0) {
      console.log(`[LIMIT] Expired ${data.expired} limit orders`);
    }
  } catch (err) {
    // Silent — không crash loop
  }
  limitCheckRunning = false;
}

// ═══════════════════════════════════════════════════════════
// API ENDPOINTS
// ═══════════════════════════════════════════════════════════

// Health check
app.get('/', (req, res) => {
  res.json({
    status: 'online',
    uptime: process.uptime(),
    queue: tradeQueue.size,
    closing: closingSet.size,
    priceCache: priceCache.size,
  });
});

// Schedule trade (gọi bởi Base44 backend)
app.post('/notify-new-trade', (req, res) => {
  const { secret, trade } = req.body;
  if (secret !== RAILWAY_SECRET) return res.status(401).json({ error: 'Unauthorized' });
  if (!trade || !trade.id) return res.status(400).json({ error: 'Missing trade' });
  scheduleTrade(trade);
  console.log(`[NOTIFY] Scheduled trade ${trade.id} (${trade.symbol})`);
  res.json({ success: true, queued: tradeQueue.size });
});

// Manual close
app.post('/close-trade', async (req, res) => {
  const { secret, trade_id, symbol } = req.body;
  if (secret !== RAILWAY_SECRET) return res.status(401).json({ error: 'Unauthorized' });
  const result = await closeTrade(trade_id, symbol || 'BTC', 'manual');
  res.json({ success: true, result });
});

// Get status
app.get('/status', (req, res) => {
  const trades = [];
  for (const [id, entry] of tradeQueue) {
    trades.push({
      id,
      symbol: entry.trade?.symbol,
      side: entry.trade?.side,
      close_time: entry.trade?.close_time,
      remaining_ms: entry.trade?.close_time ? new Date(entry.trade.close_time).getTime() - Date.now() : 0,
    });
  }
  res.json({ queue: tradeQueue.size, closing: closingSet.size, trades });
});

// ═══════════════════════════════════════════════════════════
// START SERVER
// ═══════════════════════════════════════════════════════════
app.listen(PORT, () => {
  console.log(`[MICEX] Trade Engine v3 + Limit Orders running on port ${PORT}`);
  console.log(`[MICEX] BASE44_FUNCTION_URL: ${BASE44_FUNCTION_URL}`);

  // Sync open trades on startup
  syncOpenTrades();

  // Liquidation loop: 500ms
  setInterval(liquidationLoop, 500);

  // Sync open trades: 30s
  setInterval(syncOpenTrades, 30000);

  // Signal bot: 60s
  if (TG_BOT_TOKEN && TG_CHAT_ID) {
    setTimeout(sendSignal, 5000);
    setInterval(sendSignal, SIGNAL_INTERVAL_MS);
    console.log('[SIGNAL] Bot started');
  }

  // ⭐ Limit Order check: 5 giây
  setInterval(checkLimitOrders, 5000);
  console.log('[LIMIT] Limit order checker started (5s interval)');
});
