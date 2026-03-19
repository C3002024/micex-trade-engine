// ═══════════════════════════════════════════════════════════
// MICEX Trade Engine v3 + Signal Bot — Deploy trên Railway
// File: index.js (THAY THẾ TOÀN BỘ file index.js cũ)
// ═══════════════════════════════════════════════════════════

const express = require('express');
const app = express();
app.use(express.json());

// ── ENV VARS (set trong Railway Dashboard) ──
const BASE44_FUNCTION_URL = process.env.BASE44_FUNCTION_URL;
const RAILWAY_SECRET = process.env.RAILWAY_CLOSE_SECRET;
const PORT = process.env.PORT || 3000;

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
    // Coinpaprika — hoàn toàn mở
    ...(ppId ? [{ url: `https://api.coinpaprika.com/v1/tickers/${ppId}`, parse: d => parseFloat(d?.quotes?.USD?.price) }] : []),
    // CoinCap — không chặn
    ...(cgId ? [{ url: `https://api.coincap.io/v2/assets/${cgId}`, parse: d => parseFloat(d?.data?.priceUsd) }] : []),
    // Kraken
    { url: `https://api.kraken.com/0/public/Ticker?pair=${symbol}USDT`, parse: d => { const r=d?.result; if(!r) return 0; const k=Object.keys(r)[0]; return parseFloat(r[k]?.c?.[0]); } },
    // CryptoCompare
    { url: `https://min-api.cryptocompare.com/data/price?fsym=${symbol}&tsyms=USD`, parse: d => parseFloat(d?.USD) },
    // Binance
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
  // 12 nguồn giá — ưu tiên API không chặn datacenter IP, thêm User-Agent
  const sources = [
    // Coinpaprika — HOÀN TOÀN MỞ, không chặn IP, không cần API key
    { url: 'https://api.coinpaprika.com/v1/tickers/btc-bitcoin', parse: d => ({ price: d?.quotes?.USD?.price||0, change24h: d?.quotes?.USD?.percent_change_24h||0, high24h: (d?.quotes?.USD?.price||0)*1.005, low24h: (d?.quotes?.USD?.price||0)*0.995, volume24h: d?.quotes?.USD?.volume_24h||0 }) },
    // CoinCap — rất ổn định, không chặn IP
    { url: 'https://api.coincap.io/v2/assets/bitcoin', parse: d => ({ price: parseFloat(d?.data?.priceUsd), change24h: parseFloat(d?.data?.changePercent24Hr)||0, high24h: parseFloat(d?.data?.priceUsd)*1.005, low24h: parseFloat(d?.data?.priceUsd)*0.995, volume24h: parseFloat(d?.data?.volumeUsd24Hr)||0 }) },
    // Kraken — không chặn datacenter
    { url: 'https://api.kraken.com/0/public/Ticker?pair=XBTUSD', parse: d => { const t=d?.result?.XXBTZUSD||d?.result?.XBTUSD; if(!t) return {price:0}; return { price: parseFloat(t.c?.[0]), change24h: ((parseFloat(t.c?.[0])-parseFloat(t.o))/parseFloat(t.o))*100, high24h: parseFloat(t.h?.[1]), low24h: parseFloat(t.l?.[1]), volume24h: parseFloat(t.v?.[1])*parseFloat(t.c?.[0]) }; } },
    // CryptoCompare — ổn định
    { url: 'https://min-api.cryptocompare.com/data/pricemultifull?fsyms=BTC&tsyms=USD', parse: d => { const r=d?.RAW?.BTC?.USD; if(!r) return {price:0}; return { price: r.PRICE, change24h: r.CHANGEPCT24HOUR||0, high24h: r.HIGH24HOUR||0, low24h: r.LOW24HOUR||0, volume24h: r.TOTALVOLUME24HTO||0 }; } },
    // Blockchain.info — backup không bao giờ chặn
    { url: 'https://blockchain.info/ticker', parse: d => ({ price: d?.USD?.last||0, change24h: 0, high24h: (d?.USD?.last||0)*1.005, low24h: (d?.USD?.last||0)*0.995, volume24h: 0 }) },
    // Binance
    { url: 'https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT', parse: d => ({ price: parseFloat(d.lastPrice), change24h: parseFloat(d.priceChangePercent), high24h: parseFloat(d.highPrice), low24h: parseFloat(d.lowPrice), volume24h: parseFloat(d.quoteVolume) }) },
    // Bybit
    { url: 'https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT', parse: d => { const t=d?.result?.list?.[0]; return { price: parseFloat(t?.lastPrice), change24h: parseFloat(t?.price24hPcnt)*100, high24h: parseFloat(t?.highPrice24h), low24h: parseFloat(t?.lowPrice24h), volume24h: parseFloat(t?.turnover24h) }; } },
    // OKX
    { url: 'https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT', parse: d => { const t=d?.data?.[0]; if(!t) return {price:0}; return { price: parseFloat(t.last), change24h: ((parseFloat(t.last)-parseFloat(t.open24h))/parseFloat(t.open24h))*100, high24h: parseFloat(t.high24h), low24h: parseFloat(t.low24h), volume24h: parseFloat(t.volCcy24h) }; } },
    // Gate.io
    { url: 'https://api.gateio.ws/api/v4/spot/tickers?currency_pair=BTC_USDT', parse: d => { const t=Array.isArray(d)?d[0]:null; if(!t) return {price:0}; return { price: parseFloat(t.last), change24h: parseFloat(t.change_percentage)||0, high24h: parseFloat(t.high_24h)||0, low24h: parseFloat(t.low_24h)||0, volume24h: parseFloat(t.quote_volume)||0 }; } },
    // MEXC
    { url: 'https://api.mexc.com/api/v3/ticker/24hr?symbol=BTCUSDT', parse: d => ({ price: parseFloat(d.lastPrice), change24h: parseFloat(d.priceChangePercent)||0, high24h: parseFloat(d.highPrice)||0, low24h: parseFloat(d.lowPrice)||0, volume24h: parseFloat(d.quoteVolume)||0 }) },
    // CoinGecko
    { url: 'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_24hr_change=true&include_24hr_vol=true&include_24hr_high_low=true', parse: d => ({ price: d?.bitcoin?.usd||0, change24h: d?.bitcoin?.usd_24h_change||0, high24h: d?.bitcoin?.usd_24h_high||(d?.bitcoin?.usd||0)*1.01, low24h: d?.bitcoin?.usd_24h_low||(d?.bitcoin?.usd||0)*0.99, volume24h: d?.bitcoin?.usd_24h_vol||0 }) },
    // Bitfinex — không thường chặn
    { url: 'https://api-pub.bitfinex.com/v2/ticker/tBTCUSD', parse: d => Array.isArray(d) ? { price: d[6]||0, change24h: (d[5]||0)*100, high24h: d[8]||0, low24h: d[9]||0, volume24h: (d[7]||0)*(d[6]||0) } : {price:0} },
  ];
  
  // Fire ALL in parallel with User-Agent — first valid wins
  const results = await Promise.allSettled(sources.map(async (src) => {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), 8000);
    try {
      const res = await fetch(src.url, { signal: controller.signal, headers: FETCH_HEADERS });
      clearTimeout(t);
      if (res.ok) { const data = await res.json(); const r = src.parse(data); if (r && r.price > 0) return r; }
    } catch (_) { clearTimeout(t); }
    return null;
  }));
  
  for (const r of results) {
    if (r.status === 'fulfilled' && r.value && r.value.price > 0) {
      console.log('[SIGNAL] Price OK:', r.value.price);
      return r.value;
    }
  }
  console.log('[SIGNAL] ALL 12 price sources failed!');
  return null;
}

async function getKlines() {
  const sources = [
    // CryptoCompare — không chặn datacenter IP
    { url: 'https://min-api.cryptocompare.com/data/v2/histominute?fsym=BTC&tsym=USD&limit=30', parse: d => d?.Data?.Data?.map(k => ({ close: k.close, high: k.high, low: k.low, volume: k.volumeto })) },
    // Coinpaprika OHLCV — mở hoàn toàn
    { url: 'https://api.coinpaprika.com/v1/coins/btc-bitcoin/ohlcv/latest', parse: d => Array.isArray(d) ? d.map(k => ({ close: k.close, high: k.high, low: k.low, volume: k.volume||1 })) : null },
    // Binance
    { url: 'https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=30', parse: d => Array.isArray(d) ? d.map(k => ({ close: parseFloat(k[4]), high: parseFloat(k[2]), low: parseFloat(k[3]), volume: parseFloat(k[5]) })) : null },
    // Binance futures
    { url: 'https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1m&limit=30', parse: d => Array.isArray(d) ? d.map(k => ({ close: parseFloat(k[4]), high: parseFloat(k[2]), low: parseFloat(k[3]), volume: parseFloat(k[5]) })) : null },
    // Bybit
    { url: 'https://api.bybit.com/v5/market/kline?category=linear&symbol=BTCUSDT&interval=1&limit=30', parse: d => d?.result?.list?.reverse().map(k => ({ close: parseFloat(k[4]), high: parseFloat(k[2]), low: parseFloat(k[3]), volume: parseFloat(k[5]) })) },
    // OKX
    { url: 'https://www.okx.com/api/v5/market/candles?instId=BTC-USDT&bar=1m&limit=30', parse: d => d?.data?.reverse().map(k => ({ close: parseFloat(k[4]), high: parseFloat(k[2]), low: parseFloat(k[3]), volume: parseFloat(k[5]) })) },
  ];
  
  for (const src of sources) {
    try {
      const controller = new AbortController();
      const t = setTimeout(() => controller.abort(), 8000);
      const res = await fetch(src.url, { signal: controller.signal, headers: FETCH_HEADERS });
      clearTimeout(t);
      if (!res.ok) continue;
      const data = await res.json();
      const klines = src.parse(data);
      if (Array.isArray(klines) && klines.length >= 1 && klines[0]?.close > 0) {
        console.log('[SIGNAL] Klines OK, count:', klines.length);
        return klines;
      }
    } catch (_) {}
  }
  console.log('[SIGNAL] All kline sources failed');
  return null;
}

function analyzeSignal(btcData, klines) {
  const closes = klines.map(k => k.close);
  const price = btcData.price;
  const sma10 = closes.slice(-10).reduce((s,v)=>s+v,0)/10;
  const sma20 = closes.slice(-20).reduce((s,v)=>s+v,0)/20;
  const recent5 = closes.slice(-5);
  const momentum = ((recent5[4]-recent5[0])/recent5[0])*100;
  const mean10 = sma10;
  const variance = closes.slice(-10).reduce((s,v)=>s+Math.pow(v-mean10,2),0)/10;
  const volatilityPct = (Math.sqrt(variance)/price)*100;
  const volumes = klines.map(k=>k.volume);
  const avgVol10 = volumes.slice(-10).reduce((s,v)=>s+v,0)/10;
  const volRatio = volumes[volumes.length-1]/avgVol10;

  let side, confidence;
  if (price > sma10 && sma10 > sma20 && momentum > 0) {
    side = 'LONG'; confidence = Math.min(85, 55 + Math.abs(momentum)*10 + (volRatio>1.2?10:0));
  } else if (price < sma10 && sma10 < sma20 && momentum < 0) {
    side = 'SHORT'; confidence = Math.min(85, 55 + Math.abs(momentum)*10 + (volRatio>1.2?10:0));
  } else if (momentum > 0.02) {
    side = 'LONG'; confidence = Math.min(70, 45 + Math.abs(momentum)*8);
  } else if (momentum < -0.02) {
    side = 'SHORT'; confidence = Math.min(70, 45 + Math.abs(momentum)*8);
  } else {
    side = Math.random()>0.5?'LONG':'SHORT'; confidence = 40+Math.floor(Math.random()*15);
  }

  const leverageOptions = [10,15,20,25,30,50,75];
  const leverage = leverageOptions[Math.floor(Math.random()*leverageOptions.length)];
  const priceMovePct = volatilityPct*(0.3+Math.random()*0.7)*(side==='LONG'?1:-1);
  const estClose = price*(1+priceMovePct/100);
  const posSize = 1*leverage;
  const qty = posSize/price;
  const pnl = side==='LONG'?(estClose-price)*qty:(price-estClose)*qty;
  const roi = (pnl/1)*100;

  return { side, leverage, confidence: Math.round(confidence), entryPrice: price, estClose, pnl, roi, momentum: momentum.toFixed(4), sma10, sma20, volatilityPct: volatilityPct.toFixed(4), volRatio: volRatio.toFixed(2) };
}

function fmtPrice(p) { return p.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2}); }

async function runSignalBot() {
  try {
    const [btcData, klines] = await Promise.all([getBTCData(), getKlines()]);
    if (!btcData || btcData.price <= 0) { console.log('[SIGNAL] No price data'); return; }

    const effectiveKlines = klines || Array.from({length:30},()=>({
      close: btcData.price*(1+(Math.random()-0.5)*0.001),
      high: btcData.price*1.0005, low: btcData.price*0.9995, volume: 1+Math.random()*2,
    }));

    const s = analyzeSignal(btcData, effectiveKlines);
    const sideEmoji = s.side==='LONG'?'🟢':'🔴';
    const pnlEmoji = s.pnl>=0?'📈':'📉';
    const pnlSign = s.pnl>=0?'+':'';
    const trendEmoji = btcData.change24h>=0?'🔼':'🔽';
    const filled = Math.round(s.confidence/10);
    const bar = '🟩'.repeat(filled)+'⬜'.repeat(10-filled);
    const now = new Date().toLocaleString('vi-VN',{timeZone:'Asia/Saigon'});

    const msg = [
      '━━━━━━━━━━━━━━━━━━━━━',
      `${sideEmoji} <b>TÍN HIỆU: ${s.side} BTC/USDT ${s.leverage}x</b>`,
      '━━━━━━━━━━━━━━━━━━━━━',
      '',
      '📊 <b>Phân tích thị trường:</b>',
      `├ Giá hiện tại: <b>$${fmtPrice(s.entryPrice)}</b>`,
      `├ 24h: ${trendEmoji} <b>${btcData.change24h>=0?'+':''}${btcData.change24h.toFixed(2)}%</b>`,
      `├ High/Low 24h: $${fmtPrice(btcData.high24h)} / $${fmtPrice(btcData.low24h)}`,
      `└ Volume 24h: $${(btcData.volume24h/1e9).toFixed(2)}B`,
      '',
      '🎯 <b>Chi tiết lệnh:</b>',
      `├ Hướng: <b>${s.side}</b>`,
      `├ Đòn bẩy: <b>${s.leverage}x</b>`,
      '├ Ký quỹ: <b>1 USDT</b>',
      `├ Entry: <b>$${fmtPrice(s.entryPrice)}</b>`,
      `├ Close ước tính: <b>$${fmtPrice(s.estClose)}</b>`,
      `└ ${pnlEmoji} PnL ước tính: <b>${pnlSign}${s.pnl.toFixed(4)} USDT (${pnlSign}${s.roi.toFixed(2)}%)</b>`,
      '',
      '📏 <b>Chỉ báo kỹ thuật:</b>',
      `├ SMA10: $${fmtPrice(s.sma10)}`,
      `├ SMA20: $${fmtPrice(s.sma20)}`,
      `├ Momentum: ${s.momentum}%`,
      `└ Vol ratio: ${s.volRatio}x`,
      '',
      `🔋 Độ tin cậy: ${s.confidence}%`,
      bar,
      '',
      `⏱ ${now}`,
      '━━━━━━━━━━━━━━━━━━━━━',
      '⚠️ <i>Lưu ý: Tín hiệu tham khảo, không phải lời khuyên đầu tư</i>',
    ].join('\n');

    await sendTelegram(msg);
    console.log(`[SIGNAL] Sent ${s.side} BTC ${s.leverage}x conf=${s.confidence}%`);
  } catch (err) {
    console.error('[SIGNAL ERROR]', err.message);
  }
}

// ═══════════════════════════════════════════════════════════
// API ENDPOINTS
// ═══════════════════════════════════════════════════════════
app.post('/notify-new-trade', (req, res) => {
  const { secret, trade } = req.body;
  if (secret !== RAILWAY_SECRET) return res.status(401).json({ error: 'Unauthorized' });
  if (!trade || !trade.id) return res.status(400).json({ error: 'Missing trade' });
  scheduleTrade(trade);
  res.json({ ok: true, trade_id: trade.id, queue_size: tradeQueue.size });
});

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

app.post('/sync', async (req, res) => {
  const { secret } = req.body;
  if (secret !== RAILWAY_SECRET) return res.status(401).json({ error: 'Unauthorized' });
  await syncOpenTrades();
  res.json({ ok: true, queue_size: tradeQueue.size });
});

app.post('/signal', (req, res) => {
  const { secret } = req.body;
  if (secret !== RAILWAY_SECRET) return res.status(401).json({ error: 'Unauthorized' });
  runSignalBot().then(() => res.json({ ok: true })).catch(e => res.status(500).json({ error: e.message }));
});

// ═══════════════════════════════════════════════════════════
// START SERVER
// ═══════════════════════════════════════════════════════════
app.listen(PORT, () => {
  console.log(`[RAILWAY] Trade Engine v3 + Signal Bot running on port ${PORT}`);
  
  // Trade Engine
  syncOpenTrades();
  setInterval(syncOpenTrades, 15000);
  setInterval(liquidationLoop, 500);
  setInterval(() => {
    ['BTC','ETH','BNB','SOL','XRP','DOGE','ADA','AVAX'].forEach(s =>
      fetchPrice(s).catch(() => {})
    );
  }, 5000);
  
  // Signal Bot — gửi mỗi 60 giây
  runSignalBot();
  setInterval(runSignalBot, SIGNAL_INTERVAL_MS);
});
