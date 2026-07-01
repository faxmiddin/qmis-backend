/**
 * DOGE Stratum Proxy
 * prohashing.com — ro'yxatdansiz DOGE wallet ishlaydi
 * Format: username=DOGE_address, password=algo=scrypt,c=DOGE
 */
const WebSocket = require('ws');
const net = require('net');

const PORT = process.env.PORT || 3000;

const POOLS = [
  // prohashing — DOGE wallet to'g'ridan qabul qiladi
  { host: 'prohashing.com', port: 3333, 
    passFormat: 'algo=scrypt,c=DOGE,n=worker1' },
  // backup
  { host: 'eu.litecoinpool.org', port: 3333,
    passFormat: 'x' },
  { host: 'us.litecoinpool.org', port: 3333,
    passFormat: 'x' },
];

const wss = new WebSocket.Server({ port: PORT, perMessageDeflate: false });
console.log('[PROXY] DOGE Stratum Proxy started, port:', PORT);

wss.on('connection', (ws) => {
  console.log('[PROXY] Client connected');

  let tcp = null;
  let buffer = '';
  let walletAddr = 'DLg6Lj4e4sXDz42LT2qTLZWy6q1Wqkiukj';
  let currentPoolIdx = 0;

  function sendPool(obj) {
    if (tcp && !tcp.destroyed) {
      const line = JSON.stringify(obj) + '\n';
      console.log('[PROXY] →Pool:', line.trim().substring(0, 150));
      tcp.write(line);
    }
  }

  function connectPool(idx) {
    if (idx >= POOLS.length) {
      console.log('[PROXY] All pools failed');
      try { ws.close(1011, 'All pools failed'); } catch(_) {}
      return;
    }

    const pool = POOLS[idx];
    currentPoolIdx = idx;
    console.log('[PROXY] Trying:', pool.host + ':' + pool.port);

    tcp = new net.Socket();
    buffer = '';

    const timer = setTimeout(() => {
      console.log('[PROXY] Timeout:', pool.host);
      tcp.destroy();
      connectPool(idx + 1);
    }, 12000);

    tcp.connect(pool.port, pool.host, () => {
      clearTimeout(timer);
      console.log('[PROXY] TCP OK:', pool.host);

      // Subscribe
      sendPool({
        id: 1,
        method: 'mining.subscribe',
        params: ['cgminer/4.10.0', null, pool.host, pool.port]
      });

      // Authorize — darhol yuboramiz
      setTimeout(() => {
        const pass = pool.passFormat || 'x';
        sendPool({
          id: 2,
          method: 'mining.authorize',
          params: [walletAddr + '.worker1', pass]
        });
        console.log('[PROXY] Auth sent:', walletAddr.substring(0,15) + '...', 'pass:', pass);
      }, 500);
    });

    tcp.on('data', (chunk) => {
      buffer += chunk.toString('utf8');
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';

      for (const line of lines) {
        const t = line.trim();
        if (!t) continue;
        console.log('[PROXY] Pool→:', t.substring(0, 200));

        if (ws.readyState === WebSocket.OPEN) ws.send(t);

        try {
          const msg = JSON.parse(t);
          if (msg.id === 2) {
            if (msg.result === true) {
              console.log('[PROXY] ✅ AUTHORIZED! Mining active!');
            } else {
              console.log('[PROXY] ❌ Auth failed:', JSON.stringify(msg.error), '— trying next pool');
              // Keyingi pool
              tcp.destroy();
              connectPool(idx + 1);
            }
          }
        } catch(_) {}
      }
    });

    tcp.on('error', (err) => {
      clearTimeout(timer);
      console.log('[PROXY] Error:', pool.host, err.message);
      tcp.destroy();
      connectPool(idx + 1);
    });

    tcp.on('close', () => {
      clearTimeout(timer);
      console.log('[PROXY] Closed:', pool.host);
      if (ws.readyState === WebSocket.OPEN) ws.close(1000, 'Pool closed');
    });
  }

  ws.on('message', (data) => {
    const raw = data.toString();
    console.log('[PROXY] Browser→:', raw.substring(0, 150));

    let msg;
    try { msg = JSON.parse(raw); } catch(e) { return; }

    if (msg.method === 'mining.subscribe') {
      // Wallet manzilini olish
      connectPool(0);
    }
    else if (msg.method === 'mining.authorize') {
      // Wallet manzilini yangilaymiz
      walletAddr = (msg.params[0] || '').split('.')[0] || walletAddr;
      console.log('[PROXY] Wallet updated:', walletAddr.substring(0,15) + '...');
    }
    else if (msg.method === 'mining.submit') {
      console.log('[PROXY] ⛏ Share submit!');
      sendPool(msg);
    }
    else {
      sendPool(msg);
    }
  });

  ws.on('close', () => {
    console.log('[PROXY] Browser disconnected');
    if (tcp) tcp.destroy();
  });

  ws.on('error', () => {
    if (tcp) tcp.destroy();
  });
});
