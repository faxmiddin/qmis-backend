/**
 * DOGE Stratum Proxy — Real ASIC style
 * Bitmain Antminer L3+ ulanish protokoli
 */
const WebSocket = require('ws');
const net = require('net');

const PORT = process.env.PORT || 3000;

const POOLS = [
  { host: 'pool.aikapool.com', port: 7915 },
  { host: 'prohashing.com', port: 3333 },
  { host: 'eu.litecoinpool.org', port: 3333 },
];

const wss = new WebSocket.Server({ port: PORT, perMessageDeflate: false });
console.log('[PROXY] Started on port', PORT);

wss.on('connection', (ws, req) => {
  console.log('[PROXY] Client connected from:', req.socket.remoteAddress);
  
  let tcp = null;
  let buffer = '';
  let poolIdx = 0;
  let authorized = false;
  let walletAddr = '';
  let workerName = 'worker1';

  function sendToPool(obj) {
    if (tcp && !tcp.destroyed) {
      const line = JSON.stringify(obj) + '\n';
      console.log('[PROXY] →Pool:', line.trim());
      tcp.write(line);
    }
  }

  function tryPool(idx) {
    if (idx >= POOLS.length) {
      console.log('[PROXY] All pools failed');
      try { ws.close(1011, 'All pools failed'); } catch(_) {}
      return;
    }

    const pool = POOLS[idx];
    console.log('[PROXY] Trying:', pool.host + ':' + pool.port);

    tcp = new net.Socket();
    buffer = '';
    authorized = false;

    // Real ASIC timeout: 15 soniya
    const timeout = setTimeout(() => {
      console.log('[PROXY] Timeout:', pool.host);
      tcp.destroy();
      tryPool(idx + 1);
    }, 15000);

    tcp.connect(pool.port, pool.host, () => {
      console.log('[PROXY] TCP OK:', pool.host);
      clearTimeout(timeout);

      // Real Antminer L3+ subscribe format
      sendToPool({
        id: 1,
        method: 'mining.subscribe',
        params: [
          'cgminer/4.10.0',
          null,
          pool.host,
          pool.port
        ]
      });
    });

    tcp.on('data', (chunk) => {
      buffer += chunk.toString('utf8');
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';

      for (const line of lines) {
        const t = line.trim();
        if (!t) continue;

        console.log('[PROXY] Pool→:', t.substring(0, 200));

        // Pool javobini parse qilamiz
        let msg;
        try { msg = JSON.parse(t); } catch(e) { 
          // JSON emas — to'g'ridan brauzerga yuboramiz
          if (ws.readyState === WebSocket.OPEN) ws.send(t);
          continue;
        }

        // Subscribe javob — pool extranonce beradi
        if (msg.id === 1 && msg.result) {
          console.log('[PROXY] Subscribe OK, extranonce:', msg.result[1]);
          // Brauzerga yuboramiz
          if (ws.readyState === WebSocket.OPEN) ws.send(t);
          
          // Authorize ni pool ga yuboramiz (wallet brauzerdan keladi)
          if (walletAddr) {
            sendToPool({
              id: 2,
              method: 'mining.authorize',
              params: [walletAddr + '.' + workerName, 'x']
            });
          }
        }

        // Authorize javob
        else if (msg.id === 2) {
          if (msg.result === true) {
            authorized = true;
            console.log('[PROXY] AUTHORIZED! Wallet:', walletAddr);
          } else {
            console.log('[PROXY] Auth FAILED:', JSON.stringify(msg.error));
          }
          if (ws.readyState === WebSocket.OPEN) ws.send(t);
        }

        // Mining notify, difficulty, extranonce — to'g'ri yuborish
        else {
          if (ws.readyState === WebSocket.OPEN) ws.send(t);
        }
      }
    });

    tcp.on('error', (err) => {
      console.log('[PROXY] TCP Error:', pool.host, err.message);
      clearTimeout(timeout);
      tcp.destroy();
      tryPool(idx + 1);
    });

    tcp.on('close', () => {
      console.log('[PROXY] TCP Closed:', pool.host);
      if (ws.readyState === WebSocket.OPEN) {
        ws.close(1000, 'Pool closed');
      }
    });
  }

  // Brauzerdan kelgan xabarlar
  ws.on('message', (data) => {
    const raw = data.toString();
    console.log('[PROXY] Browser→:', raw.substring(0, 200));

    let msg;
    try { msg = JSON.parse(raw); } catch(e) { return; }

    // Subscribe — wallet manzilini saqlaymiz
    if (msg.method === 'mining.subscribe') {
      // Pool ga o'zimiz subscribe qilamiz (yuqorida)
      // Lekin wallet ni keyingi authorize uchun saqlaymiz
      console.log('[PROXY] Got subscribe from browser');
      // Pool ulanishni boshlaymiz
      tryPool(poolIdx);
    }

    // Authorize — wallet manzilini olamiz
    else if (msg.method === 'mining.authorize') {
      walletAddr = msg.params[0].split('.')[0];
      workerName = msg.params[0].split('.')[1] || 'worker1';
      console.log('[PROXY] Wallet:', walletAddr, 'Worker:', workerName);

      // Pool ga yuboramiz
      sendToPool({
        id: 2,
        method: 'mining.authorize',
        params: [walletAddr + '.' + workerName, 'x']
      });
    }

    // Share submit
    else if (msg.method === 'mining.submit') {
      sendToPool(msg);
    }

    // Boshqa xabarlar
    else {
      sendToPool(msg);
    }
  });

  ws.on('close', () => {
    console.log('[PROXY] Browser disconnected');
    if (tcp) tcp.destroy();
  });

  ws.on('error', (err) => {
    console.error('[PROXY] WS Error:', err.message);
    if (tcp) tcp.destroy();
  });
});
