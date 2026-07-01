/**
 * DOGE Stratum Proxy — eu.litecoinpool.org
 * Real ASIC (cgminer) format
 */
const WebSocket = require('ws');
const net = require('net');

const PORT = process.env.PORT || 3000;

// FAQAT ISHLAYDIGANLAR!
const POOLS = [
  { host: 'eu.litecoinpool.org', port: 3333 },
  { host: 'us.litecoinpool.org', port: 3333 },
];

const wss = new WebSocket.Server({ port: PORT, perMessageDeflate: false });
console.log('[PROXY] Started on port', PORT);
console.log('[PROXY] Primary pool: eu.litecoinpool.org:3333');

wss.on('connection', (ws, req) => {
  console.log('[PROXY] Client connected');

  let tcp = null;
  let buffer = '';
  let walletAddr = '';
  let workerName = 'worker1';
  let subscribeOK = false;

  function sendToPool(obj) {
    if (tcp && !tcp.destroyed) {
      const line = JSON.stringify(obj) + '\n';
      console.log('[PROXY] →Pool:', line.trim().substring(0, 150));
      tcp.write(line);
    }
  }

  function sendToBrowser(data) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(data);
    }
  }

  function connectPool(idx) {
    if (idx >= POOLS.length) {
      console.log('[PROXY] All pools failed');
      try { ws.close(1011, 'All pools failed'); } catch(_) {}
      return;
    }

    const pool = POOLS[idx];
    console.log('[PROXY] Connecting:', pool.host + ':' + pool.port);

    tcp = new net.Socket();
    buffer = '';
    subscribeOK = false;

    tcp.setTimeout(12000);

    tcp.connect(pool.port, pool.host, () => {
      console.log('[PROXY] TCP OK:', pool.host);

      // Real cgminer subscribe
      sendToPool({
        id: 1,
        method: 'mining.subscribe',
        params: ['cgminer/4.10.0', null, pool.host, pool.port]
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

        // Brauzerga yuboramiz
        sendToBrowser(t);

        // Subscribe OK — authorize yuboramiz
        let msg;
        try { msg = JSON.parse(t); } catch(e) { continue; }

        if (msg.id === 1 && msg.result && !subscribeOK) {
          subscribeOK = true;
          console.log('[PROXY] Subscribe OK! Extranonce:', msg.result[1]);

          // Wallet bo'lsa authorize yuboramiz
          if (walletAddr) {
            setTimeout(() => {
              sendToPool({
                id: 2,
                method: 'mining.authorize',
                params: [walletAddr + '.' + workerName, 'x']
              });
            }, 100);
          }
        }

        if (msg.id === 2) {
          if (msg.result === true) {
            console.log('[PROXY] ✅ AUTHORIZED! Wallet:', walletAddr);
          } else {
            console.log('[PROXY] ❌ Auth failed:', JSON.stringify(msg.error));
          }
        }
      }
    });

    tcp.on('timeout', () => {
      console.log('[PROXY] Timeout:', pool.host);
      tcp.destroy();
      connectPool(idx + 1);
    });

    tcp.on('error', (err) => {
      console.log('[PROXY] Error:', pool.host, err.message);
      tcp.destroy();
      connectPool(idx + 1);
    });

    tcp.on('close', () => {
      console.log('[PROXY] Pool closed:', pool.host);
      if (ws.readyState === WebSocket.OPEN) {
        ws.close(1000, 'Pool closed');
      }
    });
  }

  // Browser → Pool
  ws.on('message', (data) => {
    const raw = data.toString();
    console.log('[PROXY] Browser→:', raw.substring(0, 200));

    let msg;
    try { msg = JSON.parse(raw); } catch(e) { return; }

    if (msg.method === 'mining.subscribe') {
      // Pool ga ulanamiz — subscribe brauzerdan keldi
      connectPool(0);
    }
    else if (msg.method === 'mining.authorize') {
      // Wallet manzilini saqlaymiz
      const parts = (msg.params[0] || '').split('.');
      walletAddr = parts[0];
      workerName = parts[1] || 'worker1';
      console.log('[PROXY] Wallet:', walletAddr.substring(0, 15) + '...');

      // Pool ga yuboramiz
      sendToPool({
        id: 2,
        method: 'mining.authorize',
        params: [walletAddr + '.' + workerName, 'x']
      });
    }
    else if (msg.method === 'mining.submit') {
      console.log('[PROXY] Share submit!');
      sendToPool(msg);
    }
    else {
      sendToPool(msg);
    }
  });

  ws.on('close', () => {
    console.log('[PROXY] Browser disconnected');
    if (tcp) tcp.destroy();
  });

  ws.on('error', (err) => {
    console.error('[PROXY] WS error:', err.message);
    if (tcp) tcp.destroy();
  });
});
