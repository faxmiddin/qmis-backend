const WebSocket = require('ws');
const net = require('net');

const PORT = process.env.PORT || 3000;

// aikapool.com IP manzili bilan (DNS muammosi hal)
const POOL_HOST = '37.26.136.250'; // pool.aikapool.com IP
const POOL_PORT = 7915;
const USERNAME = 'faxmiddin_yoldoshev.worker1';
const PASSWORD = 'x';

const wss = new WebSocket.Server({ port: PORT, perMessageDeflate: false });
console.log('[PROXY] DOGE Stratum Proxy started, port:', PORT);
console.log('[PROXY] Pool IP:', POOL_HOST + ':' + POOL_PORT);
console.log('[PROXY] User:', USERNAME);

wss.on('connection', (ws) => {
  console.log('[PROXY] Client connected');

  let tcp = null;
  let buffer = '';

  function sendPool(obj) {
    if (tcp && !tcp.destroyed) {
      const line = JSON.stringify(obj) + '\n';
      console.log('[PROXY] →Pool:', line.trim().substring(0, 200));
      tcp.write(line);
    }
  }

  tcp = new net.Socket();

  const timer = setTimeout(() => {
    console.log('[PROXY] Connection timeout');
    tcp.destroy();
    try { ws.close(1011, 'Timeout'); } catch(_) {}
  }, 15000);

  tcp.connect(POOL_PORT, POOL_HOST, () => {
    clearTimeout(timer);
    console.log('[PROXY] ✅ TCP connected to aikapool!');

    // Subscribe
    sendPool({
      id: 1,
      method: 'mining.subscribe',
      params: ['cgminer/4.10.0', null, 'pool.aikapool.com', POOL_PORT]
    });

    // Authorize
    setTimeout(() => {
      sendPool({
        id: 2,
        method: 'mining.authorize',
        params: [USERNAME, PASSWORD]
      });
      console.log('[PROXY] Auth sent:', USERNAME);
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
        if (msg.id === 1 && msg.result) {
          console.log('[PROXY] ✅ Subscribe OK! Extranonce:', msg.result[1]);
        }
        if (msg.id === 2) {
          if (msg.result === true) {
            console.log('[PROXY] ✅✅✅ AUTHORIZED! Real DOGE mining active!');
          } else {
            console.log('[PROXY] ❌ Auth failed:', JSON.stringify(msg.error));
          }
        }
      } catch(_) {}
    }
  });

  tcp.on('error', (err) => {
    clearTimeout(timer);
    console.log('[PROXY] TCP Error:', err.message);
    try { ws.close(1011, err.message); } catch(_) {}
  });

  tcp.on('close', () => {
    clearTimeout(timer);
    console.log('[PROXY] Pool closed');
    if (ws.readyState === WebSocket.OPEN) ws.close(1000, 'Pool closed');
  });

  ws.on('message', (data) => {
    const raw = data.toString();
    let msg;
    try { msg = JSON.parse(raw); } catch(e) { return; }
    if (msg.method === 'mining.submit') {
      console.log('[PROXY] ⛏ Share submitted!');
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
