const WebSocket = require('ws');
const net = require('net');

const PORT = process.env.PORT || 3000;

// aikapool DOGE — barcha portlarni sinash
const POOL_CONFIGS = [
  { host: '37.26.136.250', port: 7915, name: 'aikapool:7915' },
  { host: '37.26.136.250', port: 3032, name: 'aikapool:3032' },
  { host: '37.26.136.250', port: 80,   name: 'aikapool:80' },
  { host: '37.26.136.250', port: 443,  name: 'aikapool:443' },
];

const USERNAME = 'faxmiddin';
const PASSWORD = 'Algaritm.30,d=64'; // d=64 static difficulty

const wss = new WebSocket.Server({ port: PORT, perMessageDeflate: false });
console.log('[PROXY] DOGE Stratum Proxy started, port:', PORT);

wss.on('connection', (ws) => {
  console.log('[PROXY] Client connected');
  let tcp = null;
  let buffer = '';
  let configIdx = 0;

  function tryConfig(idx) {
    if (idx >= POOL_CONFIGS.length) {
      console.log('[PROXY] All configs failed');
      try { ws.close(1011, 'All failed'); } catch(_) {}
      return;
    }

    const cfg = POOL_CONFIGS[idx];
    console.log('[PROXY] Trying:', cfg.name);

    tcp = new net.Socket();
    buffer = '';

    const timer = setTimeout(() => {
      console.log('[PROXY] Timeout:', cfg.name);
      tcp.destroy();
      tryConfig(idx + 1);
    }, 10000);

    tcp.connect(cfg.port, cfg.host, () => {
      clearTimeout(timer);
      console.log('[PROXY] ✅ Connected:', cfg.name);

      const sub = JSON.stringify({
        id: 1,
        method: 'mining.subscribe',
        params: ['cgminer/4.10.0', null, 'pool.aikapool.com', cfg.port]
      });
      tcp.write(sub + '\n');

      setTimeout(() => {
        const auth = JSON.stringify({
          id: 2,
          method: 'mining.authorize',
          params: [USERNAME, PASSWORD]
        });
        tcp.write(auth + '\n');
        console.log('[PROXY] Auth sent:', USERNAME, 'via', cfg.name);
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
          if (msg.id === 1 && msg.result) console.log('[PROXY] ✅ Subscribe OK via', cfg.name);
          if (msg.id === 2 && msg.result === true) console.log('[PROXY] ✅✅✅ AUTHORIZED via', cfg.name);
          if (msg.id === 2 && msg.result === false) console.log('[PROXY] ❌ Auth failed via', cfg.name);
        } catch(_) {}
      }
    });

    tcp.on('error', (err) => {
      clearTimeout(timer);
      console.log('[PROXY] Error:', cfg.name, err.message);
      tcp.destroy();
      tryConfig(idx + 1);
    });

    tcp.on('close', () => {
      clearTimeout(timer);
      console.log('[PROXY] Closed:', cfg.name);
      if (ws.readyState === WebSocket.OPEN) ws.close(1000, 'Pool closed');
    });
  }

  ws.on('message', (data) => {
    const raw = data.toString();
    let msg;
    try { msg = JSON.parse(raw); } catch(e) { return; }
    if (msg.method === 'mining.subscribe') {
      tryConfig(0);
    } else if (msg.method === 'mining.submit') {
      console.log('[PROXY] ⛏ Share submitted!');
      if (tcp && !tcp.destroyed) tcp.write(raw + '\n');
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
