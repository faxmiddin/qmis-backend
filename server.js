const WebSocket = require('ws');
const net = require('net');

const PORT = process.env.PORT || 3000;

// DOGE Scrypt poollar
const POOLS = [
  { host: 'stratum.litecoinpool.org', port: 3333 },
  { host: 'pool.aikapool.com', port: 7915 },
  { host: 'doge.portal.io', port: 4444 },
];

const server = new WebSocket.Server({ port: PORT });
console.log('DOGE Stratum Proxy v4 started on port', PORT);

server.on('connection', (ws, req) => {
  console.log('Client connected');
  connectToPool(ws, 0);
});

function connectToPool(ws, idx) {
  if (idx >= POOLS.length) {
    console.error('All pools failed');
    try { ws.close(1011, 'All pools failed'); } catch(_) {}
    return;
  }

  const pool = POOLS[idx];
  console.log(`Trying pool ${idx + 1}/${POOLS.length}: ${pool.host}:${pool.port}`);

  const tcp = new net.Socket();
  let buffer = '';
  let gotData = false;
  let timer;

  tcp.setTimeout(10000); // 10 soniya timeout

  tcp.connect(pool.port, pool.host, () => {
    console.log('TCP connected to:', pool.host);

    // Subscribe yuborish
    const sub = JSON.stringify({
      id: 1,
      method: 'mining.subscribe',
      params: ['cgminer/4.10.0', null]
    });
    tcp.write(sub + '\n');
    console.log('Subscribe sent');

    // 8 soniyada javob kelmasa keyingi pool
    timer = setTimeout(() => {
      if (!gotData) {
        console.log('No response from', pool.host, '— trying next');
        tcp.destroy();
        connectToPool(ws, idx + 1);
      }
    }, 8000);
  });

  tcp.on('data', (data) => {
    if (!gotData) {
      gotData = true;
      clearTimeout(timer);
      console.log('Got data from pool:', pool.host);
    }

    buffer += data.toString();
    const lines = buffer.split('\n');
    buffer = lines.pop();

    for (const line of lines) {
      const t = line.trim();
      if (t && ws.readyState === WebSocket.OPEN) {
        console.log('Pool→Client:', t.substring(0, 120));
        ws.send(t);
      }
    }
  });

  tcp.on('timeout', () => {
    console.log('TCP timeout:', pool.host);
    tcp.destroy();
    if (!gotData) connectToPool(ws, idx + 1);
  });

  tcp.on('error', (err) => {
    console.error('TCP error:', pool.host, err.message);
    clearTimeout(timer);
    tcp.destroy();
    if (!gotData) connectToPool(ws, idx + 1);
  });

  tcp.on('close', () => {
    console.log('Pool closed:', pool.host);
    clearTimeout(timer);
    if (ws.readyState === WebSocket.OPEN) {
      ws.close(1000, 'Pool closed');
    }
  });

  ws.on('message', (data) => {
    const msg = data.toString();
    console.log('Client→Pool:', msg.substring(0, 120));
    if (!tcp.destroyed) {
      tcp.write(msg + '\n');
    }
  });

  ws.on('close', () => {
    console.log('Client disconnected');
    clearTimeout(timer);
    tcp.destroy();
  });

  ws.on('error', () => {
    tcp.destroy();
  });
}
