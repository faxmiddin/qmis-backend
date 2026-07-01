const WebSocket = require('ws');
const net = require('net');

const PORT = process.env.PORT || 3000;

const POOLS = [
  { host: 'pool.aikapool.com', port: 7915 },
  { host: 'prohashing.com', port: 3333 },
  { host: 'stratum.litecoinpool.org', port: 3333 },
];

const wss = new WebSocket.Server({ 
  port: PORT,
  perMessageDeflate: false 
});

console.log(`[PROXY] DOGE Stratum Proxy started on port ${PORT}`);

wss.on('connection', (ws, req) => {
  const ip = req.socket.remoteAddress;
  console.log(`[PROXY] Client connected: ${ip}`);
  
  let tcpSocket = null;
  let buffer = '';
  let currentPoolIdx = 0;
  let connected = false;

  function tryPool(idx) {
    if (idx >= POOLS.length) {
      console.log('[PROXY] All pools exhausted');
      ws.close(1011, 'No pools available');
      return;
    }

    const pool = POOLS[idx];
    console.log(`[PROXY] Connecting to ${pool.host}:${pool.port}`);

    const tcp = new net.Socket();
    tcpSocket = tcp;
    buffer = '';
    connected = false;

    const connectTimeout = setTimeout(() => {
      if (!connected) {
        console.log(`[PROXY] Connect timeout: ${pool.host}`);
        tcp.destroy();
        tryPool(idx + 1);
      }
    }, 10000);

    tcp.connect(pool.port, pool.host, () => {
      console.log(`[PROXY] TCP connected: ${pool.host}`);
      
      // Subscribe
      const msg = JSON.stringify({
        id: 1,
        method: 'mining.subscribe',
        params: ['cgminer/4.10.0', null]
      }) + '\n';
      tcp.write(msg);
      console.log(`[PROXY] Subscribe sent to ${pool.host}`);

      // Data timeout — javob kelmasa keyingi pool
      const dataTimeout = setTimeout(() => {
        if (!connected) {
          console.log(`[PROXY] No data from ${pool.host}, trying next`);
          tcp.destroy();
          tryPool(idx + 1);
        }
      }, 8000);

      tcp.on('data', (chunk) => {
        if (!connected) {
          connected = true;
          clearTimeout(connectTimeout);
          clearTimeout(dataTimeout);
          console.log(`[PROXY] Got data from ${pool.host} ✓`);
        }

        buffer += chunk.toString('utf8');
        const lines = buffer.split('\n');
        buffer = lines.pop(); // Oxirgi to'liq bo'lmagan qator

        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed) continue;
          
          console.log(`[PROXY] Pool→Client: ${trimmed.substring(0, 150)}`);
          
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(trimmed);
          }
        }
      });

      tcp.on('error', (err) => {
        clearTimeout(connectTimeout);
        clearTimeout(dataTimeout);
        console.error(`[PROXY] TCP error ${pool.host}: ${err.message}`);
        if (!connected) tryPool(idx + 1);
        else if (ws.readyState === WebSocket.OPEN) ws.close(1011, err.message);
      });

      tcp.on('close', () => {
        clearTimeout(connectTimeout);
        clearTimeout(dataTimeout);
        console.log(`[PROXY] TCP closed: ${pool.host}`);
        if (ws.readyState === WebSocket.OPEN) ws.close(1000, 'Pool disconnected');
      });
    });

    tcp.on('error', (err) => {
      clearTimeout(connectTimeout);
      console.error(`[PROXY] Connect error ${pool.host}: ${err.message}`);
      tryPool(idx + 1);
    });
  }

  // Client → Pool
  ws.on('message', (data) => {
    const msg = data.toString();
    console.log(`[PROXY] Client→Pool: ${msg.substring(0, 150)}`);
    if (tcpSocket && !tcpSocket.destroyed) {
      tcpSocket.write(msg + '\n');
    }
  });

  ws.on('close', (code, reason) => {
    console.log(`[PROXY] Client disconnected: ${code} ${reason}`);
    if (tcpSocket) tcpSocket.destroy();
  });

  ws.on('error', (err) => {
    console.error(`[PROXY] WS error: ${err.message}`);
    if (tcpSocket) tcpSocket.destroy();
  });

  // Start connecting
  tryPool(0);
});

wss.on('error', (err) => {
  console.error('[PROXY] Server error:', err.message);
});

process.on('uncaughtException', (err) => {
  console.error('[PROXY] Uncaught:', err.message);
});
