const WebSocket = require('ws');
const net = require('net');

const PORT = process.env.PORT || 3000;
const POOL_HOST = 'pool.aikapool.com';
const POOL_PORT = 7915;

const wss = new WebSocket.Server({ port: PORT });

console.log(`DOGE Stratum Proxy started on port ${PORT}`);

wss.on('connection', (ws, req) => {
  console.log('New connection from:', req.headers.origin || 'unknown');

  let buffer = '';
  const tcp = new net.Socket();

  tcp.connect(POOL_PORT, POOL_HOST, () => {
    console.log('Connected to pool:', POOL_HOST + ':' + POOL_PORT);
  });

  tcp.on('data', (data) => {
    buffer += data.toString();
    const lines = buffer.split('\n');
    buffer = lines.pop();
    for (const line of lines) {
      const t = line.trim();
      if (t && ws.readyState === WebSocket.OPEN) {
        console.log('Pool→Client:', t.substring(0, 100));
        ws.send(t);
      }
    }
  });

  tcp.on('error', (err) => {
    console.error('TCP error:', err.message);
    try { ws.close(1011, err.message); } catch(_) {}
  });

  tcp.on('close', () => {
    console.log('Pool connection closed');
    try { ws.close(1000, 'Pool closed'); } catch(_) {}
  });

  ws.on('message', (data) => {
    const msg = data.toString();
    console.log('Client→Pool:', msg.substring(0, 100));
    tcp.write(msg + '\n');
  });

  ws.on('close', () => {
    console.log('Client disconnected');
    tcp.destroy();
  });

  ws.on('error', (err) => {
    console.error('WS error:', err.message);
    tcp.destroy();
  });
});

// Health check
const http = require('http');
http.createServer((req, res) => {
  res.writeHead(200);
  res.end('DOGE Stratum Proxy OK\n');
}).listen(PORT + 1, () => {
  console.log('Health check on port', PORT + 1);
});
