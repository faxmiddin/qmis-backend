/**
 * DOGE Stratum WebSocket→TCP Proxy
 * Node.js — Railway.app uchun
 */

const WebSocket = require('ws');
const net = require('net');

const PORT = process.env.PORT || 3000;
const POOL_HOST = 'pool.aikapool.com';
const POOL_PORT = 7915;

const wss = new WebSocket.Server({ port: PORT });

console.log(`DOGE Stratum Proxy started on port ${PORT}`);
console.log(`Pool: ${POOL_HOST}:${POOL_PORT}`);

wss.on('connection', (ws, req) => {
  console.log('Browser ulandi:', req.socket.remoteAddress);

  let buffer = '';
  let tcpConnected = false;

  // Pool ga TCP ulanish
  const tcp = new net.Socket();

  tcp.connect(POOL_PORT, POOL_HOST, () => {
    console.log('Pool ga ulandi:', POOL_HOST);
    tcpConnected = true;
  });

  // Pool → Browser
  tcp.on('data', (data) => {
    buffer += data.toString();
    const lines = buffer.split('\n');
    buffer = lines.pop();
    for (const line of lines) {
      const t = line.trim();
      if (t && ws.readyState === WebSocket.OPEN) {
        console.log('Pool→Browser:', t.substring(0, 80));
        ws.send(t);
      }
    }
  });

  tcp.on('error', (err) => {
    console.error('TCP xato:', err.message);
    ws.close(1011, 'Pool error: ' + err.message);
  });

  tcp.on('close', () => {
    console.log('Pool ulanishi yopildi');
    if (ws.readyState === WebSocket.OPEN) {
      ws.close(1000, 'Pool closed');
    }
  });

  // Browser → Pool
  ws.on('message', (data) => {
    const msg = data.toString();
    console.log('Browser→Pool:', msg.substring(0, 80));
    if (tcpConnected) {
      tcp.write(msg + '\n');
    }
  });

  ws.on('close', () => {
    console.log('Browser uzildi');
    tcp.destroy();
  });

  ws.on('error', (err) => {
    console.error('WS xato:', err.message);
    tcp.destroy();
  });
});
