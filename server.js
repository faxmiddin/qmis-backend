// server.js — DOGE COORDINATOR SERVER
// Bir nechta browser clientni bitta pool ulanishga birlashtiradi
// Har client alohida "sub-worker" sifatida ishlaydi

const WebSocket = require('ws');
const net = require('net');

const PORT = process.env.PORT || 3000;

const POOL_HOST = '37.26.136.250'; // pool.aikapool.com
const POOL_PORT = 7915;
const USERNAME = 'faxmiddin';
const PASSWORD = 'x';

// ============================================================
// COORDINATOR STATE
// ============================================================
const clients = new Map(); // clientId -> WebSocket
let poolSocket = null;
let poolBuffer = '';
let currentJob = null;
let currentDifficulty = 1;
let extranonce1 = '';
let extranonce2Size = 4;
let authorized = false;
let nextClientId = 1;

let totalSharesAccepted = 0;
let totalSharesRejected = 0;
let totalHashrate = 0; // clientlar hisobot qilgan jami hashrate

console.log('[COORDINATOR] Starting...');

// ============================================================
// POOL CONNECTION (bitta ulanish, hamma client uchun umumiy)
// ============================================================
function connectPool() {
  console.log('[COORDINATOR] Connecting to pool:', POOL_HOST + ':' + POOL_PORT);
  poolSocket = new net.Socket();
  poolBuffer = '';
  authorized = false;

  poolSocket.connect(POOL_PORT, POOL_HOST, () => {
    console.log('[COORDINATOR] Pool TCP connected');
    sendToPool({ id: 1, method: 'mining.subscribe', params: ['doge-coordinator/1.0', null] });
  });

  poolSocket.on('data', (chunk) => {
    poolBuffer += chunk.toString('utf8');
    const lines = poolBuffer.split('\n');
    poolBuffer = lines.pop() || '';

    for (const line of lines) {
      const t = line.trim();
      if (!t) continue;
      handlePoolMessage(t);
    }
  });

  poolSocket.on('error', (err) => {
    console.error('[COORDINATOR] Pool error:', err.message);
  });

  poolSocket.on('close', () => {
    console.log('[COORDINATOR] Pool closed, reconnecting in 5s...');
    authorized = false;
    setTimeout(connectPool, 5000);
  });
}

function sendToPool(obj) {
  if (poolSocket && !poolSocket.destroyed) {
    const line = JSON.stringify(obj) + '\n';
    poolSocket.write(line);
  }
}

function handlePoolMessage(line) {
  let msg;
  try { msg = JSON.parse(line); } catch(e) { return; }

  console.log('[POOL→]', line.substring(0, 150));

  // Subscribe response
  if (msg.id === 1 && msg.result) {
    extranonce1 = msg.result[1] || '';
    extranonce2Size = msg.result[2] || 4;
    console.log('[COORDINATOR] Subscribed, extranonce1=' + extranonce1);
    sendToPool({ id: 2, method: 'mining.authorize', params: [USERNAME, PASSWORD] });
  }

  // Authorize response
  if (msg.id === 2) {
    authorized = (msg.result === true);
    console.log('[COORDINATOR] Authorized:', authorized);
    broadcastToClients({ type: 'auth', authorized });
  }

  // New job
  if (msg.method === 'mining.notify') {
    currentJob = msg.params;
    console.log('[COORDINATOR] New job:', currentJob[0], '-> broadcasting to', clients.size, 'clients');
    broadcastToClients({
      type: 'job',
      job: currentJob,
      extranonce1,
      extranonce2Size,
      difficulty: currentDifficulty
    });
  }

  // Difficulty change
  if (msg.method === 'mining.set_difficulty') {
    currentDifficulty = msg.params[0];
    console.log('[COORDINATOR] Difficulty:', currentDifficulty);
    broadcastToClients({ type: 'difficulty', difficulty: currentDifficulty });
  }

  // Share submit response
  if (msg.id >= 1000) {
    const clientId = Math.floor(msg.id / 1000);
    const client = clients.get(clientId);
    if (msg.result === true) {
      totalSharesAccepted++;
      console.log('[COORDINATOR] ✅ SHARE ACCEPTED! Total:', totalSharesAccepted);
    } else {
      totalSharesRejected++;
      console.log('[COORDINATOR] ❌ Share rejected:', JSON.stringify(msg.error));
    }
    if (client && client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify({
        type: 'shareResult',
        accepted: msg.result === true,
        error: msg.error
      }));
    }
    broadcastStats();
  }
}

function broadcastToClients(data) {
  const msg = JSON.stringify(data);
  for (const [id, ws] of clients) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(msg);
    }
  }
}

function broadcastStats() {
  broadcastToClients({
    type: 'stats',
    clients: clients.size,
    totalHashrate,
    accepted: totalSharesAccepted,
    rejected: totalSharesRejected
  });
}

// ============================================================
// WEBSOCKET SERVER (clientlar shu yerga ulanadi)
// ============================================================
const wss = new WebSocket.Server({ port: PORT, perMessageDeflate: false });
console.log('[COORDINATOR] WebSocket server on port', PORT);

wss.on('connection', (ws) => {
  const clientId = nextClientId++;
  clients.set(clientId, ws);
  ws.clientHashrate = 0;
  console.log('[COORDINATOR] Client', clientId, 'connected. Total clients:', clients.size);

  // Darhol holat va joriy job yuboramiz
  ws.send(JSON.stringify({ type: 'welcome', clientId }));
  if (authorized && currentJob) {
    ws.send(JSON.stringify({
      type: 'job',
      job: currentJob,
      extranonce1,
      extranonce2Size,
      difficulty: currentDifficulty
    }));
  }
  broadcastStats();

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch(e) { return; }

    if (msg.type === 'submit') {
      // Client share topdi -> pool ga uzatamiz
      // Unique id: clientId * 1000 + kichik counter (kolliziyani oldini olish uchun)
      const submitId = clientId * 1000 + (Date.now() % 1000);
      sendToPool({
        id: submitId,
        method: 'mining.submit',
        params: [USERNAME, msg.jobId, msg.extraNonce2, msg.ntime, msg.nonce]
      });
      console.log('[COORDINATOR] Client', clientId, 'submitted share, nonce=' + msg.nonce);
    }

    if (msg.type === 'hashrate') {
      ws.clientHashrate = msg.value;
      totalHashrate = Array.from(clients.values()).reduce((sum, c) => sum + (c.clientHashrate || 0), 0);
    }
  });

  ws.on('close', () => {
    clients.delete(clientId);
    totalHashrate = Array.from(clients.values()).reduce((sum, c) => sum + (c.clientHashrate || 0), 0);
    console.log('[COORDINATOR] Client', clientId, 'disconnected. Total clients:', clients.size);
    broadcastStats();
  });

  ws.on('error', () => {
    clients.delete(clientId);
  });
});

// Har 3 soniyada statistika yangilanadi (real-time hashrate uchun)
setInterval(() => {
  totalHashrate = Array.from(clients.values()).reduce((sum, c) => sum + (c.clientHashrate || 0), 0);
  broadcastStats();
}, 3000);

connectPool();
