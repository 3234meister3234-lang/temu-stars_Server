const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');

const PORT = process.env.PORT || 3000;

// Persistenter Datenspeicher (JSON-Dateien)
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch (e) {}
const BUGS_FILE = path.join(DATA_DIR, 'bugs.json');
const STORE_FILE = path.join(DATA_DIR, 'store.json');
function readJson(file, fallback) {
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch (e) { return fallback; }
}
function writeJson(file, obj) {
  try { fs.writeFileSync(file, JSON.stringify(obj)); } catch (e) {}
}
function readBody(req, res, handler) {
  let body = '';
  req.on('data', chunk => { body += chunk; if (body.length > 1e6) req.destroy(); });
  req.on('end', () => {
    try {
      const result = handler(JSON.parse(body));
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(result));
    } catch (e) {
      res.writeHead(400);
      res.end(JSON.stringify({ error: e.message }));
    }
  });
}

// Ermittle lokale WLAN / LAN IPv4 Adresse
function getLocalIp() {
  const ifs = os.networkInterfaces();
  for (const name of Object.keys(ifs)) {
    for (const net of ifs[name]) {
      if (net.family === 'IPv4' && !net.internal) {
        return net.address;
      }
    }
  }
  return 'localhost';
}

const localIp = getLocalIp();

// Multiplayer Relay State
const clients = new Set();
let messageQueue = [];
const matchmakingQueues = new Map();
const queueEntries = new Map();
const MATCH_WAIT_MS = 10000;
function cancelQueuedClient(client) {
  const entry = queueEntries.get(client);
  if (!entry) return;
  queueEntries.delete(client);
  const q = matchmakingQueues.get(entry.mode);
  if (q) { const i=q.entries.indexOf(entry); if(i>=0) q.entries.splice(i,1); if(!q.entries.length) { clearTimeout(q.timer); matchmakingQueues.delete(entry.mode); } }
}
function sendServerMatch(entries, mode) {
  const max = Math.max(2, Math.min(10, Number(entries[0].max) || 6));
  const matchId = 'srv_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2,8);
  const roster = entries.slice(0,max).map((entry, slot) => ({ ...entry.player, slot, team: max===2 ? slot : (slot < Math.ceil(max/2) ? 0 : 1), human:true }));
  const packet = { type:'mm_server_match', mode, matchId, max, roster };
  for (const entry of entries.slice(0,max)) {
    queueEntries.delete(entry.client);
    entry.client.matchId = matchId;
    if (entry.client.readyState === 1) entry.client.send(packet);
  }
}
function startQueueTimer(mode, q) {
  if (q.timer) return;
  q.timer = setTimeout(() => {
    q.timer = null;
    const current = matchmakingQueues.get(mode);
    if (!current || current !== q) return;
    if (q.entries.length >= 2) {
      const group = q.entries.splice(0, Math.min(q.entries[0].max, q.entries.length));
      if (q.entries.length) startQueueTimer(mode, q); else matchmakingQueues.delete(mode);
      sendServerMatch(group, mode);
    } else if (q.entries.length) {
      // Keep searching. Clients can cancel when their local search expires.
      startQueueTimer(mode, q);
    } else matchmakingQueues.delete(mode);
  }, MATCH_WAIT_MS);
}
function enqueueMatchmaking(client, data) {
  cancelQueuedClient(client);
  const mode = String(data.mode || 'duel').slice(0,32);
  const max = Math.max(2, Math.min(10, Number(data.max) || 6));
  let q = matchmakingQueues.get(mode);
  if (!q) { q={ entries:[], timer:null }; matchmakingQueues.set(mode,q); }
  const entry={ client, mode, max, player:{ peerId:String(data.peerId||'').slice(0,80), name:String(data.name||'Spieler').slice(0,24), brawler:String(data.brawler||'shelly').slice(0,32), skin:Math.max(0,Math.min(20,Number(data.skin)||0)), lvl:Math.max(1,Math.min(11,Number(data.lvl)||1)), isAdmin:!!data.isAdmin } };
  q.entries.push(entry); queueEntries.set(client,entry); startQueueTimer(mode,q);
  if (q.entries.length >= max) {
    clearTimeout(q.timer); q.timer=null;
    const group=q.entries.splice(0,max); if(q.entries.length) startQueueTimer(mode,q); else matchmakingQueues.delete(mode);
    sendServerMatch(group,mode);
  }
}

const server = http.createServer((req, res) => {
  // CORS-Header
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', '*');
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  const url = req.url.split('?')[0];

  // API: Nachricht senden
  if (url === '/api/send' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      try {
        const data = JSON.parse(body);
        data.timestamp = Date.now();
        messageQueue.push(data);
        if (messageQueue.length > 200) messageQueue.shift();

        // Broadcast an alle WebSocket Clients
        clients.forEach(c => {
          if (c.readyState === 1) {
            try { c.send(JSON.stringify(data)); } catch(e) {}
          }
        });

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true }));
      } catch(e) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: e.message }));
      }
    });
    return;
  }

  // Healthcheck (Hosting-Dienste)
  if (url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
    return;
  }

  // API: Bug melden
  if (url === '/api/bug' && req.method === 'POST') {
    readBody(req, res, (data) => {
      const bugs = readJson(BUGS_FILE, []);
      bugs.push({
        time: new Date().toISOString(),
        name: String(data.name || '').slice(0, 40),
        text: String(data.text || '').slice(0, 2000),
        version: String(data.version || '').slice(0, 20),
        ua: String(data.ua || '').slice(0, 200)
      });
      writeJson(BUGS_FILE, bugs.slice(-1000));
      return { ok: true };
    });
    return;
  }
  if (url === '/api/bugs' && req.method === 'GET') {
    const key = (req.url.split('key=')[1] || '').split('&')[0];
    if (!process.env.ADMIN_KEY || key !== process.env.ADMIN_KEY) {
      res.writeHead(403); res.end('forbidden'); return;
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(readJson(BUGS_FILE, [])));
    return;
  }

  // API: Spielerdaten speichern / laden (id = Spieler-ID)
  if (url === '/api/save' && req.method === 'POST') {
    readBody(req, res, (data) => {
      const id = String(data.id || '').replace(/[^\w-]/g, '').slice(0, 80);
      if (!id) throw new Error('id fehlt');
      const store = readJson(STORE_FILE, {});
      store[id] = { data: data.data, updated: Date.now() };
      writeJson(STORE_FILE, store);
      return { ok: true };
    });
    return;
  }
  if (url === '/api/load' && req.method === 'GET') {
    const id = decodeURIComponent((req.url.split('id=')[1] || '').split('&')[0]).replace(/[^\w-]/g, '').slice(0, 80);
    const entry = readJson(STORE_FILE, {})[id];
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(entry || { data: null }));
    return;
  }

  // API: Nachrichten abrufen (Polling Fallback)
  if (url === '/api/poll') {
    const since = parseInt(req.url.split('since=')[1] || '0', 10);
    const msgs = messageQueue.filter(m => m.timestamp > since);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ messages: msgs, now: Date.now() }));
    return;
  }

  // API: Server Info & Lokale IP
  if (url === '/api/info') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ip: localIp, port: PORT, clients: clients.size }));
    return;
  }

  // Hauptseite: index.html
  const filePath = path.join(__dirname, 'index.html');
  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Fehler beim Laden von index.html: ' + err.message);
      return;
    }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(data);
  });
});

// Native WebSocket Relay
server.on('upgrade', (req, socket, head) => {
  const crypto = require('crypto');
  const key = req.headers['sec-websocket-key'];
  if (!key) {
    socket.destroy();
    return;
  }
  const accept = crypto.createHash('sha1')
    .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11')
    .digest('base64');

  const headers = [
    'HTTP/1.1 101 Switching Protocols',
    'Upgrade: websocket',
    'Connection: Upgrade',
    'Sec-WebSocket-Accept: ' + accept
  ];
  socket.write(headers.join('\r\n') + '\r\n\r\n');

  const wsClient = {
    readyState: 1,
    send(msg) {
      if (typeof msg !== 'string') msg = JSON.stringify(msg);
      const payload = Buffer.from(msg);
      const len = payload.length;
      let frame;
      if (len < 126) {
        frame = Buffer.concat([Buffer.from([0x81, len]), payload]);
      } else if (len < 65536) {
        frame = Buffer.concat([Buffer.from([0x81, 126, len >> 8, len & 255]), payload]);
      } else {
        frame = Buffer.concat([Buffer.from([0x81, 127, 0,0,0,0, len >> 24, (len >> 16) & 255, (len >> 8) & 255, len & 255]), payload]);
      }
      try { socket.write(frame); } catch(e) {}
    }
  };

  clients.add(wsClient);

  let buf = Buffer.alloc(0);
  socket.on('data', chunk => {
    buf = Buffer.concat([buf, chunk]);
    while (buf.length >= 2) {
      const isFin = (buf[0] & 0x80) !== 0;
      const opcode = buf[0] & 0x0f;
      const isMasked = (buf[1] & 0x80) !== 0;
      let payloadLen = buf[1] & 0x7f;
      let offset = 2;

      if (payloadLen === 126) {
        if (buf.length < 4) break;
        payloadLen = buf.readUInt16BE(2);
        offset = 4;
      } else if (payloadLen === 127) {
        if (buf.length < 10) break;
        payloadLen = Number(buf.readBigUInt64BE(2));
        offset = 10;
      }

      const maskLen = isMasked ? 4 : 0;
      if (buf.length < offset + maskLen + payloadLen) break;

      let payload = buf.slice(offset + maskLen, offset + maskLen + payloadLen);
      if (isMasked) {
        const mask = buf.slice(offset, offset + 4);
        for (let i = 0; i < payload.length; i++) {
          payload[i] ^= mask[i % 4];
        }
      }

      buf = buf.slice(offset + maskLen + payloadLen);

      if (opcode === 8) { // CLOSE
        socket.end();
        break;
      }
      if (opcode === 9) { // PING
        socket.write(Buffer.from([0x8A, 0x00])); // PONG
        continue;
      }

      if (opcode === 1) { // TEXT
        const str = payload.toString('utf8');
        try {
          const data = JSON.parse(str);
          if (data && data.type === 'mm_identity') { wsClient.peerId = String(data.peerId || '').slice(0,80); continue; }
          if (data && data.type === 'mm_join' && data.serverQueue) { enqueueMatchmaking(wsClient, data); continue; }
          if (data && data.type === 'mm_cancel') { cancelQueuedClient(wsClient); continue; }
          if (data && data.type === 'mm_start' && data.matchId) {
            const peers = new Set((data.roster || []).filter(Boolean).map(player => player.peerId));
            clients.forEach(c => { if (c.peerId && peers.has(c.peerId)) c.matchId = data.matchId; });
          }
          const matchPacket = data && data.matchId && ['act','sync','score_sync','match_over','unit_die','unit_respawn','pin'].includes(data.type);
          if (matchPacket) {
            clients.forEach(c => { if (c !== wsClient && c.readyState === 1 && c.matchId === data.matchId) c.send(str); });
            continue;
          }
          data.timestamp = Date.now();
          messageQueue.push(data);
          if (messageQueue.length > 200) messageQueue.shift();

          // Broadcast an alle anderen Clients
          clients.forEach(c => {
            if (c !== wsClient && c.readyState === 1) {
              c.send(str);
            }
          });
        } catch(e) {}
      }
    }
  });

  socket.on('close', () => {
    wsClient.readyState = 3;
    cancelQueuedClient(wsClient);
    clients.delete(wsClient);
  });
  socket.on('error', () => {
    wsClient.readyState = 3;
    cancelQueuedClient(wsClient);
    clients.delete(wsClient);
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('\n================================================================');
  console.log('  🚀 BRAWL STARS TEMU - KOSTENLOSER MULTIPLAYER SERVER AKTIV!');
  console.log('================================================================');
  console.log('  💻 Am PC öffnen:     http://localhost:' + PORT);
  console.log('  📱 Am Handy öffnen:  http://' + localIp + ':' + PORT);
  console.log('================================================================');
  console.log('  ✅ Tippe am Handy einfach http://' + localIp + ':' + PORT + ' in Safari / Chrome ein!');
  console.log('  ✅ Kein Dateien-Versenden mehr nötig - das Spiel lädt direkt!');
  console.log('  ✅ 100% Kostenlos, ultra-schnell & garantiert verbunden!');
  console.log('================================================================\n');
});
