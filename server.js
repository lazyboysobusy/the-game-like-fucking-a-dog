'use strict';
const http = require('http');

// ================= 방 관리 핵심 로직 (테스트에서도 그대로 사용) =================
// - 방 주인(host)과 참가자는 "소켓 id"가 아니라 게임이 정한 고유 id 로 구분해요.
//   그래서 Cloud Run 이 연결을 끊었다가(WebSocket 은 기본 5분마다 끊겨요) 다시 붙어도
//   같은 사람으로 인식하고 방을 그대로 이어줘요.
// - 연결이 끊겨도 GRACE_MS 동안은 방을 유지하고, 그 안에 돌아오면 아무 일도 없던 것처럼 이어져요.
function attachGameServer(io, opts) {
  const GRACE_MS = (opts && opts.graceMs) || 20000;
  const MAX_PER_SEC = 200;                 // 한 사람이 1초에 보낼 수 있는 메시지 수(남용 방지)
  const rooms = new Map();                 // code -> { host: id|null, peers: Map<id,{sid,timer}> }

  const validCode = c => /^[A-Z0-9]{6}$/.test(c);

  function dropPeer(code, id) {
    const room = rooms.get(code); if (!room) return;
    const peer = room.peers.get(id); if (!peer) return;
    clearTimeout(peer.timer); room.peers.delete(id);
    if (room.host === id) {                // 방장이 완전히 나감 → 방 종료
      io.to(code).emit('player-disconnected', { id, host: true });
      rooms.delete(code); return;
    }
    io.to(code).emit('peer-left', { id });
    if (room.peers.size === 0) rooms.delete(code);
  }

  function detach(socket, immediate) {
    const d = socket.data || {};
    const code = d.code, id = d.id;
    if (!code || !id) return;
    delete socket.data.code; delete socket.data.id;
    try { socket.leave(code); } catch (e) {}
    const room = rooms.get(code), peer = room && room.peers.get(id);
    if (!peer || peer.sid !== socket.id) return;   // 이미 새 접속이 이어받았으면 건드리지 않음
    if (immediate) return dropPeer(code, id);
    peer.sid = null;                               // 잠깐 끊긴 상태: 유예 시간 동안 자리를 지켜줌
    peer.timer = setTimeout(() => dropPeer(code, id), GRACE_MS);
  }

  io.on('connection', socket => {
    socket.data = socket.data || {};
    let cnt = 0, win = Date.now();

    socket.on('enter-room', (msg, cb) => {
      const ack = typeof cb === 'function' ? cb : () => {};
      msg = msg || {};
      const code = String(msg.code || '').trim().toUpperCase();
      const id = String(msg.id || '').slice(0, 40);
      if (!validCode(code)) return ack({ ok: false, error: '방 코드는 6자리여야 해요.' });
      if (!id) return ack({ ok: false, error: '잘못된 요청이에요.' });
      if (socket.data.code) detach(socket, true);

      let room = rooms.get(code);
      if (msg.role === 'host') {
        if (room && room.host && room.host !== id) return ack({ ok: false, error: '이미 사용 중인 방 코드예요. 다시 만들어 주세요.' });
        if (!room) { room = { host: id, peers: new Map() }; rooms.set(code, room); }
        else if (!room.host) room.host = id;           // 서버가 새로 뜬 뒤 참가자가 먼저 들어온 경우
      } else {
        if (!room) {
          if (!msg.rejoin) return ack({ ok: false, error: '방을 찾을 수 없어요. 방 코드와 방장 접속을 확인해 주세요.' });
          room = { host: null, peers: new Map() }; rooms.set(code, room);   // 재접속이면 방장이 곧 돌아올 거라 자리를 만들어 둠
        }
        const guests = [...room.peers.keys()].filter(k => k !== room.host).length;
        if (!room.peers.has(id) && guests >= 3) return ack({ ok: false, error: '방이 가득 찼어요. (최대 4명)' });
      }

      let peer = room.peers.get(id);
      if (!peer) { peer = { sid: null, timer: null }; room.peers.set(id, peer); }
      clearTimeout(peer.timer); peer.timer = null; peer.sid = socket.id;
      socket.join(code); socket.data.code = code; socket.data.id = id;
      ack({ ok: true });
    });

    socket.on('room-event', msg => {
      const code = socket.data.code;
      if (!code || !rooms.has(code) || !msg || typeof msg.event !== 'string') return;
      const now = Date.now();
      if (now - win > 1000) { win = now; cnt = 0; }
      if (++cnt > MAX_PER_SEC) return;
      socket.to(code).emit('room-event', { event: msg.event, payload: msg.payload });
    });

    socket.on('leave-room', () => detach(socket, true));
    socket.on('disconnect', () => detach(socket, false));
  });

  return { rooms };
}

// ================= 실행부 =================
if (require.main === module) {
  const { Server } = require('socket.io');
  const server = http.createServer((req, res) => {
    if (req.url === '/health') { res.writeHead(200, { 'content-type': 'application/json' }); return res.end('{"ok":true}'); }
    res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
    res.end('Game server is running!');
  });
  const io = new Server(server, {
    cors: { origin: '*' },
    maxHttpBufferSize: 1e5,
    pingInterval: 15000,
    pingTimeout: 20000
  });
  attachGameServer(io);
  const port = process.env.PORT || 8080;
  server.listen(port, '0.0.0.0', () => console.log('Game server listening on ' + port));
  process.on('SIGTERM', () => { io.close(); setTimeout(() => process.exit(0), 2000).unref(); });
}
module.exports = { attachGameServer };
