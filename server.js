const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });
const rooms = new Map();

// 정적 파일 제공 설정
app.use(express.static(path.join(__dirname)));

// 루트 경로로 접속 시 index.html 반환
app.get('/', (_req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

app.get('/health', (_req, res) => res.json({ ok: true }));

io.on('connection', socket => {
  socket.on('enter-room', ({ code, role }, callback) => {
    code = String(code || '').trim().toUpperCase();
    if (!/^[A-Z0-9]{6}$/.test(code)) return callback?.({ ok:false, error:'방 코드는 6자리여야 해요.' });
    let room = rooms.get(code);
    if (role === 'host') {
      if (room && room.host !== socket.id) return callback?.({ ok:false, error:'이미 사용 중인 방 코드예요. 다시 만들어 주세요.' });
      if (!room) { room = { host: socket.id, members: new Set() }; rooms.set(code, room); }
    } else {
      if (!room) return callback?.({ ok:false, error:'방을 찾을 수 없어요. 방 코드와 방장 접속을 확인해 주세요.' });
      if (room.members.size >= 3 && !room.members.has(socket.id)) return callback?.({ ok:false, error:'방이 가득 찼어요.' });
      room.members.add(socket.id);
    }
    socket.join(code); socket.data.roomCode = code; socket.data.role = role;
    callback?.({ ok:true });
  });
  socket.on('room-event', msg => {
    const code = socket.data.roomCode;
    if (!code || !rooms.has(code) || !msg || typeof msg.event !== 'string') return;
    socket.to(code).emit('room-event', { event: msg.event, payload: msg.payload });
  });
  socket.on('leave-room', () => leave(socket));
  socket.on('disconnect', () => leave(socket));
});

function leave(socket) {
  const code = socket.data.roomCode; if (!code) return;
  const room = rooms.get(code);
  if (room) {
    if (room.host === socket.id) { io.to(code).emit('player-disconnected'); rooms.delete(code); }
    else { room.members.delete(socket.id); }
  }
  socket.leave(code); delete socket.data.roomCode;
}

const port = process.env.PORT || 8080;
server.listen(port, '0.0.0.0', () => console.log(`Game server listening on ${port}`));
