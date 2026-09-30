import { connect } from "cloudflare:sockets";

const ORIGIN_HOST = "64.181.249.69";
const ORIGIN_PORT = 4001;
const WS_HOST = "aocdisn.pages.dev";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export async function onRequest(context) {
  const { request } = context;
  const url = new URL(request.url);

  if ((request.headers.get("Upgrade") || "").toLowerCase() !== "websocket") {
    return new Response("WebSocket only", { status: 426 });
  }

  const secKey = request.headers.get("Sec-WebSocket-Key");
  if (!secKey) {
    return new Response("Missing Sec-WebSocket-Key", { status: 400 });
  }

  let socket;
  try {
    socket = connect(
      { hostname: ORIGIN_HOST, port: ORIGIN_PORT },
      { allowHalfOpen: true }
    );
    await socket.opened;
  } catch (e) {
    return new Response("Origin connect failed", { status: 502 });
  }

  const writer = socket.writable.getWriter();
  const reader = socket.readable.getReader();

  const protocol = request.headers.get("Sec-WebSocket-Protocol");
  let handshake =
    `GET ${url.pathname}${url.search} HTTP/1.1\r\n` +
    `Host: ${WS_HOST}\r\n` +
    "Upgrade: websocket\r\n" +
    "Connection: Upgrade\r\n" +
    `Sec-WebSocket-Key: ${secKey}\r\n` +
    "Sec-WebSocket-Version: 13\r\n";

  if (protocol) {
    handshake += `Sec-WebSocket-Protocol: ${protocol}\r\n`;
  }
  handshake += "\r\n";

  try {
    await writer.write(encoder.encode(handshake));
  } catch (e) {
    await safeClose(socket);
    return new Response("Origin handshake write failed", { status: 502 });
  }

  let prebuffer;
  try {
    prebuffer = await readHandshake(reader);
  } catch (e) {
    await safeClose(socket);
    return new Response("Origin handshake read failed", { status: 502 });
  }

  if (!prebuffer.statusLine.includes(" 101 ")) {
    await safeClose(socket);
    return new Response("Origin rejected WebSocket: " + prebuffer.statusLine, {
      status: 502,
    });
  }

  const pair = new WebSocketPair();
  const client = pair[0];
  const server = pair[1];
  server.binaryType = "arraybuffer";
  server.accept({ allowHalfOpen: true });

  let writeChain = Promise.resolve();
  let closed = false;

  const queueFrame = (frame) => {
    writeChain = writeChain
      .then(() => writer.write(frame))
      .catch(async () => {
        if (!closed) {
          closed = true;
          try { server.close(1011, "upstream write failed"); } catch {}
          await safeClose(socket);
        }
      });
  };

  server.addEventListener("message", (event) => {
    if (closed) return;
    const isText = typeof event.data === "string";
    const payload = isText ? encoder.encode(event.data) : toUint8(event.data);
    queueFrame(makeClientFrame(payload, isText ? 0x1 : 0x2));
  });

  server.addEventListener("close", (event) => {
    if (closed) return;
    closed = true;
    const code = event.code && event.code !== 1005 ? event.code : 1000;
    const reason = encoder.encode(event.reason || "");
    const payload = new Uint8Array(2 + reason.length);
    payload[0] = (code >> 8) & 0xff;
    payload[1] = code & 0xff;
    payload.set(reason, 2);
    queueFrame(makeClientFrame(payload, 0x8));
    writeChain.finally(() => safeClose(socket));
  });

  server.addEventListener("error", async () => {
    if (closed) return;
    closed = true;
    await safeClose(socket);
  });

  const pump = pumpOrigin(
    reader,
    prebuffer.leftover,
    server,
    queueFrame,
    socket,
    () => closed,
    () => { closed = true; }
  );

  context.waitUntil(pump);

  return new Response(null, {
    status: 101,
    webSocket: client,
  });
}

async function readHandshake(reader) {
  let buf = new Uint8Array(0);

  while (buf.length < 16384) {
    const idx = headerEnd(buf);
    if (idx !== -1) {
      const headerBytes = buf.slice(0, idx);
      const leftover = buf.slice(idx + 4);
      const headerText = decoder.decode(headerBytes);
      const statusLine = headerText.split("\r\n", 1)[0] || "";
      return { statusLine, leftover };
    }

    const { value, done } = await reader.read();
    if (done) throw new Error("origin closed during handshake");
    buf = concat(buf, value);
  }

  throw new Error("origin handshake headers too large");
}

async function pumpOrigin(
  reader,
  initial,
  server,
  queueFrame,
  socket,
  isClosed,
  markClosed
) {
  let buf = initial || new Uint8Array(0);
  let fragOpcode = 0;
  let fragParts = [];

  try {
    while (!isClosed()) {
      if (buf.length === 0) {
        const { value, done } = await reader.read();
        if (done) break;
        buf = value;
      }

      const parsed = parseServerFrame(buf);
      if (!parsed) {
        const { value, done } = await reader.read();
        if (done) break;
        buf = concat(buf, value);
        continue;
      }

      buf = buf.slice(parsed.consumed);
      const { fin, opcode, payload } = parsed;

      if (opcode === 0x8) {
        markClosed();
        const code =
          payload.length >= 2 ? (payload[0] << 8) | payload[1] : 1000;
        const reason =
          payload.length > 2 ? decoder.decode(payload.slice(2)) : "";
        try {
          server.close(validCloseCode(code) ? code : 1000, reason.slice(0, 120));
        } catch {
          try { server.close(1000, ""); } catch {}
        }
        break;
      }

      if (opcode === 0x9) {
        queueFrame(makeClientFrame(payload, 0xA));
        continue;
      }

      if (opcode === 0xA) {
        continue;
      }

      if (opcode === 0x1 || opcode === 0x2) {
        if (fin) {
          sendMessage(server, opcode, payload);
        } else {
          fragOpcode = opcode;
          fragParts = [payload];
        }
        continue;
      }

      if (opcode === 0x0 && fragOpcode) {
        fragParts.push(payload);
        if (fin) {
          const full = concatMany(fragParts);
          sendMessage(server, fragOpcode, full);
          fragOpcode = 0;
          fragParts = [];
        }
      }
    }
  } catch {
    if (!isClosed()) {
      markClosed();
      try { server.close(1011, "upstream read failed"); } catch {}
    }
  } finally {
    await safeClose(socket);
  }
}

function sendMessage(server, opcode, payload) {
  if (opcode === 0x1) {
    server.send(decoder.decode(payload));
  } else {
    const copy = payload.slice();
    server.send(copy.buffer);
  }
}

function parseServerFrame(buf) {
  if (buf.length < 2) return null;

  const b0 = buf[0];
  const b1 = buf[1];
  const fin = (b0 & 0x80) !== 0;
  const opcode = b0 & 0x0f;
  const masked = (b1 & 0x80) !== 0;
  let len = b1 & 0x7f;
  let pos = 2;

  if (len === 126) {
    if (buf.length < pos + 2) return null;
    len = (buf[pos] << 8) | buf[pos + 1];
    pos += 2;
  } else if (len === 127) {
    if (buf.length < pos + 8) return null;
    let n = 0n;
    for (let i = 0; i < 8; i++) n = (n << 8n) | BigInt(buf[pos + i]);
    if (n > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error("frame too large");
    len = Number(n);
    pos += 8;
  }

  let mask;
  if (masked) {
    if (buf.length < pos + 4) return null;
    mask = buf.slice(pos, pos + 4);
    pos += 4;
  }

  if (buf.length < pos + len) return null;

  const payload = buf.slice(pos, pos + len);
  if (masked) {
    for (let i = 0; i < payload.length; i++) {
      payload[i] ^= mask[i & 3];
    }
  }

  return {
    fin,
    opcode,
    payload,
    consumed: pos + len,
  };
}

function makeClientFrame(payload, opcode) {
  const data = toUint8(payload);
  const len = data.length;

  let ext = 0;
  if (len >= 126 && len <= 0xffff) ext = 2;
  else if (len > 0xffff) ext = 8;

  const header = new Uint8Array(2 + ext + 4);
  header[0] = 0x80 | (opcode & 0x0f);

  let pos = 2;
  if (ext === 0) {
    header[1] = 0x80 | len;
  } else if (ext === 2) {
    header[1] = 0x80 | 126;
    header[2] = (len >> 8) & 0xff;
    header[3] = len & 0xff;
    pos = 4;
  } else {
    header[1] = 0x80 | 127;
    let n = BigInt(len);
    for (let i = 7; i >= 0; i--) {
      header[2 + i] = Number(n & 0xffn);
      n >>= 8n;
    }
    pos = 10;
  }

  const mask = crypto.getRandomValues(new Uint8Array(4));
  header.set(mask, pos);

  const out = new Uint8Array(header.length + len);
  out.set(header, 0);

  for (let i = 0; i < len; i++) {
    out[header.length + i] = data[i] ^ mask[i & 3];
  }

  return out;
}

function toUint8(value) {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  return encoder.encode(String(value));
}

function concat(a, b) {
  const aa = toUint8(a);
  const bb = toUint8(b);
  const out = new Uint8Array(aa.length + bb.length);
  out.set(aa, 0);
  out.set(bb, aa.length);
  return out;
}

function concatMany(parts) {
  let total = 0;
  for (const p of parts) total += p.length;
  const out = new Uint8Array(total);
  let offset = 0;
  for (const p of parts) {
    out.set(p, offset);
    offset += p.length;
  }
  return out;
}

function headerEnd(buf) {
  for (let i = 0; i <= buf.length - 4; i++) {
    if (
      buf[i] === 13 &&
      buf[i + 1] === 10 &&
      buf[i + 2] === 13 &&
      buf[i + 3] === 10
    ) {
      return i;
    }
  }
  return -1;
}

function validCloseCode(code) {
  return (
    code === 1000 ||
    (code >= 1001 && code <= 1014 && ![1004, 1005, 1006].includes(code)) ||
    (code >= 3000 && code <= 4999)
  );
}

async function safeClose(socket) {
  try {
    await socket.close();
  } catch {}
}
