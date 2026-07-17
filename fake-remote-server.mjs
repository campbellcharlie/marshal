#!/usr/bin/env node
/**
 * fake-remote-server — a minimal HTTP MCP server for marshal's remote-transport probe.
 * Speaks BOTH transports, selected by {mode}/FAKE_REMOTE_MODE:
 *   http (Streamable HTTP) — POST / with a JSON-RPC request → JSON-RPC reply in the body.
 *   sse  (classic SSE)     — GET / → `event: endpoint` naming /messages; POST /messages → 202, and the
 *                            reply is pushed back on the held SSE stream.
 * Exposes two tools (alpha with a description, beta without). Zero deps.
 */
import http from 'node:http';
import { fileURLToPath } from 'node:url';

const TOOLS = [
  { name: 'alpha', description: 'does alpha', inputSchema: { type: 'object', properties: {} } },
  { name: 'beta', inputSchema: { type: 'object', properties: {} } },
];
function rpcResult(m) {
  if (m.method === 'initialize') return { protocolVersion: '2024-11-05', capabilities: { tools: {} }, serverInfo: { name: 'fake-remote', version: '0.0.1' } };
  if (m.method === 'tools/list') return { tools: TOOLS };
  if (m.method === 'tools/call') return { content: [{ type: 'text', text: `ok:${m.params?.name}` }] };
  if (m.method === 'ping') return {};
  return undefined;
}
function reply(m) { const r = rpcResult(m); return r === undefined ? { jsonrpc: '2.0', id: m.id, error: { code: -32601, message: 'method not found' } } : { jsonrpc: '2.0', id: m.id, result: r }; }

export function startFakeRemote({ mode = 'http' } = {}) {
  let sseStream = null;
  const server = http.createServer((req, res) => {
    if (mode === 'sse') {
      if (req.method === 'GET') {
        res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
        res.write('event: endpoint\ndata: /messages\n\n');
        sseStream = res;
        req.on('close', () => { if (sseStream === res) sseStream = null; });
        return;
      }
      let body = ''; req.on('data', (d) => body += d); req.on('end', () => {
        res.writeHead(202).end();
        let m; try { m = JSON.parse(body); } catch { return; }
        if (m.id != null && sseStream) sseStream.write(`event: message\ndata: ${JSON.stringify(reply(m))}\n\n`);
      });
      return;
    }
    // Streamable HTTP
    if (req.method !== 'POST') { res.writeHead(405).end(); return; }
    let body = ''; req.on('data', (d) => body += d); req.on('end', () => {
      let m; try { m = JSON.parse(body); } catch { res.writeHead(400).end(); return; }
      if (m.id == null) { res.writeHead(202).end(); return; }                 // notification
      const headers = { 'Content-Type': 'application/json' };
      if (m.method === 'initialize') headers['Mcp-Session-Id'] = 'fake-session';
      res.writeHead(200, headers); res.end(JSON.stringify(reply(m)));
    });
  });
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const port = server.address().port;
      resolve({ port, url: `http://127.0.0.1:${port}/`, server, drop: () => { try { sseStream?.destroy(); } catch {} } });
    });
  });
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  startFakeRemote({ mode: process.env.FAKE_REMOTE_MODE || 'http' }).then(({ port }) => process.stdout.write(`LISTENING ${port}\n`));
}
