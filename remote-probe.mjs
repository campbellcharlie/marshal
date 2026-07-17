#!/usr/bin/env node
/**
 * Falsifiable probe: remote MCP transports (Streamable HTTP + classic SSE).
 * PASS = marshal connects to a remote HTTP MCP server and an SSE MCP server, namespaces + aggregates
 * their tools (title + [backend] desc), routes a tool call over each transport, auto-detects the
 * transport, and reconnects after an SSE stream drops. Fake servers run in-process — no real network.
 */
import { spawn } from 'node:child_process';
import { writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { startFakeRemote } from './fake-remote-server.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
let ok = true;
const check = (label, cond) => { console.log(`  ${cond ? '🟩 PASS' : '🟥 FAIL'} — ${label}`); if (!cond) ok = false; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function marshalClient(cfgObj, tag) {
  const cfg = `/tmp/marshal-remote-${tag}-${process.pid}.json`;
  writeFileSync(cfg, JSON.stringify(cfgObj));
  const m = spawn('node', [join(HERE, 'marshal.mjs')], { stdio: ['pipe', 'pipe', 'inherit'], env: { ...process.env, MARSHAL_DAEMON_IDLE: '300', MARSHAL_CONFIG: cfg, MARSHAL_SOCK: `/tmp/marshal-remote-${tag}-${process.pid}.sock`, MARSHAL_AUDIT: `/tmp/marshal-remote-${tag}-${process.pid}.jsonl` } });
  const pending = new Map(); let nextId = 1; let buf = '';
  m.stdout.setEncoding('utf8');
  m.stdout.on('data', (d) => { buf += d; let i; while ((i = buf.indexOf('\n')) >= 0) { const line = buf.slice(0, i); buf = buf.slice(i + 1); if (!line.trim()) continue; let msg; try { msg = JSON.parse(line); } catch { continue; } if (msg.id != null && pending.has(msg.id)) { const p = pending.get(msg.id); pending.delete(msg.id); p(msg); } } });
  const rpc = (method, params) => new Promise((res) => { const id = nextId++; pending.set(id, res); m.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n'); });
  const notify = (method) => m.stdin.write(JSON.stringify({ jsonrpc: '2.0', method }) + '\n');
  return { m, rpc, notify };
}

async function scenario(label, transport, mode) {
  console.log(`=== ${label} (transport=${transport}, server=${mode}) ===`);
  const fake = await startFakeRemote({ mode });
  const { m, rpc, notify } = marshalClient({ backends: [{ name: 'remote', url: fake.url, transport }] }, label);
  await rpc('initialize', { protocolVersion: '2024-11-05', capabilities: {} });
  notify('notifications/initialized');
  await sleep(1200);
  const list = (await rpc('tools/list', {})).result?.tools || [];
  const alpha = list.find((t) => t.name === 'remote.alpha');
  check(`[${label}] remote tool exposed namespaced (remote.alpha)`, !!alpha);
  check(`[${label}] title + [backend] description applied`, alpha?.title === 'remote · alpha' && alpha?.description === '[remote] does alpha');
  const call = await rpc('tools/call', { name: 'remote.alpha', arguments: {} });
  check(`[${label}] call routes over the ${mode} transport`, !!call.result && !call.result.isError && /ok:alpha/.test(JSON.stringify(call.result)));
  if (mode === 'sse') {
    fake.drop();                                   // simulate a dropped SSE stream
    await sleep(1800);                             // marshal should reconnect (backoff starts at 300ms)
    const call2 = await rpc('tools/call', { name: 'remote.alpha', arguments: {} });
    check(`[${label}] reconnects after SSE stream drop`, !!call2.result && !call2.result.isError && /ok:alpha/.test(JSON.stringify(call2.result)));
  }
  m.kill(); fake.server.close();
}

(async () => {
  await scenario('http', 'http', 'http');
  await scenario('sse', 'sse', 'sse');
  await scenario('auto→http', 'auto', 'http');
  await scenario('auto→sse', 'auto', 'sse');
  console.log(ok ? '\n✅ PROBE PASSED — remote HTTP + SSE MCP backends connect, aggregate, route, auto-detect, reconnect.' : '\n❌ PROBE FAILED');
  process.exit(ok ? 0 : 1);
})();
