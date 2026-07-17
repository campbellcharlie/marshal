#!/usr/bin/env node
/**
 * Falsifiable probe: per-call timeout (b1).
 * A backend that HANGS (answers initialize/tools/list but never tools/call) must not stall the client
 * forever. PASS = the call returns an isError "timed out" result at ~MARSHAL_CALL_TIMEOUT, not never.
 * Uses a temp config with a single hanging fake backend — never touches the user's live fleet.
 */
import { spawn } from 'node:child_process';
import { writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const HERE = dirname(fileURLToPath(import.meta.url));
const TIMEOUT = 800;
const cfg = `/tmp/marshal-timeout-${process.pid}.json`;
writeFileSync(cfg, JSON.stringify({ backends: [{ name: 'fake', command: 'node', args: [join(HERE, 'fake-backend.mjs')] }] }));
const m = spawn('node', [join(HERE, 'marshal.mjs')], { stdio: ['pipe', 'pipe', 'inherit'], env: { ...process.env, FAKE_MODE: 'hang', MARSHAL_CALL_TIMEOUT: String(TIMEOUT), MARSHAL_CONFIG: cfg, MARSHAL_SOCK: `/tmp/marshal-timeout-${process.pid}.sock`, MARSHAL_AUDIT: `/tmp/marshal-timeout-${process.pid}.jsonl` } });
const pending = new Map(); let nextId = 1; let buf = '';
m.stdout.setEncoding('utf8');
m.stdout.on('data', (d) => { buf += d; let i; while ((i = buf.indexOf('\n')) >= 0) { const line = buf.slice(0, i); buf = buf.slice(i + 1); if (!line.trim()) continue; let msg; try { msg = JSON.parse(line); } catch { continue; } if (msg.id != null && pending.has(msg.id)) { const p = pending.get(msg.id); pending.delete(msg.id); p(msg); } } });
const rpc = (method, params) => new Promise((res) => { const id = nextId++; pending.set(id, res); m.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n'); });
const notify = (method) => m.stdin.write(JSON.stringify({ jsonrpc: '2.0', method }) + '\n');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let ok = true;
const check = (label, cond) => { console.log(`  ${cond ? '🟩 PASS' : '🟥 FAIL'} — ${label}`); if (!cond) ok = false; };

(async () => {
  await rpc('initialize', { protocolVersion: '2024-11-05', capabilities: {} });
  notify('notifications/initialized');
  await sleep(1000);                                        // backend answers list, so tools appear

  const list = (await rpc('tools/list', {})).result?.tools || [];
  check('hanging backend still lists its tools (init/list work)', list.some((t) => t.name === 'fake.alpha'));

  const t0 = Date.now();
  const call = await rpc('tools/call', { name: 'fake.alpha', arguments: {} });   // this would hang forever without the timeout
  const elapsed = Date.now() - t0;
  const text = call.result?.content?.[0]?.text || '';
  check(`call returns instead of hanging (elapsed ${elapsed}ms)`, elapsed < TIMEOUT + 1500);
  check('call is marked isError', call.result?.isError === true);
  check(`error says "timed out" (got: ${JSON.stringify(text).slice(0, 60)})`, /timed out/.test(text));

  console.log(ok ? '\n✅ PROBE PASSED — a hung backend times out; the client is not stalled forever.' : '\n❌ PROBE FAILED');
  m.kill(); process.exit(ok ? 0 : 1);
})();
