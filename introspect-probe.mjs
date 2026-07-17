#!/usr/bin/env node
/**
 * Falsifiable probe: observability (a + b2).
 * PASS = backend tools carry a `title` and a `[backend]`-prefixed description (so the sub-tool is legible,
 * not just "marshal"), AND marshal exposes a `marshal.recent` tool that reports which backend tool ran.
 * Uses a temp config with a single fake backend — never touches the user's live momento/serval.
 */
import { spawn } from 'node:child_process';
import { writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const HERE = dirname(fileURLToPath(import.meta.url));
const cfg = `/tmp/marshal-introspect-${process.pid}.json`;
writeFileSync(cfg, JSON.stringify({ backends: [{ name: 'fake', command: 'node', args: [join(HERE, 'fake-backend.mjs')] }] }));
const m = spawn('node', [join(HERE, 'marshal.mjs')], { stdio: ['pipe', 'pipe', 'inherit'], env: { ...process.env, MARSHAL_CONFIG: cfg, MARSHAL_SOCK: `/tmp/marshal-introspect-${process.pid}.sock`, MARSHAL_AUDIT: `/tmp/marshal-introspect-${process.pid}.jsonl` } });
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
  await sleep(1000);

  const list = (await rpc('tools/list', {})).result?.tools || [];
  const alpha = list.find((t) => t.name === 'fake.alpha');
  const beta = list.find((t) => t.name === 'fake.beta');
  const recent = list.find((t) => t.name === 'marshal.recent');

  check('backend tool exposed namespaced (fake.alpha)', !!alpha);
  check('tool carries a `title` (spec display name)', alpha?.title === 'fake · alpha');
  check('description is `[backend]`-prefixed (from backend desc)', alpha?.description === '[fake] does alpha');
  check('description synthesized when backend gives none (beta)', beta?.description === '[fake] beta');
  check('marshal.recent introspection tool is exposed', !!recent && !!recent.title);

  const call = await rpc('tools/call', { name: 'fake.alpha', arguments: { q: 'hi' } });
  check('fake.alpha call succeeds', call.result && !call.result.isError);

  const rec = await rpc('tools/call', { name: 'marshal.recent', arguments: { limit: 5 } });
  const text = rec.result?.content?.[0]?.text || '';
  let rows = []; try { rows = JSON.parse(text); } catch {}
  check('marshal.recent returns the fake.alpha call (backend+tool visible)', rows.some((r) => r.backend === 'fake' && r.tool === 'alpha' && r.ok === true));
  check('marshal.recent does NOT list its own row', !rows.some((r) => r.tool === 'recent'));

  console.log(ok ? '\n✅ PROBE PASSED — sub-tool is legible via title/description + marshal.recent.' : '\n❌ PROBE FAILED');
  m.kill(); process.exit(ok ? 0 : 1);
})();
