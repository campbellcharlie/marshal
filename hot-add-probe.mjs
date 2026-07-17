#!/usr/bin/env node
/**
 * Verifies hot-add/remove: edit marshal.config.json at runtime → the new backend spawns and its tools
 * appear (no restart); removing it stops the backend and drops its tools. Uses temp sock/config; the
 * hot-added backend is a second momento instance ("momento2", same command — reader-safe on the DB).
 */
import { spawn } from 'node:child_process';
import { readFileSync, writeFileSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const real = JSON.parse(readFileSync(process.env.MARSHAL_CONFIG || join(HERE, 'marshal.config.json'), 'utf8'));
const momento = (real.backends || []).find((b) => b.name === 'momento');
if (!momento) { console.error('no momento backend'); process.exit(2); }
const dir = mkdtempSync(join(tmpdir(), 'marshal-hot-'));
const cfg = join(dir, 'cfg.json');
const writeCfg = (list) => writeFileSync(cfg, JSON.stringify({ backends: list }));
writeCfg([momento]);
const env = { ...process.env, MARSHAL_SOCK: join(dir, 'm.sock'), MARSHAL_AUDIT: join(dir, 'audit.jsonl'), MARSHAL_CONFIG: cfg };
const m = spawn('node', [join(HERE, 'marshal.mjs')], { stdio: ['pipe', 'pipe', 'inherit'], env });
const pending = new Map(); let nid = 1; let buf = '';
m.stdout.setEncoding('utf8');
m.stdout.on('data', (d) => { buf += d; let i; while ((i = buf.indexOf('\n')) >= 0) { const l = buf.slice(0, i); buf = buf.slice(i + 1); if (!l.trim()) continue; let x; try { x = JSON.parse(l); } catch { continue; } if (x.id != null && pending.has(x.id)) { pending.get(x.id)(x); pending.delete(x.id); } } });
const rpc = (method, params) => new Promise((r) => { const id = nid++; pending.set(id, r); m.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n'); });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const ns = async () => (await rpc('tools/list', {})).result.tools.map((t) => t.name);
let ok = true; const check = (l, c) => { console.log(`  ${c ? '🟩 PASS' : '🟥 FAIL'} — ${l}`); if (!c) ok = false; };

(async () => {
  await rpc('initialize', { protocolVersion: '2024-11-05', capabilities: {} });
  m.stdin.write(JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }) + '\n');
  await sleep(1500);
  const before = await ns();
  check(`baseline: momento.* only (${before.length})`, before.length > 0 && before.every((n) => n.startsWith('momento.')));

  writeCfg([momento, { name: 'momento2', command: momento.command, args: momento.args }]);   // HOT-ADD
  await sleep(2500);
  const added = await ns();
  check(`hot-add: momento2.* appeared without restart (${added.length} total)`, added.some((n) => n.startsWith('momento2.')) && added.length > before.length);

  writeCfg([momento]);                                                                         // HOT-REMOVE
  await sleep(2000);
  const removed = await ns();
  check(`hot-remove: momento2.* gone, back to baseline (${removed.length})`, !removed.some((n) => n.startsWith('momento2.')) && removed.length === before.length);

  console.log(ok ? '\n✅ HOT-ADD PASSED — config edits reconcile live (no restart).' : '\n❌ HOT-ADD FAILED');
  m.kill(); process.exit(ok ? 0 : 1);
})();
