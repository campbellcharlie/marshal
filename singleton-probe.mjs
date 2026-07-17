#!/usr/bin/env node
/**
 * Verifies the singleton: two marshals, one socket. The FIRST becomes primary (spawns backends);
 * the SECOND becomes a proxy (spawns NOTHING) yet still serves tools through the primary.
 * Uses temp sock/audit + a momento-only config (read from marshal.config.json — no hardcoded paths).
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
const dir = mkdtempSync(join(tmpdir(), 'marshal-singleton-'));
const cfg = join(dir, 'cfg.json'); writeFileSync(cfg, JSON.stringify({ backends: [momento] }));
const env = { ...process.env, MARSHAL_SOCK: join(dir, 'm.sock'), MARSHAL_AUDIT: join(dir, 'audit.jsonl'), MARSHAL_CONFIG: cfg };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let ok = true; const check = (l, c) => { console.log(`  ${c ? '🟩 PASS' : '🟥 FAIL'} — ${l}`); if (!c) ok = false; };

function driver(proc) {
  const pending = new Map(); let nid = 1; let buf = '';
  proc.stdout.setEncoding('utf8');
  proc.stdout.on('data', (d) => { buf += d; let i; while ((i = buf.indexOf('\n')) >= 0) { const l = buf.slice(0, i); buf = buf.slice(i + 1); if (!l.trim()) continue; let m; try { m = JSON.parse(l); } catch { continue; } if (m.id != null && pending.has(m.id)) { pending.get(m.id)(m); pending.delete(m.id); } } });
  return { rpc: (method, params) => new Promise((r) => { const id = nid++; pending.set(id, r); proc.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n'); }), notify: (method) => proc.stdin.write(JSON.stringify({ jsonrpc: '2.0', method }) + '\n') };
}
const kids = (pid) => { try { return require('node:child_process').execSync(`pgrep -P ${pid}`).toString().trim().split('\n').filter(Boolean); } catch { return []; } };

(async () => {
  const A = spawn('node', [join(HERE, 'marshal.mjs')], { stdio: ['pipe', 'pipe', 'inherit'], env });
  await sleep(1800);                                       // A becomes primary + momento ready
  const B = spawn('node', [join(HERE, 'marshal.mjs')], { stdio: ['pipe', 'pipe', 'inherit'], env });
  await sleep(1200);                                       // B detects primary, becomes proxy

  const { execSync } = await import('node:child_process');
  const aKids = execSync(`pgrep -P ${A.pid}`).toString().trim().split('\n').filter(Boolean);
  let bKids = ''; try { bKids = execSync(`pgrep -P ${B.pid}`).toString().trim(); } catch { bKids = ''; }
  check(`primary (A) spawned the backend (${aKids.length} child)`, aKids.length >= 1);
  check(`proxy (B) spawned NO backends`, bKids === '');

  const b = driver(B);
  const init = await b.rpc('initialize', { protocolVersion: '2024-11-05', capabilities: {} });
  check(`proxy B serves initialize (via primary) → name=${init.result?.serverInfo?.name}`, init.result?.serverInfo?.name === 'marshal');
  b.notify('notifications/initialized');
  const l = await b.rpc('tools/list', {});
  check(`proxy B lists tools through primary (${(l.result?.tools || []).length})`, (l.result?.tools || []).some((t) => t.name.startsWith('momento.')));
  const call = await b.rpc('tools/call', { name: 'momento.get_recent', arguments: { n: 1 } });
  check(`proxy B routes a real tool call to the primary's backend`, call.result && !call.result.isError);

  console.log(ok ? '\n✅ SINGLETON PASSED — 2 marshals, 1 backend fleet; proxy serves via primary.' : '\n❌ SINGLETON FAILED');
  try { A.kill(); B.kill(); } catch {}
  process.exit(ok ? 0 : 1);
})();
