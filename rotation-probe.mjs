#!/usr/bin/env node
/**
 * Verifies audit rotation: with a tiny size cap, the active log rotates into timestamped segments,
 * old segments are pruned to AUDIT_KEEP, and the hash chain stays UNBROKEN across segment boundaries
 * (concatenate segments in order → each row's prev = sha256 of the previous line).
 */
import { spawn } from 'node:child_process';
import { readFileSync, writeFileSync, mkdtempSync, readdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';

const HERE = dirname(fileURLToPath(import.meta.url));
const real = JSON.parse(readFileSync(process.env.MARSHAL_CONFIG || join(HERE, 'marshal.config.json'), 'utf8'));
const momento = (real.backends || []).find((b) => b.name === 'momento');
if (!momento) { console.error('no momento backend'); process.exit(2); }
const dir = mkdtempSync(join(tmpdir(), 'marshal-rot-'));
const cfg = join(dir, 'cfg.json'); writeFileSync(cfg, JSON.stringify({ backends: [momento] }));
const auditPath = join(dir, 'audit.jsonl');
const KEEP = 2;
const env = { ...process.env, MARSHAL_SOCK: join(dir, 'm.sock'), MARSHAL_AUDIT: auditPath, MARSHAL_CONFIG: cfg, MARSHAL_AUDIT_MAX: '400', MARSHAL_AUDIT_KEEP: String(KEEP) };
const m = spawn('node', [join(HERE, 'marshal.mjs')], { stdio: ['pipe', 'pipe', 'inherit'], env });
const pending = new Map(); let nid = 1; let buf = '';
m.stdout.setEncoding('utf8');
m.stdout.on('data', (d) => { buf += d; let i; while ((i = buf.indexOf('\n')) >= 0) { const l = buf.slice(0, i); buf = buf.slice(i + 1); if (!l.trim()) continue; let x; try { x = JSON.parse(l); } catch { continue; } if (x.id != null && pending.has(x.id)) { pending.get(x.id)(x); pending.delete(x.id); } } });
const rpc = (method, params) => new Promise((r) => { const id = nid++; pending.set(id, r); m.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n'); });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const sha = (s) => createHash('sha256').update(s).digest('hex');
let ok = true; const check = (l, c) => { console.log(`  ${c ? '🟩 PASS' : '🟥 FAIL'} — ${l}`); if (!c) ok = false; };

(async () => {
  await rpc('initialize', { protocolVersion: '2024-11-05', capabilities: {} });
  m.stdin.write(JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }) + '\n');
  await sleep(1500);
  for (let i = 0; i < 20; i++) await rpc('tools/call', { name: 'momento.get_recent', arguments: { n: 1 } });  // many rows → rotations
  await sleep(200);

  const segs = readdirSync(dir).filter((f) => f.startsWith('audit.jsonl.') && /\.\d{4}-\d{2}-\d{2}T/.test(f)).sort();
  check(`rotation happened (${segs.length} rotated segment(s))`, segs.length >= 1);
  check(`retention held (${segs.length} ≤ KEEP=${KEEP})`, segs.length <= KEEP);
  // concatenate retained segments (chronological) + active, verify chain continuity across boundaries
  const lines = [...segs.map((s) => readFileSync(join(dir, s), 'utf8')), readFileSync(auditPath, 'utf8')].join('').trim().split('\n').filter(Boolean);
  let cont = true;
  for (let i = 1; i < lines.length; i++) { if (JSON.parse(lines[i]).prev !== sha(lines[i - 1])) { cont = false; console.log(`    break at combined row ${i}`); break; } }
  check(`hash chain UNBROKEN across ${segs.length} segment boundaries (${lines.length} rows)`, cont && lines.length > 3);
  console.log(ok ? '\n✅ ROTATION PASSED — segments rotate, prune, and the chain spans them.' : '\n❌ ROTATION FAILED');
  m.kill(); process.exit(ok ? 0 : 1);
})();
