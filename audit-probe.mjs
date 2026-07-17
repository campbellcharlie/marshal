#!/usr/bin/env node
/**
 * Verifies marshal's audit trail: rows written at the chokepoint, args REDACTED (a sentinel value
 * must never appear), and the hash chain valid (tamper-evident). Uses a temp audit file + momento-only
 * config (read from marshal.config.json, so no hardcoded paths).
 */
import { spawn } from 'node:child_process';
import { readFileSync, writeFileSync, mkdtempSync } from 'node:fs';
import { tmpdir, homedir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';

const HERE = dirname(fileURLToPath(import.meta.url));
const real = JSON.parse(readFileSync(process.env.MARSHAL_CONFIG || join(HERE, 'marshal.config.json'), 'utf8'));
const momento = (real.backends || []).find((b) => b.name === 'momento');
if (!momento) { console.error('no momento backend in config'); process.exit(2); }
const dir = mkdtempSync(join(tmpdir(), 'marshal-audit-'));
const auditFile = join(dir, 'audit.jsonl');
const cfgFile = join(dir, 'cfg.json');
writeFileSync(cfgFile, JSON.stringify({ backends: [momento] }));
const SENTINEL = 'REDACT_SENTINEL_XYZ';

const m = spawn('node', [join(HERE, 'marshal.mjs')], { stdio: ['pipe', 'pipe', 'inherit'], env: { ...process.env, MARSHAL_AUDIT: auditFile, MARSHAL_SOCK: join(dir, 'm.sock'), MARSHAL_CONFIG: cfgFile } });
const pending = new Map(); let nextId = 1; let buf = '';
m.stdout.setEncoding('utf8');
m.stdout.on('data', (d) => { buf += d; let i; while ((i = buf.indexOf('\n')) >= 0) { const l = buf.slice(0, i); buf = buf.slice(i + 1); if (!l.trim()) continue; let x; try { x = JSON.parse(l); } catch { continue; } if (x.id != null && pending.has(x.id)) { pending.get(x.id)(x); pending.delete(x.id); } } });
const rpc = (method, params) => new Promise((r) => { const id = nextId++; pending.set(id, r); m.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n'); });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let ok = true; const check = (l, c) => { console.log(`  ${c ? '🟩 PASS' : '🟥 FAIL'} — ${l}`); if (!c) ok = false; };

(async () => {
  await rpc('initialize', { protocolVersion: '2024-11-05', capabilities: {} });
  m.stdin.write(JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }) + '\n');
  await sleep(1500);
  await rpc('tools/call', { name: 'momento.search', arguments: { query: SENTINEL } });   // secret-ish arg value
  await rpc('tools/call', { name: 'momento.get_recent', arguments: { n: 1 } });
  await sleep(200);

  const raw = readFileSync(auditFile, 'utf8');
  const lines = raw.trim().split('\n').filter(Boolean);
  const rows = lines.map((l) => JSON.parse(l));
  check(`audit rows written (${rows.length})`, rows.length >= 3);
  check('call rows carry provenance (backend/tool/arg_keys/ms/prev)', rows.some((r) => r.event === 'call' && r.backend === 'momento' && r.tool && Array.isArray(r.arg_keys) && typeof r.ms === 'number' && r.prev));
  check('REDACTION — raw arg value never appears in the log', !raw.includes(SENTINEL));
  check('arg_keys are key:type only (no values)', rows.filter((r) => r.event === 'call').every((r) => (r.arg_keys || []).every((k) => /^[^:]+:(number|boolean|object|str\[\d+\]|array\[\d+\])$/.test(k))));
  let prev = '0'.repeat(64), chain = true;
  for (const l of lines) { const r = JSON.parse(l); if (r.prev !== prev) { chain = false; break; } prev = createHash('sha256').update(l).digest('hex'); }
  check('hash chain valid (tamper-evident, genesis→…)', chain);
  console.log('  sample row:', JSON.stringify(rows.find((r) => r.event === 'call')));
  console.log(ok ? '\n✅ AUDIT PROBE PASSED' : '\n❌ AUDIT PROBE FAILED');
  m.kill(); process.exit(ok ? 0 : 1);
})();
