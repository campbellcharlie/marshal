#!/usr/bin/env node
/**
 * Falsifiable probe: per-tool rolling stats — expectation · surprise · drift · trust.
 * marshal learns a prior for each `backend.tool` from its own audit log, then:
 *   • logs exp_ok/exp_ms on a call once it has a basis (≥ STAT_MIN samples),
 *   • tags a `surprise` when the actual outcome deviates from that prior (fail / slow),
 *   • emits a one-shot `drift` audit event when recent success-rate sags below baseline,
 *   • exposes learned trust per tool via the marshal.health introspection tool.
 * Drives outcomes through the fake backend ({fail}/{slow} args). Never touches the live fleet.
 */
import { spawn } from 'node:child_process';
import { writeFileSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const HERE = dirname(fileURLToPath(import.meta.url));
const AUDIT = `/tmp/marshal-stats-${process.pid}.jsonl`;
const cfg = `/tmp/marshal-stats-${process.pid}.json`;
writeFileSync(cfg, JSON.stringify({ backends: [{ name: 'fake', command: 'node', args: [join(HERE, 'fake-backend.mjs')] }] }));
const m = spawn('node', [join(HERE, 'marshal.mjs')], { stdio: ['pipe', 'pipe', 'inherit'], env: { ...process.env, MARSHAL_DAEMON_IDLE: '300', MARSHAL_CONFIG: cfg, MARSHAL_SOCK: `/tmp/marshal-stats-${process.pid}.sock`, MARSHAL_AUDIT: AUDIT } });
const pending = new Map(); let nextId = 1; let buf = '';
m.stdout.setEncoding('utf8');
m.stdout.on('data', (d) => { buf += d; let i; while ((i = buf.indexOf('\n')) >= 0) { const line = buf.slice(0, i); buf = buf.slice(i + 1); if (!line.trim()) continue; let msg; try { msg = JSON.parse(line); } catch { continue; } if (msg.id != null && pending.has(msg.id)) { const p = pending.get(msg.id); pending.delete(msg.id); p(msg); } } });
const rpc = (method, params) => new Promise((res) => { const id = nextId++; pending.set(id, res); m.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n'); });
const notify = (method) => m.stdin.write(JSON.stringify({ jsonrpc: '2.0', method }) + '\n');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const call = (args) => rpc('tools/call', { name: 'fake.alpha', arguments: args || {} });
const auditRows = () => { try { return readFileSync(AUDIT, 'utf8').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l)); } catch { return []; } };

let ok = true;
const check = (label, cond) => { console.log(`  ${cond ? '🟩 PASS' : '🟥 FAIL'} — ${label}`); if (!cond) ok = false; };

(async () => {
  await rpc('initialize', { protocolVersion: '2024-11-05', capabilities: {} });
  notify('notifications/initialized');
  await sleep(800);                                          // backend spawns + lists tools

  // 1) Build a success prior: enough fast wins to fill the drift window with 1s.
  for (let i = 0; i < 12; i++) await call({});

  const callRows = () => auditRows().filter((r) => r.event === 'call' && r.tool === 'alpha');
  const withExp = callRows().filter((r) => typeof r.exp_ok === 'number' && typeof r.exp_ms === 'number');
  check(`expectation logged once there's a basis (${withExp.length} rows carry exp_ok/exp_ms)`, withExp.length >= 5);
  check('no surprise tag on a run of expected successes', callRows().every((r) => !r.surprise));

  // 2) A slow success against a fast prior → `slow` surprise.
  await call({ slow: 700 });
  const slowRow = callRows().at(-1);
  check(`slow call tagged surprise:slow (got ${JSON.stringify(slowRow?.surprise)}, ms ${slowRow?.ms})`, slowRow?.surprise === 'slow' && slowRow?.ok === true);

  // 3) A failure against a high-trust prior → `fail` surprise.
  await call({ fail: true });
  const failRow = callRows().at(-1);
  check(`failure tagged surprise:fail (got ${JSON.stringify(failRow?.surprise)}, ok ${failRow?.ok})`, failRow?.surprise === 'fail' && failRow?.ok === false);

  // 4) Sustained failures → one debounced `drift` reliability event.
  for (let i = 0; i < 8; i++) await call({ fail: true });
  const drifts = auditRows().filter((r) => r.event === 'drift' && r.kind === 'reliability' && r.tool === 'alpha');
  check(`drift event fired when recent success-rate sagged (${drifts.length} drift rows)`, drifts.length >= 1);
  check('drift is debounced — not one per failing call', drifts.length <= 2);

  // 5) marshal.health surfaces learned trust + the active drift flag, least-trusted first.
  const health = JSON.parse((await rpc('tools/call', { name: 'marshal.health' })).result?.content?.[0]?.text || '[]');
  const alpha = health.find((h) => h.tool === 'fake.alpha');
  check(`marshal.health reports fake.alpha (n=${alpha?.n}, trust=${alpha?.trust})`, alpha && alpha.n >= 20 && typeof alpha.trust === 'number');
  check('marshal.health shows the reliability drift flag', alpha?.drift === 'reliability');
  check('marshal.health exposes an expected-latency estimate', typeof alpha?.exp_ms === 'number');

  console.log(ok ? '\n✅ PROBE PASSED — marshal predicts, flags surprise, detects drift, and reports learned trust.' : '\n❌ PROBE FAILED');
  m.kill(); process.exit(ok ? 0 : 1);
})();
