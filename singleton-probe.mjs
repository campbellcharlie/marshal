#!/usr/bin/env node
/**
 * Verifies the singleton daemon + cross-session survival:
 *  - two marshals share ONE backend fleet; each is a thin PROXY, the fleet lives in a DETACHED daemon.
 *  - KEY FIX: killing the session that STARTED the daemon (A) does NOT take the fleet down — the other
 *    session (B) stays connected and keeps working. (This is the bug that disconnected us: a sibling
 *    session's exit SIGINT'd the shared primary and killed everyone.)
 * Uses temp sock/audit + a momento-only config (read from marshal.config.json — no hardcoded paths).
 */
import { spawn, execSync } from 'node:child_process';
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
const env = { ...process.env, MARSHAL_SOCK: join(dir, 'm.sock'), MARSHAL_AUDIT: join(dir, 'audit.jsonl'), MARSHAL_CONFIG: cfg, MARSHAL_DAEMON_IDLE: '2000' };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let ok = true; const check = (l, c) => { console.log(`  ${c ? '🟩 PASS' : '🟥 FAIL'} — ${l}`); if (!c) ok = false; };
const childrenOf = (pid) => { try { return execSync(`pgrep -P ${pid}`).toString().trim().split('\n').filter(Boolean); } catch { return []; } };

function driver(proc) {
  const pending = new Map(); let nid = 1; let buf = '';
  proc.stdout.setEncoding('utf8');
  proc.stdout.on('data', (d) => { buf += d; let i; while ((i = buf.indexOf('\n')) >= 0) { const l = buf.slice(0, i); buf = buf.slice(i + 1); if (!l.trim()) continue; let m; try { m = JSON.parse(l); } catch { continue; } if (m.id != null && pending.has(m.id)) { pending.get(m.id)(m); pending.delete(m.id); } } });
  return { rpc: (method, params) => new Promise((r) => { const id = nid++; pending.set(id, r); proc.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n'); }), notify: (method) => proc.stdin.write(JSON.stringify({ jsonrpc: '2.0', method }) + '\n') };
}

(async () => {
  const A = spawn('node', [join(HERE, 'marshal.mjs')], { stdio: ['pipe', 'pipe', 'inherit'], env });
  await sleep(2000);                                        // A proxies + spawns the detached daemon; momento comes up
  const B = spawn('node', [join(HERE, 'marshal.mjs')], { stdio: ['pipe', 'pipe', 'inherit'], env });
  await sleep(1200);                                        // B connects to the SAME daemon

  const aKids = childrenOf(A.pid);                          // A's child is the daemon it launched
  const daemon = aKids[0];
  check(`A launched the detached daemon (${aKids.length} child)`, aKids.length === 1);
  check(`B spawned NOTHING (pure proxy)`, childrenOf(B.pid).length === 0);
  check(`the daemon owns the backend fleet`, !!daemon && childrenOf(daemon).length >= 1);

  const b = driver(B);
  const init = await b.rpc('initialize', { protocolVersion: '2024-11-05', capabilities: {} });
  check(`proxy B serves initialize via the daemon → name=${init.result?.serverInfo?.name}`, init.result?.serverInfo?.name === 'marshal');
  b.notify('notifications/initialized');
  const l1 = await b.rpc('tools/list', {});
  check(`proxy B lists tools through the daemon`, (l1.result?.tools || []).some((t) => t.name.startsWith('momento.')));

  // THE FIX: kill A — the session that started the daemon. A dying must NOT take the fleet down.
  console.log(`  … killing session A (the daemon starter, pid ${A.pid}) …`);
  try { A.kill('SIGINT'); } catch {}
  await sleep(1500);
  check(`daemon SURVIVES A's death (still running)`, !!daemon && (() => { try { process.kill(daemon, 0); return true; } catch { return false; } })());
  const l2 = await b.rpc('tools/list', {});
  check(`proxy B STILL lists tools after A died (no disconnect)`, (l2.result?.tools || []).some((t) => t.name.startsWith('momento.')));
  const call = await b.rpc('tools/call', { name: 'momento.get_recent', arguments: { n: 1 } });
  check(`proxy B STILL routes a real tool call after A died`, call.result && !call.result.isError);

  console.log(ok ? '\n✅ SINGLETON PASSED — 1 detached daemon fleet; a sibling session dying never disconnects the others.' : '\n❌ SINGLETON FAILED');
  try { B.kill(); if (daemon) execSync(`kill ${daemon}`); } catch {}
  process.exit(ok ? 0 : 1);
})();
