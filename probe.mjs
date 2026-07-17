#!/usr/bin/env node
/**
 * Falsifiable probe: does marshal survive a backend crash?
 * Drives marshal over stdio: list+call a momento tool, KILL the momento backend, then list+call again.
 * PASS = the client connection never drops and the tool works again after respawn (self-heal).
 */
import { spawn, execSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const HERE = dirname(fileURLToPath(import.meta.url));
const m = spawn('node', [join(HERE, 'marshal.mjs')], { stdio: ['pipe','pipe','inherit'], env: { ...process.env, MARSHAL_SOCK: `/tmp/marshal-${process.pid}.sock`, MARSHAL_AUDIT: `/tmp/marshal-${process.pid}.jsonl`, MARSHAL_DAEMON_IDLE: '300' } });
const pending = new Map(); let nextId = 1; let buf = '';
m.stdout.setEncoding('utf8');
m.stdout.on('data', (d) => {
  buf += d; let i;
  while ((i = buf.indexOf('\n')) >= 0) {
    const line = buf.slice(0, i); buf = buf.slice(i + 1); if (!line.trim()) continue;
    let msg; try { msg = JSON.parse(line); } catch { continue; }
    if (msg.method === 'notifications/tools/list_changed') { console.log('  ← notification: tools/list_changed'); continue; }
    if (msg.id != null && pending.has(msg.id)) { const p = pending.get(msg.id); pending.delete(msg.id); p(msg); }
  }
});
const rpc = (method, params) => new Promise((res) => { const id = nextId++; pending.set(id, res); m.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n'); });
const notify = (method) => m.stdin.write(JSON.stringify({ jsonrpc: '2.0', method }) + '\n');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let ok = true;
const check = (label, cond) => { console.log(`  ${cond ? '🟩 PASS' : '🟥 FAIL'} — ${label}`); if (!cond) ok = false; };

(async () => {
  const init = await rpc('initialize', { protocolVersion: '2024-11-05', capabilities: {} });
  check(`initialize → serverInfo.name == marshal`, init.result?.serverInfo?.name === 'marshal');
  notify('notifications/initialized');
  await sleep(1500);                                       // let momento backend come up

  const l1 = await rpc('tools/list', {});
  const n1 = (l1.result?.tools || []).length;
  check(`tools/list exposes namespaced tools (got ${n1})`, n1 > 0 && l1.result.tools.every((t) => t.name.includes('.')) && l1.result.tools.some((t) => t.name.startsWith('momento.')));

  const c1 = await rpc('tools/call', { name: 'momento.get_recent', arguments: { n: 1 } });
  check(`call momento.get_recent works before kill`, c1.result && !c1.result.isError);

  // Kill ONLY this probe's momento backend — never the user's live momento. The proxy (m) spawned a
  // detached daemon; the daemon owns the backend. So the backend is m's GRANDCHILD: m → daemon → momento.
  const daemon = execSync(`pgrep -P ${m.pid}`).toString().trim().split('\n').filter(Boolean)[0];
  const kids = daemon ? execSync(`pgrep -P ${daemon}`).toString().trim().split('\n').filter(Boolean) : [];
  console.log(`  … killing this probe's momento backend (daemon ${daemon}, backend pid ${kids.join(',')}) …`);
  for (const pid of kids) { try { execSync(`kill ${pid}`); } catch {} }
  await sleep(2000);                                       // daemon should respawn + re-init the backend

  const l2 = await rpc('tools/list', {});
  const n2 = (l2.result?.tools || []).length;
  check(`tools survive the crash (still ${n2}, connection never dropped)`, n2 === n1 && n1 > 0);

  const c2 = await rpc('tools/call', { name: 'momento.get_recent', arguments: { n: 1 } });
  check(`call works AGAIN after respawn (self-heal)`, c2.result && !c2.result.isError);

  console.log(ok ? '\n✅ PROBE PASSED — marshal self-heals; the client never saw a disconnect.' : '\n❌ PROBE FAILED');
  m.kill(); try { if (daemon) execSync(`kill ${daemon}`); } catch {}     // proxy + detached daemon
  process.exit(ok ? 0 : 1);
})();
