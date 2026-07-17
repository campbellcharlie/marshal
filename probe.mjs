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
const m = spawn('node', [join(HERE, 'marshal.mjs')], { stdio: ['pipe', 'pipe', 'inherit'] });
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
  check(`tools/list exposes namespaced momento tools (got ${n1})`, n1 > 0 && l1.result.tools.every((t) => t.name.startsWith('momento.')));

  const c1 = await rpc('tools/call', { name: 'momento.get_recent', arguments: { n: 1 } });
  check(`call momento.get_recent works before kill`, c1.result && !c1.result.isError);

  // Kill ONLY marshal's own child (the momento backend it spawned) — never the user's live momento.
  const kids = execSync(`pgrep -P ${m.pid}`).toString().trim().split('\n').filter(Boolean);
  console.log(`  … killing marshal's momento backend child (pid ${kids.join(',')}) …`);
  for (const pid of kids) { try { execSync(`kill ${pid}`); } catch {} }
  await sleep(2000);                                       // marshal should respawn + re-init

  const l2 = await rpc('tools/list', {});
  const n2 = (l2.result?.tools || []).length;
  check(`tools survive the crash (still ${n2}, connection never dropped)`, n2 === n1 && n1 > 0);

  const c2 = await rpc('tools/call', { name: 'momento.get_recent', arguments: { n: 1 } });
  check(`call works AGAIN after respawn (self-heal)`, c2.result && !c2.result.isError);

  console.log(ok ? '\n✅ PROBE PASSED — marshal self-heals; the client never saw a disconnect.' : '\n❌ PROBE FAILED');
  m.kill(); process.exit(ok ? 0 : 1);
})();
