#!/usr/bin/env node
// Quick aggregate check: momento + serval behind ONE marshal endpoint, tools namespaced. No kill.
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url'; import { dirname, join } from 'node:path';
const HERE = dirname(fileURLToPath(import.meta.url));
const m = spawn('node', [join(HERE, 'marshal.mjs')], { stdio: ['pipe', 'pipe', 'inherit'] });
const pending = new Map(); let nextId = 1; let buf = '';
m.stdout.setEncoding('utf8');
m.stdout.on('data', (d) => { buf += d; let i; while ((i = buf.indexOf('\n')) >= 0) { const l = buf.slice(0, i); buf = buf.slice(i + 1); if (!l.trim()) continue; let x; try { x = JSON.parse(l); } catch { continue; } if (x.id != null && pending.has(x.id)) { pending.get(x.id)(x); pending.delete(x.id); } } });
const rpc = (method, params) => new Promise((r) => { const id = nextId++; pending.set(id, r); m.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n'); });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
(async () => {
  await rpc('initialize', { protocolVersion: '2024-11-05', capabilities: {} });
  m.stdin.write(JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }) + '\n');
  await sleep(3000);                                        // let both backends attach/list
  const l = await rpc('tools/list', {});
  const tools = (l.result?.tools || []).map((t) => t.name);
  const byNs = {}; for (const n of tools) { const ns = n.split('.')[0]; byNs[ns] = (byNs[ns] || 0) + 1; }
  console.log(`fleet exposes ${tools.length} tools behind one endpoint:`);
  for (const [ns, c] of Object.entries(byNs)) console.log(`  ${ns}.* → ${c}`);
  const ok = (byNs.momento || 0) > 0 && (byNs.serval || 0) > 0;
  console.log(ok ? '🟩 aggregate OK — both backends namespaced behind one marshal' : '🟥 aggregate incomplete');
  m.kill(); process.exit(ok ? 0 : 1);
})();
