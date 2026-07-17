#!/usr/bin/env node
/**
 * Isolated, time-boxed probe: can serval run as a marshal backend (stdio MCP over --mcp)?
 * Spawns ONE serval, does initialize → initialized → tools/list, prints the tool count, then kills it.
 * Does NOT go through marshal and does NOT touch the user's live serval instance beyond a transient spawn.
 * Hard-exits after the deadline so a hang can't stall.
 */
import { spawn } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const HERE = dirname(fileURLToPath(import.meta.url));
const cfg = JSON.parse(readFileSync(process.env.MARSHAL_CONFIG || join(HERE, 'marshal.config.json'), 'utf8'));
const b = (cfg.backends || []).find((x) => x.name === 'serval');
if (!b) { console.error('no "serval" backend in marshal.config.json — copy from the example and set its path'); process.exit(2); }
const s = spawn(b.command, b.args || [], { stdio: ['pipe', 'pipe', 'inherit'] });
const pending = new Map(); let nextId = 1; let buf = ''; let done = false;
s.stdout.setEncoding('utf8');
s.stdout.on('data', (d) => {
  buf += d; let i;
  while ((i = buf.indexOf('\n')) >= 0) {
    const line = buf.slice(0, i); buf = buf.slice(i + 1); if (!line.trim()) continue;
    let m; try { m = JSON.parse(line); } catch { continue; }
    if (m.id != null && pending.has(m.id)) { const p = pending.get(m.id); pending.delete(m.id); p(m); }
  }
});
s.on('error', (e) => finish(`🟥 spawn error: ${e.message}`, 1));
const rpc = (method, params) => new Promise((res, rej) => { const id = nextId++; pending.set(id, res); s.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n'); });
const notify = (method) => s.stdin.write(JSON.stringify({ jsonrpc: '2.0', method }) + '\n');
function finish(msg, code) { if (done) return; done = true; console.log(msg); try { s.kill(); } catch {} process.exit(code); }

const deadline = setTimeout(() => finish('🟥 TIMEOUT (15s) — serval did not complete init+list as a spawned stdio backend', 1), 15000);

(async () => {
  try {
    const init = await rpc('initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'marshal-probe', version: '0.1.0' } });
    console.log(`  init → serverInfo: ${JSON.stringify(init.result?.serverInfo || init.error || 'none')}`);
    notify('notifications/initialized');
    const l = await rpc('tools/list', {});
    const tools = l.result?.tools || [];
    clearTimeout(deadline);
    finish(`🟩 serval works as a spawned backend — ${tools.length} tools (e.g. ${tools.slice(0, 4).map((t) => t.name).join(', ')})`, tools.length ? 0 : 1);
  } catch (e) { clearTimeout(deadline); finish(`🟥 serval failed: ${e.message}`, 1); }
})();
