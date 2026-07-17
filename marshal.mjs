#!/usr/bin/env node
/**
 * marshal — a minimal MCP aggregator + supervisor.
 *
 * One stdio MCP endpoint the client (Claude Code) connects to. Behind it, marshal spawns each backend
 * MCP server (momento/serval/lorg) as a supervised child, namespaces their tools (`momento.search`),
 * multiplexes calls to the owning backend, and — the whole point — **auto-respawns a crashed backend**
 * so the client connection never drops. When a backend's tools change (crash/respawn/ready), marshal
 * emits `notifications/tools/list_changed` so the client refreshes.
 *
 * Zero deps. Config: marshal.config.json (or $MARSHAL_CONFIG) = { "backends": [{name,command,args}] }.
 * Routes it into Claude Code as a single MCP server: `node /path/to/marshal.mjs`.
 */
import { spawn } from 'node:child_process';
import { readFileSync, appendFileSync, mkdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { homedir } from 'node:os';
import { createHash } from 'node:crypto';

const HERE = dirname(fileURLToPath(import.meta.url));
const CONFIG = process.env.MARSHAL_CONFIG || join(HERE, 'marshal.config.json');
const backendsCfg = JSON.parse(readFileSync(CONFIG, 'utf8')).backends || [];
const err = (m) => process.stderr.write(`[marshal] ${m}\n`);

// ── audit trail ─────────────────────────────────────────────────────────────────────────────────
// Every tool call flows through marshal's one chokepoint, so it's the place to record provenance.
// Append-only JSONL, hash-chained (each row carries prev = sha256 of the previous LINE → tamper-evident,
// continuing across restarts). REDACTED: we log arg KEYS + type/length, never raw values (a log of
// values would itself be an exfil surface).
const AUDIT = process.env.MARSHAL_AUDIT || join(homedir(), '.marshal', 'audit.jsonl');
let lastHash = '0'.repeat(64);
try { const ls = readFileSync(AUDIT, 'utf8').trim().split('\n').filter(Boolean); if (ls.length) lastHash = createHash('sha256').update(ls[ls.length - 1]).digest('hex'); } catch {}
const redactArgs = (a) => (a && typeof a === 'object')
  ? Object.entries(a).map(([k, v]) => `${k}:${Array.isArray(v) ? `array[${v.length}]` : typeof v === 'string' ? `str[${v.length}]` : typeof v}`)
  : [];
function audit(entry) {
  try {
    mkdirSync(dirname(AUDIT), { recursive: true });
    const line = JSON.stringify({ ts: new Date().toISOString(), ...entry, prev: lastHash });
    appendFileSync(AUDIT, line + '\n');
    lastHash = createHash('sha256').update(line).digest('hex');
  } catch (e) { err(`audit write failed: ${e.message}`); }
}

let clientReady = false;                                   // client finished initialize → safe to notify
function toClient(o) { process.stdout.write(JSON.stringify(o) + '\n'); }
function notifyToolsChanged() { if (clientReady) toClient({ jsonrpc: '2.0', method: 'notifications/tools/list_changed' }); }

// ── a supervised backend MCP child we speak JSON-RPC to ─────────────────────────────────────────
class Backend {
  constructor(cfg) {
    this.name = cfg.name; this.command = cfg.command; this.args = cfg.args || [];
    this.tools = []; this.pending = new Map(); this.nextId = 1; this.buf = ''; this.ready = false;
    this.start();
  }
  start() {
    this.ready = false;
    this.proc = spawn(this.command, this.args, { stdio: ['pipe', 'pipe', 'inherit'] });
    this.proc.stdout.setEncoding('utf8');
    this.proc.stdout.on('data', (d) => this.onData(d));
    this.proc.on('error', (e) => err(`backend ${this.name} spawn error: ${e.message}`));
    this.proc.on('exit', (code) => {
      this.ready = false; this.tools = [];
      this.pending.forEach((p) => p.reject(new Error(`backend ${this.name} restarted`)));
      this.pending.clear();
      notifyToolsChanged();                                // tools just vanished — tell the client
      audit({ event: 'backend_exit', backend: this.name, code });
      err(`backend ${this.name} exited (code ${code}) — respawning in 300ms`);
      setTimeout(() => this.start(), 300);
    });
    this.init();
  }
  onData(d) {
    this.buf += d; let i;
    while ((i = this.buf.indexOf('\n')) >= 0) {
      const line = this.buf.slice(0, i); this.buf = this.buf.slice(i + 1);
      if (!line.trim()) continue;
      let m; try { m = JSON.parse(line); } catch { continue; }
      if (m.id != null && this.pending.has(m.id)) {
        const p = this.pending.get(m.id); this.pending.delete(m.id);
        m.error ? p.reject(new Error(m.error.message || 'backend error')) : p.resolve(m.result);
      }
    }
  }
  req(method, params) {
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      try { this.proc.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n'); }
      catch (e) { this.pending.delete(id); reject(e); }
    });
  }
  async init() {
    try {
      await this.req('initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'marshal', version: '0.1.0' } });
      this.proc.stdin.write(JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }) + '\n');
      const r = await this.req('tools/list', {});
      // namespace each tool as `<backend>.<tool>`; keep the original name for routing.
      this.tools = (r.tools || []).map((t) => ({ ...t, name: `${this.name}.${t.name}`, _orig: t.name }));
      this.ready = true;
      audit({ event: 'backend_ready', backend: this.name, tools: this.tools.length });
      err(`backend ${this.name} ready: ${this.tools.length} tools`);
      notifyToolsChanged();
    } catch (e) { err(`backend ${this.name} init failed: ${e.message}`); }
  }
  callTool(orig, args) { return this.req('tools/call', { name: orig, arguments: args || {} }); }
}

const backends = backendsCfg.map((c) => new Backend(c));
const publicTools = () => backends.flatMap((b) => b.tools.map(({ _orig, ...t }) => t));
function route(name) {
  for (const b of backends) { const t = b.tools.find((x) => x.name === name); if (t) return { b, orig: t._orig }; }
  return null;
}

// ── server side: speak MCP to the client over stdio ─────────────────────────────────────────────
async function handle(req) {
  const { id, method, params = {} } = req;
  const isNotif = id == null;
  switch (method) {
    case 'initialize':
      toClient({ jsonrpc: '2.0', id, result: {
        protocolVersion: params.protocolVersion || '2024-11-05',
        capabilities: { tools: { listChanged: true } },
        serverInfo: { name: 'marshal', version: '0.1.0' },
      } });
      return;
    case 'notifications/initialized': clientReady = true; return;
    case 'tools/list': toClient({ jsonrpc: '2.0', id, result: { tools: publicTools() } }); return;
    case 'ping': toClient({ jsonrpc: '2.0', id, result: {} }); return;
    case 'tools/call': {
      const r = route(params.name);
      if (!r) { audit({ event: 'call', tool: params.name, ok: false, error: 'unknown_tool' }); toClient({ jsonrpc: '2.0', id, result: { content: [{ type: 'text', text: `Error: unknown tool "${params.name}" (backend down or restarting?)` }], isError: true } }); return; }
      const t0 = Date.now(); const arg_keys = redactArgs(params.arguments);
      try {
        const res = await r.b.callTool(r.orig, params.arguments);
        audit({ event: 'call', backend: r.b.name, tool: r.orig, arg_keys, ok: !res?.isError, ms: Date.now() - t0, result_bytes: JSON.stringify(res).length });
        toClient({ jsonrpc: '2.0', id, result: res });
      } catch (e) {
        audit({ event: 'call', backend: r.b.name, tool: r.orig, arg_keys, ok: false, ms: Date.now() - t0, error: e.message });
        toClient({ jsonrpc: '2.0', id, result: { content: [{ type: 'text', text: `Error (backend ${r.b.name}): ${e.message}` }], isError: true } });
      }
      return;
    }
    default: if (!isNotif) toClient({ jsonrpc: '2.0', id, error: { code: -32601, message: `method not found: ${method}` } });
  }
}

let buf = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', (d) => {
  buf += d; let i;
  while ((i = buf.indexOf('\n')) >= 0) {
    const line = buf.slice(0, i); buf = buf.slice(i + 1);
    if (!line.trim()) continue;
    let m; try { m = JSON.parse(line); } catch { toClient({ jsonrpc: '2.0', id: null, error: { code: -32700, message: 'parse error' } }); continue; }
    handle(m);
  }
});
const shutdown = () => { for (const b of backends) { try { b.proc.kill(); } catch {} } process.exit(0); };
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
err(`marshal up — supervising ${backends.map((b) => b.name).join(', ') || '(no backends configured)'}`);
