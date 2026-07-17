#!/usr/bin/env node
/**
 * marshal — a minimal MCP aggregator + supervisor, with a singleton primary.
 *
 * One stdio MCP endpoint the client (Claude Code) connects to. Behind it, marshal spawns each backend
 * MCP server (momento/serval) as a supervised child, namespaces their tools (`momento.search`),
 * multiplexes calls, and auto-respawns a crashed backend so the client connection never drops.
 *
 * SINGLETON: Claude Code pre-warms spare sessions, so it may launch marshal more than once. To avoid
 * duplicate backends (serval :52849, momento DB) and a corrupted audit chain, the FIRST marshal becomes
 * the PRIMARY (owns backends + audit, listens on ~/.marshal/marshal.sock). Any later marshal detects it
 * and becomes a thin PROXY: it pipes its stdio ⇄ the primary's socket and spawns nothing. Every session
 * still gets a fully working MCP; only one backend fleet exists.
 *
 * Zero deps. Config: marshal.config.json ($MARSHAL_CONFIG) = { "backends": [{name,command,args}] }.
 */
import { spawn } from 'node:child_process';
import { readFileSync, appendFileSync, mkdirSync, rmSync, statSync, renameSync, readdirSync, unlinkSync, watch } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join, basename } from 'node:path';
import { homedir } from 'node:os';
import { createHash } from 'node:crypto';
import { createServer, connect } from 'node:net';

const HERE = dirname(fileURLToPath(import.meta.url));
const CONFIG = process.env.MARSHAL_CONFIG || join(HERE, 'marshal.config.json');
const backendsCfg = JSON.parse(readFileSync(CONFIG, 'utf8')).backends || [];
const SOCK = process.env.MARSHAL_SOCK || join(homedir(), '.marshal', 'marshal.sock');
const CALL_TIMEOUT = Number(process.env.MARSHAL_CALL_TIMEOUT || 120_000);  // reject a backend request that never answers (hang) so the client never stalls forever
const err = (m) => process.stderr.write(`[marshal] ${m}\n`);

// ── audit trail (primary only) ──────────────────────────────────────────────────────────────────
const AUDIT = process.env.MARSHAL_AUDIT || join(homedir(), '.marshal', 'audit.jsonl');
const AUDIT_MAX = Number(process.env.MARSHAL_AUDIT_MAX || 5_000_000);   // rotate the active log at ~5 MB
const AUDIT_KEEP = Number(process.env.MARSHAL_AUDIT_KEEP || 10);        // retain this many rotated segments
let lastHash = '0'.repeat(64);
try { const ls = readFileSync(AUDIT, 'utf8').trim().split('\n').filter(Boolean); if (ls.length) lastHash = createHash('sha256').update(ls[ls.length - 1]).digest('hex'); } catch {}
const redactArgs = (a) => (a && typeof a === 'object')
  ? Object.entries(a).map(([k, v]) => `${k}:${Array.isArray(v) ? `array[${v.length}]` : typeof v === 'string' ? `str[${v.length}]` : typeof v}`)
  : [];
// Rotate the active log to a timestamped segment. lastHash is UNCHANGED, so the next file's first row
// anchors to this segment's tip — the hash chain continues unbroken across rotations (verify by
// concatenating segments in order). Prune the oldest beyond AUDIT_KEEP.
function rotateAudit() {
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  renameSync(AUDIT, `${AUDIT}.${ts}`);
  const base = dirname(AUDIT), name = AUDIT.split('/').pop();
  const segs = readdirSync(base).filter((f) => f.startsWith(name + '.') && /\.\d{4}-\d{2}-\d{2}T/.test(f)).sort();
  for (const old of segs.slice(0, -AUDIT_KEEP)) { try { unlinkSync(join(base, old)); } catch {} }
}
function audit(entry) {
  try {
    mkdirSync(dirname(AUDIT), { recursive: true });
    try { if (statSync(AUDIT).size >= AUDIT_MAX) rotateAudit(); } catch {}
    const line = JSON.stringify({ ts: new Date().toISOString(), ...entry, prev: lastHash });
    appendFileSync(AUDIT, line + '\n');
    lastHash = createHash('sha256').update(line).digest('hex');
  } catch (e) { err(`audit write failed: ${e.message}`); }
}

// ── client sinks (primary serves N clients: its own stdio + each connected proxy) ────────────────
const sinks = new Set();
let started = false;                                       // any client finished initialize
function broadcast(o) { const s = JSON.stringify(o); for (const sink of sinks) sink(s); }
function notifyToolsChanged() {
  if (!started) return;
  broadcast({ jsonrpc: '2.0', method: 'notifications/tools/list_changed' });
  if (backends.some((b) => b.resources.length || b.resourceTemplates.length)) broadcast({ jsonrpc: '2.0', method: 'notifications/resources/list_changed' });
  if (backends.some((b) => b.prompts.length)) broadcast({ jsonrpc: '2.0', method: 'notifications/prompts/list_changed' });
}

// ── a supervised backend MCP child ──────────────────────────────────────────────────────────────
class Backend {
  constructor(cfg) { this.name = cfg.name; this.command = cfg.command; this.args = cfg.args || []; this.tools = []; this.resources = []; this.resourceTemplates = []; this.prompts = []; this.caps = {}; this.pending = new Map(); this.nextId = 1; this.buf = ''; this.ready = false; this.start(); }
  start() {
    this.ready = false;
    this.proc = spawn(this.command, this.args, { stdio: ['pipe', 'pipe', 'inherit'] });
    this.proc.stdout.setEncoding('utf8');
    this.proc.stdout.on('data', (d) => this.onData(d));
    this.proc.on('error', (e) => err(`backend ${this.name} spawn error: ${e.message}`));
    this.proc.on('exit', (code) => {
      this.ready = false; this.tools = []; this.resources = []; this.resourceTemplates = []; this.prompts = [];
      this.pending.forEach((p) => p.reject(new Error(`backend ${this.name} restarted`))); this.pending.clear();
      notifyToolsChanged(); audit({ event: 'backend_exit', backend: this.name, code });
      if (this.stopping) { err(`backend ${this.name} removed (hot-remove)`); return; }
      err(`backend ${this.name} exited (code ${code}) — respawning in 300ms`);
      setTimeout(() => this.start(), 300);
    });
    this.init();
  }
  onData(d) {
    this.buf += d; let i;
    while ((i = this.buf.indexOf('\n')) >= 0) {
      const line = this.buf.slice(0, i); this.buf = this.buf.slice(i + 1); if (!line.trim()) continue;
      let m; try { m = JSON.parse(line); } catch { continue; }
      if (m.id != null && this.pending.has(m.id)) { const p = this.pending.get(m.id); this.pending.delete(m.id); m.error ? p.reject(new Error(m.error.message || 'backend error')) : p.resolve(m.result); }
    }
  }
  req(method, params) {
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      // A backend can hang (answer never comes) rather than exit — the exit-triggered self-heal never fires,
      // so without this timer the pending promise (and the client's call) waits forever. Reject on timeout.
      const timer = setTimeout(() => { if (this.pending.delete(id)) reject(new Error(`backend ${this.name} timed out after ${CALL_TIMEOUT}ms (${method})`)); }, CALL_TIMEOUT);
      this.pending.set(id, { resolve: (v) => { clearTimeout(timer); resolve(v); }, reject: (e) => { clearTimeout(timer); reject(e); } });
      try { this.proc.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n'); } catch (e) { clearTimeout(timer); this.pending.delete(id); reject(e); }
    });
  }
  async init() {
    try {
      const initRes = await this.req('initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'marshal', version: '0.1.0' } });
      this.caps = initRes?.capabilities || {};
      this.proc.stdin.write(JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }) + '\n');
      const r = await this.req('tools/list', {});
      this.tools = (r.tools || []).map((t) => ({
        ...t,
        name: `${this.name}.${t.name}`,
        title: t.title || `${this.name} · ${t.name}`,                                 // spec `title` = human-facing display name, so the sub-tool (not just "marshal") is legible
        description: t.description ? `[${this.name}] ${t.description}` : `[${this.name}] ${t.name}`,
        _orig: t.name,
      }));
      // Aggregate resources & prompts too — only if the backend advertises the capability (else the
      // request would be an error). Resources route by their (opaque, assumed-unique) uri; prompts are
      // namespaced like tools. A backend without these caps simply contributes nothing.
      if (this.caps.resources) {
        try { this.resources = ((await this.req('resources/list', {})).resources || []).map((x) => ({ ...x, _backend: this.name })); } catch (e) { err(`backend ${this.name} resources/list failed: ${e.message}`); }
        try { this.resourceTemplates = ((await this.req('resources/templates/list', {})).resourceTemplates || []).map((x) => ({ ...x, _backend: this.name })); } catch {}
      }
      if (this.caps.prompts) {
        try { this.prompts = ((await this.req('prompts/list', {})).prompts || []).map((p) => ({ ...p, name: `${this.name}.${p.name}`, title: p.title || `${this.name} · ${p.name}`, _orig: p.name })); } catch (e) { err(`backend ${this.name} prompts/list failed: ${e.message}`); }
      }
      this.ready = true; audit({ event: 'backend_ready', backend: this.name, tools: this.tools.length, resources: this.resources.length, prompts: this.prompts.length });
      err(`backend ${this.name} ready: ${this.tools.length} tools, ${this.resources.length} resources, ${this.prompts.length} prompts`); notifyToolsChanged();
    } catch (e) { err(`backend ${this.name} init failed: ${e.message}`); }
  }
  callTool(orig, args) { return this.req('tools/call', { name: orig, arguments: args || {} }); }
}

let backends = [];                                         // populated only by the primary

// marshal's OWN tools (not from any backend). The client UI labels every call "marshal" and hides which
// backend tool ran — this lets an agent read that back from the audit log at the one chokepoint.
const MARSHAL_TOOLS = [{
  name: 'marshal.recent',
  title: 'marshal · recent calls',
  description: 'marshal introspection: the most recent tool calls marshal routed (from its audit log) — backend, tool, ok, ms, ts. Use to see which backend tool actually ran behind "marshal". Args: { limit?: number = 20, backend?: string }.',
  inputSchema: { type: 'object', properties: { limit: { type: 'number', description: 'max rows (default 20)' }, backend: { type: 'string', description: 'filter to one backend name' } } },
}];
function recentCalls(limit = 20, backend) {
  let lines = []; try { lines = readFileSync(AUDIT, 'utf8').trim().split('\n').filter(Boolean); } catch {}
  const rows = [];
  for (let i = lines.length - 1; i >= 0 && rows.length < (Number(limit) || 20); i--) {
    let o; try { o = JSON.parse(lines[i]); } catch { continue; }
    if (o.event !== 'call' || o.tool === 'recent') continue;                          // skip non-calls and marshal.recent's own rows
    if (backend && o.backend !== backend) continue;
    rows.push({ ts: o.ts, backend: o.backend || null, tool: o.tool, ok: o.ok, ms: o.ms ?? null, ...(o.error ? { error: o.error } : {}) });
  }
  return rows;                                                                        // newest-first
}
const publicTools = () => [...MARSHAL_TOOLS, ...backends.flatMap((b) => b.tools.map(({ _orig, ...t }) => t))];
function route(name) { for (const b of backends) { const t = b.tools.find((x) => x.name === name); if (t) return { b, orig: t._orig }; } return null; }

// Resources & prompts aggregation. Resources keep their original uri and route by uri lookup; prompts are
// namespaced (`backend.name`) and route by stripping the prefix, mirroring tools.
const publicResources = () => backends.flatMap((b) => b.resources.map(({ _backend, ...r }) => r));
const publicResourceTemplates = () => backends.flatMap((b) => b.resourceTemplates.map(({ _backend, ...r }) => r));
const publicPrompts = () => backends.flatMap((b) => b.prompts.map(({ _orig, ...p }) => p));
function routeResource(uri) { for (const b of backends) if (b.resources.some((r) => r.uri === uri)) return b; return null; }
function routePrompt(name) { for (const b of backends) { const p = b.prompts.find((x) => x.name === name); if (p) return { b, orig: p._orig }; } return null; }

// Hot-add/remove: re-read the config and reconcile the running backends against it (primary only).
// Edit marshal.config.json → the new backend spawns and its tools appear (via tools/list_changed);
// a removed one is stopped. No restart. (Human-gated by design — no agent-callable "add backend" tool.)
function reconcile() {
  let want; try { want = JSON.parse(readFileSync(CONFIG, 'utf8')).backends || []; } catch (e) { err(`reconcile: bad config, ignored: ${e.message}`); return; }
  const names = new Set(want.map((b) => b.name));
  for (const c of want) if (!backends.some((b) => b.name === c.name)) { backends.push(new Backend(c)); audit({ event: 'backend_add', backend: c.name }); err(`hot-add backend: ${c.name}`); }
  for (const b of backends) if (!names.has(b.name)) { b.stopping = true; try { b.proc.kill(); } catch {} audit({ event: 'backend_remove', backend: b.name }); }
  backends = backends.filter((b) => names.has(b.name));
  notifyToolsChanged();
}

// ── serve one MCP request to a given client sink ────────────────────────────────────────────────
async function handle(req, sink) {
  const { id, method, params = {} } = req;
  switch (method) {
    case 'initialize': started = true; sink(JSON.stringify({ jsonrpc: '2.0', id, result: { protocolVersion: params.protocolVersion || '2024-11-05', capabilities: { tools: { listChanged: true }, resources: { listChanged: true }, prompts: { listChanged: true } }, serverInfo: { name: 'marshal', version: '0.1.0' } } })); return;
    case 'notifications/initialized': return;
    case 'tools/list': sink(JSON.stringify({ jsonrpc: '2.0', id, result: { tools: publicTools() } })); return;
    case 'resources/list': sink(JSON.stringify({ jsonrpc: '2.0', id, result: { resources: publicResources() } })); return;
    case 'resources/templates/list': sink(JSON.stringify({ jsonrpc: '2.0', id, result: { resourceTemplates: publicResourceTemplates() } })); return;
    case 'prompts/list': sink(JSON.stringify({ jsonrpc: '2.0', id, result: { prompts: publicPrompts() } })); return;
    case 'resources/read': {
      const b = routeResource(params.uri);
      if (!b) { audit({ event: 'read', uri: params.uri, ok: false, error: 'unknown_resource' }); sink(JSON.stringify({ jsonrpc: '2.0', id, result: { contents: [], isError: true, _meta: { error: `unknown resource "${params.uri}"` } } })); return; }
      const t0 = Date.now();
      try { const res = await b.req('resources/read', { uri: params.uri }); audit({ event: 'read', backend: b.name, uri: params.uri, ok: true, ms: Date.now() - t0, result_bytes: JSON.stringify(res).length }); sink(JSON.stringify({ jsonrpc: '2.0', id, result: res })); }
      catch (e) { audit({ event: 'read', backend: b.name, uri: params.uri, ok: false, ms: Date.now() - t0, error: e.message }); sink(JSON.stringify({ jsonrpc: '2.0', id, error: { code: -32603, message: `resource read failed (backend ${b.name}): ${e.message}` } })); }
      return;
    }
    case 'prompts/get': {
      const r = routePrompt(params.name);
      if (!r) { audit({ event: 'prompt', prompt: params.name, ok: false, error: 'unknown_prompt' }); sink(JSON.stringify({ jsonrpc: '2.0', id, error: { code: -32602, message: `unknown prompt "${params.name}"` } })); return; }
      const t0 = Date.now(); const arg_keys = redactArgs(params.arguments);
      try { const res = await r.b.req('prompts/get', { name: r.orig, arguments: params.arguments || {} }); audit({ event: 'prompt', backend: r.b.name, prompt: r.orig, arg_keys, ok: true, ms: Date.now() - t0, result_bytes: JSON.stringify(res).length }); sink(JSON.stringify({ jsonrpc: '2.0', id, result: res })); }
      catch (e) { audit({ event: 'prompt', backend: r.b.name, prompt: r.orig, arg_keys, ok: false, ms: Date.now() - t0, error: e.message }); sink(JSON.stringify({ jsonrpc: '2.0', id, error: { code: -32603, message: `prompt get failed (backend ${r.b.name}): ${e.message}` } })); }
      return;
    }
    case 'ping': sink(JSON.stringify({ jsonrpc: '2.0', id, result: {} })); return;
    case 'tools/call': {
      if (params.name === 'marshal.recent') {                                         // marshal's own introspection — no backend to route to
        const rows = recentCalls(params.arguments?.limit, params.arguments?.backend);
        audit({ event: 'call', backend: 'marshal', tool: 'recent', arg_keys: redactArgs(params.arguments), ok: true, ms: 0 });
        sink(JSON.stringify({ jsonrpc: '2.0', id, result: { content: [{ type: 'text', text: JSON.stringify(rows, null, 2) }] } })); return;
      }
      const r = route(params.name);
      if (!r) { audit({ event: 'call', tool: params.name, ok: false, error: 'unknown_tool' }); sink(JSON.stringify({ jsonrpc: '2.0', id, result: { content: [{ type: 'text', text: `Error: unknown tool "${params.name}" (backend down or restarting?)` }], isError: true } })); return; }
      const t0 = Date.now(); const arg_keys = redactArgs(params.arguments);
      try { const res = await r.b.callTool(r.orig, params.arguments); audit({ event: 'call', backend: r.b.name, tool: r.orig, arg_keys, ok: !res?.isError, ms: Date.now() - t0, result_bytes: JSON.stringify(res).length }); sink(JSON.stringify({ jsonrpc: '2.0', id, result: res })); }
      catch (e) { audit({ event: 'call', backend: r.b.name, tool: r.orig, arg_keys, ok: false, ms: Date.now() - t0, error: e.message }); sink(JSON.stringify({ jsonrpc: '2.0', id, result: { content: [{ type: 'text', text: `Error (backend ${r.b.name}): ${e.message}` }], isError: true } })); }
      return;
    }
    default: if (id != null) sink(JSON.stringify({ jsonrpc: '2.0', id, error: { code: -32601, message: `method not found: ${method}` } }));
  }
}

// line-delimited JSON reader over a stream → onMsg(parsedObject)
function readLines(stream, onMsg, onErr) {
  let buf = ''; stream.setEncoding('utf8');
  stream.on('data', (d) => { buf += d; let i; while ((i = buf.indexOf('\n')) >= 0) { const l = buf.slice(0, i); buf = buf.slice(i + 1); if (!l.trim()) continue; let m; try { m = JSON.parse(l); } catch { onErr && onErr(); continue; } onMsg(m); } });
}

// ── roles ───────────────────────────────────────────────────────────────────────────────────────
function runPrimary() {
  mkdirSync(dirname(SOCK), { recursive: true });
  try { rmSync(SOCK); } catch {}                           // clear a stale socket (dead primary)
  const server = createServer((conn) => {                  // each proxy connection = another client
    const sink = (s) => { try { conn.write(s + '\n'); } catch {} };
    sinks.add(sink);
    readLines(conn, (m) => handle(m, sink));
    conn.on('close', () => sinks.delete(sink));
    conn.on('error', () => sinks.delete(sink));
  });
  server.on('error', (e) => { if (e.code === 'EADDRINUSE') { err('lost primary race — becoming proxy'); tryConnect(); } else err(`socket server error: ${e.message}`); });
  server.listen(SOCK, () => {
    backends = backendsCfg.map((c) => new Backend(c));     // ONLY the primary spawns backends
    const stdioSink = (s) => process.stdout.write(s + '\n');
    sinks.add(stdioSink);
    readLines(process.stdin, (m) => handle(m, stdioSink), () => stdioSink(JSON.stringify({ jsonrpc: '2.0', id: null, error: { code: -32700, message: 'parse error' } })));
    let wt; try { watch(dirname(CONFIG), (_e, fn) => { if (!fn || fn === basename(CONFIG)) { clearTimeout(wt); wt = setTimeout(reconcile, 300); } }); } catch (e) { err(`config watch unavailable: ${e.message}`); }
    err(`PRIMARY up — supervising ${backendsCfg.map((b) => b.name).join(', ') || '(none)'} on ${SOCK} (watching config for hot-add/remove)`);
  });
  const shutdown = () => { for (const b of backends) { try { b.proc.kill(); } catch {} } try { rmSync(SOCK); } catch {} process.exit(0); };
  process.on('SIGINT', shutdown); process.on('SIGTERM', shutdown);
}
function runProxy(sock) {                                  // thin forwarder to the primary
  err('PROXY — forwarding to the primary marshal (no backends spawned)');
  process.stdin.pipe(sock); sock.pipe(process.stdout);
  sock.on('close', () => process.exit(0));                 // primary gone → exit; Claude re-spawns → one promotes
  sock.on('error', () => process.exit(0));
  process.on('SIGINT', () => process.exit(0)); process.on('SIGTERM', () => process.exit(0));
}
function tryConnect() {
  const c = connect(SOCK);
  c.on('connect', () => runProxy(c));
  c.on('error', () => runPrimary());                       // no live primary → become one
}
tryConnect();
