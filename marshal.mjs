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
import { readFileSync, appendFileSync, mkdirSync, rmSync, statSync, renameSync, readdirSync, unlinkSync, watch, openSync } from 'node:fs';
import { fileURLToPath, URL } from 'node:url';
import { dirname, join, basename } from 'node:path';
import { homedir } from 'node:os';
import { createHash } from 'node:crypto';
import { createServer, connect } from 'node:net';
import http from 'node:http';
import https from 'node:https';

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

// ── per-tool rolling stats: expectation · surprise · drift · trust (daemon only) ──────────────────
// One in-memory record per `backend.tool`, seeded from the audit log on startup and updated on every
// routed call. Three cheap, model-free signals fall out of the same numbers:
//   • EXPECTATION — before a call, predict ok-probability + latency; the call row logs exp_ok/exp_ms and
//                   a `surprise` tag when the ACTUAL outcome deviates from that prior (the learning signal).
//   • DRIFT       — when a tool's RECENT success-rate sags below its long-run baseline, emit a one-shot
//                   `drift` audit event (debounced). Behaviour changed, not death — self-heal can't see this.
//   • TRUST       — a volume-shrunk success rate in [0,1], surfaced via marshal.health (flag, don't gate).
const STAT_MIN = 5;              // samples needed before we predict / judge drift (else: no basis, stay silent)
const EWMA_A = 0.2;              // latency smoothing weight on the newest sample
const SLOW_X = 3;                // actual > SLOW_X × expected (and past a floor) ⇒ a `slow` surprise
const DRIFT_DROP = 0.3;          // recent success-rate this far below baseline ⇒ reliability drift
const DRIFT_WIN = 10;            // sliding window defining "recent" behaviour
const stats = new Map();         // `${backend}.${tool}` → { n, ok, ewma, recent[], drifted }
let seeding = false;             // suppress drift events while replaying historical audit rows
function statOf(key) {
  let s = stats.get(key);
  if (!s) { s = { n: 0, ok: 0, ewma: 0, recent: [], drifted: null }; stats.set(key, s); }
  return s;
}
const trustOf = (s) => (s.ok + 1) / (s.n + 2);               // Laplace-smoothed ok-rate — never degenerate at 0/1
function expect(key) {
  const s = stats.get(key);
  if (!s || s.n < STAT_MIN) return {};                       // no basis yet → predict nothing
  return { exp_ok: Math.round(trustOf(s) * 100) / 100, exp_ms: Math.round(s.ewma) };
}
// Fold one outcome into the stat; return a short surprise tag (or undefined) and fire a debounced drift event.
function record(key, backend, tool, ok, ms) {
  const s = statOf(key);
  const had = s.n >= STAT_MIN, priorOk = trustOf(s), priorMs = s.ewma;
  s.n++; if (ok) s.ok++;
  s.ewma = s.ewma ? EWMA_A * ms + (1 - EWMA_A) * s.ewma : ms;
  s.recent.push(ok ? 1 : 0); if (s.recent.length > DRIFT_WIN) s.recent.shift();
  let surprise;                                              // judged against the PRIOR (pre-update) expectation
  if (had) {
    if (ok && priorOk < 0.5) surprise = 'recover';           // expected failure, got success
    else if (!ok && priorOk > 0.8) surprise = 'fail';        // expected success, got failure
    else if (ok && priorMs && ms > Math.max(priorMs * SLOW_X, priorMs + 500)) surprise = 'slow';
  }
  if (s.n >= STAT_MIN * 2 && s.recent.length >= DRIFT_WIN) { // recent window departs from long-run baseline?
    const recentOk = s.recent.reduce((a, b) => a + b, 0) / s.recent.length;
    const drifting = trustOf(s) - recentOk >= DRIFT_DROP;
    if (drifting && s.drifted !== 'reliability') { s.drifted = 'reliability'; if (!seeding) audit({ event: 'drift', backend, tool, kind: 'reliability', recent: Math.round(recentOk * 100) / 100, baseline: Math.round(trustOf(s) * 100) / 100 }); }
    else if (!drifting && s.drifted === 'reliability') s.drifted = null;   // recovered → re-arm
  }
  return surprise;
}
// Replay existing call rows so expectations/trust survive a daemon restart (best-effort; drift muted).
function seedStats() {
  seeding = true;
  let lines = []; try { lines = readFileSync(AUDIT, 'utf8').trim().split('\n').filter(Boolean); } catch {}
  for (const l of lines) {
    let o; try { o = JSON.parse(l); } catch { continue; }
    if (o.event !== 'call' || !o.backend || o.backend === 'marshal' || o.tool === 'recent' || typeof o.ms !== 'number') continue;
    record(`${o.backend}.${o.tool}`, o.backend, o.tool, !!o.ok, o.ms);
  }
  for (const s of stats.values()) s.drifted = null;          // start armed; history should not pre-trip drift
  seeding = false;
}
function healthReport() {
  const rows = [];
  for (const [key, s] of stats) rows.push({ tool: key, n: s.n, trust: Math.round(trustOf(s) * 100) / 100, exp_ms: s.ewma ? Math.round(s.ewma) : null, drift: s.drifted || null });
  return rows.sort((a, b) => a.trust - b.trust);             // least-trusted first — the ones to eyeball
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

// ── a supervised backend MCP server (a spawned stdio child, or a remote HTTP/SSE MCP server) ──────
// Config: stdio = {name, command, args}; remote = {name, url, headers?, transport?}. `transport` is
// 'http' (Streamable HTTP), 'sse' (classic SSE), or 'auto' (try HTTP, fall back to SSE). The JSON-RPC
// dispatch, timeout, aggregation, routing, audit and self-heal are all transport-agnostic — only how a
// message is sent and how death is detected differs per transport.
class Backend {
  constructor(cfg) {
    this.name = cfg.name; this.command = cfg.command; this.args = cfg.args || [];
    this.url = cfg.url || null; this.headers = cfg.headers || {};
    this.transport = cfg.transport || (this.url ? 'auto' : 'stdio');
    this.tools = []; this.resources = []; this.resourceTemplates = []; this.prompts = []; this.caps = {};
    this.pending = new Map(); this.nextId = 1; this.buf = ''; this.sbuf = ''; this.ready = false; this.retries = 0;
    this.start();
  }
  start() {
    this.reconnecting = false; this.ready = false;
    if (this.url) return this.startRemote();
    this.proc = spawn(this.command, this.args, { stdio: ['pipe', 'pipe', 'inherit'] });   // stdio: supervised child
    this.proc.stdout.setEncoding('utf8');
    this.proc.stdout.on('data', (d) => this.onData(d));
    this.proc.on('error', (e) => err(`backend ${this.name} spawn error: ${e.message}`));
    this.proc.on('exit', (code) => this.onClose(`exited (code ${code})`));
    this.init().catch((e) => err(`backend ${this.name} init failed: ${e.message}`));
  }
  // Remote transports. HTTP has no long-lived socket (init/req are POSTs); SSE holds a GET stream open
  // and POSTs requests to the endpoint the server advertises.
  startRemote() {
    this.sessionId = null; this.postUrl = null; this.sbuf = '';
    if (this.transport === 'sse') { this.transportKind = 'sse'; return this.startSSE(); }
    this.transportKind = 'http';                                                        // 'http' or 'auto' → try Streamable HTTP first
    this.init().then(() => {}, (e) => {
      if (this.transport === 'auto') { err(`backend ${this.name} HTTP init failed (${e.message}); trying SSE`); this.transportKind = 'sse'; this.startSSE(); }
      else this.onClose(`init failed: ${e.message}`);
    });
  }
  startSSE() {
    const u = new URL(this.url); const mod = u.protocol === 'https:' ? https : http;
    const req = mod.request(u, { method: 'GET', headers: { Accept: 'text/event-stream', ...this.headers } }, (res) => {
      if (res.statusCode !== 200) { res.resume(); return this.onClose(`SSE GET ${res.statusCode}`); }
      res.setEncoding('utf8'); this.sseRes = res;
      res.on('data', (d) => this.feedSSE(d));
      res.on('end', () => this.onClose('SSE stream ended'));
      res.on('error', (e) => this.onClose(`SSE stream error: ${e.message}`));
    });
    req.on('error', (e) => this.onClose(`SSE connect error: ${e.message}`));
    req.end(); this.sseReq = req;
  }
  // Parse SSE frames (event:/data:) from a stream. The classic-SSE GET stream first emits an `endpoint`
  // frame naming the POST URL (which triggers init); every other frame carries a JSON-RPC message.
  feedSSE(chunk) {
    this.sbuf += chunk; let idx;
    while ((idx = this.sbuf.indexOf('\n\n')) >= 0) {
      const frame = this.sbuf.slice(0, idx); this.sbuf = this.sbuf.slice(idx + 2);
      let event = 'message', data = '';
      for (const l of frame.split('\n')) { if (l.startsWith('event:')) event = l.slice(6).trim(); else if (l.startsWith('data:')) data += l.slice(5).trim(); }
      if (event === 'endpoint') { this.postUrl = new URL(data, this.url).href; if (!this.initStarted) { this.initStarted = true; this.init().catch((e) => this.onClose(`init failed: ${e.message}`)); } }
      else if (data) { try { this.dispatch(JSON.parse(data)); } catch {} }
    }
  }
  // POST a JSON-RPC message. For Streamable HTTP the reply is in the POST body (JSON or an SSE stream) →
  // dispatch it; for classic SSE the POST is just accepted (202) and the reply arrives on the GET stream.
  httpPost(url, body, readReply) {
    return new Promise((resolve, reject) => {
      const u = new URL(url); const mod = u.protocol === 'https:' ? https : http;
      const headers = { 'Content-Type': 'application/json', Accept: 'application/json, text/event-stream', ...this.headers };
      if (this.sessionId) headers['Mcp-Session-Id'] = this.sessionId;
      const req = mod.request(u, { method: 'POST', headers }, (res) => {
        const sid = res.headers['mcp-session-id']; if (sid) this.sessionId = sid;
        const ct = res.headers['content-type'] || '';
        res.setEncoding('utf8');
        if (readReply && ct.includes('text/event-stream')) { res.on('data', (d) => this.feedSSE(d)); res.on('end', resolve); res.on('error', reject); return; }
        let data = ''; res.on('data', (d) => data += d);
        res.on('end', () => {
          if (!readReply) return resolve();
          if (res.statusCode >= 400) return reject(new Error(`HTTP ${res.statusCode}`));
          if (data.trim()) { try { this.dispatch(JSON.parse(data)); return resolve(); } catch {} }
          reject(new Error(`no JSON-RPC reply (status ${res.statusCode}, type ${ct || 'none'})`));   // not Streamable HTTP → lets 'auto' fall back to SSE fast
        });
        res.on('error', reject);
      });
      req.on('error', reject); req.write(body); req.end();
    });
  }
  // Transport-agnostic send. Returns a promise (remote) or undefined (stdio); req() awaits it either way.
  send(obj) {
    const line = JSON.stringify(obj);
    if (!this.url) return this.proc.stdin.write(line + '\n');
    if (this.transportKind === 'sse') return this.httpPost(this.postUrl, line, false);
    return this.httpPost(this.url, line, true);
  }
  notify(method, params) { try { const r = this.send({ jsonrpc: '2.0', method, params }); if (r && r.catch) r.catch(() => {}); } catch {} }
  onData(d) {
    this.buf += d; let i;
    while ((i = this.buf.indexOf('\n')) >= 0) {
      const line = this.buf.slice(0, i); this.buf = this.buf.slice(i + 1); if (!line.trim()) continue;
      let m; try { m = JSON.parse(line); } catch { continue; }
      this.dispatch(m);
    }
  }
  // Match a JSON-RPC response to its pending request (shared by every transport).
  dispatch(m) {
    if (m.id != null && this.pending.has(m.id)) { const p = this.pending.get(m.id); this.pending.delete(m.id); m.error ? p.reject(new Error(m.error.message || 'backend error')) : p.resolve(m.result); }
  }
  // A transport died (process exit or connection drop). Reject in-flight calls and reconnect with capped
  // exponential backoff (300ms → 30s) so a permanently-down backend doesn't hammer-retry — which matters
  // most for a remote URL that can't be respawned, only re-connected. `retries` resets to 0 on ready.
  onClose(reason) {
    if (this.reconnecting) return;
    this.reconnecting = true; this.ready = false; this.initStarted = false;
    this.tools = []; this.resources = []; this.resourceTemplates = []; this.prompts = [];
    this.pending.forEach((p) => p.reject(new Error(`backend ${this.name} restarted`))); this.pending.clear();
    notifyToolsChanged(); audit({ event: 'backend_exit', backend: this.name, reason });
    if (this.stopping) { err(`backend ${this.name} removed (hot-remove)`); return; }
    const delay = Math.min(300 * 2 ** this.retries, 30_000); this.retries++;
    err(`backend ${this.name} ${reason} — reconnecting in ${delay}ms`);
    setTimeout(() => this.start(), delay);
  }
  stop() { this.stopping = true; try { this.proc?.kill(); this.sseReq?.destroy(); this.sseRes?.destroy(); } catch {} }
  req(method, params) {
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      // A backend can hang (answer never comes) rather than exit — the exit-triggered self-heal never fires,
      // so without this timer the pending promise (and the client's call) waits forever. Reject on timeout.
      const timer = setTimeout(() => { if (this.pending.delete(id)) reject(new Error(`backend ${this.name} timed out after ${CALL_TIMEOUT}ms (${method})`)); }, CALL_TIMEOUT);
      this.pending.set(id, { resolve: (v) => { clearTimeout(timer); resolve(v); }, reject: (e) => { clearTimeout(timer); reject(e); } });
      // send() is sync for stdio, a POST promise for remote — a transport-level failure rejects this call.
      Promise.resolve().then(() => this.send({ jsonrpc: '2.0', id, method, params })).catch((e) => { clearTimeout(timer); if (this.pending.delete(id)) reject(e); });
    });
  }
  // Throws on core-handshake failure; the caller (start / startRemote / feedSSE) decides whether to log
  // (stdio), fall back to another transport (auto), or reconnect (remote).
  async init() {
    const initRes = await this.req('initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'marshal', version: '0.1.0' } });
    this.caps = initRes?.capabilities || {};
    this.notify('notifications/initialized');
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
    // Behavioural canary: a backend that reconnects advertising FEWER tools than before changed its
    // contract (removed/renamed a tool) — liveness self-heal can't see this. Flag it; don't block.
    if (this.lastToolCount != null && this.tools.length < this.lastToolCount) audit({ event: 'drift', backend: this.name, kind: 'tools', was: this.lastToolCount, now: this.tools.length });
    this.lastToolCount = this.tools.length;
    this.ready = true; this.retries = 0; audit({ event: 'backend_ready', backend: this.name, transport: this.transportKind || 'stdio', tools: this.tools.length, resources: this.resources.length, prompts: this.prompts.length });
    err(`backend ${this.name} ready (${this.transportKind || 'stdio'}): ${this.tools.length} tools, ${this.resources.length} resources, ${this.prompts.length} prompts`); notifyToolsChanged();
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
}, {
  name: 'marshal.health',
  title: 'marshal · tool health',
  description: 'marshal introspection: per-tool reliability learned from the audit log — n (calls seen), trust (0-1 volume-shrunk success rate), exp_ms (expected latency), drift (reliability | null). Least-trusted first, so a flaky or drifting backend tool surfaces at the top. Args: none.',
  inputSchema: { type: 'object', properties: {} },
}];
function recentCalls(limit = 20, backend) {
  let lines = []; try { lines = readFileSync(AUDIT, 'utf8').trim().split('\n').filter(Boolean); } catch {}
  const rows = [];
  for (let i = lines.length - 1; i >= 0 && rows.length < (Number(limit) || 20); i--) {
    let o; try { o = JSON.parse(lines[i]); } catch { continue; }
    if (o.event !== 'call' || o.tool === 'recent') continue;                          // skip non-calls and marshal.recent's own rows
    if (backend && o.backend !== backend) continue;
    rows.push({ ts: o.ts, backend: o.backend || null, tool: o.tool, ok: o.ok, ms: o.ms ?? null, ...(o.surprise ? { surprise: o.surprise } : {}), ...(o.error ? { error: o.error } : {}) });
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
  for (const b of backends) if (!names.has(b.name)) { b.stop(); audit({ event: 'backend_remove', backend: b.name }); }
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
      if (params.name === 'marshal.health') {                                         // learned per-tool reliability
        audit({ event: 'call', backend: 'marshal', tool: 'health', arg_keys: [], ok: true, ms: 0 });
        sink(JSON.stringify({ jsonrpc: '2.0', id, result: { content: [{ type: 'text', text: JSON.stringify(healthReport(), null, 2) }] } })); return;
      }
      const r = route(params.name);
      if (!r) { audit({ event: 'call', tool: params.name, ok: false, error: 'unknown_tool' }); sink(JSON.stringify({ jsonrpc: '2.0', id, result: { content: [{ type: 'text', text: `Error: unknown tool "${params.name}" (backend down or restarting?)` }], isError: true } })); return; }
      const t0 = Date.now(); const arg_keys = redactArgs(params.arguments);
      const key = `${r.b.name}.${r.orig}`, ex = expect(key);                          // predict BEFORE the call from prior calls
      try { const res = await r.b.callTool(r.orig, params.arguments); const ms = Date.now() - t0, ok = !res?.isError; const surprise = record(key, r.b.name, r.orig, ok, ms); audit({ event: 'call', backend: r.b.name, tool: r.orig, arg_keys, ok, ms, result_bytes: JSON.stringify(res).length, ...ex, ...(surprise ? { surprise } : {}) }); sink(JSON.stringify({ jsonrpc: '2.0', id, result: res })); }
      catch (e) { const ms = Date.now() - t0; const surprise = record(key, r.b.name, r.orig, false, ms); audit({ event: 'call', backend: r.b.name, tool: r.orig, arg_keys, ok: false, ms, error: e.message, ...ex, ...(surprise ? { surprise } : {}) }); sink(JSON.stringify({ jsonrpc: '2.0', id, result: { content: [{ type: 'text', text: `Error (backend ${r.b.name}): ${e.message}` }], isError: true } })); }
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
// The backend fleet lives in a DETACHED daemon that owns the socket — NOT inside any session's process.
// Every session-spawned marshal (the first one included) is a thin PROXY to that daemon. So when the
// session that happened to start the daemon exits — Claude sends *its* marshal a SIGINT — only that proxy
// dies; the daemon (in its own process group) and every other session's connection survive. That is what
// stops the cross-session disconnect: no single session's lifecycle owns the fleet.
const DAEMON_IDLE = Number(process.env.MARSHAL_DAEMON_IDLE || 60_000);   // daemon self-exits this long after its LAST client leaves

function runDaemon() {                                      // `--daemon`: don't fight an already-running daemon
  const probe = connect(SOCK);
  probe.on('connect', () => { probe.destroy(); process.exit(0); });      // a live daemon owns the socket → stand down
  probe.on('error', () => { probe.destroy(); startDaemon(); });          // stale/absent → take over
}
function startDaemon() {
  mkdirSync(dirname(SOCK), { recursive: true });
  try { rmSync(SOCK); } catch {}                           // clear a stale socket (dead daemon)
  let idleTimer = null;
  const shutdown = () => { for (const b of backends) b.stop(); try { rmSync(SOCK); } catch {} process.exit(0); };
  const armIdle = () => { clearTimeout(idleTimer); if (sinks.size === 0) idleTimer = setTimeout(shutdown, DAEMON_IDLE); };
  const server = createServer((conn) => {                  // each connection = one client (a session's proxy)
    clearTimeout(idleTimer);
    const sink = (s) => { try { conn.write(s + '\n'); } catch {} };
    sinks.add(sink);
    readLines(conn, (m) => handle(m, sink));
    conn.on('close', () => { sinks.delete(sink); armIdle(); });
    conn.on('error', () => { sinks.delete(sink); armIdle(); });
  });
  server.on('error', (e) => { if (e.code === 'EADDRINUSE') process.exit(0); else err(`daemon socket error: ${e.message}`); });
  server.listen(SOCK, () => {
    backends = backendsCfg.map((c) => new Backend(c));     // the daemon — and only the daemon — spawns backends
    let wt; try { watch(dirname(CONFIG), (_e, fn) => { if (!fn || fn === basename(CONFIG)) { clearTimeout(wt); wt = setTimeout(reconcile, 300); } }); } catch (e) { err(`config watch unavailable: ${e.message}`); }
    armIdle();                                             // if nobody ever connects, self-exit after idle
    err(`DAEMON up — supervising ${backendsCfg.map((b) => b.name).join(', ') || '(none)'} on ${SOCK} (detached; idle-exit ${DAEMON_IDLE}ms)`);
  });
  process.on('SIGTERM', shutdown);                         // explicit stop only; a detached daemon never receives a dying session's SIGINT
}
function runProxy(sock) {                                  // thin forwarder to the daemon — spawns no backends
  process.stdin.pipe(sock); sock.pipe(process.stdout);
  sock.on('close', () => process.exit(0));                 // daemon gone → exit; Claude re-spawns and re-launches/re-connects
  sock.on('error', () => process.exit(0));
  process.on('SIGINT', () => process.exit(0)); process.on('SIGTERM', () => process.exit(0));
  err('PROXY — forwarding to the marshal daemon (no backends spawned)');
}
function spawnDaemon() {                                   // launch the detached, session-independent daemon
  try {
    mkdirSync(dirname(SOCK), { recursive: true });
    let out = 'ignore'; try { out = openSync(join(dirname(SOCK), 'daemon.log'), 'a'); } catch {}
    const child = spawn(process.execPath, [fileURLToPath(import.meta.url), '--daemon'], { detached: true, stdio: ['ignore', out, out], env: process.env });
    child.unref();                                         // this session may exit without taking the daemon down
  } catch (e) { err(`failed to spawn daemon: ${e.message}`); }
}
function tryConnect(attempt = 0) {
  const c = connect(SOCK);
  c.on('connect', () => runProxy(c));
  c.on('error', () => {
    if (attempt === 0) spawnDaemon();                      // no daemon yet → launch one, then poll for it
    if (attempt < 50) setTimeout(() => tryConnect(attempt + 1), 100);    // ~5s of retries while it binds
    else { err('could not reach marshal daemon'); process.exit(1); }
  });
}
if (process.argv.includes('--daemon')) runDaemon();
else tryConnect();
