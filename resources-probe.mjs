#!/usr/bin/env node
/**
 * Falsifiable probe: resources & prompts aggregation (c).
 * PASS = marshal advertises resources+prompts caps, lists a backend's resources (original uri) and
 * namespaced prompts, reads a resource through the seam, and gets a prompt (namespaced → routed).
 * Uses a temp config with one FAKE_CAPS backend — never touches the user's live fleet.
 */
import { spawn } from 'node:child_process';
import { writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const HERE = dirname(fileURLToPath(import.meta.url));
const cfg = `/tmp/marshal-res-${process.pid}.json`;
writeFileSync(cfg, JSON.stringify({ backends: [{ name: 'fake', command: 'node', args: [join(HERE, 'fake-backend.mjs')] }] }));
const m = spawn('node', [join(HERE, 'marshal.mjs')], { stdio: ['pipe', 'pipe', 'inherit'], env: { ...process.env, MARSHAL_DAEMON_IDLE: '300', FAKE_CAPS: '1', MARSHAL_CONFIG: cfg, MARSHAL_SOCK: `/tmp/marshal-res-${process.pid}.sock`, MARSHAL_AUDIT: `/tmp/marshal-res-${process.pid}.jsonl` } });
const pending = new Map(); let nextId = 1; let buf = '';
m.stdout.setEncoding('utf8');
m.stdout.on('data', (d) => { buf += d; let i; while ((i = buf.indexOf('\n')) >= 0) { const line = buf.slice(0, i); buf = buf.slice(i + 1); if (!line.trim()) continue; let msg; try { msg = JSON.parse(line); } catch { continue; } if (msg.id != null && pending.has(msg.id)) { const p = pending.get(msg.id); pending.delete(msg.id); p(msg); } } });
const rpc = (method, params) => new Promise((res) => { const id = nextId++; pending.set(id, res); m.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n'); });
const notify = (method) => m.stdin.write(JSON.stringify({ jsonrpc: '2.0', method }) + '\n');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let ok = true;
const check = (label, cond) => { console.log(`  ${cond ? '🟩 PASS' : '🟥 FAIL'} — ${label}`); if (!cond) ok = false; };

(async () => {
  const init = await rpc('initialize', { protocolVersion: '2024-11-05', capabilities: {} });
  const caps = init.result?.capabilities || {};
  check('initialize advertises resources + prompts caps', !!caps.resources && !!caps.prompts);
  notify('notifications/initialized');
  await sleep(1000);

  const res = (await rpc('resources/list', {})).result?.resources || [];
  check('resources/list aggregates backend resource (original uri preserved)', res.some((r) => r.uri === 'fake://doc1'));

  const tmpl = (await rpc('resources/templates/list', {})).result?.resourceTemplates || [];
  check('resources/templates/list aggregates template', tmpl.some((t) => t.uriTemplate === 'fake://doc/{n}'));

  const read = await rpc('resources/read', { uri: 'fake://doc1' });
  check('resources/read routes by uri and returns contents', read.result?.contents?.[0]?.text === 'body of fake://doc1');

  const badRead = await rpc('resources/read', { uri: 'fake://nope' });
  check('unknown resource uri is rejected, not routed', !!badRead.result?.isError || !!badRead.error);

  const prompts = (await rpc('prompts/list', {})).result?.prompts || [];
  const greet = prompts.find((p) => p.name === 'fake.greet');
  check('prompts/list exposes namespaced prompt (fake.greet)', !!greet);
  check('prompt carries a title', greet?.title === 'fake · greet');

  const got = await rpc('prompts/get', { name: 'fake.greet', arguments: { who: 'world' } });
  check('prompts/get strips namespace, routes, returns message', got.result?.messages?.[0]?.content?.text === 'hello world');

  console.log(ok ? '\n✅ PROBE PASSED — resources & prompts are aggregated and routed, not just tools.' : '\n❌ PROBE FAILED');
  m.kill(); process.exit(ok ? 0 : 1);
})();
