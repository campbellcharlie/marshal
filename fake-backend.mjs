#!/usr/bin/env node
/**
 * fake-backend — a minimal MCP stdio server used only by marshal's probes.
 * Exposes two tools (alpha with a description, beta without). FAKE_MODE controls tools/call:
 *   ok   (default) → responds immediately
 *   hang           → never responds (simulates a wedged backend, to exercise marshal's per-call timeout)
 * FAKE_CAPS=1 additionally advertises resources + prompts capabilities and serves one of each
 * (to exercise marshal's resources/prompts aggregation).
 */
const MODE = process.env.FAKE_MODE || 'ok';
const CAPS = process.env.FAKE_CAPS === '1';
let buf = '';
process.stdin.setEncoding('utf8');
const send = (o) => process.stdout.write(JSON.stringify(o) + '\n');
process.stdin.on('data', (d) => {
  buf += d; let i;
  while ((i = buf.indexOf('\n')) >= 0) {
    const line = buf.slice(0, i); buf = buf.slice(i + 1); if (!line.trim()) continue;
    let m; try { m = JSON.parse(line); } catch { continue; }
    const { id, method } = m;
    if (method === 'initialize') send({ jsonrpc: '2.0', id, result: { protocolVersion: '2024-11-05', capabilities: { tools: {}, ...(CAPS ? { resources: {}, prompts: {} } : {}) }, serverInfo: { name: 'fake', version: '0.0.1' } } });
    else if (method === 'notifications/initialized') { /* no-op */ }
    else if (method === 'ping') send({ jsonrpc: '2.0', id, result: {} });
    else if (method === 'tools/list') send({ jsonrpc: '2.0', id, result: { tools: [
      { name: 'alpha', description: 'does alpha', inputSchema: { type: 'object', properties: {} } },
      { name: 'beta', inputSchema: { type: 'object', properties: {} } },
    ] } });
    else if (method === 'tools/call') {
      if (MODE === 'hang') return;                                       // deliberately never answer
      send({ jsonrpc: '2.0', id, result: { content: [{ type: 'text', text: `ok:${m.params?.name}` }] } });
    } else if (method === 'resources/list') send({ jsonrpc: '2.0', id, result: { resources: CAPS ? [{ uri: 'fake://doc1', name: 'Doc One', mimeType: 'text/plain' }] : [] } });
    else if (method === 'resources/templates/list') send({ jsonrpc: '2.0', id, result: { resourceTemplates: CAPS ? [{ uriTemplate: 'fake://doc/{n}', name: 'Doc N' }] : [] } });
    else if (method === 'resources/read') send({ jsonrpc: '2.0', id, result: { contents: [{ uri: m.params?.uri, mimeType: 'text/plain', text: `body of ${m.params?.uri}` }] } });
    else if (method === 'prompts/list') send({ jsonrpc: '2.0', id, result: { prompts: CAPS ? [{ name: 'greet', description: 'a greeting prompt', arguments: [{ name: 'who', required: true }] }] : [] } });
    else if (method === 'prompts/get') send({ jsonrpc: '2.0', id, result: { messages: [{ role: 'user', content: { type: 'text', text: `hello ${m.params?.arguments?.who}` } }] } });
    else if (id != null) send({ jsonrpc: '2.0', id, error: { code: -32601, message: `method not found: ${method}` } });
  }
});
