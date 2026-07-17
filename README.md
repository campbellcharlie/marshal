# marshal

**One MCP endpoint that supervises your fleet.** marshal is a minimal, zero-dependency MCP aggregator:
Claude Code (or any MCP client) connects to *one* stdio server — marshal — and behind it marshal spawns,
namespaces, multiplexes, and **auto-respawns** your backend MCP servers (momento, serval, …). When a
backend crashes, marshal restarts it transparently and the client connection **never drops**.

## Why
When a backing MCP server dies mid-session, the client loses those tools until a manual `/mcp` reconnect.
marshal fixes that at the seam: it keeps one stable connection to the client and treats the flaky
backends as supervised children — the fleet self-heals without the human in the loop.

## What it does
- **One connection, many servers** — exposes every backend's tools under a `backend.tool` namespace
  (`momento.search`, `serval.navigate`), routed to the owning child.
- **Self-heal** — a crashed backend is respawned (300 ms backoff), re-initialized, and its tools
  refreshed; marshal emits `notifications/tools/list_changed` so the client updates.
- **Audit trail** — every tool call is logged append-only + **hash-chained** (tamper-evident) to
  `~/.marshal/audit.jsonl` (`$MARSHAL_AUDIT`). **Redacted**: arg *keys + type/length*, never raw values
  (a log of values would itself be an exfil surface). Row: `{ts,backend,tool,arg_keys,ok,ms,result_bytes,prev}`.
  **Rotates** at `MARSHAL_AUDIT_MAX` (5 MB) into timestamped segments, keeping `MARSHAL_AUDIT_KEEP` (10) —
  and the chain continues unbroken across segments (next segment's first `prev` = the prior segment's tip).
- **Singleton** — Claude Code pre-warms spare sessions, so it may launch marshal more than once. The
  first becomes the **primary** (owns backends + audit, listens on `~/.marshal/marshal.sock`); any later
  one becomes a thin **proxy** (pipes stdio ⇄ the primary, spawns nothing). One backend fleet, one audit
  writer — no `:52849`/DB contention or interleaved-log corruption. Verified by `singleton-probe.mjs`.
- **Zero deps** — ~180 lines of Node, hand-rolled MCP stdio (no SDK).

## Use
```bash
cp marshal.config.example.json marshal.config.json    # then edit paths to your backends
node marshal.mjs                                       # (marshal speaks MCP on stdio)
```
Wire it into Claude Code as the **single** MCP server (replace the direct momento/serval entries):
```json
{ "mcpServers": { "marshal": { "command": "node", "args": ["/path/to/marshal/marshal.mjs"] } } }
```
Config (`marshal.config.json`, gitignored — machine-specific):
```json
{ "backends": [ { "name": "momento", "command": "node", "args": [".../momento/dist/server.js"] } ] }
```

## Verify (falsifiable probes)
- `node probe.mjs` — lists + calls a momento tool, **kills marshal's backend child**, confirms the
  tools survive and calls work again after respawn (self-heal). Kills only its own child, never a live server.
- `node check-fleet.mjs` — confirms momento + serval are both namespaced behind one endpoint.

## Status / gaps (v0.1)
- 🟩 momento + serval verified (123 tools behind one endpoint); self-heal proven.
- 🟩 serval's `--mcp` is an *attach-bridge* to its persistent server → no singleton clash, auth preserved.
- 🟡 no per-request **timeout** yet — respawn triggers on child *exit*, not a *hang*.
- 🟡 `lorg` isn't a registered MCP server (CLI/skill), so it's not a backend.
