<div align="center">

# ⇄ Marshal

**`Marshal` puts your whole MCP fleet behind one endpoint — and keeps it alive.**

![mission](https://img.shields.io/badge/mission-one_endpoint_that_supervises_your_fleet-7d3cff)
![license](https://img.shields.io/badge/license-MIT-green)
![platform](https://img.shields.io/badge/platform-Node_(stdio_MCP)-black)
![deps](https://img.shields.io/badge/dependencies-zero-blue)
![agent](https://img.shields.io/badge/wires-Claude_Code-orange)

[What & Why](#what--why) · [How it works](#how-it-works) · [What's verified](#whats-verified) · [Quickstart](#quickstart) · [Features](#features) · [Audit trail](#audit-trail) · [Layout](#layout)

</div>

---

## What & Why

MCP backends die. When a backing server (momento, serval, …) crashes or drops mid-session, the client
loses **every one of its tools** until a manual `/mcp` reconnect. And because Claude Code launches each
configured MCP **per session** — including pre-warmed spares — a stateful server like a browser gets
started two or three times over: duplicate processes, port contention, split state.

`Marshal` fixes both at the seam. It's one stdio MCP server the client connects to; behind it, it
**spawns, namespaces, multiplexes, and supervises** your backends. A crashed backend is respawned
transparently — the client connection never drops. And no matter how many sessions Claude opens, exactly
**one** backend fleet runs.

> **The client should never see a backend die.**

That's the whole product: the aggregation you'd get from any gateway, plus the two things most gateways
*don't* do — **self-heal** and a **tamper-evident audit trail** — in ~390 lines of zero-dependency Node.

## How it works

- **One endpoint, many servers.** Every backend's tools are exposed under a `backend.tool` namespace
  (`momento.search`, `serval.navigate`), routed to the owning child. **Resources and prompts** are
  aggregated too (capability-gated per backend): resources route by their original uri, prompts are
  namespaced like tools.
- **stdio *and* remote backends.** A backend is either a spawned stdio child (`{name, command, args}`) or a
  **remote MCP server** (`{name, url, headers?, transport?}`) — marshal speaks **Streamable HTTP** and
  classic **SSE**; `transport: "auto"` (default) tries HTTP first and falls back to SSE. All the aggregation,
  routing, audit and self-heal are transport-agnostic. Zero deps — remote transport is built on `node:http/https`.
- **Legible sub-tools.** The client labels every call by *server* — so a fleet behind one server would all
  read as "marshal". Each proxied tool/prompt carries a spec `title` (`momento · search`) and a
  `[backend]`-prefixed description; the `marshal.recent` tool reports which backend tool actually ran.
- **Self-heal.** A dead backend (crashed child *or* dropped remote connection) is reconnected with capped
  exponential backoff (300 ms → 30 s), re-initialized, and its tools refreshed; marshal emits
  `notifications/tools/list_changed` so the client updates. The client connection is never interrupted. A
  backend that *hangs* (never answers) rather than dies is caught by a per-call timeout
  (`MARSHAL_CALL_TIMEOUT`, default 120 s) so the client is never stalled forever.
- **Detached daemon, session-independent.** Claude launches marshal per session, but no session *owns* the
  fleet: the first launch spawns a **detached daemon** (own process group) that owns the backends + audit
  and listens on `~/.marshal/marshal.sock`; every session's marshal — the first included — is a thin
  **proxy** that pipes its stdio ⇄ the daemon and spawns nothing. So when the session that happened to
  start the daemon exits (Claude sends *its* marshal a SIGINT), only that proxy dies — the daemon and every
  other session's connection **survive**. The daemon self-exits `MARSHAL_DAEMON_IDLE` (default 60 s) after
  its last client leaves. One fleet, one audit writer, regardless of session count or churn.
- **Hot-add / remove.** The daemon watches `marshal.config.json`; edit it and a backend spawns or stops
  live, no restart. Human-gated (a config file — not an agent-callable tool an injected agent could abuse).

## What's verified

Marshal's **mechanisms are verified** by falsifiable probes you can run (`node <probe>.mjs`):

| Probe | Proves |
|---|---|
| `probe.mjs` | **self-heal** — kill a backend; tools survive and calls work after respawn |
| `singleton-probe.mjs` | **singleton + survival** — 2 marshals → 1 detached daemon fleet; killing the session that started it does NOT disconnect the others |
| `audit-probe.mjs` | **audit** — rows written, args **redacted**, hash chain valid |
| `rotation-probe.mjs` | **rotation** — log rotates + prunes; chain unbroken across segments |
| `hot-add-probe.mjs` | **hot-add** — edit config → backend appears/leaves with no restart |
| `introspect-probe.mjs` | **observability** — tools carry `title` + `[backend]` desc; `marshal.recent` reports the real sub-tool |
| `timeout-probe.mjs` | **timeout** — a hung backend times out with an error, not an infinite stall |
| `resources-probe.mjs` | **resources/prompts** — backend resources + prompts are aggregated and routed |
| `remote-probe.mjs` | **remote transports** — HTTP + SSE MCP backends connect, aggregate, route, auto-detect, reconnect |

What is **not** benchmarked: any comparative claim (faster / lighter / better than gateway *X*). Marshal is
right-sized for a single machine + stdio MCP; if you need HTTP transport, auth, multi-tenant, or
multi-machine scale, reach for a full gateway (ContextForge, MetaMCP). Treat efficiency or superiority
claims as unproven until a number is published.

## Quickstart

```bash
git clone https://github.com/campbellcharlie/marshal.git ~/src/marshal
cd ~/src/marshal
cp marshal.config.example.json marshal.config.json    # then set the paths to your backends
```

Wire it into Claude Code as the **single** MCP server (replace the direct backend entries):

```bash
claude mcp add -s user marshal /opt/homebrew/bin/node -- ~/src/marshal/marshal.mjs
# restart Claude Code
```

`marshal.config.json` (gitignored — machine-specific). A backend is a stdio child *or* a remote URL:

```json
{ "backends": [
  { "name": "momento", "command": "node", "args": [".../momento/dist/server.js"] },
  { "name": "remote", "url": "https://example.com/mcp", "headers": { "Authorization": "Bearer TOKEN" }, "transport": "auto" }
] }
```

Knobs (env): `MARSHAL_CALL_TIMEOUT` (120 s) · `MARSHAL_DAEMON_IDLE` (60 s) · `MARSHAL_AUDIT_MAX` (5 MB) · `MARSHAL_AUDIT_KEEP` (10) · `MARSHAL_AUDIT` · `MARSHAL_SOCK` · `MARSHAL_CONFIG`.

## Features

| Feature | What it does | Verified |
|---|---|---|
| **Aggregate** | many MCP servers behind one endpoint — tools, resources & prompts namespaced | ✅ |
| **stdio + remote** | backends are stdio children **or** remote MCP servers (Streamable HTTP / SSE, auto-detected) | ✅ |
| **Legible sub-tools** | `title` + `[backend]` desc on each tool; `marshal.recent` shows the real sub-tool | ✅ |
| **Self-heal** | dead backend (crash or dropped connection) reconnected with capped backoff; client never disconnects | ✅ |
| **Call timeout** | a hung (non-exiting) backend call times out instead of stalling the client | ✅ |
| **Detached daemon** | N marshal instances → 1 detached daemon fleet; a session exiting never disconnects the others | ✅ |
| **Audit trail** | hash-chained, redacted, one JSONL row per tool call | ✅ |
| **Rotation** | size-based rotation + retention; chain spans segments | ✅ |
| **Hot-add / remove** | edit the config → backend spawns/stops live, no restart | ✅ |

## Audit trail

Every call flows through marshal's one chokepoint, so it logs one append-only JSONL row per event to
`~/.marshal/audit.jsonl` (`$MARSHAL_AUDIT`) — `call` (tool), `read` (resource), `prompt` (prompt get),
plus backend lifecycle (`backend_ready`/`_exit`/`_add`/`_remove`):

```json
{"ts":"…","event":"call","backend":"serval","tool":"navigate","arg_keys":["url:str[42]"],"ok":true,"ms":412,"result_bytes":1840,"prev":"7c9c34…"}
```

- **Redacted** — arg *keys + type/length* only, never raw values (a log of values would itself be an exfil surface).
- **Hash-chained** — each row's `prev` = sha256 of the previous line, so the log is **tamper-evident**; the chain continues unbroken across rotations.
- **Rotates** at `MARSHAL_AUDIT_MAX` into timestamped segments, keeping `MARSHAL_AUDIT_KEEP`.

<details>
<summary><strong>Known limitations (v0.1)</strong></summary>

- **Timeout is per-call, not a hang-triggered restart** — a hung call times out (`MARSHAL_CALL_TIMEOUT`), but marshal does not yet kill+respawn a *persistently* unresponsive backend; only death (exit / dropped connection) triggers reconnect.
- **Remote transport is client-side only** — marshal *connects to* HTTP/SSE MCP servers; it still serves its own clients over stdio (no HTTP listener, auth, or multi-tenant — use a full gateway for that).
- **Remote auth is static** — bearer tokens/headers come from the config file (human-gated); no OAuth flow or token refresh.
- **Daemon death → brief gap** — if the daemon itself dies (not just a session), connected proxies exit; the next marshal launch re-spawns it (seconds). A detached daemon also means one lingering background process until it idle-exits.
- **`fs.watch` is best-effort** — hot-add is a convenience, not a guarantee on every filesystem.

</details>

## Layout

```
marshal.mjs                 # the aggregator + supervisor (zero deps, ~390 lines)
marshal.config.example.json # copy → marshal.config.json (gitignored)
probe.mjs                   # self-heal probe
singleton-probe.mjs         # detached-daemon singleton + cross-session survival probe
audit-probe.mjs             # audit chain + redaction probe
rotation-probe.mjs          # rotation + retention probe
hot-add-probe.mjs           # live config-reconcile probe
introspect-probe.mjs        # title/description + marshal.recent observability probe
timeout-probe.mjs           # per-call hang-timeout probe
resources-probe.mjs         # resources + prompts aggregation probe
remote-probe.mjs            # remote HTTP + SSE transport probe (connect/aggregate/route/reconnect)
fake-backend.mjs            # minimal stdio MCP backend fixture used by the probes above
fake-remote-server.mjs      # minimal HTTP/SSE MCP server fixture for remote-probe
serval-probe.mjs            # isolated serval backend check
check-fleet.mjs             # aggregate sanity (all backends namespaced)
```

## License

[MIT](LICENSE) © 2026 Charlie Campbell
