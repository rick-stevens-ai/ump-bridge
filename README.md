# UMP stdio→HTTP bridge (zero-dependency MCP server)

For Dropbox-less fleet hosts that cannot run the Node `ump` binary over a
local markdown store but CAN reach the m1 UMP HTTP server
(`http://100.86.220.115:4999`). The UMP HTTP server does not speak MCP,
so this bridge speaks MCP-over-stdio (JSON-RPC 2.0) to Hermes (or any MCP
client) and proxies each tool call to the UMP REST surface, auto-injecting
`scope.owner` (the operator DID) so private cards are actually returned.

Pure stdlib — runs under any Python ≥3.8 regardless of `mcp` package churn.

## Install

Copy `ump_bridge.py` to `~/.hermes/mcp/ump_bridge.py` on the fleet host,
then register in `~/.hermes/config.yaml`:

```yaml
mcp_servers:
  ump:
    command: python3
    args: [/home/<user>/.hermes/mcp/ump_bridge.py]
    env:
      UMP_HTTP_BASE: http://100.86.220.115:4999
```

Verify: `hermes mcp test ump` → Connected + 4 tools.

## Tools

- `ump_recall(query, limit=10, kind?)` — search; kind wraps into
  `filter.kind` (array) per the UMP server contract
- `ump_remember(title, text, tags?)` — write; idempotent by content hash
- `ump_get(id)` — **path-based** `GET /ump/memory/<id>`
- `ump_capabilities()` — server capabilities

## The two bugs this bridge got wrong initially (fixed 2026-10-04)

Found by the 100-test suite in `tests/`:

1. **Get route:** the UMP HTTP binding has no `/ump/get?id=` query route —
   get is path-based: `GET /ump/memory/<urlencoded-id>`. The old bridge
   404'd on every get-by-id.
2. **Kind filter:** `InMemoryStore.search` reads `req.filter.kind` as an
   **array** of allowed kinds. A top-level `kind` string is silently
   ignored, so recall returned unfiltered results. The bridge now wraps
   the arg: `{"filter": {"kind": [kind]}}`.

## Tests

`tests/test_ump_bridge.py` — 100 tests (70 unit against a scripted fake
backend; 30 integration against the real m1 server). All writes carry the
tag `ump-mcp-test-20261004`; identical re-POSTs merge by content hash.

```bash
python3 tests/test_ump_bridge.py        # ~60s, expect: OK (100 tests)
```

## Fleet deployment status

- prokko: `~/.hermes/mcp/ump_bridge.py` (this repo's copy, verified)
- mini / nuc7 / nuc13: older copies still carry the two bugs — update
  from this repo.
