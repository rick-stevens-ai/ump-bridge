#!/usr/bin/env python3
"""UMP stdio->HTTP bridge (zero-dependency stdlib MCP server).

For Dropbox-less fleet hosts (nuc7/Lost, nuc13/Paradise, prokko) that cannot
run the Node `ump` binary over a local markdown store but CAN reach the m1
UMP HTTP server (tailnet 100.86.220.115:4999). The HTTP server does not speak
MCP (/mcp -> 404), so this script speaks MCP-over-stdio (JSON-RPC 2.0) to
Hermes and proxies each tool call to the m1 REST surface
(/ump/{recall,remember,get,capabilities}), auto-injecting scope.owner (the
operator DID) so private cards are actually returned.

No external packages required — pure stdlib — so it runs under any Hermes
bundled interpreter regardless of mcp v1/v2 API churn.

Register in ~/.hermes/config.yaml:
  mcp_servers:
    ump:
      command: python3            # any python3 >=3.8
      args: [ /home/<user>/.hermes/mcp/ump_bridge.py ]
      env:
        UMP_HTTP_BASE: http://100.86.220.115:4999
      enabled: true

Verify:  hermes mcp test ump   ->  Connected + 4 tools
"""
import json
import os
import sys
import urllib.parse
import urllib.request
import urllib.error

UMP_HTTP_BASE = os.environ.get("UMP_HTTP_BASE", "http://100.86.220.115:4999").rstrip("/")
# UMP is keyed to the operator, not the agent. All production cards are
# visibility:private, so recall returns [] unless scope.owner is set.
OPERATOR_DID = os.environ.get(
    "UMP_OWNER_DID",
    "did:key:z6MkhVT1qj17Mtan98kgTqCXjBsCNkM5fmjts1oXqTZtEASU",
)
TIMEOUT = int(os.environ.get("UMP_HTTP_TIMEOUT", "30"))
PROTOCOL_VERSION = "2025-06-18"


def _post(path, payload):
    req = urllib.request.Request(
        UMP_HTTP_BASE + path,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            return r.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        return json.dumps({"error": "HTTP %d" % e.code, "detail": e.read().decode("utf-8", "replace")})
    except Exception as e:
        return json.dumps({"error": str(e)})


def _get(path):
    try:
        with urllib.request.urlopen(UMP_HTTP_BASE + path, timeout=TIMEOUT) as r:
            return r.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        return json.dumps({"error": "HTTP %d" % e.code, "detail": e.read().decode("utf-8", "replace")})
    except Exception as e:
        return json.dumps({"error": str(e)})


# ---- tool implementations -------------------------------------------------

def t_recall(args):
    payload = {
        "query": args.get("query", ""),
        "limit": args.get("limit", 10),
        "scope": {"owner": OPERATOR_DID},
    }
    if args.get("kind"):
        # UMP InMemoryStore.search reads req.filter.kind as an ARRAY of
        # allowed kinds — a top-level "kind" string is silently ignored.
        kind = args["kind"]
        payload["filter"] = {"kind": kind if isinstance(kind, list) else [kind]}
    return _post("/ump/recall", payload)


def t_remember(args):
    body = {"title": args.get("title", ""), "text": args.get("text", "")}
    if args.get("tags"):
        body["tags"] = args["tags"]
    payload = {
        "kind": args.get("kind", "semantic"),
        "scope": {"owner": OPERATOR_DID, "visibility": "private"},
        "body": body,
    }
    return _post("/ump/remember", payload)


def t_get(args):
    # The UMP HTTP binding's get route is PATH-BASED:
    #   GET /ump/memory/<id>
    # (there is no /ump/get?id= query route — that returned 404 "no route").
    rid = urllib.parse.quote(args.get("id", ""), safe="")
    return _get("/ump/memory/" + rid)


def t_capabilities(args):
    return _get("/ump/capabilities")


TOOLS = {
    "ump_recall": {
        "fn": t_recall,
        "description": "Search shared UMP memory by query. scope.owner auto-injected so private cards return. Optional kind filter (semantic|episodic|procedural|working|identity).",
        "schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "limit": {"type": "integer", "default": 10},
                "kind": {"type": "string"},
            },
            "required": ["query"],
        },
    },
    "ump_remember": {
        "fn": t_remember,
        "description": "Write a card to shared UMP. Requires title+text. Idempotent by content hash (identical re-POST merges).",
        "schema": {
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "text": {"type": "string"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "kind": {"type": "string", "default": "semantic"},
            },
            "required": ["title", "text"],
        },
    },
    "ump_get": {
        "fn": t_get,
        "description": "Fetch a UMP record by URN id. No scope needed.",
        "schema": {"type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"]},
    },
    "ump_capabilities": {
        "fn": t_capabilities,
        "description": "Report UMP server capabilities (kinds, bindings, conformance).",
        "schema": {"type": "object", "properties": {}},
    },
}


# ---- MCP JSON-RPC stdio loop ----------------------------------------------

def _send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def _result(rid, result):
    _send({"jsonrpc": "2.0", "id": rid, "result": result})


def _error(rid, code, message):
    _send({"jsonrpc": "2.0", "id": rid, "error": {"code": code, "message": message}})


def handle(msg):
    method = msg.get("method")
    rid = msg.get("id")
    if method == "initialize":
        _result(rid, {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "ump", "version": "1.0.0"},
        })
    elif method == "notifications/initialized":
        pass  # notification, no response
    elif method == "tools/list":
        _result(rid, {"tools": [
            {"name": n, "description": t["description"], "inputSchema": t["schema"]}
            for n, t in TOOLS.items()
        ]})
    elif method == "tools/call":
        params = msg.get("params", {})
        name = params.get("name")
        args = params.get("arguments", {}) or {}
        tool = TOOLS.get(name)
        if not tool:
            _error(rid, -32602, "Unknown tool: %s" % name)
            return
        try:
            text = tool["fn"](args)
        except Exception as e:
            text = json.dumps({"error": str(e)})
        _result(rid, {"content": [{"type": "text", "text": text}]})
    elif method == "ping":
        _result(rid, {})
    elif rid is not None:
        _error(rid, -32601, "Method not found: %s" % method)


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except Exception:
            continue
        try:
            handle(msg)
        except Exception as e:
            if msg.get("id") is not None:
                _error(msg.get("id"), -32603, str(e))


if __name__ == "__main__":
    main()
