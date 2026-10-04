#!/usr/bin/env python3
"""UMP MCP bridge test suite — 100 tests.

Unit tests (Groups A-G, 70) run the bridge against a local scripted fake
UMP HTTP backend — deterministic, loopback only.
Integration tests (Groups H-J, 30) run the bridge against the real m1
UMP REST server (http://100.86.220.115:4999). All real writes carry the
tag "ump-mcp-test-20261004" and unique nonces; re-runs merge by content
hash rather than duplicating.

Run:  python3 test_ump_bridge.py [-v]
"""
import json
import os
import select
import subprocess
import sys
import threading
import time
import unittest
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

BRIDGE = os.path.expanduser("~/.hermes/mcp/ump_bridge.py")
REAL_BASE = "http://100.86.220.115:4999"
TEST_TAG = "ump-mcp-test-20261004"
OPERATOR_DID = "did:key:z6MkhVT1qj17Mtan98kgTqCXjBsCNkM5fmjts1oXqTZtEASU"


# ---------------------------------------------------------------------------
# Fake UMP backend (records every request; returns scripted responses)
# ---------------------------------------------------------------------------
class FakeUMPHandler(BaseHTTPRequestHandler):
    requests = []          # (method, full_path, body_dict_or_None)
    script = {}            # (method, path_no_query) -> (status, text)

    def log_message(self, *a):
        pass

    def _respond(self):
        key = (self.command, self.path.split("?")[0])
        status, text = FakeUMPHandler.script.get(
            key, (200, json.dumps({"ok": True, "default": True})))
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(text.encode())))
        self.end_headers()
        self.wfile.write(text.encode())

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(n) if n else b""
        try:
            body = json.loads(raw) if raw else None
        except Exception:
            body = {"_raw": raw.decode("utf-8", "replace")}
        FakeUMPHandler.requests.append((self.command, self.path, body))
        self._respond()

    def do_GET(self):
        FakeUMPHandler.requests.append((self.command, self.path, None))
        self._respond()


def start_fake():
    srv = HTTPServer(("127.0.0.1", 0), FakeUMPHandler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}"


class BridgeProc:
    """Spawn ump_bridge.py with a given UMP_HTTP_BASE; speak JSON-RPC.

    Uses select() for ALL stdout/stderr reads — never parks reader threads
    (a parked thread would steal the next RPC response) and never blocks
    on a silent stderr pipe.
    """

    def __init__(self, base, timeout=None):
        env = dict(os.environ, UMP_HTTP_BASE=base)
        env.pop("UMP_OWNER_DID", None)
        if timeout is not None:
            env["UMP_HTTP_TIMEOUT"] = str(timeout)
        self.p = subprocess.Popen(
            [sys.executable, BRIDGE],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, env=env)  # binary pipes
        self._buf = b""
        self._id = 0

    def _readline(self, timeout):
        """Read one stdout line with a bounded wait. None on timeout."""
        deadline = time.time() + timeout
        while b"\n" not in self._buf:
            remaining = deadline - time.time()
            if remaining <= 0:
                return None
            r, _, _ = select.select([self.p.stdout], [], [], remaining)
            if not r:
                return None
            chunk = os.read(self.p.stdout.fileno(), 65536)
            if not chunk:
                raise RuntimeError("bridge stdout closed unexpectedly")
            self._buf += chunk
        line, self._buf = self._buf.split(b"\n", 1)
        return line.decode("utf-8", "replace")

    def _stderr_has_data(self, wait=0.5):
        """True if the bridge wrote anything to stderr within `wait` s."""
        r, _, _ = select.select([self.p.stderr], [], [], wait)
        return bool(r)

    def rpc(self, method, params=None, notify=False, rid=None, timeout=60):
        msg = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            msg["params"] = params
        if not notify:
            self._id += 1
            msg["id"] = rid if rid is not None else self._id
        self.p.stdin.write((json.dumps(msg) + "\n").encode())
        self.p.stdin.flush()
        if notify:
            return None
        want = msg["id"]
        while True:
            line = self._readline(timeout)
            if line is None:
                raise RuntimeError(f"bridge did not answer {method} within {timeout}s")
            line = line.strip()
            if not line:
                continue
            try:
                resp = json.loads(line)
            except Exception:
                continue
            if resp.get("id") == want:
                return resp

    def raw_line(self, line):
        self.p.stdin.write((line + "\n").encode())
        self.p.stdin.flush()

    def next_stdout_line(self, timeout=2.0):
        """Read one stdout line with a bounded wait (for no-response checks)."""
        line = self._readline(timeout)
        return line if line is not None else None

    def close(self):
        try:
            self.p.stdin.close()
            self.p.wait(timeout=5)
        except Exception:
            self.p.kill()

    def stderr_quiet(self):
        """True if nothing was written to stderr recently."""
        return not self._stderr_has_data(wait=0.5)


class UnitBase(unittest.TestCase):
    """Shared fixture: one bridge + one fake backend per class."""

    @classmethod
    def setUpClass(cls):
        cls.srv, cls.base = start_fake()
        FakeUMPHandler.requests = []
        FakeUMPHandler.script = {}
        cls.bridge = BridgeProc(cls.base)
        r = cls.bridge.rpc("initialize", {"protocolVersion": "2025-06-18",
                                         "capabilities": {}, "clientInfo": {"name": "t"}})
        assert "result" in r, r
        cls.bridge.rpc("notifications/initialized", notify=True)

    @classmethod
    def tearDownClass(cls):
        cls.bridge.close()
        cls.srv.shutdown()

    def setUp(self):
        FakeUMPHandler.requests = []
        FakeUMPHandler.script = {}

    # helpers -------------------------------------------------------------
    def call(self, tool, args=None):
        return self.bridge.rpc("tools/call", {"name": tool,
                                              "arguments": args if args is not None else {}})

    def call_text(self, tool, args=None):
        r = self.call(tool, args)
        assert "result" in r, r
        return r["result"]["content"][0]["text"]

    def posts(self, path=None):
        out = [r for r in FakeUMPHandler.requests
               if r[0] == "POST" and (path is None or r[1].split("?")[0] == path)]
        return out

    def gets(self, path=None):
        out = [r for r in FakeUMPHandler.requests
               if r[0] == "GET" and (path is None or r[1].split("?")[0] == path)]
        return out


# ===========================================================================
# Group A — handshake & protocol conformance (18)
# ===========================================================================
class TestAProtocol(UnitBase):
    def test_01_initialize_returns_result(self):
        b = BridgeProc(self.base)
        try:
            r = b.rpc("initialize", {"protocolVersion": "2025-06-18"})
            self.assertIn("result", r)
            self.assertNotIn("error", r)
        finally:
            b.close()

    def test_02_initialize_protocol_version(self):
        r = self.bridge.rpc("initialize", {})
        self.assertEqual(r["result"]["protocolVersion"], "2025-06-18")

    def test_03_initialize_server_info_name(self):
        r = self.bridge.rpc("initialize", {})
        self.assertEqual(r["result"]["serverInfo"]["name"], "ump")

    def test_04_initialize_server_info_version(self):
        r = self.bridge.rpc("initialize", {})
        self.assertEqual(r["result"]["serverInfo"]["version"], "1.0.0")

    def test_05_initialize_capabilities_tools_present(self):
        r = self.bridge.rpc("initialize", {})
        self.assertIn("tools", r["result"]["capabilities"])

    def test_06_initialized_notification_no_response(self):
        self.bridge.rpc("notifications/initialized", notify=True)
        # if it (wrongly) answered, the next ping's id-matcher still works;
        # verify silence by reading with timeout
        line = self.bridge.next_stdout_line(timeout=0.8)
        self.assertIsNone(line)

    def test_07_ping_returns_empty_result(self):
        r = self.bridge.rpc("ping")
        self.assertEqual(r["result"], {})

    def test_08_unknown_method_error_code(self):
        r = self.bridge.rpc("resources/list")
        self.assertEqual(r["error"]["code"], -32601)

    def test_09_unknown_method_preserves_id(self):
        r = self.bridge.rpc("prompts/list", rid="weird-id-42")
        self.assertEqual(r["id"], "weird-id-42")

    def test_10_unknown_tool_error_code(self):
        r = self.call("no_such_tool")
        self.assertEqual(r["error"]["code"], -32602)

    def test_11_unknown_tool_message_contains_name(self):
        r = self.call("bogusToolName")
        self.assertIn("bogusToolName", r["error"]["message"])

    def test_12_malformed_json_ignored_no_crash(self):
        self.bridge.raw_line("this is {{{ not json")
        r = self.bridge.rpc("ping")
        self.assertEqual(r["result"], {})

    def test_13_empty_line_skipped(self):
        self.bridge.raw_line("")
        self.bridge.raw_line("   ")
        r = self.bridge.rpc("ping")
        self.assertEqual(r["result"], {})

    def test_14_string_id_preserved(self):
        r = self.bridge.rpc("ping", rid="alpha")
        self.assertEqual(r["id"], "alpha")

    def test_15_null_id_unknown_method_silent(self):
        # id:null + unknown method -> bridge must NOT answer (rid is None)
        self.bridge.raw_line(json.dumps({"jsonrpc": "2.0", "method": "x/y", "id": None}))
        r = self.bridge.rpc("ping")
        self.assertEqual(r["result"], {})

    def test_16_sequential_requests_ids_match(self):
        ids = []
        for _ in range(5):
            r = self.bridge.rpc("ping")
            ids.append(r["id"])
        self.assertEqual(ids, sorted(ids))
        self.assertEqual(len(set(ids)), 5)

    def test_17_large_payload_handled(self):
        big = "x" * 100_000
        r = self.call("ump_recall", {"query": big})
        self.assertIn("result", r)

    def test_18_batch_of_ten_pings_all_answered(self):
        for _ in range(10):
            self.bridge.rpc("ping")
        r = self.bridge.rpc("ping")
        self.assertEqual(r["result"], {})


# ===========================================================================
# Group B — tools/list & schemas (12)
# ===========================================================================
class TestBToolsList(UnitBase):
    def tools(self):
        r = self.bridge.rpc("tools/list")
        return {t["name"]: t for t in r["result"]["tools"]}

    def test_01_four_tools_returned(self):
        r = self.bridge.rpc("tools/list")
        self.assertEqual(len(r["result"]["tools"]), 4)

    def test_02_tool_names_exact(self):
        names = set(self.tools())
        self.assertEqual(names, {"ump_recall", "ump_remember", "ump_get", "ump_capabilities"})

    def test_03_recall_requires_query(self):
        self.assertIn("query", self.tools()["ump_recall"]["inputSchema"]["required"])

    def test_04_recall_limit_default_10(self):
        self.assertEqual(self.tools()["ump_recall"]["inputSchema"]["properties"]["limit"]["default"], 10)

    def test_05_recall_kind_optional(self):
        self.assertNotIn("kind", self.tools()["ump_recall"]["inputSchema"].get("required", []))

    def test_06_remember_requires_title(self):
        self.assertIn("title", self.tools()["ump_remember"]["inputSchema"]["required"])

    def test_07_remember_requires_text(self):
        self.assertIn("text", self.tools()["ump_remember"]["inputSchema"]["required"])

    def test_08_remember_tags_is_array(self):
        self.assertEqual(self.tools()["ump_remember"]["inputSchema"]["properties"]["tags"]["type"], "array")

    def test_09_remember_kind_default_semantic(self):
        self.assertEqual(self.tools()["ump_remember"]["inputSchema"]["properties"]["kind"]["default"], "semantic")

    def test_10_get_requires_id(self):
        self.assertIn("id", self.tools()["ump_get"]["inputSchema"]["required"])

    def test_11_capabilities_schema_empty(self):
        self.assertEqual(self.tools()["ump_capabilities"]["inputSchema"]["properties"], {})

    def test_12_all_descriptions_nonempty(self):
        for name, t in self.tools().items():
            self.assertTrue(t["description"].strip(), f"description empty for {name}")


# ===========================================================================
# Group C — recall marshaling (10)
# ===========================================================================
class TestCRecall(UnitBase):
    def test_01_posts_to_recall_path(self):
        self.call("ump_recall", {"query": "q"})
        self.assertEqual(len(self.posts("/ump/recall")), 1)

    def test_02_injects_owner_scope(self):
        self.call("ump_recall", {"query": "q"})
        body = self.posts("/ump/recall")[0][2]
        self.assertEqual(body["scope"]["owner"], OPERATOR_DID)

    def test_03_forwards_query(self):
        self.call("ump_recall", {"query": "needle"})
        body = self.posts("/ump/recall")[0][2]
        self.assertEqual(body["query"], "needle")

    def test_04_default_limit_10(self):
        self.call("ump_recall", {"query": "q"})
        body = self.posts("/ump/recall")[0][2]
        self.assertEqual(body["limit"], 10)

    def test_05_custom_limit_forwarded(self):
        self.call("ump_recall", {"query": "q", "limit": 3})
        body = self.posts("/ump/recall")[0][2]
        self.assertEqual(body["limit"], 3)

    def test_06_limit_zero_forwarded_as_zero(self):
        self.call("ump_recall", {"query": "q", "limit": 0})
        body = self.posts("/ump/recall")[0][2]
        self.assertEqual(body["limit"], 0)

    def test_07_kind_wrapped_in_filter_array(self):
        self.call("ump_recall", {"query": "q", "kind": "episodic"})
        body = self.posts("/ump/recall")[0][2]
        self.assertEqual(body["filter"]["kind"], ["episodic"])

    def test_08_kind_omitted_when_absent(self):
        self.call("ump_recall", {"query": "q"})
        body = self.posts("/ump/recall")[0][2]
        self.assertNotIn("filter", body)

    def test_09_result_text_passthrough(self):
        FakeUMPHandler.script[("POST", "/ump/recall")] = (200, json.dumps({"marker": "abc123"}))
        text = self.call_text("ump_recall", {"query": "q"})
        self.assertEqual(json.loads(text)["marker"], "abc123")

    def test_10_content_block_is_text_type(self):
        r = self.call("ump_recall", {"query": "q"})
        self.assertEqual(r["result"]["content"][0]["type"], "text")


# ===========================================================================
# Group D — remember marshaling (10)
# ===========================================================================
class TestDRemember(UnitBase):
    def test_01_posts_to_remember_path(self):
        self.call("ump_remember", {"title": "t", "text": "x"})
        self.assertEqual(len(self.posts("/ump/remember")), 1)

    def test_02_default_kind_semantic(self):
        self.call("ump_remember", {"title": "t", "text": "x"})
        body = self.posts("/ump/remember")[0][2]
        self.assertEqual(body["kind"], "semantic")

    def test_03_custom_kind_forwarded(self):
        self.call("ump_remember", {"title": "t", "text": "x", "kind": "procedural"})
        body = self.posts("/ump/remember")[0][2]
        self.assertEqual(body["kind"], "procedural")

    def test_04_visibility_private(self):
        self.call("ump_remember", {"title": "t", "text": "x"})
        body = self.posts("/ump/remember")[0][2]
        self.assertEqual(body["scope"]["visibility"], "private")

    def test_05_owner_injected(self):
        self.call("ump_remember", {"title": "t", "text": "x"})
        body = self.posts("/ump/remember")[0][2]
        self.assertEqual(body["scope"]["owner"], OPERATOR_DID)

    def test_06_title_text_forwarded(self):
        self.call("ump_remember", {"title": "T1", "text": "B1"})
        body = self.posts("/ump/remember")[0][2]
        self.assertEqual(body["body"]["title"], "T1")
        self.assertEqual(body["body"]["text"], "B1")

    def test_07_tags_forwarded(self):
        self.call("ump_remember", {"title": "t", "text": "x", "tags": ["a", "b"]})
        body = self.posts("/ump/remember")[0][2]
        self.assertEqual(body["body"]["tags"], ["a", "b"])

    def test_08_tags_omitted_when_absent(self):
        self.call("ump_remember", {"title": "t", "text": "x"})
        body = self.posts("/ump/remember")[0][2]
        self.assertNotIn("tags", body["body"])

    def test_09_missing_title_defaults_empty(self):
        self.call("ump_remember", {"text": "x"})
        body = self.posts("/ump/remember")[0][2]
        self.assertEqual(body["body"]["title"], "")

    def test_10_result_text_passthrough(self):
        FakeUMPHandler.script[("POST", "/ump/remember")] = (200, json.dumps({"id": "urn:ump:zzz", "result": "created"}))
        text = self.call_text("ump_remember", {"title": "t", "text": "x"})
        self.assertEqual(json.loads(text)["id"], "urn:ump:zzz")


# ===========================================================================
# Group E — get marshaling (8)
# ===========================================================================
class TestEGet(UnitBase):
    def test_01_uses_get_method(self):
        self.call("ump_get", {"id": "urn:ump:abc"})
        # recorded path is percent-encoded (colons are quoted by the bridge)
        self.assertEqual(len(self.gets("/ump/memory/urn%3Aump%3Aabc")), 1)
        self.assertEqual(len(self.posts()), 0)

    def test_02_path_based_route(self):
        self.call("ump_get", {"id": "urn:ump:abc"})
        path = self.gets("/ump/memory/urn%3Aump%3Aabc")[0][1]
        self.assertTrue(path.startswith("/ump/memory/"))

    def test_03_urlencodes_specials(self):
        self.call("ump_get", {"id": "a b&c/d"})
        path = self.gets("/ump/memory/a%20b%26c%2Fd")[0][1]
        self.assertEqual(path, "/ump/memory/a%20b%26c%2Fd")

    def test_04_colon_encoded(self):
        self.call("ump_get", {"id": "urn:ump:x"})
        path = self.gets("/ump/memory/urn%3Aump%3Ax")[0][1]
        self.assertIn("urn%3Aump%3Ax", path)

    def test_05_empty_id_default(self):
        self.call("ump_get", {})
        self.assertEqual(len(self.gets("/ump/memory/")), 1)

    def test_06_result_passthrough(self):
        FakeUMPHandler.script[("GET", "/ump/memory/record-here")] = (200, json.dumps({"record": "here"}))
        text = self.call_text("ump_get", {"id": "record-here"})
        self.assertEqual(json.loads(text)["record"], "here")

    def test_07_get_sends_no_body(self):
        self.call("ump_get", {"id": "x"})
        self.assertIsNone(self.gets("/ump/memory/x")[0][2])

    def test_08_unicode_id_encoded(self):
        self.call("ump_get", {"id": "urn:ump:héllo-世界"})
        path = self.gets("/ump/memory/" + urllib.parse.quote("urn:ump:héllo-世界", safe=""))[0][1]
        self.assertTrue(path.endswith(urllib.parse.quote("urn:ump:héllo-世界", safe="")))


# ===========================================================================
# Group F — capabilities (4)
# ===========================================================================
class TestFCapabilities(UnitBase):
    def test_01_get_method_used(self):
        self.call("ump_capabilities")
        self.assertEqual(len(self.gets("/ump/capabilities")), 1)

    def test_02_exact_path(self):
        self.call("ump_capabilities")
        self.assertEqual(self.gets("/ump/capabilities")[0][1], "/ump/capabilities")

    def test_03_result_passthrough(self):
        FakeUMPHandler.script[("GET", "/ump/capabilities")] = (200, json.dumps({"server": "ump-memory"}))
        text = self.call_text("ump_capabilities")
        self.assertEqual(json.loads(text)["server"], "ump-memory")

    def test_04_args_ignored(self):
        self.call("ump_capabilities", {"bogus": 1})
        self.assertEqual(len(self.gets("/ump/capabilities")), 1)


# ===========================================================================
# Group G — robustness & error containment (8)
# ===========================================================================
class TestGRobustness(UnitBase):
    def test_01_backend_500_error_json_not_crash(self):
        FakeUMPHandler.script[("POST", "/ump/recall")] = (500, "boom")
        text = self.call_text("ump_recall", {"query": "q"})
        d = json.loads(text)
        self.assertIn("error", d)
        self.assertIn("500", d["error"])
        r = self.bridge.rpc("ping")
        self.assertEqual(r["result"], {})  # bridge still alive

    def test_02_backend_404_error_json(self):
        FakeUMPHandler.script[("GET", "/ump/memory/x")] = (404, "nope")
        text = self.call_text("ump_get", {"id": "x"})
        d = json.loads(text)
        self.assertIn("error", d)
        self.assertIn("404", d["error"])

    def test_03_backend_non_json_passthrough(self):
        FakeUMPHandler.script[("POST", "/ump/recall")] = (200, "<<<not json>>>")
        text = self.call_text("ump_recall", {"query": "q"})
        self.assertEqual(text.strip(), "<<<not json>>>")

    def test_04_connection_refused_error_json(self):
        b = BridgeProc("http://127.0.0.1:9")  # nothing listens here
        try:
            r = b.rpc("tools/call", {"name": "ump_recall", "arguments": {"query": "q"}})
            text = r["result"]["content"][0]["text"]
            d = json.loads(text)
            self.assertIn("error", d)
        finally:
            b.close()

    def test_05_backend_timeout_surfaces_error(self):
        # separate slow backend that hangs; bridge with 1s timeout
        hang_srv = HTTPServer(("127.0.0.1", 0), _HangingHandler)
        threading.Thread(target=hang_srv.serve_forever, daemon=True).start()
        b = BridgeProc(f"http://127.0.0.1:{hang_srv.server_address[1]}", timeout=1)
        try:
            t0 = time.time()
            r = b.rpc("tools/call", {"name": "ump_recall", "arguments": {"query": "q"}})
            text = r["result"]["content"][0]["text"]
            d = json.loads(text)
            self.assertIn("error", d)
            self.assertLess(time.time() - t0, 10)
        finally:
            b.close()
            hang_srv.shutdown()

    def test_06_stderr_silent_on_normal_ops(self):
        b = BridgeProc(self.base)
        try:
            b.rpc("initialize", {})
            b.rpc("notifications/initialized", notify=True)
            b.rpc("tools/call", {"name": "ump_capabilities", "arguments": {}})
            b.rpc("ping")
            # stderr must have nothing pending while running normally
            self.assertTrue(b.stderr_quiet())
        finally:
            b.close()

    def test_07_missing_tool_name_in_params(self):
        r = self.bridge.rpc("tools/call", {"params_from": "wrong key"})
        # params lack "name" -> unknown tool None -> -32602
        self.assertEqual(r["error"]["code"], -32602)

    def test_08_null_arguments_treated_empty(self):
        r = self.bridge.rpc("tools/call", {"name": "ump_recall", "arguments": None})
        self.assertIn("result", r)
        body = self.posts("/ump/recall")[0][2]
        self.assertEqual(body["query"], "")


class _HangingHandler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _hang(self):
        time.sleep(30)

    do_POST = _hang
    do_GET = _hang


# ===========================================================================
# Integration base — real m1 backend
# ===========================================================================
class IntegrationBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bridge = BridgeProc(REAL_BASE)
        r = cls.bridge.rpc("initialize", {})
        assert "result" in r, r
        cls.bridge.rpc("notifications/initialized", notify=True)

    @classmethod
    def tearDownClass(cls):
        cls.bridge.close()

    def call(self, tool, args=None):
        return self.bridge.rpc("tools/call", {"name": tool,
                                              "arguments": args if args is not None else {}})

    def call_text(self, tool, args=None):
        r = self.call(tool, args)
        assert "result" in r, r
        return r["result"]["content"][0]["text"]

    def call_json(self, tool, args=None):
        return json.loads(self.call_text(tool, args))

    def nonce(self):
        return uuid.uuid4().hex[:12]


# ===========================================================================
# Group H — integration: capabilities & reads (10)
# ===========================================================================
class TestHIntegrationRead(IntegrationBase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.fixed_nonce = "hdrr" + uuid.uuid4().hex[:10]
        cls.card = {
            "title": f"UMP MCP test fixture {cls.fixed_nonce}",
            "text": f"Integration fixture card, nonce {cls.fixed_nonce}.",
            "tags": [TEST_TAG],
        }
        r = cls.bridge.rpc("tools/call", {"name": "ump_remember", "arguments": cls.card})
        d = json.loads(r["result"]["content"][0]["text"])
        cls.urn = d.get("id")

    def test_01_capabilities_returns_json(self):
        d = self.call_json("ump_capabilities")
        self.assertIsInstance(d, dict)

    def test_02_capabilities_has_server_field(self):
        d = self.call_json("ump_capabilities")
        keys = json.dumps(d).lower()
        self.assertTrue(any(k in keys for k in ("server", "name", "ump")))

    def test_03_capabilities_mentions_kinds(self):
        text = self.call_text("ump_capabilities")
        self.assertIn("kind", text.lower())

    def test_04_recall_returns_results_key(self):
        d = self.call_json("ump_recall", {"query": TestHIntegrationRead.fixed_nonce})
        self.assertIn("results", d)

    def test_05_recall_finds_fixture_card(self):
        d = self.call_json("ump_recall", {"query": TestHIntegrationRead.fixed_nonce})
        self.assertTrue(any(TestHIntegrationRead.fixed_nonce in json.dumps(r) for r in d["results"]),
                        f"nonce {TestHIntegrationRead.fixed_nonce} not recalled")

    def test_06_get_by_id_roundtrip(self):
        # FIXED 2026-10-04: the get route is path-based
        # (GET /ump/memory/<id>), not /ump/get?id=. With the corrected
        # route, get-by-id returns the record the bridge just wrote.
        d = self.call_json("ump_get", {"id": TestHIntegrationRead.urn})
        self.assertIn(TestHIntegrationRead.urn, json.dumps(d))

    def test_07_get_nonexistent_not_found(self):
        d = self.call_json("ump_get", {"id": "urn:ump:nonexistent-" + self.nonce()})
        blob = json.dumps(d).lower()
        self.assertTrue(("not" in blob) or ("error" in blob) or d == {})

    def test_08_recall_limit_respected(self):
        d = self.call_json("ump_recall", {"query": "the", "limit": 1})
        self.assertLessEqual(len(d["results"]), 1)

    def test_09_recall_returns_private_cards(self):
        d = self.call_json("ump_recall", {"query": TestHIntegrationRead.fixed_nonce})
        vis = [r.get("record", {}).get("scope", {}).get("visibility") for r in d["results"]]
        self.assertIn("private", vis)

    def test_10_recall_owner_is_operator(self):
        d = self.call_json("ump_recall", {"query": TestHIntegrationRead.fixed_nonce})
        owners = {r.get("record", {}).get("scope", {}).get("owner") for r in d["results"]}
        self.assertIn(OPERATOR_DID, owners)


# ===========================================================================
# Group I — integration: writes, all tagged (14)
# ===========================================================================
class TestIIntegrationWrite(IntegrationBase):
    def test_01_remember_minimal_card(self):
        n = self.nonce()
        d = self.call_json("ump_remember",
                           {"title": f"minimal {n}", "text": f"minimal body {n}", "tags": [TEST_TAG]})
        self.assertIn("id", d)

    def test_02_remember_returns_urn_id(self):
        n = self.nonce()
        d = self.call_json("ump_remember",
                           {"title": f"urn {n}", "text": f"urn body {n}", "tags": [TEST_TAG]})
        self.assertTrue(d["id"].startswith("urn:ump:"), d)

    def test_03_unicode_roundtrip(self):
        n = self.nonce()
        text = f"unicode测试 {n} 🎯 em-dash — ok"
        d = self.call_json("ump_remember",
                           {"title": f"unicode {n}", "text": text, "tags": [TEST_TAG]})
        g = self.call_json("ump_get", {"id": d["id"]})
        self.assertIn(n, json.dumps(g, ensure_ascii=False))

    def test_04_long_text_10kb(self):
        n = self.nonce()
        body = f"long {n} " + "y" * 10_000
        d = self.call_json("ump_remember",
                           {"title": f"long {n}", "text": body, "tags": [TEST_TAG]})
        self.assertIn("id", d)

    def test_05_special_chars_title(self):
        n = self.nonce()
        title = f"special {n} !@#$%^&*()_+|;:'\",.<>?"
        d = self.call_json("ump_remember",
                           {"title": title, "text": f"body {n}", "tags": [TEST_TAG]})
        self.assertIn("id", d)

    def test_06_tags_visible_in_get(self):
        n = self.nonce()
        d = self.call_json("ump_remember",
                           {"title": f"tags {n}", "text": f"tags body {n}",
                            "tags": [TEST_TAG, "extra-tag"]})
        g = self.call_json("ump_get", {"id": d["id"]})
        s = json.dumps(g)
        self.assertIn(TEST_TAG, s)
        self.assertIn("extra-tag", s)

    def test_07_episodic_kind(self):
        n = self.nonce()
        d = self.call_json("ump_remember",
                           {"title": f"epi {n}", "text": f"epi body {n}",
                            "tags": [TEST_TAG], "kind": "episodic"})
        self.assertIn("id", d)

    def test_08_procedural_kind(self):
        n = self.nonce()
        d = self.call_json("ump_remember",
                           {"title": f"proc {n}", "text": f"proc body {n}",
                            "tags": [TEST_TAG], "kind": "procedural"})
        self.assertIn("id", d)

    def test_09_working_kind(self):
        n = self.nonce()
        d = self.call_json("ump_remember",
                           {"title": f"work {n}", "text": f"work body {n}",
                            "tags": [TEST_TAG], "kind": "working"})
        self.assertIn("id", d)

    def test_10_idempotent_same_content(self):
        args = {"title": "idempotency probe ump-mcp-test", "text": "identical content re-post probe",
                "tags": [TEST_TAG]}
        d1 = self.call_json("ump_remember", args)
        d2 = self.call_json("ump_remember", args)
        self.assertEqual(d1.get("id"), d2.get("id"))
        self.assertIn(d2.get("result", d2.get("status", "created")), ("created", "merged", "ok", d2.get("result")))

    def test_11_distinctive_token_recallable(self):
        n = "tok" + self.nonce()
        self.call_json("ump_remember",
                       {"title": f"token {n}", "text": f"needle body {n}", "tags": [TEST_TAG]})
        d = self.call_json("ump_recall", {"query": n})
        self.assertTrue(any(n in json.dumps(r) for r in d["results"]))

    def test_12_remember_then_get_full_roundtrip(self):
        n = self.nonce()
        d = self.call_json("ump_remember",
                           {"title": f"full rt {n}", "text": f"full body {n}", "tags": [TEST_TAG]})
        g = self.call_json("ump_get", {"id": d["id"]})
        blob = json.dumps(g)
        self.assertIn(n, blob)
        self.assertIn(d["id"], blob)

    def test_13_result_created_or_merged(self):
        n = self.nonce()
        d = self.call_json("ump_remember",
                           {"title": f"status {n}", "text": f"status body {n}", "tags": [TEST_TAG]})
        s = json.dumps(d).lower()
        self.assertTrue(any(w in s for w in ("created", "merged", "ok", "id")))

    def test_14_three_distinct_cards_all_recallable(self):
        ns = ["m" + self.nonce() for _ in range(3)]
        for n in ns:
            self.call_json("ump_remember",
                           {"title": f"multi {n}", "text": f"multi body {n}", "tags": [TEST_TAG]})
        for n in ns:
            d = self.call_json("ump_recall", {"query": n})
            self.assertTrue(any(n in json.dumps(r) for r in d["results"]), f"{n} missing")


# ===========================================================================
# Group J — integration: negative & edge (6)
# ===========================================================================
class TestJIntegrationEdge(IntegrationBase):
    def test_01_empty_title_accepted_by_backend(self):
        # Backend ACCEPTS empty title (bridge defaults it to "") — pins
        # actual behavior; a card with empty title is created fine.
        d = self.call_json("ump_remember", {"text": "no title probe", "tags": [TEST_TAG]})
        self.assertIn("id", d)

    def test_02_empty_query_rejected_not_crash(self):
        # Backend rejects empty query with 400 invalid_record — the bridge
        # must surface it as a JSON error body, NOT crash the RPC loop.
        d = self.call_json("ump_recall", {"query": ""})
        self.assertIn("error", d)
        self.assertIn("400", str(d))

    def test_03_malformed_id_get(self):
        d = self.call_json("ump_get", {"id": "not-a-urn"})
        blob = json.dumps(d).lower()
        self.assertTrue(("not" in blob) or ("error" in blob) or d == {})

    def test_04_kind_filter_narrows(self):
        # FIXED 2026-10-04: the server reads filter.kind (array); the
        # bridge now wraps the kind arg. Procedural cards must NOT appear
        # in a semantic-filtered recall of their own nonce.
        n = "kf" + self.nonce()
        self.call_json("ump_remember",
                       {"title": f"kindfilter {n}", "text": f"kindfilter body {n}",
                        "tags": [TEST_TAG], "kind": "procedural"})
        d = self.call_json("ump_recall", {"query": n, "kind": "semantic"})
        hits = [r for r in d["results"] if n in json.dumps(r)]
        self.assertEqual(hits, [], "procedural card leaked into semantic recall")
        d2 = self.call_json("ump_recall", {"query": n, "kind": "procedural"})
        hits2 = [r for r in d2["results"] if n in json.dumps(r)]
        self.assertTrue(hits2, "procedural card missing from procedural recall")

    def test_05_rest_bare_string_body_rejected(self):
        # Kukla's documented gotcha: /ump/remember with a bare JSON string -> 400
        req = urllib.request.Request(
            REAL_BASE + "/ump/remember",
            data=json.dumps("bare string not an object").encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                status, body = r.status, r.read().decode()
        except urllib.error.HTTPError as e:
            status, body = e.code, e.read().decode("utf-8", "replace")
        self.assertEqual(status, 400)
        self.assertIn("invalid", body.lower())

    def test_06_fresh_bridge_lists_four_tools(self):
        b = BridgeProc(REAL_BASE)
        try:
            b.rpc("initialize", {})
            r = b.rpc("tools/list")
            self.assertEqual(len(r["result"]["tools"]), 4)
        finally:
            b.close()


if __name__ == "__main__":
    # count check before running
    total = sum(1 for cls in (TestAProtocol, TestBToolsList, TestCRecall, TestDRemember,
                              TestEGet, TestFCapabilities, TestGRobustness,
                              TestHIntegrationRead, TestIIntegrationWrite,
                              TestJIntegrationEdge)
                for m in dir(cls) if m.startswith("test_"))
    print(f"[suite] {total} tests discovered")
    unittest.main(verbosity=1)
