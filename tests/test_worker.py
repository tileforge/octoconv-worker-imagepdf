import ast
import http.server
import json
import ssl
import subprocess
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pypdf import PdfReader
from io import BytesIO

import octoconv_worker_imagepdf.client as client_mod
from octoconv_worker_imagepdf.client import Run, finish_assignment, renew_wait, serve
from octoconv_worker_imagepdf.convert import png_to_pdf
from octoconv_worker_imagepdf.measure import budget_seconds, measure
from octoconv_worker_imagepdf.messages import (
    complete_request,
    fail_request,
    hello_request,
    manifest_sha256,
    progress_request,
    pull_request,
    stopped_request,
    validator,
)
from octoconv_worker_imagepdf.origin import OriginRejected, request, require_https

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "testdata" / "sample.png"


class ConvertTest(unittest.TestCase):
    def test_sample_png_is_one_page_pdf(self):
        pdf = png_to_pdf(SAMPLE.read_bytes())
        self.assertTrue(pdf.startswith(b"%PDF-"))
        reader = PdfReader(BytesIO(pdf))
        self.assertEqual(len(reader.pages), 1)
        self.assertGreater(float(reader.pages[0].mediabox.width), 0)

    def test_rejects_non_png(self):
        with self.assertRaises(ValueError):
            png_to_pdf(b"not a png")


class SchemaTest(unittest.TestCase):
    def test_worker_messages_match_published_schema(self):
        check = validator()
        pdf = png_to_pdf(SAMPLE.read_bytes())
        bodies = [
            hello_request("00000000-0000-4000-8000-000000000001", "ab" * 32, 1),
            pull_request("00000000-0000-4000-8000-000000000002", 1, 20),
            progress_request(0),
            progress_request(99),
            complete_request(pdf),
            fail_request("engine_error", "convert"),
            stopped_request("terminate"),
        ]
        for body in bodies:
            check.validate(body)
        self.assertEqual(len(manifest_sha256()), 64)


class OriginTest(unittest.TestCase):
    def test_plaintext_is_rejected(self):
        with self.assertRaises(OriginRejected):
            require_https("http://core.example/worker/v1/sessions")
        with self.assertRaises(OriginRejected):
            request("GET", "http://127.0.0.1/file")

    def test_redirect_is_not_followed(self):
        cert, key = _local_cert()
        seen = []

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                seen.append(self.path)
                if self.path == "/first":
                    self.send_response(302)
                    self.send_header("Location", "https://other.example/secret")
                    self.end_headers()
                    return
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"nope")

            def log_message(self, fmt, *args):
                return

        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(cert, key)
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.socket = ctx.wrap_socket(server.socket, server_side=True)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = server.server_address[1]
            client = ssl._create_unverified_context()
            with self.assertRaises(OriginRejected):
                request("GET", f"https://127.0.0.1:{port}/first", context=client)
        finally:
            server.shutdown()
            server.server_close()
        self.assertEqual(seen, ["/first"])


class SessionTest(unittest.TestCase):
    def test_one_session_pulls_until_drain(self):
        cert, key = _local_cert()
        seen = []
        pulls = {"n": 0}
        idle = {"cancel_attempt_ids": [], "drain": False, "terminate": False}
        session_id = "00000000-0000-4000-8000-000000000010"
        hello = {
            "protocol_version": 1,
            "session_id": session_id,
            "session_token": "ab" * 32,
            "control": idle,
            "pull_wait_seconds": 0,
            "heartbeat_interval_seconds": 3600,
            "lease_seconds": 30,
            "session_max_lifetime_seconds": 3600,
            "max_message_bytes": 262144,
            "file_link_max_ttl_seconds": 300,
            "clock_skew_margin_seconds": 5,
            "pull_rate_per_second": 2,
            "progress_rate_per_second": 4,
            "file_access": "direct",
        }
        check = validator()
        check.validate(hello)

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                seen.append(("POST", self.path, raw, self.headers.get("Authorization")))
                if self.path.endswith("/pull"):
                    pulls["n"] += 1
                    control = dict(idle)
                    if pulls["n"] >= 2:
                        control["drain"] = True
                    body = {"protocol_version": 1, "assignments": [], "control": control}
                    check.validate(body)
                else:
                    body = hello
                payload = json.dumps(body).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_DELETE(self):
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                seen.append(("DELETE", self.path, raw, self.headers.get("Authorization")))
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, fmt, *args):
                return

        ctx_server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx_server.load_cert_chain(cert, key)
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.socket = ctx_server.wrap_socket(server.socket, server_side=True)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = server.server_address[1]
            serve(
                f"https://127.0.0.1:{port}",
                ssl._create_unverified_context(),
                "00000000-0000-4000-8000-000000000001",
                "cd" * 32,
            )
        finally:
            server.shutdown()
            server.server_close()

        self.assertEqual(
            [item[1] for item in seen],
            [
                "/worker/v1/sessions",
                f"/worker/v1/sessions/{session_id}/pull",
                f"/worker/v1/sessions/{session_id}/pull",
                f"/worker/v1/sessions/{session_id}",
            ],
        )
        self.assertIsNone(seen[0][3])
        self.assertEqual(seen[1][3], "Bearer " + "ab" * 32)
        self.assertEqual(seen[2][0], "POST")
        self.assertEqual(seen[3][0], "DELETE")
        first = json.loads(seen[1][2])["pull_request_id"]
        second = json.loads(seen[2][2])["pull_request_id"]
        self.assertNotEqual(first, second)
        check.validate(json.loads(seen[0][2]))
        check.validate(json.loads(seen[1][2]))
        check.validate(json.loads(seen[2][2]))
        check.validate(json.loads(seen[3][2]))


class BoundaryTest(unittest.TestCase):
    def test_sources_do_not_name_postgres_redis_or_go(self):
        banned = ("psycopg", "redis", "boto", "minio", "subprocess")
        for path in (ROOT / "src").rglob("*.py"):
            tree = ast.parse(path.read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    names = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom):
                    names = [node.module or ""]
                else:
                    continue
                for name in names:
                    for item in banned:
                        self.assertNotIn(item, name)

    def test_thirty_runs_floor_the_budget_at_ten_seconds(self):
        report = measure(30)
        self.assertEqual(report["runs"], 30)
        self.assertEqual(len(report["seconds"]), 30)
        seconds, p99 = budget_seconds(report["seconds"])
        self.assertEqual(report["attempt_budget_seconds"], seconds)
        self.assertGreaterEqual(seconds, 10)
        self.assertGreaterEqual(seconds, p99 * 2)
        self.assertEqual(report["pdf_bytes"] > 0, True)


class LeaseTest(unittest.TestCase):
    def test_renew_waits_half_until_the_lease_covers_the_deadline(self):
        now = datetime(2026, 11, 11, tzinfo=timezone.utc)
        self.assertEqual(
            renew_wait(30, now + timedelta(seconds=30), now + timedelta(seconds=120), now),
            15,
        )
        self.assertEqual(
            renew_wait(30, now + timedelta(seconds=30), now + timedelta(seconds=10), now),
            30,
        )
        self.assertEqual(renew_wait(30, now - timedelta(seconds=1), now + timedelta(seconds=120), now), 0)

    def test_plaintext_file_url_is_not_fetched(self):
        assignment = _assignment("http://127.0.0.1/file", "https://127.0.0.1/out")
        with self.assertRaises(OriginRejected):
            finish_assignment(
                "https://core.example",
                ssl.create_default_context(),
                "ab" * 32,
                assignment,
                Run(),
                30,
            )

    def test_open_lease_is_renewed_before_complete(self):
        paths = self._attempt(terminate=False)
        self.assertLess(paths.index("lease"), paths.index("complete"))
        self.assertNotIn("stopped", paths)
        self.assertNotIn("fail", paths)

    def test_terminate_reports_stopped(self):
        paths = self._attempt(terminate=True)
        self.assertIn("stopped", paths)
        self.assertNotIn("complete", paths)
        self.assertNotIn("fail", paths)
        self.assertEqual(paths.count("pull"), 1)
        self.assertNotIn("00000000-0000-4000-8000-000000000010", paths)

    def test_lease_drain_deletes_without_another_pull(self):
        applied = threading.Event()
        real_apply = client_mod.apply_control
        real_convert = client_mod.png_to_pdf

        def wrapped_apply(run, core, ctx, token, control):
            real_apply(run, core, ctx, token, control)
            if control.get("drain"):
                applied.set()

        def blocked_convert(png):
            if not applied.wait(8):
                raise AssertionError("lease drain was not applied")
            return real_convert(png)

        client_mod.apply_control = wrapped_apply
        client_mod.png_to_pdf = blocked_convert
        try:
            paths = self._attempt(terminate=False, lease_drain=True)
        finally:
            client_mod.apply_control = real_apply
            client_mod.png_to_pdf = real_convert
        self.assertLess(paths.index("lease"), paths.index("complete"))
        self.assertEqual(paths.count("pull"), 1)
        self.assertIn("00000000-0000-4000-8000-000000000010", paths)
        self.assertNotIn("stopped", paths)
        self.assertNotIn("fail", paths)

    def test_cancel_during_convert_skips_later_progress(self):
        paths = self._attempt(terminate=False, cancel=True)
        progress = [
            json.loads(raw)["percent"]
            for command, path, raw, _auth in self.seen
            if command == "POST" and path.endswith("/progress")
        ]
        self.assertEqual(progress, [0])
        self.assertIn("stopped", paths)
        self.assertNotIn("complete", paths)
        self.assertNotIn("fail", paths)

    def test_idle_terminate_stops_the_pull_loop(self):
        cert, key = _local_cert()
        seen = []
        beats = {"n": 0}
        session_id = "00000000-0000-4000-8000-000000000010"
        hello = _hello(session_id, 30, 1)
        idle = {"cancel_attempt_ids": [], "drain": False, "terminate": False}
        holder = {}

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                seen.append((self.command, self.path, raw, self.headers.get("Authorization")))
                if self.path == "/worker/v1/sessions":
                    body = hello
                elif self.path.endswith("/pull"):
                    body = {"protocol_version": 1, "assignments": [], "control": dict(idle)}
                elif self.path.endswith("/heartbeat"):
                    beats["n"] += 1
                    control = dict(idle)
                    if beats["n"] == 1:
                        control["terminate"] = True
                    body = {"protocol_version": 1, "control": control}
                else:
                    body = {}
                _write(self, body)

            def do_DELETE(self):
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                seen.append((self.command, self.path, raw, self.headers.get("Authorization")))
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, fmt, *args):
                return

        ctx_server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx_server.load_cert_chain(cert, key)
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.socket = ctx_server.wrap_socket(server.socket, server_side=True)
        threading.Thread(target=server.serve_forever, daemon=True).start()

        def run():
            try:
                serve(
                    f"https://127.0.0.1:{server.server_address[1]}",
                    ssl._create_unverified_context(),
                    "00000000-0000-4000-8000-000000000001",
                    "cd" * 32,
                )
            except Exception as exc:
                holder["error"] = exc

        worker = threading.Thread(target=run, daemon=True)
        worker.start()
        worker.join(5)
        alive = worker.is_alive()
        server.shutdown()
        server.server_close()
        worker.join(2)
        self.assertFalse(alive)
        self.assertNotIn("error", holder)
        paths = [item[1].rsplit("/", 1)[-1] for item in seen if item[0] != "GET"]
        self.assertGreaterEqual(beats["n"], 1)
        self.assertGreaterEqual(paths.count("pull"), 1)
        self.assertNotIn(session_id, paths)
        self.assertNotIn("stopped", paths)

    def test_renew_past_expiry_skips_later_progress(self):
        cert, key = _local_cert()
        seen = []
        release = threading.Event()
        pulls = {"n": 0}
        now = datetime.now(timezone.utc)
        lease_at = _stamp(now + timedelta(seconds=2))
        deadline = _stamp(now + timedelta(hours=1))
        hello = _hello("00000000-0000-4000-8000-000000000010", 1, 3600)
        idle = {"cancel_attempt_ids": [], "drain": False, "terminate": False}
        original = client_mod.png_to_pdf

        def blocked_convert(png, _orig=original):
            if not release.wait(5):
                raise AssertionError("lease renew did not finish")
            return _orig(png)

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                seen.append((self.command, self.path, raw, self.headers.get("Authorization")))
                if self.path == "/worker/v1/sessions":
                    body = hello
                elif self.path.endswith("/pull"):
                    pulls["n"] += 1
                    if pulls["n"] == 1:
                        port = server.server_address[1]
                        body = {
                            "protocol_version": 1,
                            "assignments": [
                                _assignment(
                                    f"https://127.0.0.1:{port}/png",
                                    f"https://127.0.0.1:{port}/out",
                                    lease_at,
                                    deadline,
                                )
                            ],
                            "control": dict(idle),
                        }
                    else:
                        control = dict(idle)
                        control["drain"] = True
                        body = {"protocol_version": 1, "assignments": [], "control": control}
                elif self.path.endswith("/lease"):
                    release.wait(4)
                    body = {
                        "protocol_version": 1,
                        "lease_expires_at": deadline,
                        "attempt_deadline": deadline,
                        "control": dict(idle),
                    }
                    try:
                        _write(self, body)
                    except Exception:
                        pass
                    release.set()
                    return
                elif self.path.endswith("/progress"):
                    payload = json.loads(raw.decode() or "{}")
                    if payload.get("percent") == 99:
                        _write(self, {"code": "attempt_not_active"}, status=409)
                        return
                    body = {}
                elif self.path.endswith("/stopped"):
                    release.set()
                    body = {}
                else:
                    body = {}
                _write(self, body)

            def do_GET(self):
                seen.append((self.command, self.path, b"", self.headers.get("Authorization")))
                payload = SAMPLE.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "image/png")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_PUT(self):
                self.rfile.read(int(self.headers.get("Content-Length") or 0))
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_DELETE(self):
                self.rfile.read(int(self.headers.get("Content-Length") or 0))
                seen.append((self.command, self.path, b"", self.headers.get("Authorization")))
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, fmt, *args):
                return

        ctx_server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx_server.load_cert_chain(cert, key)
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.socket = ctx_server.wrap_socket(server.socket, server_side=True)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        client_mod.png_to_pdf = blocked_convert
        try:
            serve(
                f"https://127.0.0.1:{server.server_address[1]}",
                ssl._create_unverified_context(),
                "00000000-0000-4000-8000-000000000001",
                "cd" * 32,
            )
        finally:
            client_mod.png_to_pdf = original
            server.shutdown()
            server.server_close()
        progress = [
            json.loads(raw)["percent"]
            for command, path, raw, _auth in seen
            if command == "POST" and path.endswith("/progress")
        ]
        reasons = [
            json.loads(raw)["reason"]
            for command, path, raw, _auth in seen
            if command == "POST" and path.endswith("/stopped")
        ]
        paths = [item[1].rsplit("/", 1)[-1] for item in seen if item[0] != "GET"]
        self.assertEqual(progress, [0])
        self.assertEqual(reasons, ["lease_lost"])
        self.assertNotIn("complete", paths)
        self.assertNotIn("fail", paths)

    def test_terminate_during_pull_reports_stopped_without_fetch(self):
        cert, key = _local_cert()
        seen = []
        seen_terminate = threading.Event()
        pulls = {"n": 0}
        beats = {"n": 0}
        now = datetime.now(timezone.utc)
        lease_at = _stamp(now + timedelta(hours=2))
        deadline = _stamp(now + timedelta(hours=1))
        session_id = "00000000-0000-4000-8000-000000000010"
        hello = _hello(session_id, 30, 1)
        idle = {"cancel_attempt_ids": [], "drain": False, "terminate": False}
        holder = {}
        real_apply = client_mod.apply_control

        def wrapped_apply(run, core, ctx, token, control):
            real_apply(run, core, ctx, token, control)
            if control.get("terminate"):
                seen_terminate.set()

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                seen.append((self.command, self.path, raw, self.headers.get("Authorization")))
                if self.path == "/worker/v1/sessions":
                    body = hello
                elif self.path.endswith("/pull"):
                    pulls["n"] += 1
                    if pulls["n"] == 1:
                        seen_terminate.wait(4)
                        port = server.server_address[1]
                        body = {
                            "protocol_version": 1,
                            "assignments": [
                                _assignment(
                                    f"https://127.0.0.1:{port}/png",
                                    f"https://127.0.0.1:{port}/out",
                                    lease_at,
                                    deadline,
                                )
                            ],
                            "control": dict(idle),
                        }
                    else:
                        control = dict(idle)
                        control["drain"] = True
                        body = {"protocol_version": 1, "assignments": [], "control": control}
                elif self.path.endswith("/heartbeat"):
                    beats["n"] += 1
                    control = dict(idle)
                    if beats["n"] == 1:
                        control["terminate"] = True
                    body = {"protocol_version": 1, "control": control}
                else:
                    body = {}
                _write(self, body)

            def do_GET(self):
                seen.append((self.command, self.path, b"", self.headers.get("Authorization")))
                payload = SAMPLE.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "image/png")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_PUT(self):
                self.rfile.read(int(self.headers.get("Content-Length") or 0))
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_DELETE(self):
                self.rfile.read(int(self.headers.get("Content-Length") or 0))
                seen.append((self.command, self.path, b"", self.headers.get("Authorization")))
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, fmt, *args):
                return

        ctx_server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx_server.load_cert_chain(cert, key)
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.socket = ctx_server.wrap_socket(server.socket, server_side=True)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        client_mod.apply_control = wrapped_apply

        def run():
            try:
                serve(
                    f"https://127.0.0.1:{server.server_address[1]}",
                    ssl._create_unverified_context(),
                    "00000000-0000-4000-8000-000000000001",
                    "cd" * 32,
                )
            except Exception as exc:
                holder["error"] = exc

        worker = threading.Thread(target=run, daemon=True)
        worker.start()
        worker.join(6)
        alive = worker.is_alive()
        try:
            server.shutdown()
            server.server_close()
        finally:
            client_mod.apply_control = real_apply
            worker.join(2)
        self.assertFalse(alive)
        self.assertNotIn("error", holder)
        gets = [item for item in seen if item[0] == "GET"]
        reasons = [
            json.loads(raw)["reason"]
            for command, path, raw, _auth in seen
            if command == "POST" and path.endswith("/stopped")
        ]
        paths = [item[1].rsplit("/", 1)[-1] for item in seen if item[0] != "GET"]
        self.assertEqual(gets, [])
        self.assertEqual(reasons, ["terminate"])
        self.assertEqual(paths.count("pull"), 1)
        self.assertNotIn("complete", paths)
        self.assertNotIn(session_id, paths)

    def test_pull_cancel_reports_stopped_without_fetch(self):
        cert, key = _local_cert()
        seen = []
        pulls = {"n": 0}
        now = datetime.now(timezone.utc)
        lease_at = _stamp(now + timedelta(hours=2))
        deadline = _stamp(now + timedelta(hours=1))
        hello = _hello("00000000-0000-4000-8000-000000000010", 30, 3600)
        idle = {"cancel_attempt_ids": [], "drain": False, "terminate": False}
        attempt_id = "00000000-0000-4000-8000-000000000022"

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                seen.append((self.command, self.path, raw, self.headers.get("Authorization")))
                if self.path == "/worker/v1/sessions":
                    body = hello
                elif self.path.endswith("/pull"):
                    pulls["n"] += 1
                    if pulls["n"] == 1:
                        port = server.server_address[1]
                        control = dict(idle)
                        control["cancel_attempt_ids"] = [attempt_id]
                        body = {
                            "protocol_version": 1,
                            "assignments": [
                                _assignment(
                                    f"https://127.0.0.1:{port}/png",
                                    f"https://127.0.0.1:{port}/out",
                                    lease_at,
                                    deadline,
                                )
                            ],
                            "control": control,
                        }
                    else:
                        control = dict(idle)
                        control["drain"] = True
                        body = {"protocol_version": 1, "assignments": [], "control": control}
                else:
                    body = {}
                _write(self, body)

            def do_GET(self):
                seen.append((self.command, self.path, b"", self.headers.get("Authorization")))
                payload = SAMPLE.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "image/png")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_DELETE(self):
                self.rfile.read(int(self.headers.get("Content-Length") or 0))
                seen.append((self.command, self.path, b"", self.headers.get("Authorization")))
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, fmt, *args):
                return

        ctx_server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx_server.load_cert_chain(cert, key)
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.socket = ctx_server.wrap_socket(server.socket, server_side=True)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            serve(
                f"https://127.0.0.1:{server.server_address[1]}",
                ssl._create_unverified_context(),
                "00000000-0000-4000-8000-000000000001",
                "cd" * 32,
            )
        finally:
            server.shutdown()
            server.server_close()
        reasons = [
            json.loads(raw)["reason"]
            for command, path, raw, _auth in seen
            if command == "POST" and path.endswith("/stopped")
        ]
        paths = [item[1].rsplit("/", 1)[-1] for item in seen if item[0] != "GET"]
        self.assertEqual([item for item in seen if item[0] == "GET"], [])
        self.assertEqual(reasons, ["cancel"])
        self.assertNotIn("complete", paths)
        self.assertNotIn("fail", paths)

    def test_serve_waits_for_the_stop_report(self):
        cert, key = _local_cert()
        seen = []
        started = threading.Event()
        release = threading.Event()
        now = datetime.now(timezone.utc)
        lease_at = _stamp(now + timedelta(hours=2))
        deadline = _stamp(now + timedelta(hours=1))
        hello = _hello("00000000-0000-4000-8000-000000000010", 30, 1)
        idle = {"cancel_attempt_ids": [], "drain": False, "terminate": False}
        beats = {"n": 0}
        pulls = {"n": 0}
        holder = {}
        original = client_mod.png_to_pdf

        def blocked_convert(png, _orig=original):
            if not started.wait(5):
                raise AssertionError("stopped request did not start")
            return _orig(png)

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                seen.append((self.command, self.path, raw, self.headers.get("Authorization")))
                if self.path == "/worker/v1/sessions":
                    body = hello
                elif self.path.endswith("/pull"):
                    pulls["n"] += 1
                    if pulls["n"] == 1:
                        port = server.server_address[1]
                        body = {
                            "protocol_version": 1,
                            "assignments": [
                                _assignment(
                                    f"https://127.0.0.1:{port}/png",
                                    f"https://127.0.0.1:{port}/out",
                                    lease_at,
                                    deadline,
                                )
                            ],
                            "control": dict(idle),
                        }
                    else:
                        control = dict(idle)
                        control["drain"] = True
                        body = {"protocol_version": 1, "assignments": [], "control": control}
                elif self.path.endswith("/heartbeat"):
                    beats["n"] += 1
                    control = dict(idle)
                    if beats["n"] == 1:
                        control["terminate"] = True
                    body = {"protocol_version": 1, "control": control}
                elif self.path.endswith("/stopped"):
                    started.set()
                    release.wait(3)
                    body = {}
                elif self.path.endswith("/progress"):
                    payload = json.loads(raw.decode() or "{}")
                    if payload.get("percent") == 99:
                        _write(self, {"code": "attempt_not_active"}, status=409)
                        return
                    body = {}
                else:
                    body = {}
                _write(self, body)

            def do_GET(self):
                seen.append((self.command, self.path, b"", self.headers.get("Authorization")))
                payload = SAMPLE.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "image/png")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_PUT(self):
                self.rfile.read(int(self.headers.get("Content-Length") or 0))
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, fmt, *args):
                return

        ctx_server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx_server.load_cert_chain(cert, key)
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.socket = ctx_server.wrap_socket(server.socket, server_side=True)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        client_mod.png_to_pdf = blocked_convert

        def run():
            try:
                serve(
                    f"https://127.0.0.1:{server.server_address[1]}",
                    ssl._create_unverified_context(),
                    "00000000-0000-4000-8000-000000000001",
                    "cd" * 32,
                )
            except Exception as exc:
                holder["error"] = exc

        worker = threading.Thread(target=run, daemon=True)
        worker.start()
        try:
            self.assertTrue(started.wait(5))
            deadline_at = time.monotonic() + 1
            while time.monotonic() < deadline_at:
                if not worker.is_alive():
                    self.fail("serve returned before /stopped finished")
                time.sleep(0.05)
            release.set()
            worker.join(4)
        finally:
            release.set()
            client_mod.png_to_pdf = original
            server.shutdown()
            server.server_close()
            worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertNotIn("error", holder)
        reasons = [
            json.loads(raw)["reason"]
            for command, path, raw, _auth in seen
            if command == "POST" and path.endswith("/stopped")
        ]
        paths = [item[1].rsplit("/", 1)[-1] for item in seen]
        self.assertEqual(reasons, ["terminate"])
        self.assertNotIn("complete", paths)

    def _attempt(self, terminate, lease_drain=False, cancel=False):
        cert, key = _local_cert()
        seen = []
        release = threading.Event()
        pulls = {"n": 0}
        beats = {"n": 0}
        now = datetime.now(timezone.utc)
        if terminate or cancel:
            lease_at = _stamp(now + timedelta(hours=2))
            deadline = _stamp(now + timedelta(hours=1))
            lease_seconds = 30
            beat = 1
        else:
            lease_at = _stamp(now + timedelta(seconds=30))
            deadline = _stamp(now + timedelta(hours=1))
            lease_seconds = 1
            beat = 3600
        idle = {"cancel_attempt_ids": [], "drain": False, "terminate": False}
        session_id = "00000000-0000-4000-8000-000000000010"
        hello = _hello(session_id, lease_seconds, beat)

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                seen.append((self.command, self.path, raw, self.headers.get("Authorization")))
                if self.path == "/worker/v1/sessions":
                    body = hello
                elif self.path.endswith("/pull"):
                    pulls["n"] += 1
                    if pulls["n"] == 1:
                        body = {
                            "protocol_version": 1,
                            "assignments": [
                                _assignment(
                                    f"https://127.0.0.1:{server.server_address[1]}/png",
                                    f"https://127.0.0.1:{server.server_address[1]}/out",
                                    lease_at,
                                    deadline,
                                )
                            ],
                            "control": idle,
                        }
                    else:
                        control = dict(idle)
                        control["drain"] = True
                        body = {"protocol_version": 1, "assignments": [], "control": control}
                elif self.path.endswith("/heartbeat"):
                    beats["n"] += 1
                    control = dict(idle)
                    if terminate and beats["n"] == 1:
                        control["terminate"] = True
                    if cancel and beats["n"] == 1:
                        control["cancel_attempt_ids"] = ["00000000-0000-4000-8000-000000000022"]
                    body = {"protocol_version": 1, "control": control}
                elif self.path.endswith("/progress"):
                    payload = json.loads(raw.decode() or "{}")
                    if cancel and payload.get("percent") == 99:
                        _write(self, {"code": "attempt_not_active"}, status=409)
                        return
                    body = {}
                elif self.path.endswith("/lease"):
                    release.set()
                    control = dict(idle)
                    if lease_drain:
                        control["drain"] = True
                    body = {
                        "protocol_version": 1,
                        "lease_expires_at": deadline,
                        "attempt_deadline": deadline,
                        "control": control,
                    }
                elif self.path.endswith("/stopped"):
                    release.set()
                    body = {}
                else:
                    body = {}
                _write(self, body)

            def do_GET(self):
                seen.append((self.command, self.path, b"", self.headers.get("Authorization")))
                if not cancel and not release.wait(8):
                    _write(self, {}, status=500)
                    return
                payload = SAMPLE.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "image/png")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_PUT(self):
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                seen.append((self.command, self.path, raw, self.headers.get("Authorization")))
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_DELETE(self):
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                seen.append((self.command, self.path, raw, self.headers.get("Authorization")))
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, fmt, *args):
                return

        ctx_server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx_server.load_cert_chain(cert, key)
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.socket = ctx_server.wrap_socket(server.socket, server_side=True)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        original_convert = client_mod.png_to_pdf
        if cancel:
            def blocked_convert(png, _orig=original_convert):
                if not release.wait(8):
                    raise AssertionError("cancel did not report stopped")
                return _orig(png)

            client_mod.png_to_pdf = blocked_convert
        self.seen = seen
        try:
            port = server.server_address[1]
            serve(
                f"https://127.0.0.1:{port}",
                ssl._create_unverified_context(),
                "00000000-0000-4000-8000-000000000001",
                "cd" * 32,
            )
        finally:
            client_mod.png_to_pdf = original_convert
            server.shutdown()
            server.server_close()
        gets = [item for item in seen if item[0] == "GET"]
        self.assertEqual(gets[0][3], None)
        return [item[1].rsplit("/", 1)[-1] for item in seen if item[0] != "GET"]


def _stamp(moment):
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _hello(session_id, lease_seconds, beat):
    return {
        "protocol_version": 1,
        "session_id": session_id,
        "session_token": "ab" * 32,
        "control": {"cancel_attempt_ids": [], "drain": False, "terminate": False},
        "pull_wait_seconds": 0,
        "heartbeat_interval_seconds": beat,
        "lease_seconds": lease_seconds,
        "session_max_lifetime_seconds": 3600,
        "max_message_bytes": 262144,
        "file_link_max_ttl_seconds": 300,
        "clock_skew_margin_seconds": 5,
        "pull_rate_per_second": 2,
        "progress_rate_per_second": 4,
        "file_access": "direct",
    }


def _assignment(source, target, lease_at=None, deadline=None):
    moment = lease_at or "2026-11-11T12:00:30Z"
    end = deadline or "2026-11-11T12:02:00Z"
    return {
        "job_id": "00000000-0000-4000-8000-000000000020",
        "step_id": "00000000-0000-4000-8000-000000000021",
        "attempt_id": "00000000-0000-4000-8000-000000000022",
        "attempt_token": "cd" * 32,
        "capability_id": "octoconv.convert.imagepdf",
        "capability_version": 1,
        "attempt_deadline": end,
        "lease_expires_at": moment,
        "limits": {"max_output_bytes": 104857600},
        "inputs": [
            {
                "role": "source",
                "ordinal": 0,
                "format": "png",
                "href": source,
                "expires_at": end,
            }
        ],
        "outputs": [
            {
                "role": "primary",
                "ordinal": 0,
                "format": "pdf",
                "href": target,
                "expires_at": end,
            }
        ],
    }


def _write(handler, body, status=200):
    payload = json.dumps(body).encode()
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(payload)))
    handler.end_headers()
    handler.wfile.write(payload)


def _local_cert():
    directory = ROOT / ".venv-certs"
    directory.mkdir(exist_ok=True)
    cert = directory / "cert.pem"
    key = directory / "key.pem"
    if cert.exists() and key.exists():
        return cert, key
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-nodes",
            "-subj",
            "/CN=127.0.0.1",
        ],
        check=True,
        capture_output=True,
    )
    return cert, key


if __name__ == "__main__":
    unittest.main()
