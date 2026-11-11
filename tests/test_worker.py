import ast
import http.server
import json
import ssl
import subprocess
import threading
import unittest
from pathlib import Path

from pypdf import PdfReader
from io import BytesIO

from octoconv_worker_imagepdf.client import serve
from octoconv_worker_imagepdf.convert import png_to_pdf
from octoconv_worker_imagepdf.measure import budget_seconds, measure
from octoconv_worker_imagepdf.messages import (
    complete_request,
    fail_request,
    hello_request,
    manifest_sha256,
    progress_request,
    pull_request,
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
