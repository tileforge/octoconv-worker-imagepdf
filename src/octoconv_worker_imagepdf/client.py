"""Dial the core, pull one assignment, and put back one PDF."""

import json
import os
import ssl
import threading
import time
import uuid
from pathlib import Path

from octoconv_worker_imagepdf.convert import png_to_pdf
from octoconv_worker_imagepdf.messages import (
    complete_request,
    dumps,
    fail_request,
    hello_request,
    manifest_sha256,
    progress_request,
    pull_request,
)
from octoconv_worker_imagepdf.origin import OriginRejected, request, require_https

CAPABILITY_ID = "octoconv.convert.imagepdf"


class ProtocolError(Exception):
    def __init__(self, status, code):
        self.status = status
        self.code = code
        super().__init__(code or str(status))


def core_context():
    if os.environ.get("IMAGEPDF_FILE_ACCESS") == "gateway":
        raise SystemExit("gateway file access is not implemented")
    core = os.environ.get("IMAGEPDF_CORE_URL", "")
    require_https(core)
    ctx = ssl.create_default_context(cafile=os.environ.get("IMAGEPDF_CLIENT_CA") or None)
    cert = os.environ.get("IMAGEPDF_CLIENT_CERT")
    key = os.environ.get("IMAGEPDF_CLIENT_KEY")
    if cert and key:
        ctx.load_cert_chain(cert, key)
    return core.rstrip("/"), ctx


def _headers(token, attempt, content_type):
    headers = {}
    if token:
        headers["Authorization"] = "Bearer " + token
    if attempt:
        headers["Octoconv-Attempt"] = attempt
    if content_type:
        headers["Content-Type"] = content_type
    elif token or attempt:
        headers["Content-Type"] = "application/json"
    return headers


def post_json(core, ctx, path, body, token="", attempt=""):
    status, payload = request(
        "POST",
        core + path,
        body=dumps(body) if isinstance(body, dict) else body,
        headers=_headers(token, attempt, "application/json"),
        context=ctx,
        limit=1024 * 1024,
    )
    if status < 200 or status >= 300:
        code = ""
        try:
            code = json.loads(payload).get("code", "")
        except json.JSONDecodeError:
            code = ""
        raise ProtocolError(status, code)
    if not payload:
        return {}
    return json.loads(payload)


def delete_session(core, ctx, session, token):
    status, _ = request(
        "DELETE",
        core + "/worker/v1/sessions/" + session,
        body=dumps({"protocol_version": 1}),
        headers=_headers(token, "", "application/json"),
        context=ctx,
        limit=1024 * 1024,
    )
    if status < 200 or status >= 300:
        raise ProtocolError(status, "")


def serve(core, ctx, worker_id, manifest_sha, free_slots=1):
    hello = post_json(
        core,
        ctx,
        "/worker/v1/sessions",
        hello_request(worker_id, manifest_sha, free_slots),
    )
    session = hello["session_id"]
    token = hello["session_token"]
    stop = threading.Event()

    def beat():
        interval = hello.get("heartbeat_interval_seconds")
        if not isinstance(interval, int) or interval < 1:
            interval = 10
        while not stop.wait(interval):
            try:
                post_json(
                    core,
                    ctx,
                    "/worker/v1/sessions/" + session + "/heartbeat",
                    {"protocol_version": 1},
                    token,
                )
            except (ProtocolError, OriginRejected, OSError):
                return

    threading.Thread(target=beat, daemon=True).start()
    try:
        control = hello.get("control") or {}
        if control.get("drain"):
            delete_session(core, ctx, session, token)
            return None
        if control.get("terminate"):
            return None
        wait = hello.get("pull_wait_seconds")
        if not isinstance(wait, int):
            wait = 1
        while True:
            pull = post_json(
                core,
                ctx,
                "/worker/v1/sessions/" + session + "/pull",
                pull_request(str(uuid.uuid4()), free_slots, wait),
                token,
            )
            control = pull.get("control") or {}
            assignments = pull.get("assignments") or []
            if not assignments:
                if control.get("terminate"):
                    return None
                if control.get("drain"):
                    delete_session(core, ctx, session, token)
                    return None
                time.sleep(0.2)
                continue
            result = finish_assignment(core, ctx, token, assignments[0])
            if control.get("terminate"):
                return result
            if control.get("drain"):
                delete_session(core, ctx, session, token)
                return result
    finally:
        stop.set()


def finish_assignment(core, ctx, token, assignment):
    if assignment.get("capability_id") != CAPABILITY_ID:
        return fail(core, ctx, token, assignment, "engine_error", "capability")
    inputs = assignment.get("inputs") or []
    outputs = assignment.get("outputs") or []
    if len(inputs) != 1 or len(outputs) != 1:
        return fail(core, ctx, token, assignment, "engine_error", "assignment")
    source = inputs[0]
    target = outputs[0]
    if source.get("format") != "png" or target.get("format") != "pdf":
        return fail(core, ctx, token, assignment, "engine_error", "format")
    attempt = assignment["attempt_token"]
    attempt_id = assignment["attempt_id"]
    status, png = request("GET", source["href"], context=ctx, limit=100 * 1024 * 1024)
    if status < 200 or status >= 300:
        return fail(core, ctx, token, assignment, "engine_error", "input")
    post_json(
        core,
        ctx,
        "/worker/v1/attempts/" + attempt_id + "/progress",
        progress_request(0),
        token,
        attempt,
    )
    try:
        pdf = png_to_pdf(png)
    except (ValueError, img2pdf_error()):
        return fail(core, ctx, token, assignment, "engine_error", "convert")
    limit = int((assignment.get("limits") or {}).get("max_output_bytes") or 0)
    if limit and len(pdf) > limit:
        return fail(core, ctx, token, assignment, "engine_error", "output too large")
    post_json(
        core,
        ctx,
        "/worker/v1/attempts/" + attempt_id + "/progress",
        progress_request(99),
        token,
        attempt,
    )
    put_status, _ = request(
        "PUT",
        target["href"],
        body=pdf,
        headers={"Content-Type": "application/pdf"},
        context=ctx,
        limit=1024 * 1024,
    )
    if put_status < 200 or put_status >= 300:
        return fail(core, ctx, token, assignment, "engine_error", "output")
    return post_json(
        core,
        ctx,
        "/worker/v1/attempts/" + attempt_id + "/complete",
        complete_request(pdf),
        token,
        attempt,
    )


def img2pdf_error():
    import img2pdf

    return img2pdf.ImageOpenError


def fail(core, ctx, token, assignment, engine_code, message):
    return post_json(
        core,
        ctx,
        "/worker/v1/attempts/" + assignment["attempt_id"] + "/fail",
        fail_request(engine_code, message),
        token,
        assignment.get("attempt_token", ""),
    )


def main():
    core, ctx = core_context()
    worker_id = os.environ["IMAGEPDF_WORKER_ID"]
    manifest = os.environ.get("IMAGEPDF_MANIFEST_PATH")
    digest = manifest_sha256(Path(manifest) if manifest else None)
    serve(core, ctx, worker_id, digest)


