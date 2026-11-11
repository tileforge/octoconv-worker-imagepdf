"""Dial the core, pull one assignment, and put back one PDF."""

import json
import os
import ssl
import threading
import time
import uuid
from datetime import datetime, timezone
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
    stopped_request,
)
from octoconv_worker_imagepdf.origin import OriginRejected, request, require_https

CAPABILITY_ID = "octoconv.convert.imagepdf"


class ProtocolError(Exception):
    def __init__(self, status, code):
        self.status = status
        self.code = code
        super().__init__(code or str(status))


class StopFlag:
    def __init__(self):
        self._lock = threading.Lock()
        self.outcome = ""

    def claim(self, outcome):
        with self._lock:
            if self.outcome:
                return False
            self.outcome = outcome
            return True


class Run:
    def __init__(self):
        self.lock = threading.Lock()
        self.flag = None
        self.assignment = None
        self.draining = False


def parse_time(raw):
    if not isinstance(raw, str) or not raw:
        return None
    text = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def renew_wait(lease_seconds, lease_until, deadline, now):
    half = lease_seconds / 2 if lease_seconds and lease_seconds > 0 else 0.001
    if half <= 0:
        half = 0.001
    if lease_until is not None and deadline is not None and lease_until >= deadline:
        return max(0.0, (lease_until - now).total_seconds())
    if lease_until is None:
        return half
    remaining = (lease_until - now).total_seconds()
    if remaining < 0:
        return 0.0
    return min(half, remaining)


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


def post_stopped(core, ctx, token, assignment, reason):
    try:
        post_json(
            core,
            ctx,
            "/worker/v1/attempts/" + assignment["attempt_id"] + "/stopped",
            stopped_request(reason),
            token,
            assignment.get("attempt_token", ""),
        )
    except (ProtocolError, OriginRejected, OSError):
        return


def apply_control(run, core, ctx, token, control):
    if control.get("drain"):
        with run.lock:
            run.draining = True
    with run.lock:
        flag = run.flag
        assignment = run.assignment
    if flag is None or assignment is None:
        return
    if control.get("terminate"):
        if flag.claim("terminate"):
            post_stopped(core, ctx, token, assignment, "terminate")
        return
    attempt_id = assignment.get("attempt_id")
    if attempt_id and attempt_id in (control.get("cancel_attempt_ids") or []):
        if flag.claim("cancel"):
            post_stopped(core, ctx, token, assignment, "cancel")


def renew_loop(core, ctx, token, assignment, lease_seconds, flag, done):
    lease_until = parse_time(assignment.get("lease_expires_at"))
    deadline = parse_time(assignment.get("attempt_deadline"))
    while not done.is_set():
        wait = renew_wait(lease_seconds, lease_until, deadline, datetime.now(timezone.utc))
        if done.wait(wait):
            return
        now = datetime.now(timezone.utc)
        if lease_until is not None and now >= lease_until:
            if flag.claim("lease_lost"):
                post_stopped(core, ctx, token, assignment, "lease_lost")
            return
        if lease_until is not None and deadline is not None and lease_until >= deadline:
            continue
        try:
            body = post_json(
                core,
                ctx,
                "/worker/v1/attempts/" + assignment["attempt_id"] + "/lease",
                {"protocol_version": 1},
                token,
                assignment.get("attempt_token", ""),
            )
        except ProtocolError as err:
            if err.code == "session_closed":
                flag.claim("session_closed")
                return
            if err.code == "attempt_not_active":
                if flag.claim("lease_lost"):
                    post_stopped(core, ctx, token, assignment, "lease_lost")
                return
            continue
        except (OriginRejected, OSError):
            continue
        control = body.get("control") or {}
        if control.get("terminate"):
            if flag.claim("terminate"):
                post_stopped(core, ctx, token, assignment, "terminate")
            return
        if assignment.get("attempt_id") in (control.get("cancel_attempt_ids") or []):
            if flag.claim("cancel"):
                post_stopped(core, ctx, token, assignment, "cancel")
            return
        nxt = parse_time(body.get("lease_expires_at"))
        if nxt is not None:
            lease_until = nxt


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
    run = Run()
    lease_seconds = hello.get("lease_seconds")
    if not isinstance(lease_seconds, int) or lease_seconds < 1:
        lease_seconds = 30

    def beat():
        interval = hello.get("heartbeat_interval_seconds")
        if not isinstance(interval, int) or interval < 1:
            interval = 10
        while not stop.wait(interval):
            try:
                body = post_json(
                    core,
                    ctx,
                    "/worker/v1/sessions/" + session + "/heartbeat",
                    {"protocol_version": 1},
                    token,
                )
            except (ProtocolError, OriginRejected, OSError):
                return
            apply_control(run, core, ctx, token, body.get("control") or {})

    threading.Thread(target=beat, daemon=True).start()
    try:
        control = hello.get("control") or {}
        apply_control(run, core, ctx, token, control)
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
            apply_control(run, core, ctx, token, control)
            assignments = pull.get("assignments") or []
            if not assignments:
                if control.get("terminate"):
                    return None
                if control.get("drain") or run.draining:
                    delete_session(core, ctx, session, token)
                    return None
                time.sleep(0.2)
                continue
            result = finish_assignment(core, ctx, token, assignments[0], run, lease_seconds)
            if control.get("terminate"):
                return result
            if control.get("drain") or run.draining:
                delete_session(core, ctx, session, token)
                return result
    finally:
        stop.set()


def finish_assignment(core, ctx, token, assignment, run, lease_seconds):
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
    require_https(source["href"])
    require_https(target["href"])
    if parse_time(assignment.get("attempt_deadline")) is None or parse_time(assignment.get("lease_expires_at")) is None:
        return fail(core, ctx, token, assignment, "engine_error", "assignment")
    attempt = assignment["attempt_token"]
    attempt_id = assignment["attempt_id"]
    flag = StopFlag()
    done = threading.Event()
    with run.lock:
        run.flag = flag
        run.assignment = assignment
    renewer = threading.Thread(
        target=renew_loop,
        args=(core, ctx, token, assignment, lease_seconds, flag, done),
        daemon=True,
    )
    renewer.start()
    try:
        if flag.outcome:
            return None
        status, png = request("GET", source["href"], context=ctx, limit=100 * 1024 * 1024)
        if flag.outcome:
            return None
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
        if flag.outcome:
            return None
        try:
            pdf = png_to_pdf(png)
        except (ValueError, img2pdf_error()):
            if flag.outcome:
                return None
            return fail(core, ctx, token, assignment, "engine_error", "convert")
        limit = int((assignment.get("limits") or {}).get("max_output_bytes") or 0)
        if limit and len(pdf) > limit:
            if flag.outcome:
                return None
            return fail(core, ctx, token, assignment, "engine_error", "output too large")
        post_json(
            core,
            ctx,
            "/worker/v1/attempts/" + attempt_id + "/progress",
            progress_request(99),
            token,
            attempt,
        )
        if flag.outcome:
            return None
        put_status, _ = request(
            "PUT",
            target["href"],
            body=pdf,
            headers={"Content-Type": "application/pdf"},
            context=ctx,
            limit=1024 * 1024,
        )
        if flag.outcome:
            return None
        if put_status < 200 or put_status >= 300:
            return fail(core, ctx, token, assignment, "engine_error", "output")
        if not flag.claim("complete"):
            return None
        return post_json(
            core,
            ctx,
            "/worker/v1/attempts/" + attempt_id + "/complete",
            complete_request(pdf),
            token,
            attempt,
        )
    finally:
        done.set()
        renewer.join(timeout=5)
        with run.lock:
            if run.flag is flag:
                run.flag = None
                run.assignment = None


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


