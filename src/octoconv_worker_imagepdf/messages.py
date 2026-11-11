"""Protocol v1 message bodies. The published schema is the check."""

import hashlib
import json
from pathlib import Path

import jsonschema
from jsonschema import Draft202012Validator

_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = _ROOT / "schema" / "v1.schema.json"
MANIFEST_PATH = _ROOT / "schema" / "manifest-imagepdf.json"


def load_schema():
    return json.loads(SCHEMA_PATH.read_text())


def validator():
    return Draft202012Validator(load_schema(), format_checker=jsonschema.FormatChecker())


def manifest_sha256(path: Path | None = None) -> str:
    raw = (path or MANIFEST_PATH).read_bytes()
    return hashlib.sha256(raw).hexdigest()


def hello_request(worker_id: str, manifest_sha: str, free_slots: int) -> dict:
    return {
        "protocol_version": 1,
        "worker_id": worker_id,
        "manifest_sha256": manifest_sha,
        "free_slots": free_slots,
    }


def pull_request(pull_request_id: str, free_slots: int, wait_seconds: int) -> dict:
    return {
        "protocol_version": 1,
        "pull_request_id": pull_request_id,
        "free_slots": free_slots,
        "wait_seconds": wait_seconds,
    }


def progress_request(percent: int) -> dict:
    return {"protocol_version": 1, "percent": percent}


def complete_request(pdf: bytes) -> dict:
    return {
        "protocol_version": 1,
        "artifacts": [
            {
                "role": "primary",
                "ordinal": 0,
                "format": "pdf",
                "byte_size": len(pdf),
                "content_type": "application/pdf",
                "sha256": hashlib.sha256(pdf).hexdigest(),
            }
        ],
    }


def fail_request(engine_code: str, message: str) -> dict:
    return {
        "protocol_version": 1,
        "engine_code": engine_code,
        "message": message,
    }


def dumps(body: dict) -> bytes:
    return json.dumps(body, separators=(",", ":")).encode()
