"""HTTPS-only dialing. Redirects are not followed."""

import http.client
import ssl
from urllib.parse import urlsplit


class OriginRejected(Exception):
    """The URL is not HTTPS, or the peer answered with a redirect."""


def require_https(raw: str):
    parsed = urlsplit(raw)
    if parsed.scheme != "https" or not parsed.hostname:
        raise OriginRejected(raw)
    return parsed


def request(method, raw_url, body=None, headers=None, timeout=60, context=None, limit=8 * 1024 * 1024):
    parsed = require_https(raw_url)
    port = parsed.port or 443
    path = parsed.path or "/"
    if parsed.query:
        path = path + "?" + parsed.query
    ctx = context or ssl.create_default_context()
    conn = http.client.HTTPSConnection(parsed.hostname, port, timeout=timeout, context=ctx)
    resp = None
    try:
        conn.request(method, path, body=body, headers=headers or {})
        resp = conn.getresponse()
        if 300 <= resp.status < 400:
            resp.read(1024)
            raise OriginRejected(raw_url)
        payload = resp.read(limit + 1)
        if len(payload) > limit:
            raise OriginRejected(raw_url)
        return resp.status, payload
    finally:
        if resp is not None:
            resp.close()
        conn.close()
