# octoconv-worker-imagepdf

Outbound Worker Protocol version 1 client for capability `octoconv.convert.imagepdf` version 1. One PNG becomes one single-page PDF. The process dials the core and the file URLs. It does not open Postgres, Redis, or a Go package.

The published schema in `schema/v1.schema.json` and the manifest in `schema/manifest-imagepdf.json` are copied from `tileforge/octoconv` commit `756c4ab242779c3d3c5a5a9400450fd63f6633d8`. `testdata/sample.png` is the same bytes as `internal/e2e/testdata/sample.png` in that commit. `core_sha` of that core stays `02fcd190d6b01df08c955abb2937019a46aaf8c6`.

## Run

Set `IMAGEPDF_CORE_URL` to an `https://` core URL, plus `IMAGEPDF_CLIENT_CERT`, `IMAGEPDF_CLIENT_KEY`, `IMAGEPDF_CLIENT_CA`, and `IMAGEPDF_WORKER_ID`. `IMAGEPDF_MANIFEST_PATH` defaults to `schema/manifest-imagepdf.json`. A plaintext URL is refused. A redirect is not followed. `IMAGEPDF_FILE_ACCESS=gateway` refuses to start.

```sh
python3 -m venv .venv
.venv/bin/pip install -e .
.venv/bin/python -m octoconv_worker_imagepdf
```

The process opens one session and pulls on it until the core says drain or terminate. Drain, including a drain carried on the lease response or a heartbeat while the worker is idle, deletes the session before the next pull. An attempt in hand still finishes first. Terminate on hello, pull, heartbeat, or lease stops further pulls. An assignment that arrives after terminate, or whose id was cancelled on the pull or on a heartbeat while that pull was in flight, is reported stopped and is not fetched. The process waits until an in-flight stopped report finishes. It heartbeats the session. While an attempt is in hand it renews the lease until that lease covers the attempt deadline. The renewal request ends at the current lease expiry. A failed renewal, including a truncated response, is sent again before the lease expires. Terminate, cancel, and a lost lease are reported as stopped. A cancel that lands during conversion is not followed by progress 99. A cancel that arrives while a progress request is in flight is handled as stopped. The lease keeps renewing until the core accepts completion. A plaintext file URL is refused before the fetch. The core client API still answers png to pdf with 422, and a pull for this capability still comes back empty. This process does not change that binary.

## Budget

Thirty runs of `testdata/sample.png` set the attempt budget to the ceiling of p99 times 2, and at least 10 seconds. A job still caps the attempt at its own deadline.

```sh
.venv/bin/python -m octoconv_worker_imagepdf.measure
```

The run in `measurements/sample-png-30.json` was taken at 2026-11-11T08:40:02Z with Python 3.15.2 and img2pdf 0.6.3. Nearest-rank p99 was 0.010163 seconds, so the budget is 10 seconds. That integer stays in this repository.

## Tests

```sh
.venv/bin/python -m unittest discover -s tests
```
