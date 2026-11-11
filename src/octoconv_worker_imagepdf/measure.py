"""Time thirty png-to-pdf runs of testdata/sample.png."""

import hashlib
import json
import math
import platform
import time
from datetime import datetime, timezone
from pathlib import Path

import img2pdf

from octoconv_worker_imagepdf.convert import png_to_pdf

_ROOT = Path(__file__).resolve().parents[2]
SAMPLE = _ROOT / "testdata" / "sample.png"


def percentile(samples, fraction):
    ordered = sorted(samples)
    index = math.ceil(fraction * len(ordered)) - 1
    return ordered[max(0, index)]


def budget_seconds(samples):
    p99 = percentile(samples, 0.99)
    return max(10, math.ceil(p99 * 2)), p99


def measure(runs=30):
    png = SAMPLE.read_bytes()
    samples = []
    pdf = b""
    for _ in range(runs):
        started = time.perf_counter()
        pdf = png_to_pdf(png)
        samples.append(time.perf_counter() - started)
    seconds, p99 = budget_seconds(samples)
    return {
        "measured_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "python": platform.python_version(),
        "img2pdf": img2pdf.__version__,
        "sample": "testdata/sample.png",
        "sample_sha256": hashlib.sha256(png).hexdigest(),
        "runs": runs,
        "seconds": [round(item, 6) for item in samples],
        "p99_seconds": round(p99, 6),
        "attempt_budget_seconds": seconds,
        "pdf_bytes": len(pdf),
        "formula": "max(10, ceil(p99 * 2))",
    }


def main():
    print(json.dumps(measure(), indent=2))


if __name__ == "__main__":
    main()
