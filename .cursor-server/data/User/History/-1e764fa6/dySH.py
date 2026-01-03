#!/usr/bin/env python3
"""
Send test logs to Grafana Loki via the Push API.

Works with the docker-compose in this folder (default Loki URL: http://localhost:3100).
No external dependencies (stdlib only).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from typing import Dict, Tuple


def parse_kv_pairs(items: list[str]) -> Dict[str, str]:
    labels: Dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"Invalid label '{item}'. Expected key=value.")
        k, v = item.split("=", 1)
        k, v = k.strip(), v.strip()
        if not k:
            raise ValueError(f"Invalid label '{item}'. Empty key.")
        labels[k] = v
    return labels


def build_payload(labels: Dict[str, str], line: str, ts_ns: int | None = None) -> bytes:
    if ts_ns is None:
        ts_ns = time.time_ns()

    payload = {
        "streams": [
            {
                "stream": labels,
                "values": [
                    [str(ts_ns), line],
                ],
            }
        ]
    }
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def post_loki_push(loki_url: str, payload: bytes, timeout_s: float = 5.0) -> Tuple[int, str]:
    url = loki_url.rstrip("/") + "/loki/api/v1/push"
    req = urllib.request.Request(
        url=url,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return resp.status, body
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
        return e.code, body


def main() -> int:
    p = argparse.ArgumentParser(description="Send test logs to Grafana Loki (Push API).")
    p.add_argument("--loki-url", default="http://localhost:3100", help="Base Loki URL (default: http://localhost:3100)")
    p.add_argument(
        "--label",
        action="append",
        default=[],
        help="Stream label in key=value form (repeatable). Example: --label job=test --label env=dev",
    )
    p.add_argument("--message", default="hello from send_test_logs.py", help="Log line to send.")
    p.add_argument("--count", type=int, default=1, help="How many log lines to send.")
    p.add_argument("--interval", type=float, default=0.0, help="Seconds to sleep between messages (default: 0).")
    p.add_argument("--timeout", type=float, default=5.0, help="HTTP timeout seconds (default: 5).")

    args = p.parse_args()

    labels = {"job": "test-logger"}
    labels.update(parse_kv_pairs(args.label))

    if args.count < 1:
        print("ERROR: --count must be >= 1", file=sys.stderr)
        return 2
    if args.interval < 0:
        print("ERROR: --interval must be >= 0", file=sys.stderr)
        return 2

    for i in range(args.count):
        line = args.message
        if args.count > 1:
            line = f"{line} ({i + 1}/{args.count})"

        payload = build_payload(labels=labels, line=line)
        status, body = post_loki_push(args.loki_url, payload, timeout_s=args.timeout)
        if status < 200 or status >= 300:
            print(f"ERROR: Loki push failed: HTTP {status}", file=sys.stderr)
            if body:
                print(body, file=sys.stderr)
            return 1

        print(f"OK: sent to {args.loki_url} labels={labels} line={line}")
        if args.interval and i + 1 < args.count:
            time.sleep(args.interval)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


