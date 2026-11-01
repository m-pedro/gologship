#!/usr/bin/env python3
"""Live HTTP access log stream simulator with optional 5xx anomaly spike injection."""
import argparse
import os
import random
import time
from datetime import datetime, timezone

IPS = [f"10.0.1.{i}" for i in range(1, 25)] + ["192.168.10.99", "172.16.0.5"]
ENDPOINTS = [
    ("GET", "/api/v1/users", 512),
    ("POST", "/api/v1/checkout", 2048),
    ("GET", "/api/v1/products", 4096),
    ("GET", "/healthz", 64),
]

def format_line(rng: random.Random, force_error: bool = False) -> str:
    ip = rng.choice(IPS)
    now = datetime.now(timezone.utc).strftime("%d/%b/%Y:%H:%H:%S +0000")
    method, path, base_bytes = rng.choice(ENDPOINTS)
    if force_error:
        status = rng.choice([500, 502, 503])
        latency = round(rng.uniform(250.0, 950.0), 2)
    else:
        status = rng.choices([200, 201, 304, 404, 500], weights=[80, 8, 5, 5, 2])[0]
        latency = round(rng.expovariate(1.0 / 12.0) + 1.0, 2)
    return f'{ip} - - [{now}] "{method} {path} HTTP/1.1" {status} {base_bytes} {latency:.2f}\n'

def main() -> None:
    parser = argparse.ArgumentParser(description="Stream synthetic HTTP logs.")
    parser.add_argument("-o", "--output", default="data/live_access.log")
    parser.add_argument("-n", "--lines", type=int, default=500, help="Total lines (0 = infinite)")
    parser.add_argument("-r", "--rate", type=float, default=200.0, help="Lines per second")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--inject-spike", action="store_true", help="Inject 5xx error spike mid-stream")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    rng = random.Random(args.seed)
    delay = 1.0 / max(args.rate, 1.0)

    count = 0
    with open(args.output, "a", encoding="utf-8") as f:
        while args.lines == 0 or count < args.lines:
            in_spike = args.inject_spike and (150 <= (count % 300) <= 220)
            f.write(format_line(rng, force_error=in_spike))
            f.flush()
            count += 1
            if args.lines == 0 or count < 50:
                time.sleep(delay)

    print(f"Streamed {count} lines to {args.output}")

if __name__ == "__main__":
    main()
