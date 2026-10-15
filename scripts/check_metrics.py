#!/usr/bin/env python3
"""Aggregate /metrics across uvicorn workers by repeated polling."""

from __future__ import annotations

import json
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def main() -> None:
    url = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000/metrics"
    rounds = int(sys.argv[2]) if len(sys.argv) > 2 else 40
    seen: dict[int, dict] = {}
    for _ in range(rounds):
        with urllib.request.urlopen(url, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        seen[data["worker_pid"]] = data
        time.sleep(0.05)

    total_io = sum(v["io_heavy_events"]["processed"] for v in seen.values())
    total_cpu = sum(v["cpu_heavy_events"]["processed"] for v in seen.values())
    print(f"workers_seen={len(seen)} total_io={total_io} total_cpu={total_cpu}")
    for pid, m in sorted(seen.items()):
        print(f"pid={pid} io={m['io_heavy_events']} cpu={m['cpu_heavy_events']}")


if __name__ == "__main__":
    main()
