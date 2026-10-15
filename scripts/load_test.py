#!/usr/bin/env python3
"""Sustained load producer + metrics polling for throughput observation."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
import urllib.request
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from aiokafka import AIOKafkaProducer

from app.config import settings


def fetch_metrics(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8"))


async def burst(producer: AIOKafkaProducer, topic: str, count: int) -> None:
    batch = []
    for i in range(count):
        payload = {
            "id": str(uuid.uuid4()),
            "seq": i,
            "topic": topic,
            "ts": time.time(),
        }
        batch.append(producer.send(topic, json.dumps(payload).encode("utf-8")))
    await asyncio.gather(*batch)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Load test both Kafka topics")
    parser.add_argument("--io-count", type=int, default=500)
    parser.add_argument("--cpu-count", type=int, default=50)
    parser.add_argument("--metrics-url", default="http://127.0.0.1:8000/metrics")
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--wait-seconds", type=float, default=60.0)
    args = parser.parse_args()

    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()
    try:
        print(f"Sending {args.io_count} I/O + {args.cpu_count} CPU messages...")
        t0 = time.monotonic()
        await asyncio.gather(
            burst(producer, settings.io_topic, args.io_count),
            burst(producer, settings.cpu_topic, args.cpu_count),
        )
        print(f"Produce finished in {time.monotonic() - t0:.2f}s")
    finally:
        await producer.stop()

    print("Polling /metrics (note: each uvicorn worker has its own counters)...")
    deadline = time.monotonic() + args.wait_seconds
    last = None
    while time.monotonic() < deadline:
        try:
            snap = fetch_metrics(args.metrics_url)
            last = snap
            io = snap["io_heavy_events"]
            cpu = snap["cpu_heavy_events"]
            print(
                f"pid={snap['worker_pid']} "
                f"io_processed={io['processed']} io_rate={io['rate_per_sec']}/s "
                f"cpu_processed={cpu['processed']} cpu_rate={cpu['rate_per_sec']}/s "
                f"io_failed={io['failed']} cpu_failed={cpu['failed']}"
            )
        except Exception as exc:  # noqa: BLE001
            print(f"metrics poll failed: {exc}")
        await asyncio.sleep(args.poll_seconds)

    if last:
        print("Final snapshot:", json.dumps(last, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
