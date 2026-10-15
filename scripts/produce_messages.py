#!/usr/bin/env python3
"""Produce test messages to io-heavy-events and/or cpu-heavy-events."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from aiokafka import AIOKafkaProducer

from app.config import settings


async def produce(topic: str, count: int, rate: float | None) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()
    delay = (1.0 / rate) if rate and rate > 0 else 0.0
    try:
        for i in range(count):
            payload = {
                "id": str(uuid.uuid4()),
                "seq": i,
                "topic": topic,
                "ts": time.time(),
            }
            await producer.send_and_wait(topic, json.dumps(payload).encode("utf-8"))
            if delay:
                await asyncio.sleep(delay)
        print(f"Produced {count} messages to {topic}")
    finally:
        await producer.stop()


async def main() -> None:
    parser = argparse.ArgumentParser(description="Produce Kafka test messages")
    parser.add_argument(
        "--topic",
        choices=["io", "cpu", "both"],
        default="both",
        help="Which topic(s) to produce to",
    )
    parser.add_argument("--count", type=int, default=100, help="Messages per topic")
    parser.add_argument(
        "--rate",
        type=float,
        default=None,
        help="Optional messages/sec throttle (per topic)",
    )
    args = parser.parse_args()

    topics: list[str] = []
    if args.topic in ("io", "both"):
        topics.append(settings.io_topic)
    if args.topic in ("cpu", "both"):
        topics.append(settings.cpu_topic)

    for topic in topics:
        await produce(topic, args.count, args.rate)


if __name__ == "__main__":
    asyncio.run(main())
