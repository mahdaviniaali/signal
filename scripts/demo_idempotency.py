#!/usr/bin/env python3
"""Demonstrate idempotent handling of a redelivered order (e.g. order 101).

Simulates: order saved → crash before Kafka commit → same message redelivered.
Produces the same event id twice; the second delivery must be skipped, not
inserted again (UNIQUE topic+event_id in SQLite).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from aiokafka import AIOKafkaProducer

from app.config import settings


def get_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8"))


async def send_once(producer: AIOKafkaProducer, topic: str, event_id: str, seq: int) -> None:
    payload = {
        "id": event_id,
        "order_id": event_id,
        "seq": seq,
        "topic": topic,
        "ts": time.time(),
    }
    await producer.send_and_wait(topic, json.dumps(payload).encode("utf-8"))
    print(f"sent id={event_id} seq={seq} -> {topic}")


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-id", default="101", help="Order/event id to redeliver")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--topic", choices=["io", "cpu"], default="io")
    parser.add_argument("--wait-seconds", type=float, default=3.0)
    args = parser.parse_args()

    topic = settings.io_topic if args.topic == "io" else settings.cpu_topic
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()
    try:
        await send_once(producer, topic, args.event_id, seq=1)
        await asyncio.sleep(args.wait_seconds)

        first = get_json(f"{args.base_url}/events/{topic}/{args.event_id}")
        print("after first delivery:", first)
        count_after_first = get_json(f"{args.base_url}/events/count?topic={topic}")["count"]

        # Redelivery of the same order (what Kafka does after crash before commit).
        await send_once(producer, topic, args.event_id, seq=2)
        await asyncio.sleep(args.wait_seconds)

        second = get_json(f"{args.base_url}/events/{topic}/{args.event_id}")
        count_after_second = get_json(f"{args.base_url}/events/count?topic={topic}")["count"]
        print("after redelivery:", second)
        print(f"rows for topic: first={count_after_first} second={count_after_second}")

        if count_after_second != count_after_first:
            raise SystemExit("FAIL: duplicate insert detected")
        if second["created_at"] != first["created_at"]:
            raise SystemExit("FAIL: row was overwritten on redelivery")
        print("OK: redelivery skipped; order was not saved twice")
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
