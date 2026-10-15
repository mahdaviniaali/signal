"""Message processing for I/O-bound and CPU-bound workloads (idempotent)."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from typing import Any

from app.config import settings
from app.metrics import metrics
from app.store import store

logger = logging.getLogger(__name__)


def _event_id(data: dict[str, Any]) -> str:
    """Business key used for deduplication (order id / event id)."""
    raw = data.get("id", data.get("order_id"))
    if raw is None:
        raise ValueError("message missing idempotency key: id or order_id")
    return str(raw)


async def process_io_message(payload: bytes) -> dict[str, Any]:
    """Simulate ~100ms I/O; skip if this order/event was already saved."""
    data = json.loads(payload.decode("utf-8"))
    event_id = _event_id(data)
    topic = settings.io_topic

    # Crash-after-save scenario: Kafka redelivers → do not insert again.
    if store.exists(topic, event_id):
        metrics.io.record_duplicate()
        logger.info("io duplicate skipped id=%s", event_id)
        return {"status": "duplicate", "id": event_id, "kind": "io"}

    await asyncio.sleep(settings.io_latency_seconds)

    # UNIQUE(topic, event_id): concurrent workers cannot both "save" the same order.
    inserted = await asyncio.to_thread(store.save, topic, event_id, data)
    if not inserted:
        metrics.io.record_duplicate()
        logger.info("io duplicate skipped (race) id=%s", event_id)
        return {"status": "duplicate", "id": event_id, "kind": "io"}

    metrics.io.record_success()
    return {"status": "ok", "id": event_id, "kind": "io"}


def _cpu_bound_work(payload: bytes, iterations: int) -> dict[str, Any]:
    """CPU-intensive work intended to run in a ProcessPoolExecutor."""
    data = json.loads(payload.decode("utf-8"))
    event_id = _event_id(data)
    digest = payload
    for _ in range(iterations):
        digest = hashlib.sha256(digest).digest()
    return {
        "status": "ok",
        "id": event_id,
        "kind": "cpu",
        "digest": digest.hex()[:16],
        "payload": data,
    }


async def process_cpu_message(payload: bytes, executor) -> dict[str, Any]:
    """Offload CPU work; skip if this order/event was already saved."""
    data = json.loads(payload.decode("utf-8"))
    event_id = _event_id(data)
    topic = settings.cpu_topic

    if store.exists(topic, event_id):
        metrics.cpu.record_duplicate()
        logger.info("cpu duplicate skipped id=%s", event_id)
        return {"status": "duplicate", "id": event_id, "kind": "cpu"}

    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        executor,
        _cpu_bound_work,
        payload,
        settings.cpu_work_iterations,
    )

    inserted = await asyncio.to_thread(store.save, topic, event_id, data)
    if not inserted:
        metrics.cpu.record_duplicate()
        logger.info("cpu duplicate skipped (race) id=%s", event_id)
        return {"status": "duplicate", "id": event_id, "kind": "cpu"}

    metrics.cpu.record_success()
    return result
