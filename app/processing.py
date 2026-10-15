"""Message processing for I/O-bound and CPU-bound workloads."""

from __future__ import annotations

import asyncio
import hashlib
import json
from typing import Any

from app.config import settings
from app.metrics import metrics


async def process_io_message(payload: bytes) -> dict[str, Any]:
    """Simulate ~100ms I/O without blocking the event loop."""
    data = json.loads(payload.decode("utf-8"))
    await asyncio.sleep(settings.io_latency_seconds)
    metrics.io.record_success()
    return {"status": "ok", "id": data.get("id"), "kind": "io"}


def _cpu_bound_work(payload: bytes, iterations: int) -> dict[str, Any]:
    """CPU-intensive work intended to run in a ProcessPoolExecutor."""
    data = json.loads(payload.decode("utf-8"))
    digest = payload
    for _ in range(iterations):
        digest = hashlib.sha256(digest).digest()
    return {
        "status": "ok",
        "id": data.get("id"),
        "kind": "cpu",
        "digest": digest.hex()[:16],
    }


async def process_cpu_message(payload: bytes, executor) -> dict[str, Any]:
    """Offload CPU work so the asyncio event loop stays responsive for I/O."""
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        executor,
        _cpu_bound_work,
        payload,
        settings.cpu_work_iterations,
    )
    metrics.cpu.record_success()
    return result
