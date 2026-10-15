"""In-memory per-process metrics for verifying throughput and failures."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from threading import Lock


@dataclass
class TopicMetrics:
    processed: int = 0
    failed: int = 0
    duplicates: int = 0
    first_at: float | None = None
    last_at: float | None = None
    _lock: Lock = field(default_factory=Lock, repr=False)

    def record_success(self) -> None:
        now = time.monotonic()
        with self._lock:
            self.processed += 1
            if self.first_at is None:
                self.first_at = now
            self.last_at = now

    def record_failure(self) -> None:
        with self._lock:
            self.failed += 1

    def record_duplicate(self) -> None:
        with self._lock:
            self.duplicates += 1

    def snapshot(self) -> dict:
        with self._lock:
            if self.first_at is None or self.last_at is None or self.processed == 0:
                rate = 0.0
                elapsed = 0.0
            else:
                elapsed = max(self.last_at - self.first_at, 1e-6)
                rate = self.processed / elapsed
            return {
                "processed": self.processed,
                "failed": self.failed,
                "duplicates": self.duplicates,
                "rate_per_sec": round(rate, 2),
                "elapsed_sec": round(elapsed, 2),
            }


class MetricsRegistry:
    def __init__(self) -> None:
        self.worker_pid = os.getpid()
        self.io = TopicMetrics()
        self.cpu = TopicMetrics()

    def snapshot(self) -> dict:
        return {
            "worker_pid": self.worker_pid,
            "io_heavy_events": self.io.snapshot(),
            "cpu_heavy_events": self.cpu.snapshot(),
        }


metrics = MetricsRegistry()
