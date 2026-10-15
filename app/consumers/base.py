"""Shared Kafka consumer loop with backpressure and safe offset commits."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable
from typing import Any

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import ConsumerStoppedError

from app.config import settings
from app.metrics import TopicMetrics

logger = logging.getLogger(__name__)

ProcessFn = Callable[[bytes], Awaitable[Any]]


class _PartitionState:
    """In-order commit watermark for one partition."""

    def __init__(self) -> None:
        self.next_to_commit: int | None = None
        self.done: set[int] = set()
        self.inflight: set[int] = set()
        self.blocked_at: int | None = None  # failed offset; stop committing past it

    def register(self, offset: int) -> None:
        if self.next_to_commit is None:
            self.next_to_commit = offset
        self.inflight.add(offset)

    def success(self, offset: int) -> int | None:
        self.inflight.discard(offset)
        self.done.add(offset)
        return self._watermark()

    def failure(self, offset: int) -> None:
        self.inflight.discard(offset)
        self.blocked_at = offset if self.blocked_at is None else min(self.blocked_at, offset)

    def clear_block(self, offset: int) -> None:
        if self.blocked_at == offset:
            self.blocked_at = None

    def _watermark(self) -> int | None:
        if self.next_to_commit is None:
            return None
        advanced: int | None = None
        while self.next_to_commit in self.done:
            if self.blocked_at is not None and self.next_to_commit >= self.blocked_at:
                break
            self.done.discard(self.next_to_commit)
            self.next_to_commit += 1
            advanced = self.next_to_commit
        return advanced


class TopicConsumer:
    """Consumes one topic with bounded in-flight work and manual commits.

    Failure contract:
    - Offsets commit only as a contiguous successful prefix (watermark).
    - Failed messages are not committed; the consumer seeks back and retries.
    - Unprocessed messages are never treated as processed.
    """

    def __init__(
        self,
        *,
        topic: str,
        group_id: str,
        max_in_flight: int,
        process_fn: ProcessFn,
        metrics: TopicMetrics,
        name: str,
    ) -> None:
        self.topic = topic
        self.group_id = group_id
        self.max_in_flight = max_in_flight
        self.process_fn = process_fn
        self.metrics = metrics
        self.name = name

        self._consumer: AIOKafkaConsumer | None = None
        self._semaphore = asyncio.Semaphore(max_in_flight)
        self._in_flight: set[asyncio.Task] = set()
        self._stop = asyncio.Event()
        self._paused = False
        self._task: asyncio.Task | None = None
        self._state: dict[TopicPartition, _PartitionState] = defaultdict(_PartitionState)
        self._commit_lock = asyncio.Lock()

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=min(50, self.max_in_flight),
        )
        await self._consumer.start()
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name=f"{self.name}-loop")
        logger.info("%s started (topic=%s group=%s)", self.name, self.topic, self.group_id)

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=settings.shutdown_timeout_seconds)
            except asyncio.TimeoutError:
                logger.warning("%s loop did not finish in time; cancelling", self.name)
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass

        if self._in_flight:
            _done, pending = await asyncio.wait(
                self._in_flight,
                timeout=settings.shutdown_timeout_seconds,
            )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
        logger.info("%s stopped", self.name)

    async def _run(self) -> None:
        assert self._consumer is not None
        consumer = self._consumer
        try:
            while not self._stop.is_set():
                await self._maybe_pause_or_resume()
                try:
                    batch = await consumer.getmany(timeout_ms=500, max_records=self.max_in_flight)
                except ConsumerStoppedError:
                    break

                if not batch:
                    continue

                for tp, messages in batch.items():
                    state = self._state[tp]
                    for msg in messages:
                        if self._stop.is_set():
                            return
                        # Avoid piling work past a known failure until it is retried.
                        if state.blocked_at is not None and msg.offset > state.blocked_at:
                            continue
                        if msg.offset in state.inflight or msg.offset in state.done:
                            continue

                        state.register(msg.offset)
                        await self._semaphore.acquire()
                        task = asyncio.create_task(
                            self._handle_message(tp, msg),
                            name=f"{self.name}-msg-{msg.offset}",
                        )
                        self._in_flight.add(task)
                        task.add_done_callback(self._on_task_done)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("%s consumer loop crashed", self.name)
            raise

    def _on_task_done(self, task: asyncio.Task) -> None:
        self._in_flight.discard(task)
        self._semaphore.release()

    async def _maybe_pause_or_resume(self) -> None:
        assert self._consumer is not None
        consumer = self._consumer
        assigned = list(consumer.assignment())
        if not assigned:
            return

        saturated = self._semaphore._value == 0  # noqa: SLF001
        if saturated and not self._paused:
            consumer.pause(*assigned)
            self._paused = True
            logger.debug("%s paused (in_flight=%s)", self.name, len(self._in_flight))
        elif not saturated and self._paused:
            consumer.resume(*assigned)
            self._paused = False
            logger.debug("%s resumed", self.name)

    async def _handle_message(self, tp: TopicPartition, msg) -> None:
        assert self._consumer is not None
        state = self._state[tp]
        try:
            await self.process_fn(msg.value or b"{}")
        except Exception:
            async with self._commit_lock:
                state.failure(msg.offset)
            self.metrics.record_failure()
            logger.exception(
                "%s failed processing topic=%s partition=%s offset=%s",
                self.name,
                msg.topic,
                msg.partition,
                msg.offset,
            )
            await asyncio.sleep(settings.failure_backoff_seconds)
            # Redeliver the failed offset; do not commit it.
            async with self._commit_lock:
                self._consumer.seek(tp, msg.offset)
                state.clear_block(msg.offset)
                state.inflight.discard(msg.offset)
                state.done.discard(msg.offset)
            return

        async with self._commit_lock:
            commit_offset = state.success(msg.offset)
            if commit_offset is not None:
                await self._consumer.commit({tp: commit_offset})
