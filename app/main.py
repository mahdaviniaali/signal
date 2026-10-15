"""FastAPI service that runs Kafka consumers in each uvicorn worker process."""

from __future__ import annotations

import logging
from concurrent.futures import ProcessPoolExecutor
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from app.config import settings
from app.consumers import CpuConsumer, IoConsumer
from app.metrics import metrics
from app.store import store

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    executor = ProcessPoolExecutor(max_workers=settings.process_pool_workers)
    io_consumer = IoConsumer()
    cpu_consumer = CpuConsumer(executor)

    app.state.executor = executor
    app.state.io_consumer = io_consumer
    app.state.cpu_consumer = cpu_consumer

    try:
        await io_consumer.start()
        await cpu_consumer.start()
        logger.info("Consumers started in worker pid=%s", metrics.worker_pid)
        yield
    finally:
        logger.info("Shutting down consumers in worker pid=%s", metrics.worker_pid)
        await io_consumer.stop()
        await cpu_consumer.stop()
        executor.shutdown(wait=True, cancel_futures=True)


app = FastAPI(
    title="High-Performance Multi-Worker Kafka Consumer",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    return {"status": "ok", "worker_pid": metrics.worker_pid}


@app.get("/metrics")
async def get_metrics():
    return metrics.snapshot()


@app.get("/events/{topic}/{event_id}")
async def get_event(topic: str, event_id: str):
    """Look up a saved order/event (for verifying idempotent storage)."""
    row = store.get(topic, event_id)
    if row is None:
        raise HTTPException(status_code=404, detail="event not found")
    return row


@app.get("/events/count")
async def events_count(topic: str | None = None):
    return {"topic": topic, "count": store.count(topic)}
