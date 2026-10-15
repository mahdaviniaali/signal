from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor

from app.config import settings
from app.consumers.base import TopicConsumer
from app.metrics import metrics
from app.processing import process_cpu_message


class CpuConsumer(TopicConsumer):
    def __init__(self, executor: ProcessPoolExecutor) -> None:
        async def _process(payload: bytes):
            return await process_cpu_message(payload, executor)

        super().__init__(
            topic=settings.cpu_topic,
            group_id=settings.cpu_group_id,
            max_in_flight=settings.cpu_max_in_flight,
            process_fn=_process,
            metrics=metrics.cpu,
            name="cpu-consumer",
        )
