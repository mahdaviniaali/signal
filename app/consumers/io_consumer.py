from app.config import settings
from app.consumers.base import TopicConsumer
from app.metrics import metrics
from app.processing import process_io_message


class IoConsumer(TopicConsumer):
    def __init__(self) -> None:
        super().__init__(
            topic=settings.io_topic,
            group_id=settings.io_group_id,
            max_in_flight=settings.io_max_in_flight,
            process_fn=process_io_message,
            metrics=metrics.io,
            name="io-consumer",
        )
