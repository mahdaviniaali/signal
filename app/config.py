from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    io_topic: str = "io-heavy-events"
    cpu_topic: str = "cpu-heavy-events"
    io_group_id: str = "io-workers"
    cpu_group_id: str = "cpu-workers"

    # Bound concurrent in-flight messages per consumer (per worker process).
    io_max_in_flight: int = 50
    cpu_max_in_flight: int = 4

    # Simulated I/O latency (seconds).
    io_latency_seconds: float = 0.1

    # CPU work intensity (iterations of hashing).
    cpu_work_iterations: int = 200_000

    process_pool_workers: int = 2
    shutdown_timeout_seconds: float = 30.0
    failure_backoff_seconds: float = 0.5

    # Shared SQLite file for idempotent order/event deduplication across workers.
    idempotency_db_path: str = "data/processed_events.db"


settings = Settings()
