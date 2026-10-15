# High-Performance Multi-Worker Kafka Consumer

FastAPI service that consumes two Kafka topics with **5 uvicorn workers**, keeping I/O processing non-blocking under CPU load.

| Topic | Workload | How it runs |
|-------|----------|-------------|
| `io-heavy-events` | ~100 ms simulated I/O | `asyncio.sleep` (does not block the event loop) |
| `cpu-heavy-events` | CPU-bound hashing | `ProcessPoolExecutor` via `run_in_executor` |

## Failure and shutdown behavior

| Event | Behavior |
|-------|----------|
| Processing failure | Offset is **not** committed. Failure metric increments. Message stays eligible for redelivery. |
| Concurrent success | Commits advance only as a **contiguous watermark** per partition (out-of-order completion cannot skip failed/unprocessed offsets). |
| Worker stop | Consumer loop stops fetching; in-flight tasks drain (with timeout); only successfully processed contiguous offsets are committed; Kafka reassigns partitions to remaining workers. |
| Backpressure | Per-worker semaphore limits in-flight messages; consumer **pauses** when saturated and **resumes** when capacity frees — no unbounded pending queue. |

## Prerequisites

- Python 3.12+
- Docker / Docker Compose

## Setup

```bash
# 1. Start Kafka (KRaft) and create 5-partition topics
docker compose up -d

# 2. Install dependencies
python -m venv .venv

# Windows
.venv\Scripts\activate
# Linux/macOS
# source .venv/bin/activate

pip install -r requirements.txt
```

## Run with 5 workers

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 5
```

Each worker process starts both consumers and joins groups `io-workers` / `cpu-workers`.

## Produce and verify

```bash
# Produce 100 messages to both topics
python scripts/produce_messages.py --topic both --count 100

# Health / metrics (one worker answers each request)
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/metrics
```

Sustained load helper:

```bash
python scripts/load_test.py --io-count 500 --cpu-count 50
```

## Configuration

Environment variables (defaults in `app/config.py`):

- `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`)
- `IO_MAX_IN_FLIGHT` (default `50`)
- `CPU_MAX_IN_FLIGHT` (default `4`)
- `IO_LATENCY_SECONDS` (default `0.1`)
- `CPU_WORK_ITERATIONS` (default `200000`)
- `PROCESS_POOL_WORKERS` (default `2`)

## Load report

See [docs/load_report.md](docs/load_report.md) for throughput notes and CPU-vs-I/O isolation observations.
