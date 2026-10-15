# Load Report: Multi-Worker Kafka Consumer

## Setup

- Kafka: Docker Compose (KRaft), topics `io-heavy-events` and `cpu-heavy-events` with **5 partitions** each
- Service: `uvicorn app.main:app --workers 5`
- I/O work: ~100 ms `asyncio.sleep` per message (`IO_MAX_IN_FLIGHT=50`)
- CPU work: SHA-256 loop in `ProcessPoolExecutor` (`CPU_MAX_IN_FLIGHT=4`, `PROCESS_POOL_WORKERS=2`)

## How to reproduce

```bash
docker compose up -d
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 5
python scripts/produce_messages.py --topic both --count 150
python scripts/check_metrics.py http://127.0.0.1:8000/metrics 50
python scripts/load_test.py --io-count 500 --cpu-count 50 --wait-seconds 90
```

`GET /metrics` is per-worker (each HTTP request hits one uvicorn process). Use `scripts/check_metrics.py` to sample multiple workers and sum counts.

## Measured results (5 workers, this machine)

After producing 150 messages to each topic (new consumer groups; topic also contained earlier backlog):

| Metric | Observed |
|--------|----------|
| Workers reporting metrics | **5** |
| I/O processed (sum across workers) | **600** (includes prior backlog replay for a fresh group with `auto_offset_reset=earliest`) |
| CPU processed (sum across workers) | **320** |
| Processing failures | **0** |
| Peak per-worker I/O rate | **~130–180 msg/s** on workers that received a tight burst |
| Sustained per-worker I/O rate | **~8–27 msg/s** over a ~10–13 s window |
| Per-worker CPU rate | **~3–6 msg/s** with `CPU_WORK_ITERATIONS≈80k` |

Exact rates depend on CPU cores, Docker overhead, and partition assignment.

## Impact of CPU load on I/O processing

**Observation:** I/O counters kept increasing while CPU messages were processed in the same worker processes (both `io_heavy_events.processed` and `cpu_heavy_events.processed` rose together; failures stayed at 0).

**Why:** CPU-bound work runs in `ProcessPoolExecutor` via `run_in_executor`. The asyncio event loop stays free to await I/O sleeps, poll Kafka, and commit offsets. A tight CPU loop on the event loop would stall I/O inside that worker.

## Backpressure under high load

When in-flight work reaches `*_MAX_IN_FLIGHT`, the consumer **pauses** partition fetch and **resumes** when slots free. This caps pending work per worker instead of growing an unbounded asyncio task queue.

## Failure and worker stop

| Event | Behavior |
|-------|----------|
| Processing exception | Offset **not** committed; consumer seeks to the failed offset; `failed` metric increments |
| Commit safety | Contiguous watermark per partition — out-of-order completion cannot skip unprocessed offsets |
| Worker shutdown | Fetch loop stops; in-flight tasks drain within `SHUTDOWN_TIMEOUT_SECONDS`; consumer closes; Kafka rebalances to remaining group members |

## Summary

| Criterion | Result |
|-----------|--------|
| Both topics processed | Yes — independent groups `io-workers` / `cpu-workers` |
| I/O responsive under CPU load | Yes — process-pool isolation |
| Stable with 5 workers | Yes — 5-partition topics + consumer groups + bounded in-flight |
| Failures / shutdown | Defined — commit-after-success watermark, seek-on-failure, graceful drain |
