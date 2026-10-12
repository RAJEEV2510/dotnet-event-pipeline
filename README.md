# EventProcessor: High-Throughput .NET Event Pipeline

![build](https://github.com/RAJEEV2510/dotnet-event-pipeline/actions/workflows/build.yml/badge.svg) ![.NET 8](https://img.shields.io/badge/.NET-8-512BD4?logo=dotnet) ![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3-FF6600?logo=rabbitmq) ![Kafka](https://img.shields.io/badge/Kafka-3.8-231F20?logo=apachekafka) ![Kubernetes](https://img.shields.io/badge/Kubernetes-kind%20e2e%20in%20CI-326CE5?logo=kubernetes) ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql) ![License: MIT](https://img.shields.io/badge/License-MIT-green)

A high-throughput event ingestion and processing pipeline built with .NET 8, PostgreSQL and a pluggable broker (**RabbitMQ or Kafka**). Designed to handle **100,000+ events per minute** with at-least-once delivery guarantees, and deployable to **Kubernetes** with manifests that CI tests end to end on every push.

```
Client --> [API + Rate Limiter] --> [In-Memory Channel] --> [Outbox Writers x3]
                                                                  |
                                                            [PostgreSQL]
                                                                  |
                                                      [Outbox Processor] --> [RabbitMQ (10 channels)]
                                                                                    |
                                                                  [Consumer Workers x10] --> [Batch Processors x3]
                                                                                                    |
                                                                                              [PostgreSQL]
```

## Why This Exists

Most event processing tutorials fall apart at scale. They use single-threaded consumers, per-row INSERTs, and single RabbitMQ channels that bottleneck at ~5k events/min.

This project demonstrates a production-grade pipeline that actually works under load:

- **100k+ events/min** sustained throughput
- **Zero data loss** via the transactional outbox pattern
- **Backpressure-aware** from HTTP ingestion to final persistence
- **Horizontally scalable** with `SKIP LOCKED`-based work distribution

## Architecture

### Ingestion Path (API)

1. HTTP POST hits `EventsController` behind a fixed-window rate limiter (3,000 req/sec)
2. Events are written to a bounded `Channel<T>` (100k capacity) -- zero-allocation, lock-free
3. **3 concurrent OutboxWriterService** instances drain the channel and bulk-write to PostgreSQL using `COPY` (binary protocol), batching up to 2,000 rows per flush
4. **OutboxProcessor** polls for unprocessed rows (1,000/batch), publishes to RabbitMQ through a **pool of 10 channels**, and marks rows as processed
5. **OutboxCleanupService** deletes processed rows older than 1 minute

### Consumer Path (Worker)

1. **10 RabbitMQ consumers** with prefetch of 250 feed into a bounded `Channel<T>` (50k capacity)
2. **3 batch processors** drain the channel and bulk-insert into the `events` table using PostgreSQL `COPY` with temp-table staging for `ON CONFLICT` dedup
3. Failed messages are retried with exponential backoff, then routed to the Dead Letter Queue

### Key Design Decisions

| Decision | Why |
|----------|-----|
| Outbox pattern | Decouples HTTP response from RabbitMQ availability. Events are never lost even if the broker is down |
| `COPY` binary import | 10-50x faster than parameterized INSERTs. A batch of 2,000 rows completes in ~5ms vs ~200ms |
| `Channel<T>` | .NET's high-performance bounded queue. Lock-free reads/writes, built-in backpressure via `BoundedChannelFullMode.Wait` |
| RabbitMQ channel pool | `IModel` is not thread-safe. A pool of 10 channels allows concurrent publishes without contention |
| `FOR UPDATE SKIP LOCKED` | Multiple OutboxProcessor instances can run without row-level contention. Enables horizontal scaling |
| Temp table + `ON CONFLICT` | Worker-side dedup. `COPY` doesn't support `ON CONFLICT`, so we stage into a temp table first |

## Getting Started

### Prerequisites

- .NET 8 SDK
- Docker & Docker Compose

### 1. Start Infrastructure

```bash
docker-compose up -d
```

This starts RabbitMQ (with management UI at `localhost:15672`) and PostgreSQL on port `54321`.

### 2. Initialize Database

Docker Compose and Kubernetes both apply `db/init.sql` automatically on first start. To create the schema by hand,
connect to PostgreSQL (`localhost:54321`, user: `postgres`, password: `password`) and run:

```sql
CREATE TABLE IF NOT EXISTS events (
    event_id UUID PRIMARY KEY,
    type VARCHAR(255) NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX idx_events_created_at ON events(created_at);

CREATE TABLE IF NOT EXISTS outbox_messages (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    type VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE NULL,
    retry_count INT DEFAULT 0
);

CREATE INDEX idx_outbox_unprocessed ON outbox_messages(id ASC)
    WHERE processed_at IS NULL AND retry_count < 5;

CREATE INDEX idx_outbox_cleanup ON outbox_messages(processed_at)
    WHERE processed_at IS NOT NULL;
```

### 3. Run

```bash
# Terminal 1 - API
cd src/EventProcessor.Api
dotnet run

# Terminal 2 - Worker
cd src/EventProcessor.Worker
dotnet run
```

### 4. Send Events

```bash
curl -X POST http://localhost:5000/api/events \
  -H "Content-Type: application/json" \
  -d '{
    "eventId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
    "type": "order.created",
    "payload": "{\"orderId\": 123}",
    "createdAt": "2025-01-01T00:00:00Z"
  }'
```

## Kafka Transport

The broker is chosen by configuration; nothing else in the pipeline changes:

```json
"Messaging": { "Transport": "Kafka" }   // or "RabbitMQ" (default)
```

| Concern | RabbitMQ path | Kafka path |
|---------|---------------|------------|
| Publish | Pooled channels, persistent messages | Idempotent producer (`enable.idempotence`, `acks=all`), LZ4, 5 ms linger to batch concurrent publishes |
| Ordering / routing | Single queue | Keyed by `EventId`, 6 partitions |
| Consume | 10 consumers, prefetch 250 | One consumer per pod in group `event-processor`; Kafka splits partitions across pods |
| Delivery guarantee | Manual ack after the batch is in PostgreSQL | Manual offset commit after the batch is in PostgreSQL (auto-commit off) |
| Duplicates | `ON CONFLICT (event_id) DO NOTHING` | Same: redelivered events after a rebalance are absorbed by the database |
| Poison / failed batches | Dead-letter exchange `events_dlx` | Re-produced to the `events-dlq` topic, then offsets committed |
| Scaling | More consumers per process | More worker pods, up to the partition count |

Topics are created on startup by `KafkaTopicInitializer` (retrying while the broker boots). The relevant code is in
`Infrastructure/Messaging/KafkaPublisher.cs` and `Worker/KafkaConsumerWorker.cs`.

Run it locally with the `kafka` Compose profile:

```bash
docker-compose --profile kafka up -d
# then run the API and worker with Messaging__Transport=Kafka
```

## Running on Kubernetes

`k8s/` holds plain manifests, tied together by the root `kustomization.yaml`:

| Manifest | What it deploys |
|----------|-----------------|
| `postgres.yaml` | PostgreSQL 15 StatefulSet with a PVC; schema from `db/init.sql` via a generated ConfigMap |
| `kafka.yaml` | Single-node Kafka 3.8 in KRaft mode (no ZooKeeper) as a StatefulSet |
| `app-config.yaml` | Shared settings (ConfigMap) and the connection string (Secret), injected as `Section__Key` env vars |
| `api.yaml` | API Deployment with `/healthz` readiness/liveness probes, Service and CPU-based HPA (1-4 pods) |
| `worker.yaml` | Worker Deployment (2 pods in one consumer group) and HPA capped at 6, the partition count |

```bash
docker build -f src/EventProcessor.Api/Dockerfile -t event-pipeline-api:local .
docker build -f src/EventProcessor.Worker/Dockerfile -t event-pipeline-worker:local .
kind create cluster && kind load docker-image event-pipeline-api:local event-pipeline-worker:local
kubectl apply -k .
kubectl -n event-pipeline port-forward svc/api 8080:80
```

The `kubernetes` job in `.github/workflows/build.yml` does exactly this on every push: it builds both images,
deploys everything to a throwaway kind cluster, posts 500 events through the API, waits until all 500 are in
PostgreSQL, and prints how the consumer group split the partitions between the worker pods.

The single Kafka broker and in-cluster Secret values are for local clusters and CI. Production would use a
3-broker cluster or a managed service (Confluent Cloud, Amazon MSK, Azure Event Hubs for Kafka) and an external secret store.

## Performance Characteristics

| Component | Throughput | Bottleneck |
|-----------|-----------|------------|
| API ingestion | ~50,000/sec | CPU (serialization) |
| Outbox writes (3 writers) | ~30,000/sec | PostgreSQL disk I/O |
| Outbox to RabbitMQ (10 channels) | ~10,000/sec | Network + broker |
| Worker consume + persist | ~15,000/sec | PostgreSQL disk I/O |
| **End-to-end pipeline** | **~1,700+/sec (100k+/min)** | Outbox processor drain rate |

Tested on a single machine. Horizontally scalable by running multiple API/Worker instances behind a load balancer.

## Production Hardening

- **At-least-once delivery** -- outbox pattern guarantees no events are lost, even during RabbitMQ outages
- **Backpressure** -- bounded channels reject with HTTP 503 when the system is saturated, preventing OOM
- **Idempotent consumers** -- `ON CONFLICT (event_id) DO NOTHING` ensures duplicate processing is safe
- **Dead Letter Queue** -- messages that fail after 3 retries are routed to `events_dlq` for manual inspection
- **Graceful shutdown** -- all in-memory buffers are drained to PostgreSQL before the process exits
- **Structured logging** -- Serilog with batch timing (`BATCH_COMPLETE: 2000 events in 8ms`)
- **Rate limiting** -- fixed-window limiter protects the API from burst traffic
- **Retry with backoff** -- exponential backoff on transient failures (2s, 4s, 8s)

## Project Structure

```
src/
  EventProcessor.Api/             # HTTP API + background services
    Controllers/
      EventsController.cs         # POST /api/events endpoint
    BackgroundServices/
      OutboxQueue.cs              # Bounded Channel<T> (100k capacity)
      OutboxWriterService.cs      # 3 concurrent writers, COPY bulk insert
      OutboxProcessor.cs          # Polls outbox, publishes to RabbitMQ
      OutboxCleanupService.cs     # Deletes processed rows
  EventProcessor.Worker/          # RabbitMQ consumer service
    EventConsumerWorker.cs        # 10 consumers, 3 batch processors
  EventProcessor.Domain/          # Domain entities
  EventProcessor.Application/     # Interfaces, DTOs, validators
  EventProcessor.Infrastructure/  # PostgreSQL + RabbitMQ implementations
    Persistence/
      PostgreSQLEventRepository.cs      # COPY via NpgsqlBinaryImporter
      PostgreSQLOutboxRepository.cs     # COPY + bulk operations
    Messaging/
      RabbitMQPublisher.cs              # Channel pool (10 channels)
      KafkaPublisher.cs                 # Idempotent producer, keyed by EventId
      KafkaTopicInitializer.cs          # Creates topics on startup
  EventProcessor.Worker/
    KafkaConsumerWorker.cs        # Consumer group member, batch insert, manual offset commit
k8s/                              # Kubernetes manifests (see "Running on Kubernetes")
db/init.sql                       # Schema, used by Compose and Kubernetes
kustomization.yaml                # kubectl apply -k .
```

## Tech Stack

- **.NET 8** -- async/await, `System.Threading.Channels`, `Parallel.ForEachAsync`
- **PostgreSQL 15** -- `COPY` binary protocol, partial indexes, `SKIP LOCKED`
- **RabbitMQ 3** -- durable queues, manual acks, dead letter exchange
- **Kafka 3.8 (KRaft)** -- idempotent producer, consumer groups, manual offset commits, DLQ topic (Confluent.Kafka client)
- **Kubernetes** -- StatefulSets, Deployments, HPA, probes, ConfigMap/Secret, Kustomize; tested on kind in GitHub Actions
- **Npgsql** -- `NpgsqlBinaryImporter` for high-speed bulk inserts
- **Dapper** -- lightweight ORM for read queries
- **Polly** -- retry policies with exponential backoff
- **FluentValidation** -- request validation
- **Serilog** -- structured logging

## License

MIT
