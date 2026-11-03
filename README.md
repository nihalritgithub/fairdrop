# FairDrop

FairDrop is a high-concurrency backend for fair allocation of limited inventory during burst-demand drops. Users join a FIFO queue, and the service allocates inventory in queue order while preventing duplicate joins and overselling under concurrent requests.

## Live API

After deployment, the service root redirects to Swagger UI:

- `/` -> Swagger UI
- `/swagger-ui.html` -> interactive API documentation
- `/v3/api-docs` -> OpenAPI JSON
- `/actuator/health` -> health check
- `/actuator/prometheus` -> Prometheus metrics

## Architecture

```text
Client
  |
  v
Spring Boot API
  |-- Redis / Valkey
  |     |-- per-user join rate limiting (atomic Lua script)
  |     `-- cache-aside Drop reads
  |
  |-- PostgreSQL (source of truth)
  |     |-- drops
  |     |-- queue_entries
  |     |-- allocations
  |     |-- outbox_events
  |     `-- processed_events / audit log
  |
  `-- Transactional Outbox
          |
          v
        Kafka
          |
          v
   Idempotent consumer
```

## Engineering guarantees

- FIFO queue positions are generated safely under concurrent joins.
- Same user joining the same drop is idempotent.
- Allocation uses PostgreSQL pessimistic locking and cannot oversell inventory.
- Client does not choose the winner; the earliest unallocated queue entry wins.
- Redis rate limiting fails closed with HTTP 503 if the limiter cannot be consulted.
- Redis cache fails open to PostgreSQL and never participates in allocation correctness.
- Allocation and outbox event are committed atomically in the same database transaction.
- Kafka delivery is at-least-once; consumers de-duplicate by event ID.
- Failed consumer messages use bounded retries and a dead-letter topic.
- Flyway owns schema evolution; Hibernate validates the schema.
- Micrometer/Prometheus metrics and correlation IDs provide observability.

## Core endpoints

```text
POST /drops
GET  /drops/{dropId}
POST /drops/{dropId}/join?userId=...
GET  /drops/{dropId}/queue/{userId}
POST /drops/{dropId}/allocate-next
```

## Local requirements

- Java 21
- PostgreSQL
- Redis-compatible server (Redis, Valkey, or Memurai)
- Kafka-compatible broker for the full asynchronous flow

Set the database password in the current shell, then run:

```cmd
set FAIRDROP_DB_PASSWORD=postgres
mvnw.cmd test
mvnw.cmd spring-boot:run
```

Swagger UI is then available at `http://localhost:8080/`.

For local development without Kafka, the outbox publisher and listener can be disabled through environment configuration while the PostgreSQL/Redis paths are exercised independently.

## Load test

A bounded JDK-only load generator is included:

```cmd
java load-test/FairDropLoadTest.java --scenario join --requests 2000 --concurrency 50
java load-test/FairDropLoadTest.java --scenario read --requests 2000 --concurrency 50
java load-test/FairDropLoadTest.java --scenario allocate --requests 500 --concurrency 50 --inventory 100 --users 200
```

The allocation scenario checks the final allocated count and duplicate winners in addition to reporting throughput and latency percentiles.

## Production configuration

Activate the production profile with:

```text
SPRING_PROFILES_ACTIVE=prod
```

See `.env.example` for the required PostgreSQL, Redis, and Kafka environment variables. Do not commit real credentials.
