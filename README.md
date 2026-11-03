# FairDrop

FairDrop is a high-concurrency backend for fair allocation of limited inventory during burst-demand drops. Users join a FIFO queue, and the service allocates inventory in queue order while preventing duplicate joins and overselling under concurrent requests.

## Live API

**Live Swagger API:**  
https://fairdrop-dq3n.onrender.com

**GitHub Repository:**  
https://github.com/nihalritgithub/fairdrop

Available endpoints:

- `/` → redirects to Swagger UI
- `/swagger-ui.html` → interactive API documentation
- `/v3/api-docs` → OpenAPI JSON
- `/actuator/health` → application health check
- `/actuator/prometheus` → Prometheus metrics

> The public API runs on Render's free tier and may require a cold start after a period of inactivity.

## Architecture

```text
Client
  |
  v
Spring Boot API
  |
  |-- Redis
  |     |-- per-user join rate limiting
  |     |     `-- atomic Lua script
  |     |
  |     `-- cache-aside Drop reads
  |
  |-- PostgreSQL (source of truth)
  |     |-- drops
  |     |-- drop_sequences
  |     |-- queue_entries
  |     |-- allocations
  |     |-- outbox_events
  |     |-- processed_events
  |     `-- allocation audit log
  |
  `-- Transactional Outbox
          |
          v
        Kafka
          |
          v
   Idempotent Consumer
```

### Public deployment

```text
GitHub
  |
  v
Render
Spring Boot / Java 21
  |
  |-- Neon PostgreSQL
  |
  `-- Redis Cloud
```

Kafka and the transactional outbox consumer flow are implemented in the repository and covered by automated tests. Kafka publishing and consumption are intentionally disabled in the free public deployment because no long-term Kafka infrastructure is required for the interactive demo.

## Engineering guarantees

- FIFO queue positions are generated safely under concurrent joins.
- The same user joining the same drop is idempotent.
- Allocation uses PostgreSQL pessimistic locking to prevent overselling.
- Clients cannot choose winners; the server selects the earliest eligible queue entry.
- Per-user join rate limiting uses an atomic Redis Lua script.
- Redis rate limiting fails closed with HTTP `503` if the limiter cannot be consulted.
- Redis caching fails open to PostgreSQL and never participates in allocation correctness.
- Allocation and outbox event creation occur atomically in the same database transaction.
- Kafka delivery follows at-least-once semantics.
- Consumers de-duplicate events using a persistent event ID ledger.
- Consumer failures use bounded retries and a dead-letter topic.
- Flyway owns database schema evolution.
- Hibernate validates the schema rather than modifying it.
- Micrometer/Prometheus metrics provide operational visibility.
- Correlation IDs propagate through HTTP requests and asynchronous events.

## Core API

```text
POST /drops
GET  /drops/{dropId}
POST /drops/{dropId}/join?userId=...
GET  /drops/{dropId}/queue/{userId}
POST /drops/{dropId}/allocate-next
```

### Example flow

```text
Create Drop
   |
   v
Users join FIFO queue
   |
   v
USER-A -> sequence 1
USER-B -> sequence 2
USER-C -> sequence 3
   |
   v
allocate-next
   |
   +--> USER-A
   |
   +--> USER-B
   |
   `--> SOLD_OUT
```

For a drop with inventory `2`, the third allocation attempt returns `SOLD_OUT`, preventing overselling.

## Technology stack

- Java 21
- Spring Boot 4
- Spring MVC
- Spring Data JPA
- Hibernate
- PostgreSQL
- Redis
- Kafka
- Flyway
- Micrometer
- Prometheus
- Swagger / OpenAPI
- Maven
- Docker
- JUnit
- Mockito

## Local development

### Requirements

- Java 21
- PostgreSQL
- Redis-compatible server such as Redis, Valkey, or Memurai
- Kafka-compatible broker for the complete asynchronous flow

Set the PostgreSQL password:

```cmd
set FAIRDROP_DB_PASSWORD=postgres
```

Run the test suite:

```cmd
mvnw.cmd test
```

Start the application:

```cmd
mvnw.cmd spring-boot:run
```

Swagger UI is available at:

```text
http://localhost:8080/
```

For local development without Kafka, the outbox publisher and Kafka listener can be disabled through environment configuration while PostgreSQL and Redis functionality continue to operate normally.

## Database migrations

Schema evolution is managed entirely through Flyway.

Current migrations:

```text
V1__create_initial_schema.sql
V2__create_outbox_events.sql
V3__create_processed_events_and_allocation_audit.sql
V4__add_drop_allocation_bounds_check.sql
```

Hibernate is configured with:

```text
ddl-auto=validate
```

so application startup validates the schema rather than modifying it automatically.

## Concurrency model

Queue joining and allocation are protected at the database level.

Queue sequence generation uses a per-drop sequence row with pessimistic locking.

Allocation locks the corresponding Drop row before reading and modifying inventory:

```text
Request A ─┐
Request B ─┼──> Lock Drop row
Request C ─┘
                 |
                 v
          Check inventory
                 |
                 v
       Select earliest eligible user
                 |
                 v
          Persist allocation
                 |
                 v
       Increment allocated count
```

This keeps allocation correctness independent of application-instance thread scheduling.

## Redis traffic protection

Join requests are rate-limited per:

```text
(dropId, userId)
```

using an atomic Redis Lua script.

The default policy allows:

```text
5 join requests / minute
```

The sixth request inside the window returns:

```text
HTTP 429 RATE_LIMIT_EXCEEDED
```

Redis is also used as a cache-aside layer for Drop reads. PostgreSQL remains the source of truth.

## Transactional outbox

When an allocation succeeds:

```text
Database Transaction
   |
   |-- save allocation
   |
   |-- increment inventory counter
   |
   `-- save outbox event
```

Only after the transaction commits does the outbox relay publish the event to Kafka.

This avoids the classic dual-write problem where the database succeeds but event publication fails.

Kafka delivery is at-least-once, so consumers use a persistent `processed_events` ledger to make event handling idempotent.

## Observability

FairDrop includes:

- correlation IDs
- allocation outcome metrics
- allocation latency metrics
- Redis cache hit/miss metrics
- rate-limit metrics
- outbox backlog metrics
- Kafka consumer outcome metrics
- Prometheus endpoint
- Spring Boot Actuator health endpoint

Health endpoint:

```text
https://fairdrop-dq3n.onrender.com/actuator/health
```

## Load testing

A bounded JDK-only load generator is included in:

```text
load-test/FairDropLoadTest.java
```

Example scenarios:

```cmd
java load-test/FairDropLoadTest.java --scenario join --requests 2000 --concurrency 50

java load-test/FairDropLoadTest.java --scenario read --requests 2000 --concurrency 50

java load-test/FairDropLoadTest.java --scenario allocate --requests 500 --concurrency 50 --inventory 100 --users 200
```

Supported scenarios include:

```text
join
join-rate-limit
read
allocate
```

The allocation scenario validates the final allocated count and checks for duplicate winners in addition to reporting throughput and latency percentiles.

Measured performance results will be added after running the load tests against the intended test environment.

## Automated testing

The project includes unit, integration, concurrency, Redis, outbox, Kafka, controller, and observability tests.

The test suite covers areas including:

- concurrent queue joins
- concurrent allocation
- overselling prevention
- idempotent joins
- Redis rate limiting
- Redis failure handling
- cache behavior
- transactional outbox behavior
- Kafka delivery
- duplicate event processing
- dead-letter handling
- HTTP flows
- application metrics

## Deployment

The public demo currently uses:

```text
Application     -> Render
PostgreSQL      -> Neon
Redis           -> Redis Cloud
Source control  -> GitHub
```

The application is packaged with Docker and deployed directly from the `main` branch.

Important environment variables include:

```text
FAIRDROP_DB_URL
FAIRDROP_DB_USERNAME
FAIRDROP_DB_PASSWORD
FAIRDROP_DB_POOL_SIZE

SPRING_DATA_REDIS_HOST
SPRING_DATA_REDIS_PORT
SPRING_DATA_REDIS_USERNAME
SPRING_DATA_REDIS_PASSWORD

FAIRDROP_OUTBOX_PUBLISHER_ENABLED
SPRING_KAFKA_LISTENER_AUTO_STARTUP
FAIRDROP_KAFKA_AUTO_CREATE_TOPICS
```

Real credentials must never be committed to the repository.

See `.env.example` for configuration examples.
