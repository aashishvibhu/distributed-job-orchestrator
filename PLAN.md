# Distributed Task/Workflow Engine — Project Plan

> **Goal**: Build a mini orchestration service that accepts async jobs via REST, publishes to Kafka, processes them idempotently, and tracks state in PostgreSQL. Demonstrates Kafka expertise, async architecture, Saga/Outbox patterns, retry + DLQ handling, and distributed systems design.

---

## 🧭 Architecture Overview

```
┌─────────┐    REST (POST /orders)   ┌──────────────────┐    Outbox Pattern    ┌──────────────┐
│  Client  │ ───────────────────────▶│  Job Orchestrator │ ──────────────────▶ │  PostgreSQL   │
└─────────┘                          │  (Spring Boot)    │                     │  (orders,     │
                                     │  port: 8080       │                     │   outbox,     │
                                     └────────┬─────────┘                     │   saga_state) │
                                              │                                └──────┬───────┘
                                     ┌────────▼─────────┐                    ┌──────▼──────────┐
                                     │   Kafka Broker    │                    │  Outbox Poller  │
                                     │   (KRaft mode)    │◀───────────────────│  (@Scheduled)   │
                                     └──┬──────────┬─────┘                    └─────────────────┘
                                        │          │
                           ┌────────────▼──┐  ┌────▼──────────────┐
                           │ order.created │  │ payment.processed │
                           │   Consumer    │  │    Consumer       │
                           │ (idempotent)  │  │  (idempotent)     │
                           └──────┬────────┘  └────────┬──────────┘
                                  │                    │
                                  └─────────┬──────────┘
                                            │
                                   ┌────────▼────────┐
                                   │ Saga Coordinator│
                                   │  (orchestrates  │
                                   │   rollbacks)    │
                                   └────────┬────────┘
                                            │
                                   ┌────────▼────────┐
                                   │    PostgreSQL    │
                                   │  (saga_state)    │
                                   └─────────────────┘
```

### End-to-End Flow

1. **POST /api/v1/orders** → `OrderService` writes Order + OutboxEvent in one database transaction
2. **OutboxPoller** (`@Scheduled 1s`) reads PENDING events, publishes to Kafka, marks PUBLISHED
3. **OrderCreatedConsumer** picks `order.created`, validates, triggers `OrderSagaCoordinator.handleOrderCreated()`
4. **SagaCoordinator** runs steps: ReserveInventory → ProcessPayment → SendNotification
5. Each step publishes to next topic → next consumer picks up → saga progresses
6. **On failure**: SagaCoordinator walks backward through completed steps calling `.compensate()`
7. **DLQ**: After 3 retries, `DeadLetterPublishingRecoverer` sends to `.DLT` topic
8. **GET /api/v1/orders/{id}/saga-status** → full saga state with step-by-step timeline

---

## 🗂️ Project Structure

```
distributed-workflow-engine/
├── pom.xml
├── docker-compose.yml
├── README.md
├── docs/
│   └── DESIGN_DECISIONS.md
│
├── src/main/java/com/workflow/engine/
│   ├── WorkflowEngineApplication.java
│   │
│   ├── api/
│   │   ├── controller/
│   │   │   ├── OrderController.java           # POST /orders, GET /orders/{id}, saga-status
│   │   │   └── AdminController.java           # DLQ replay, simulate failures
│   │   └── dto/
│   │       ├── CreateOrderRequest.java
│   │       ├── OrderStatusResponse.java
│   │       └── ApiError.java
│   │
│   ├── domain/
│   │   ├── entity/
│   │   │   ├── Order.java                     # JPA entity
│   │   │   ├── OutboxEvent.java               # Outbox table entity
│   │   │   ├── SagaState.java                 # Saga tracking entity
│   │   │   └── ProcessedEvent.java            # Idempotency dedup table
│   │   ├── enums/
│   │   │   ├── OrderStatus.java
│   │   │   ├── SagaStatus.java
│   │   │   ├── OutboxStatus.java
│   │   │   └── EventType.java
│   │   └── repository/
│   │       ├── OrderRepository.java
│   │       ├── OutboxEventRepository.java     # PESSIMISTIC_WRITE lock for polling
│   │       ├── SagaStateRepository.java
│   │       └── ProcessedEventRepository.java
│   │
│   ├── messaging/
│   │   ├── producer/
│   │   │   ├── KafkaEventPublisher.java       # Async publish with callback logging
│   │   │   └── OutboxPoller.java              # @Scheduled poller
│   │   ├── consumer/
│   │   │   ├── OrderCreatedConsumer.java      # Manual ack, idempotency guard
│   │   │   ├── InventoryReservedConsumer.java
│   │   │   ├── PaymentProcessedConsumer.java
│   │   │   └── DlqMonitor.java                # Logs DLT messages
│   │   ├── event/
│   │   │   ├── OrderCreatedEvent.java
│   │   │   ├── PaymentProcessedEvent.java
│   │   │   ├── InventoryReservedEvent.java
│   │   │   └── NotificationSentEvent.java
│   │   └── config/
│   │       ├── KafkaProducerConfig.java       # idempotent + transactional
│   │       ├── KafkaConsumerConfig.java       # read_committed, manual ack, error handler
│   │       └── KafkaTopicConfig.java          # Topic bean definitions
│   │
│   ├── saga/
│   │   ├── OrderSagaCoordinator.java          # Orchestration-based saga
│   │   ├── SagaContext.java                   # Mutable context passed through steps
│   │   ├── SagaStep.java                      # execute() + compensate() interface
│   │   ├── step/
│   │   │   ├── CreateOrderStep.java
│   │   │   ├── ReserveInventoryStep.java
│   │   │   ├── ProcessPaymentStep.java        # 90% success / 10% failure simulation
│   │   │   └── SendNotificationStep.java
│   │   └── compensating/
│   │       ├── CancelOrderStep.java
│   │       ├── ReleaseInventoryStep.java
│   │       └── RefundPaymentStep.java
│   │
│   ├── service/
│   │   ├── OrderService.java                  # Outbox Pattern — atomic writes
│   │   └── IdempotencyService.java            # DB unique constraint dedup
│   │
│   ├── common/
│   │   ├── exception/
│   │   │   ├── DuplicateEventException.java
│   │   │   ├── RetryableException.java
│   │   │   ├── InvalidPayloadException.java   # Non-retryable → DLT immediately
│   │   │   └── SagaStepFailedException.java
│   │   └── util/
│   │       ├── CorrelationIdUtil.java         # MDC-based tracing
│   │       ├── JsonUtil.java
│   │       └── CorrelationIdFilter.java       # Servlet filter for HTTP headers
│   │
│   └── monitoring/
│       └── WorkflowMetrics.java               # Micrometer counters/gauges
│
├── src/main/resources/
│   ├── application.yml
│   ├── application-docker.yml
│   └── db/migration/
│       ├── V1__create_orders_table.sql
│       ├── V2__create_outbox_table.sql
│       ├── V3__create_saga_state_table.sql
│       └── V4__create_processed_events_table.sql
│
└── src/test/java/com/workflow/engine/
    ├── integration/
    │   └── OrderSagaIntegrationTest.java      # Testcontainers: Kafka + PostgreSQL
    ├── unit/
    │   ├── OrderSagaCoordinatorTest.java       # Mockito: happy path + compensation
    │   └── IdempotencyServiceTest.java
    └── contract/
        └── OrderControllerTest.java
```

---

## 🔑 Patterns Demonstrated

| Pattern | Implementation | Key Design Decision |
|:---|:---|:---|
| **Outbox Pattern** | `OrderService` writes Order + OutboxEvent + SagaState in single `@Transactional`; `OutboxPoller` publishes to Kafka | Chose polling (`@Scheduled`) over CDC (Debezium) — simpler setup, pattern identical |
| **Saga (Orchestration)** | `OrderSagaCoordinator` executes steps sequentially, persists state after each step, walks backward calling `compensate()` on failure | Chose orchestration over choreography — centralized rollback logic, easier to test |
| **Idempotent Consumer** | `IdempotencyService` uses `processed_events` table with UNIQUE constraint on `message_id` | Combined with Kafka `read_committed` + idempotent producer = effectively exactly-once |
| **DLQ with Retry** | `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` — 3 retries with 2s fixed backoff, then DLT | `InvalidPayloadException` skips retry → DLT immediately (non-retryable) |
| **Correlation ID Tracing** | `CorrelationIdFilter` extracts/generates `X-Correlation-Id` → MDC → all log lines + Kafka headers | End-to-end traceability across REST → Kafka → DB |

---

## 📋 Implementation Phases

### Phase 1: Scaffold & Infrastructure (Day 1 — ~4 hours)

- Initialize Spring Boot 3.3+ project (Maven, Java 17)
- Dependencies: Spring Web, Spring Data JPA, Spring Kafka, Flyway, PostgreSQL Driver, Lombok, Actuator, Testcontainers, Validation, Awaitility, AssertJ
- Write `docker-compose.yml`: PostgreSQL 16, Kafka 3.7 (KRaft mode), Kafka UI
- Configure `application.yml`: Kafka producer (idempotent, `acks=all`), consumer (`read_committed`, manual ack), Flyway
- Create `KafkaTopicConfig` bean: 5 topics, 3 partitions each
- Smoke test: health endpoint green, Kafka UI visible

### Phase 2: Core Domain & Outbox Pattern (Day 2 — ~5 hours)

- Write 4 Flyway migrations (orders, outbox_events, saga_states, processed_events)
- Create 4 JPA entities with `@PrePersist`/`@PreUpdate` hooks
- Create 4 enums (OrderStatus, SagaStatus, OutboxStatus, EventType)
- Create 4 Spring Data repositories (OutboxEventRepository uses `@Lock(PESSIMISTIC_WRITE)`)
- Implement utility classes: `CorrelationIdUtil` (MDC), `JsonUtil` (Jackson)
- Implement `KafkaProducerConfig` + `KafkaEventPublisher` (async with callback)
- Implement `OrderService.createOrder()` — Outbox Pattern: Order + OutboxEvent + SagaState in one transaction
- Implement `OutboxPoller` — `@Scheduled` every 1s, reads PENDING events, publishes, marks PUBLISHED
- Create DTOs: `CreateOrderRequest` (with `@Valid`), `OrderStatusResponse`, `ApiError`
- Implement `OrderController`: POST /orders, GET /orders/{id}, GET /orders/{id}/saga-status
- Verify: POST order → DB writes + Kafka message visible in Kafka UI

### Phase 3: Kafka Consumers + Idempotency (Day 3 — ~5 hours)

- Create 4 event POJOs (OrderCreatedEvent, InventoryReservedEvent, PaymentProcessedEvent, NotificationSentEvent)
- Implement `IdempotencyService`: `isDuplicate()` + `markProcessed()` using DB UNIQUE constraint
- Create 4 exception classes: DuplicateEventException, RetryableException, InvalidPayloadException, SagaStepFailedException
- Implement `KafkaConsumerConfig`: `DefaultErrorHandler` with `DeadLetterPublishingRecoverer` (3 retries × 2s), `InvalidPayloadException` → no retry
- Implement 3 consumers: OrderCreatedConsumer, InventoryReservedConsumer, PaymentProcessedConsumer — each with idempotency check, manual ack, error handling
- Create stub `OrderSagaCoordinator` for end-to-end verification
- Verify: consumer picks up message, `processed_events` table populated, duplicates skipped

### Phase 4: Saga Orchestration (Day 4–5 — ~8 hours)

- Define `SagaContext` (mutable context with orderId, reservationId, transactionId, paymentSuccess)
- Define `SagaStep` interface: `getStepName()`, `execute(context)`, `compensate(context)`
- Implement 4 forward steps: CreateOrderStep, ReserveInventoryStep, ProcessPaymentStep (90/10 simulation), SendNotificationStep
- Implement `OrderSagaCoordinator`:
  - `handleOrderCreated()` — executes steps 0–2, pauses for async payment
  - `handlePaymentProcessed()` — on success: step 3 + COMPLETED; on failure: compensate all
  - `compensate()` — walks `completedSteps` in reverse, calls `.compensate()` on each
  - Persists saga state after each step transition (crash-resilient)
- Wire `WorkflowMetrics` counters (saga.completed, saga.compensated)
- Verify: happy path → COMPLETED, failure → COMPENSATED in reverse order

### Phase 5: DLQ & Observability (Day 6 — ~5 hours)

- Implement `DlqMonitor` — listens to `.DLT` topics, logs for alerting
- Implement `AdminController`: DLQ replay endpoint, payment failure toggle
- Implement `CorrelationIdFilter` — extracts/generates `X-Correlation-Id` on every HTTP request
- Implement `WorkflowMetrics` — Micrometer counters (saga.completed, saga.compensated, outbox.pending)
- Configure Logback MDC pattern to include `correlationId` in every log line
- Update `OutboxPoller` to report pending count to metrics
- Update `ProcessPaymentStep` to respect the admin toggle
- Verify: correlation ID in logs + headers, metrics endpoint, DLQ logging

### Phase 6: Testing & Documentation (Day 7 — ~8 hours)

- **Integration test**: Testcontainers (PostgreSQL + Kafka), verify POST → DB → Outbox → Kafka, Awaitility for async assertions
- **Unit tests**: Mockito — saga coordinator happy path, saga coordinator compensation, idempotency dedup
- **README.md**: architecture diagram (Mermaid), quick-start, API table, pattern explanations, design decisions
- **DESIGN_DECISIONS.md**: Kafka vs RabbitMQ, Outbox polling vs Debezium, orchestration vs choreography, exactly-once semantics
- **demo.sh**: full bash script — happy path, toggle failure, compensation, metrics

---

## 🧰 Tech Stack

```
Java 17
Spring Boot 3.3+
Spring Kafka 3.1+
PostgreSQL 16 (Docker)
Apache Kafka 3.7 (KRaft mode)
Flyway 10.x
Testcontainers 1.19+
Micrometer (Actuator)
Lombok
JUnit 5 + Mockito + AssertJ + Awaitility
```

---

## 📊 API Endpoints

| Method | Path | Description |
|:---|:---|:---|
| `POST` | `/api/v1/orders` | Create order, start saga |
| `GET` | `/api/v1/orders/{id}` | Get order + saga summary |
| `GET` | `/api/v1/orders/{id}/saga-status` | Get full saga state (completed steps, status) |
| `POST` | `/api/v1/admin/simulate/payment-failure?enabled=true` | Toggle 100% payment failure |
| `POST` | `/api/v1/admin/dlt/replay?topic=&key=` | Replay a DLQ message |

---

## 📊 Metrics (Actuator)

| Metric | Type | Description |
|:---|:---|:---|
| `workflow.saga.completed` | Counter | Successfully completed sagas |
| `workflow.saga.compensated` | Counter | Rolled-back sagas |
| `workflow.outbox.pending` | Gauge | Current outbox backlog |

---

## ⏱️ Time Estimate

| Phase | Task | Effort |
|:---|:---|---:|
| 1 | Scaffold + Docker + Config + Health | 4 hours |
| 2 | Entities + Flyway + Outbox Pattern + OrderService + OutboxPoller | 5 hours |
| 3 | Event POJOs + Consumers + Idempotency + ErrorHandler + DLQ routing | 5 hours |
| 4 | SagaContext + 4 forward steps + 3 compensating steps + Coordinator | 8 hours |
| 5 | DLQ monitor + Correlation filter + Micrometer metrics + Admin endpoints | 5 hours |
| 6 | Testcontainers integration test + unit tests + README + Mermaid diagrams | 8 hours |
| **Total** | | **~35 hours** |

---

## 🎯 Files Created: ~45 files

| Layer | Count |
|:---|---:|
| Config (YAML, Docker) | 3 |
| Flyway migrations | 4 |
| Entities + Enums | 8 |
| Repositories | 4 |
| DTOs | 3 |
| Controllers | 2 |
| Services | 2 |
| Kafka configs | 3 |
| Kafka consumers | 4 |
| Event POJOs | 4 |
| Saga steps (forward + compensating) | 7 |
| Coordinator + Context | 2 |
| Exceptions | 4 |
| Utilities | 3 |
| Monitoring | 1 |
| Tests | 4 |
| Docs | 2 |
