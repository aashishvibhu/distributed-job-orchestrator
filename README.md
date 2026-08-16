# Distributed Workflow Engine

> **Goal**: Build a generic, domain-agnostic workflow orchestration engine that accepts async jobs via REST, publishes to Kafka, processes them idempotently, and tracks state in PostgreSQL. The engine core is fully reusable; the **e-commerce order saga** serves as the concrete demo implementation. Demonstrates Kafka expertise, async architecture, Saga/Outbox patterns, retry + DLQ handling, and distributed systems design.

---

## 🧭 Architecture Overview

```
┌─────────┐   REST (POST /workflows)  ┌──────────────────┐   Outbox Pattern    ┌──────────────┐
│  Client  │ ────────────────────────▶│  Workflow Engine  │ ──────────────────▶ │  PostgreSQL   │
└─────────┘                           │  (Spring Boot)    │                     │  (workflows,  │
                                      │  port: 8080       │                     │   outbox,     │
                                      └────────┬─────────┘                     │   saga_state) │
                                               │                                └──────┬───────┘
                                      ┌────────▼─────────┐                    ┌──────▼──────────┐
                                      │   Kafka Broker    │                    │  Outbox Poller  │
                                      │   (KRaft mode)    │◀───────────────────│  (@Scheduled)   │
                                      └──┬──────────┬─────┘                    └─────────────────┘
                                         │          │
                            ┌────────────▼──┐  ┌────▼──────────────┐
                            │workflow.started│  │  step.completed   │
                            │   Consumer     │  │    Consumer       │
                            │ (idempotent)   │  │  (idempotent)     │
                            └──────┬─────────┘  └────────┬─────────┘
                                   │                     │
                                   └──────────┬──────────┘
                                              │
                                     ┌────────▼────────┐
                                     │ Saga Coordinator │
                                     │ (domain-agnostic)│
                                     │  orchestrates    │
                                     │  SagaStep seq.   │
                                     └────────┬────────┘
                                              │
                         ┌────────────────────┼────────────────────┐
                         ▼                    ▼                    ▼
                  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
                  │ E-Commerce   │    │ Data Pipeline│    │ Approval     │
                  │ Saga Steps   │    │ Saga Steps   │    │ Saga Steps   │
                  │ (impl #1) ✓  │    │ (future)     │    │ (future)     │
                  └──────────────┘    └──────────────┘    └──────────────┘
```

> **Key design point**: The `SagaCoordinator` and `SagaStep` interface are **domain-agnostic**. Any business domain implements `SagaStep` (with `execute()` + `compensate()`) and registers its pipeline. The engine doesn't know or care about orders, payments, or inventory — it just runs steps.

### End-to-End Flow (E-Commerce Demo)

1. **POST /api/v1/workflows** `{ "type": "ecommerce-order", "payload": { ... } }` → `WorkflowService` writes `Workflow` + `OutboxEvent` in one DB transaction
2. **OutboxPoller** (`@Scheduled 1s`) reads `PENDING` events, publishes to Kafka, marks `PUBLISHED`
3. **WorkflowStartedConsumer** picks `workflow.started`, loads the registered `SagaStep` pipeline for type `ecommerce-order`, triggers `SagaCoordinator.start()`
4. **SagaCoordinator** executes steps sequentially: `ValidateOrderStep` → `ReserveInventoryStep` → `ProcessPaymentStep` → `SendNotificationStep`
5. Each step completion publishes `step.completed` → `StepCompletedConsumer` picks up → saga advances to next step
6. **On failure**: `SagaCoordinator` walks backward through completed steps calling `.compensate()` on each
7. **DLQ**: After 3 retries, `DeadLetterPublishingRecoverer` sends to `.DLT` topic
8. **GET /api/v1/workflows/{id}/status** → full saga state with step-by-step timeline

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
│   │   │   ├── WorkflowController.java         # POST /workflows, GET /workflows/{id}, status
│   │   │   └── AdminController.java            # DLQ replay, simulate step failure
│   │   └── dto/
│   │       ├── SubmitWorkflowRequest.java       # type + payload (Map<String,Object>)
│   │       ├── WorkflowStatusResponse.java
│   │       └── ApiError.java
│   │
│   ├── domain/                                  # ── GENERIC ENGINE CORE ──
│   │   ├── entity/
│   │   │   ├── Workflow.java                    # JPA entity: id, type, status, payload (JSONB), correlationId
│   │   │   ├── OutboxEvent.java                 # Outbox table entity
│   │   │   ├── SagaState.java                   # Saga tracking: workflowId, currentStep, completedSteps[], status
│   │   │   └── ProcessedEvent.java              # Idempotency dedup table
│   │   ├── enums/
│   │   │   ├── WorkflowStatus.java              # PENDING, RUNNING, COMPLETED, FAILED, COMPENSATED
│   │   │   ├── SagaStatus.java
│   │   │   ├── OutboxStatus.java
│   │   │   └── StepStatus.java                  # PENDING, RUNNING, SUCCESS, FAILED, COMPENSATED
│   │   └── repository/
│   │       ├── WorkflowRepository.java
│   │       ├── OutboxEventRepository.java       # @Lock(PESSIMISTIC_WRITE) for polling
│   │       ├── SagaStateRepository.java
│   │       └── ProcessedEventRepository.java
│   │
│   ├── engine/                                  # ── GENERIC ENGINE CORE ──
│   │   ├── SagaCoordinator.java                 # Domain-agnostic: runs List<SagaStep>, handles compensation
│   │   ├── SagaContext.java                     # Mutable context: workflowId, stepResults (Map), correlationId
│   │   ├── SagaStep.java                        # Interface: stepName(), execute(ctx), compensate(ctx)
│   │   ├── WorkflowRegistry.java                # Maps workflow "type" → List<SagaStep> pipeline
│   │   └── StepResult.java                      # SUCCESS / FAILURE with optional error details
│   │
│   ├── saga/                                    # ── SAGA STEP IMPLEMENTATIONS ──
│   │   └── ecommerce/                           # E-commerce domain (concrete impl #1)
│   │       ├── EcommerceSagaConfig.java          # Registers pipeline: "ecommerce-order" → steps
│   │       ├── steps/
│   │       │   ├── ValidateOrderStep.java        # Validates payload, enriches context
│   │       │   ├── ReserveInventoryStep.java     # Simulates inventory reservation
│   │       │   ├── ProcessPaymentStep.java       # 90% success / 10% failure simulation
│   │       │   └── SendNotificationStep.java     # Simulates email/SMS notification
│   │       └── compensating/
│   │           ├── ReleaseInventoryStep.java     # Compensates ReserveInventoryStep
│   │           └── RefundPaymentStep.java        # Compensates ProcessPaymentStep
│   │
│   ├── messaging/
│   │   ├── producer/
│   │   │   ├── KafkaEventPublisher.java          # Async publish with callback logging
│   │   │   └── OutboxPoller.java                 # @Scheduled poller
│   │   ├── consumer/
│   │   │   ├── WorkflowStartedConsumer.java      # Generic: loads pipeline, triggers coordinator
│   │   │   ├── StepCompletedConsumer.java        # Generic: advances saga to next step
│   │   │   └── DlqMonitor.java                   # Logs DLT messages
│   │   ├── event/
│   │   │   └── WorkflowEvent.java                # Single generic event: workflowId, type, stepName, status, payload
│   │   └── config/
│   │       ├── KafkaProducerConfig.java          # idempotent + transactional
│   │       ├── KafkaConsumerConfig.java          # read_committed, manual ack, error handler
│   │       └── KafkaTopicConfig.java             # Topic bean definitions
│   │
│   ├── service/
│   │   ├── WorkflowService.java                  # Outbox Pattern — atomic writes (Workflow + OutboxEvent)
│   │   └── IdempotencyService.java               # DB unique constraint dedup
│   │
│   ├── common/
│   │   ├── exception/
│   │   │   ├── DuplicateEventException.java
│   │   │   ├── RetryableException.java
│   │   │   ├── InvalidPayloadException.java       # Non-retryable → DLT immediately
│   │   │   └── SagaStepFailedException.java
│   │   └── util/
│   │       ├── CorrelationIdUtil.java             # MDC-based tracing
│   │       ├── JsonUtil.java
│   │       └── CorrelationIdFilter.java           # Servlet filter for HTTP headers
│   │
│   └── monitoring/
│       └── WorkflowMetrics.java                   # Micrometer counters/gauges
│
├── src/main/resources/
│   ├── application.yml
│   ├── application-docker.yml
│   └── db/migration/
│       ├── V1__create_workflows_table.sql          # Generic: id, type, status, payload JSONB, correlation_id
│       ├── V2__create_outbox_table.sql
│       ├── V3__create_saga_state_table.sql
│       └── V4__create_processed_events_table.sql
│
└── src/test/java/com/workflow/engine/
    ├── integration/
    │   └── WorkflowSagaIntegrationTest.java        # Testcontainers: Kafka + PostgreSQL
    ├── unit/
    │   ├── SagaCoordinatorTest.java                # Mockito: happy path + compensation (generic)
    │   ├── EcommerceSagaTest.java                  # E-commerce specific step tests
    │   └── IdempotencyServiceTest.java
    └── contract/
        └── WorkflowControllerTest.java
```

---

## 🔑 Patterns Demonstrated

| Pattern | Implementation | Key Design Decision |
|:---|:---|:---|
| **Outbox Pattern** | `WorkflowService` writes Workflow + OutboxEvent + SagaState in single `@Transactional`; `OutboxPoller` publishes to Kafka | Chose polling (`@Scheduled`) over CDC (Debezium) — simpler setup, pattern identical |
| **Saga (Orchestration)** | `SagaCoordinator` executes `List<SagaStep>` sequentially, persists state after each step, walks backward calling `compensate()` on failure | Chose orchestration over choreography — centralized rollback logic, easier to test. Coordinator is domain-agnostic. |
| **Pluggable Steps** | `SagaStep` interface: `execute(SagaContext)` / `compensate(SagaContext)`. `WorkflowRegistry` maps workflow type → step pipeline. | New domains = new `SagaStep` impls + one config class. Engine code never changes. |
| **Idempotent Consumer** | `IdempotencyService` uses `processed_events` table with UNIQUE constraint on `message_id` | Combined with Kafka `read_committed` + idempotent producer = effectively exactly-once |
| **DLQ with Retry** | `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` — 3 retries with 2s fixed backoff, then DLT | `InvalidPayloadException` skips retry → DLT immediately (non-retryable) |
| **Correlation ID Tracing** | `CorrelationIdFilter` extracts/generates `X-Correlation-Id` → MDC → all log lines + Kafka headers | End-to-end traceability across REST → Kafka → DB, domain-agnostic |

---

## 🔌 Extensibility: Engine vs. Domain Separation

```
┌─────────────────────────────────────────────────────┐
│                 ENGINE (never changes)              │
│                                                     │
│  Workflow.java          SagaCoordinator.java        │
│  SagaStep.java          SagaContext.java            │
│  WorkflowRegistry.java  WorkflowService.java        │
│  OutboxPoller.java      IdempotencyService.java     │
│  WorkflowStartedConsumer / StepCompletedConsumer    │
│  DLQ / Retry / Correlation / Metrics                │
│                                                     │
├─────────────────────────────────────────────────────┤
│             DOMAIN IMPLEMENTATIONS                   │
│                                                     │
│  ┌─ ecommerce/ ─────────────────────────────────┐   │
│  │ EcommerceSagaConfig.java                     │   │
│  │ ValidateOrderStep      ReleaseInventoryStep  │   │
│  │ ReserveInventoryStep   RefundPaymentStep     │   │
│  │ ProcessPaymentStep                           │   │
│  │ SendNotificationStep                         │   │
│  └──────────────────────────────────────────────┘   │
│                                                     │
│  ┌─ datapipeline/ (future) ─────────────────────┐   │
│  │ DataPipelineSagaConfig.java                  │   │
│  │ ExtractStep  TransformStep  LoadStep         │   │
│  └──────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

### How to Add a New Domain

Adding a new workflow type (e.g., "data-pipeline") requires **zero engine changes**:

```java
// 1. Implement SagaStep for each stage
@Component
public class ExtractStep implements SagaStep {
    public String getStepName() { return "data-extract"; }
    public StepResult execute(SagaContext ctx) { /* ... */ }
    public StepResult compensate(SagaContext ctx) { /* ... */ }
}

// 2. Register the pipeline
@Configuration
public class DataPipelineSagaConfig {
    @Bean
    public WorkflowRegistryEntry dataPipelineEntry(
            ExtractStep extract, TransformStep transform, LoadStep load) {
        return new WorkflowRegistryEntry("data-pipeline", List.of(extract, transform, load));
    }
}

// 3. Submit via API
// POST /api/v1/workflows  { "type": "data-pipeline", "payload": { "source": "s3://..." } }
```

---

## 📋 Implementation Phases

### Phase 1: Scaffold & Infrastructure (Day 1 — ~4 hours)

- Initialize Spring Boot 3.3+ project (Maven, Java 17)
- Dependencies: Spring Web, Spring Data JPA, Spring Kafka, Flyway, PostgreSQL Driver, Lombok, Actuator, Testcontainers, Validation, Awaitility, AssertJ
- Write `docker-compose.yml`: PostgreSQL 16, Kafka 3.7 (KRaft mode), Kafka UI
- Configure `application.yml`: Kafka producer (idempotent, `acks=all`), consumer (`read_committed`, manual ack), Flyway
- Create `KafkaTopicConfig` bean: 7 topics, 3 partitions each (workflow.started, step.completed, step.failed, workflow.completed, workflow.compensated, + 2 DLTs)
- Smoke test: health endpoint green, Kafka UI visible

### Phase 2: Core Domain & Outbox Pattern (Day 2 — ~5 hours)

- Write 4 Flyway migrations (`workflows` with JSONB payload, `outbox_events`, `saga_states`, `processed_events`)
- Create 4 JPA entities: `Workflow` (generic), `OutboxEvent`, `SagaState`, `ProcessedEvent`
- Create 4 enums: `WorkflowStatus`, `SagaStatus`, `OutboxStatus`, `StepStatus`
- Create 4 Spring Data repositories (`OutboxEventRepository` uses `@Lock(PESSIMISTIC_WRITE)`)
- Implement generic engine interfaces: `SagaStep`, `SagaContext`, `StepResult`, `WorkflowRegistry`
- Implement `SagaCoordinator` (domain-agnostic — knows nothing about e-commerce)
- Implement utility classes: `CorrelationIdUtil` (MDC), `JsonUtil` (Jackson)
- Implement `KafkaProducerConfig` + `KafkaEventPublisher` (async with callback)
- Implement `WorkflowService.submitWorkflow()` — Outbox Pattern: Workflow + OutboxEvent + SagaState in one transaction
- Implement `OutboxPoller` — `@Scheduled` every 1s, reads PENDING events, publishes, marks PUBLISHED
- Create DTOs: `SubmitWorkflowRequest` (with `@Valid`, `type` + flexible `payload`), `WorkflowStatusResponse`, `ApiError`
- Implement `WorkflowController`: POST /workflows, GET /workflows/{id}, GET /workflows/{id}/status
- Verify: POST workflow → DB writes + Kafka message visible in Kafka UI

### Phase 3: Kafka Consumers + Idempotency (Day 3 — ~5 hours)

- Create generic `WorkflowEvent` POJO (single event class with type discriminator)
- Implement `IdempotencyService`: `isDuplicate()` + `markProcessed()` using DB UNIQUE constraint
- Create 4 exception classes: `DuplicateEventException`, `RetryableException`, `InvalidPayloadException`, `SagaStepFailedException`
- Implement `KafkaConsumerConfig`: `DefaultErrorHandler` with `DeadLetterPublishingRecoverer` (3 retries × 2s), `InvalidPayloadException` → no retry
- Implement `WorkflowStartedConsumer` — generic: loads pipeline from `WorkflowRegistry`, triggers `SagaCoordinator.start()`
- Implement `StepCompletedConsumer` — generic: tells `SagaCoordinator` to advance to next step
- Verify: consumer picks up message, `processed_events` table populated, duplicates skipped

### Phase 4: E-Commerce Saga Implementation (Day 4–5 — ~8 hours)

- Implement 4 e-commerce forward steps (all implement `SagaStep`):
  - `ValidateOrderStep` — validates payload, enriches `SagaContext`
  - `ReserveInventoryStep` — simulates inventory reservation
  - `ProcessPaymentStep` — 90% success / 10% failure simulation (configurable via admin toggle)
  - `SendNotificationStep` — simulates notification dispatch
- Implement 2 compensating steps:
  - `ReleaseInventoryStep` — compensates `ReserveInventoryStep`
  - `RefundPaymentStep` — compensates `ProcessPaymentStep`
- Implement `EcommerceSagaConfig` — registers `"ecommerce-order"` type with step list
- Verify `SagaCoordinator` runs the e-commerce pipeline end-to-end:
  - Happy path → all 4 steps succeed → `WorkflowStatus.COMPLETED`
  - Payment failure → compensation runs: RefundPayment → ReleaseInventory → `WorkflowStatus.COMPENSATED`
- Wire `WorkflowMetrics` counters (`workflow.saga.completed`, `workflow.saga.compensated`)
- Verify: crash-resilience — saga state persisted after each step transition

### Phase 5: DLQ & Observability (Day 6 — ~5 hours)

- Implement `DlqMonitor` — listens to `.DLT` topics, logs structured JSON for alerting
- Implement `AdminController`:
  - `POST /admin/simulate/step-failure?step=process-payment&enabled=true` — generic toggle for any step
  - `POST /admin/dlt/replay?topic=&key=` — DLQ message replay
  - `GET /admin/workflows?type=&status=` — workflow search
- Implement `CorrelationIdFilter` — extracts/generates `X-Correlation-Id` on every HTTP request
- Implement `WorkflowMetrics` — Micrometer counters (`workflow.saga.completed`, `workflow.saga.compensated`) + gauge (`workflow.outbox.pending`)
- Configure Logback MDC pattern to include `correlationId` + `workflowType` in every log line
- Update `OutboxPoller` to report pending count to metrics
- Verify: correlation ID in logs + Kafka headers, metrics endpoint, DLQ logging

### Phase 6: Testing & Documentation (Day 7 — ~8 hours)

- **Integration test**: Testcontainers (PostgreSQL + Kafka), verify POST → DB → Outbox → Kafka → Saga completion, Awaitility for async assertions
- **Unit tests**:
  - `SagaCoordinatorTest` — Mockito: generic happy path + compensation (domain-agnostic)
  - `EcommerceSagaTest` — e-commerce step tests
  - `IdempotencyServiceTest` — dedup logic
- **README.md**: architecture diagram (Mermaid), quick-start, API table, pattern explanations, how to add new domains
- **DESIGN_DECISIONS.md**: Kafka vs RabbitMQ, Outbox polling vs Debezium, orchestration vs choreography, generic engine design rationale
- **demo.sh**: full bash script — happy path, toggle failure, compensation, DLQ replay, metrics

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
| `POST` | `/api/v1/workflows` | Submit a workflow. Body: `{ "type": "ecommerce-order", "payload": {...} }` |
| `GET` | `/api/v1/workflows/{id}` | Get workflow + saga summary |
| `GET` | `/api/v1/workflows/{id}/status` | Get full saga state (completed steps, current step, status) |
| `GET` | `/api/v1/workflows?type=ecommerce-order` | List workflows by type |
| `POST` | `/api/v1/admin/simulate/step-failure?step=process-payment&enabled=true` | Toggle 100% failure on any step by name (generic) |
| `POST` | `/api/v1/admin/dlt/replay?topic=&key=` | Replay a DLQ message |

---

## 📊 Kafka Topics (Generic)

| Topic | Partitions | Purpose |
|:---|:---|:---|
| `workflow.started` | 3 | New workflow submitted, triggers `SagaCoordinator.start()` |
| `step.completed` | 3 | A saga step finished successfully, triggers next step |
| `step.failed` | 3 | A saga step failed, triggers compensation |
| `workflow.completed` | 3 | Entire saga completed successfully |
| `workflow.compensated` | 3 | Entire saga rolled back |
| `workflow.started.DLT` | 3 | Dead letter for workflow.started |
| `step.completed.DLT` | 3 | Dead letter for step.completed |

---

## 📊 Metrics (Actuator)

| Metric | Type | Description |
|:---|:---|:---|
| `workflow.saga.completed` | Counter | Successfully completed sagas (all types) |
| `workflow.saga.compensated` | Counter | Rolled-back sagas (all types) |
| `workflow.saga.completed{type=ecommerce-order}` | Counter | Per-type counters via tags |
| `workflow.outbox.pending` | Gauge | Current outbox backlog |

---

## ⏱️ Time Estimate

| Phase | Task | Effort |
|:---|:---|---:|
| 1 | Scaffold + Docker + Config + Health | 4 hours |
| 2 | Generic entities + Flyway + Outbox + WorkflowService + OutboxPoller + SagaCoordinator | 5 hours |
| 3 | Generic consumers + Idempotency + ErrorHandler + DLQ routing | 5 hours |
| 4 | E-commerce SagaStep implementations + config + integration with coordinator | 8 hours |
| 5 | DLQ monitor + Correlation filter + Micrometer metrics + Admin endpoints | 5 hours |
| 6 | Testcontainers integration test + unit tests + README + Mermaid diagrams | 8 hours |
| **Total** | | **~35 hours** |

---

## 🎯 Files Created: ~43 files

| Layer | Count |
|:---|---:|
| Config (YAML, Docker) | 3 |
| Flyway migrations | 4 |
| Generic entities + enums | 8 |
| Repositories | 4 |
| Engine core (Coordinator, Context, Step, Registry, StepResult) | 5 |
| E-commerce saga steps (forward + compensating + config) | 7 |
| DTOs | 3 |
| Controllers | 2 |
| Services | 2 |
| Kafka configs | 3 |
| Kafka consumers (generic) | 3 |
| Event POJO (single generic) | 1 |
| Exceptions | 4 |
| Utilities | 3 |
| Monitoring | 1 |
| Tests | 4 |
| Docs | 2 |
