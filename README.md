# Distributed Workflow Engine

> **Goal**: Build a generic, domain-agnostic **Workflow Engine** — a service that accepts async jobs via REST, orchestrates them over Kafka by dispatching **tasks** to a **Worker** service, and tracks state in PostgreSQL. **The Workflow Engine is the deliverable** — the task dispatcher and the worker exist only to demonstrate that the engine correctly orchestrates, compensates, and recovers. Neither the engine nor the worker contains any domain logic: new business logic is added purely as workflow definitions (ordered lists of task types). Demonstrates Kafka expertise, Saga/Outbox patterns, retry + DLQ handling, and distributed systems design.

---

## 🧭 Architecture Overview

```mermaid
flowchart TB
    Client["Client"] -->|"POST /api/v1/workflows"| Engine["Workflow Engine Service\n(port 8080)\nGeneric orchestrator\n(contains TaskDispatcher)"]

    Engine -->|"Outbox → Kafka"| Kafka["Apache Kafka\n(KRaft mode)"]
    Engine --> EDB[("workflow_engine DB\nworkflows · definitions · outbox\nsaga_state · processed_events")]

    Kafka -->|"task.request"| Worker["Worker Service\n(port 8081)\nGeneric executor (demo)"]

    Worker -->|"task.completed / task.failed"| Kafka

    Kafka -->|"task results"| Engine

    Worker --> WDB[("worker DB\nprocessed_events")]
```

> **Key design point**: The **Workflow Engine** is fully domain-agnostic. It knows only `taskType` names and workflow definitions (an ordered list of task types) — never *what* a task does. The **TaskDispatcher** is an *internal component of the engine* (it builds and publishes `task.request`); it is not a separate service. The **Worker** implements `TaskHandler` (from the shared `worker-sdk`) and executes tasks generically. Adding new business logic = inserting one workflow-definition row + registering a handler in a worker. **Zero engine changes.**

### Services

| Service | Port | Responsibility |
|:---|:---|:---|
| `workflow-engine-service` | 8080 | **The deliverable.** Accepts workflows, dispatches tasks (internal `TaskDispatcher`), runs the saga state machine, handles compensation |
| `worker-service` | 8081 | Generic executor (demo). Consumes `task.request`, simulates task execution + compensation, replies with results |

### Roles

| Role | What it does | Owns state? |
|:---|:---|:---|
| **Client** (submitter) | Sends a workflow request to the engine — declares only *what to run* and *what data* | No |
| **Workflow Engine** (deliverable) | Orchestrates the saga: state machine + task dispatch + compensation | Engine DB |
| **Task Dispatcher** | Internal component of the engine — publishes `task.request`, receives results | No (part of engine) |
| **Worker** (demo) | Executes tasks generically, returns success / failure | Worker DB |

### End-to-End Flow (Generic Demo)

1. **POST /api/v1/workflows** `{ "type": "generic-workflow", "payload": { ... } }` → `WorkflowService` writes `Workflow` + `OutboxEvent` in one DB transaction
2. **OutboxPoller** (`@Scheduled 1s`) reads `PENDING` events, publishes `workflow.started` to Kafka, marks `PUBLISHED`
3. **WorkflowStartedConsumer** (engine) loads the DB-backed workflow definition and starts the `SagaStateMachine`
4. **TaskDispatcher** (engine, internal) publishes `task.request` `{ taskType: "task-a", ... }`
5. **Worker** consumes it, routes to a generic `TaskHandler` via `TaskHandlerRegistry`, executes, publishes `task.completed`
6. **TaskResultConsumer** (engine) advances the state machine and dispatches the next task — repeating until all tasks succeed
7. **On failure**: `CompensationPlanner` walks completed tasks in reverse and dispatches `task.request` with `action=COMPENSATE`
8. **DLQ**: After 3 retries, `DeadLetterPublishingRecoverer` sends to `.DLT` topics (per service)
9. **GET /api/v1/workflows/{id}/status** → full saga state with step-by-step timeline

### Data Journey (payload)

```
Client ──> Engine DB (workflows.payload, JSONB)
       ──> Kafka task.request (payload copied into the event)
       ──> Worker TaskContext (taskId, workflowId, payload, correlationId)
       ──> TaskHandler.execute(ctx)
```

---

## 🗂️ Project Structure (Monorepo)

```
distributed-workflow-engine/
├── pom.xml                                # Parent POM (multi-module)
├── docker-compose.yml                     # Kafka 3.9, PostgreSQL × 2, Kafka UI
├── README.md
├── docs/
│   └── DESIGN_DECISIONS.md
│
├── shared/                                # ── LIBRARIES (not services) ──
│   ├── engine-contract/                   # Wire contract — single source of truth
│   │   └── src/main/java/com/workflow/contract/
│   │       ├── event/
│   │       │   ├── WorkflowStartedEvent.java
│   │       │   ├── TaskRequestEvent.java          # taskId, taskType, action, payload, correlationId
│   │       │   ├── TaskResultEvent.java           # taskId, status, result, error
│   │       │   ├── WorkflowCompletedEvent.java
│   │       │   └── WorkflowCompensatedEvent.java
│   │       ├── enums/
│   │       │   ├── WorkflowStatus.java            # PENDING, RUNNING, COMPLETED, FAILED, COMPENSATED
│   │       │   ├── TaskStatus.java                # SUCCESS, FAILED
│   │       │   ├── TaskAction.java                # EXECUTE, COMPENSATE
│   │       │   └── OutboxStatus.java              # PENDING, PUBLISHED
│   │       └── dto/
│   │           └── WorkflowDefinition.java        # type → ordered List<String> taskTypes
│   │
│   └── worker-sdk/                        # Worker-side framework — reusable
│       └── src/main/java/com/workflow/worker/
│           ├── TaskHandler.java           # interface: taskType(), execute(ctx), compensate(ctx)
│           ├── TaskContext.java           # taskId, workflowId, payload, correlationId
│           ├── TaskResult.java            # SUCCESS / FAILURE with error details
│           ├── TaskHandlerRegistry.java   # routes taskType → handler
│           ├── WorkerKafkaConfig.java     # shared consumer/producer config + error handler
│           ├── CorrelationIdUtil.java     # MDC + Kafka header propagation
│           └── JsonUtil.java
│
└── services/
    ├── workflow-engine-service/           # Port 8080 — THE deliverable
    │   ├── pom.xml
    │   └── src/main/java/com/workflow/engine/
    │       ├── WorkflowEngineApplication.java
    │       ├── api/
    │       │   ├── WorkflowController.java        # POST /workflows, GET /workflows/{id}, status
    │       │   ├── AdminController.java           # DLQ replay, force-fail, retry
    │       │   └── dto/
    │       │       ├── SubmitWorkflowRequest.java # type + payload (Map<String,Object>)
    │       │       ├── WorkflowStatusResponse.java
    │       │       └── ApiError.java
    │       ├── domain/
    │       │   ├── Workflow.java                  # id, type, status, payload JSONB, correlationId
    │       │   ├── WorkflowDefinitionEntity.java  # type, orderedTaskTypes JSONB
    │       │   ├── OutboxEvent.java               # Outbox table entity
    │       │   ├── SagaState.java                 # workflowId, currentStep, completedSteps JSONB, status
    │       │   ├── ProcessedEvent.java            # Idempotency dedup
    │       │   └── repository/                    # 5 repositories (Outbox uses @Lock(PESSIMISTIC_WRITE))
    │       ├── engine/                            # ── NEVER domain-specific ──
    │       │   ├── SagaStateMachine.java          # generic FSM: advance / fail / complete
    │       │   ├── TaskDispatcher.java            # internal: builds + publishes TaskRequestEvent
    │       │   ├── CompensationPlanner.java       # reverses completed tasks
    │       │   └── WorkflowDefinitionService.java # loads definition from DB
    │       ├── messaging/
    │       │   ├── OutboxPoller.java              # @Scheduled, PESSIMISTIC_WRITE lock
    │       │   ├── WorkflowStartedConsumer.java
    │       │   ├── TaskResultConsumer.java        # task.completed / task.failed
    │       │   ├── DlqMonitor.java                # Logs DLT messages
    │       │   └── config/
    │       │       ├── KafkaProducerConfig.java   # idempotent + transactional
    │       │       ├── KafkaConsumerConfig.java   # read_committed, manual ack, error handler
    │       │       └── KafkaTopicConfig.java      # Topic bean definitions
    │       ├── service/
    │       │   ├── WorkflowService.java           # Outbox Pattern — atomic writes
    │       │   └── IdempotencyService.java        # DB unique constraint dedup
    │       ├── common/
    │       │   ├── exception/ (DuplicateEventException, RetryableException, InvalidPayloadException, TaskFailedException)
    │       │   └── util/ (CorrelationIdUtil, JsonUtil, CorrelationIdFilter)
    │       ├── monitoring/
    │       │   └── WorkflowMetrics.java           # Micrometer counters/gauges
    │       └── resources/
    │           ├── application.yml
    │           └── db/migration/
    │               ├── V1__create_workflows_table.sql
    │               ├── V2__create_workflow_definitions_table.sql   # seed generic-workflow
    │               ├── V3__create_outbox_table.sql
    │               ├── V4__create_saga_state_table.sql
    │               └── V5__create_processed_events_table.sql
    │
    └── worker-service/                    # Port 8081 — generic executor (demo)
        ├── pom.xml
        └── src/main/java/com/workflow/workerservice/
            ├── WorkerApplication.java
            ├── task/
            │   └── GenericTaskHandler.java     # simulates execute + compensate for any taskType
            ├── messaging/
            │   ├── TaskRequestConsumer.java    # routes via TaskHandlerRegistry
            │   └── config/
            ├── domain/
            │   ├── ProcessedEvent.java
            │   └── repository/
            └── resources/
                ├── application.yml
                └── db/migration/ (processed_events)
```

---

## 🔑 Patterns Demonstrated

| Pattern | Implementation | Key Design Decision |
|:---|:---|:---|
| **Outbox Pattern** | `WorkflowService` writes Workflow + OutboxEvent in single `@Transactional`; `OutboxPoller` publishes to Kafka | Chose polling (`@Scheduled`) over CDC (Debezium) — simpler setup, identical pattern |
| **Saga (Task-based Orchestration)** | Engine's `SagaStateMachine` dispatches `task.request` events; workers execute tasks and reply `task.completed`/`task.failed`; `CompensationPlanner` walks backward on failure | Orchestration (central state machine) + choreography (event-driven task dispatch) — mirrors Temporal/Conductor |
| **Pluggable Workers** | `TaskHandler` interface in `worker-sdk`; `TaskHandlerRegistry` routes `taskType` → handler | New business logic = one definition row + a registered handler. Engine code never changes. |
| **Idempotent Consumers** | Each service owns a `processed_events` table with UNIQUE `message_id` constraint | Dedup enforced on **both sides of every hop** (engine + workers) — at-least-once → effectively-once |
| **DLQ with Retry** | Per-service `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` — 3 retries × 2s, then DLT | `InvalidPayloadException` skips retry → DLT immediately (non-retryable) |
| **Correlation ID Tracing** | `CorrelationIdFilter` → MDC → Kafka headers → propagated across engine + worker JVMs | End-to-end traceability across HTTP → Kafka → worker → Kafka → engine |
| **Database-per-service** | Each service owns its schema; no cross-service DB access | Real data isolation — workers and engine fail independently |

---

## 🔌 Extensibility: Engine vs. Worker Separation

```
┌───────────────────────────────────────────────────────────────┐
│                 WORKFLOW ENGINE SERVICE (never changes)        │
│                                                               │
│  Workflow.java             SagaStateMachine.java              │
│  WorkflowDefinitionService TaskDispatcher.java                │
│  OutboxPoller.java         CompensationPlanner.java           │
│  WorkflowStartedConsumer   TaskResultConsumer                 │
│  DLQ / Retry / Correlation / Metrics                          │
│                                                               │
│        Kafka: task.request ⇄ task.completed / task.failed     │
├───────────────────────────────────────────────────────────────┤
│                 WORKER SERVICE (pluggable, demo)              │
│                                                               │
│  ┌─ worker-service (port 8081) ─────────────────────────┐     │
│  │ GenericTaskHandler (execute + compensate)            │     │
│  │ TaskRequestConsumer → TaskHandlerRegistry            │     │
│  │ Idempotency / DLQ / Correlation                      │     │
│  └───────────────────────────────────────────────────────┘     │
└───────────────────────────────────────────────────────────────┘
```

### How to Add New Business Logic

Adding new business logic requires **zero engine changes** — only a new definition row and a handler in a worker:

```java
// 1. Register a handler for a new taskType in a worker (from worker-sdk)
@Component
public class MyTaskHandler implements TaskHandler {
    public String getTaskType() { return "my-task"; }
    public TaskResult execute(TaskContext ctx) { /* ... */ }
    public TaskResult compensate(TaskContext ctx) { /* ... */ }
}

// 2. Insert one workflow-definition row in the engine's DB
// INSERT INTO workflow_definitions (type, ordered_task_types)
// VALUES ('my-workflow', '["my-task","another-task"]');

// 3. Submit via API
// POST /api/v1/workflows  { "type": "my-workflow", "payload": { ... } }
```

---

## 📋 Implementation Phases

### Phase 1: Monorepo Scaffold & Infrastructure (Day 1 — ~5 hours)

- Initialize parent Maven POM (multi-module) with Java 21 (LTS)
- Create shared modules: `engine-contract` (event schemas + enums) and `worker-sdk` (TaskHandler, registry, Kafka plumbing)
- Dependencies: Spring Web, Spring Data JPA, Spring Kafka, Flyway, PostgreSQL Driver, Lombok, Actuator, Testcontainers, Validation, Awaitility, AssertJ
- Write `docker-compose.yml`: Kafka 3.9 (KRaft mode), PostgreSQL 16 × 2 (engine + worker), Kafka UI
- Configure per-service `application.yml`: Kafka producer (idempotent, `acks=all`), consumer (`read_committed`, manual ack), Flyway
- Create `KafkaTopicConfig` bean: 6 topics + DLTs (`workflow.started`, `task.request`, `task.completed`, `task.failed`, `workflow.completed`, `workflow.compensated`)
- Smoke test: both services' health endpoints green, Kafka UI visible

### Phase 2: Engine Service — Outbox & Domain (Day 2 — ~8 hours)

- Write 5 Flyway migrations for the engine DB (`workflows`, `workflow_definitions`, `outbox_events`, `saga_states`, `processed_events`)
- Create 5 JPA entities: `Workflow`, `WorkflowDefinitionEntity`, `OutboxEvent`, `SagaState`, `ProcessedEvent`
- Create 5 repositories (`OutboxEventRepository` uses `@Lock(PESSIMISTIC_WRITE)`)
- Implement `WorkflowService.submitWorkflow()` — Outbox Pattern: Workflow + OutboxEvent in one transaction
- Implement `OutboxPoller` — `@Scheduled` every 1s, reads PENDING events, publishes, marks PUBLISHED
- Create DTOs: `SubmitWorkflowRequest`, `WorkflowStatusResponse`, `ApiError`
- Implement `WorkflowController`: POST /workflows, GET /workflows/{id}, GET /workflows/{id}/status
- Verify: POST workflow → engine DB writes + `workflow.started` visible in Kafka UI

### Phase 3: Engine Service — State Machine & Task Dispatch (Day 3 — ~8 hours)

- Implement `SagaStateMachine` — generic FSM: advance / fail / complete / compensate
- Implement `TaskDispatcher` — builds + publishes `TaskRequestEvent` to `task.request`
- Implement `WorkflowDefinitionService` — loads `WorkflowDefinition` (ordered task types) from DB
- Implement `WorkflowStartedConsumer` — loads definition, starts state machine, dispatches first task
- Implement `TaskResultConsumer` — consumes `task.completed` / `task.failed`, advances or compensates
- Implement `CompensationPlanner` — walks completed tasks in reverse, dispatches `action=COMPENSATE`
- Implement `IdempotencyService` + `processed_events` dedup
- Create 4 exception classes: `DuplicateEventException`, `RetryableException`, `InvalidPayloadException`, `TaskFailedException`
- Verify: engine dispatches task requests and reacts to mocked results

### Phase 4: Worker Service — Generic Executor (Day 4–5 — ~8 hours)

- Scaffold `worker-service` depending on `worker-sdk` + `engine-contract`
- Write Flyway migration for the worker DB (`processed_events`)
- Implement `GenericTaskHandler` — simulates `execute` + `compensate` for any `taskType` (configurable delay + failure rate)
- Implement `TaskRequestConsumer` — routes `taskType` → handler via `TaskHandlerRegistry`, publishes `task.completed` / `task.failed`
- Seed `workflow_definitions` with `generic-workflow` → `[task-a, task-b, task-c]`
- Verify end-to-end:
  - Happy path → all tasks succeed → `WorkflowStatus.COMPLETED`
  - Task failure → compensation: reverse completed tasks → `WorkflowStatus.COMPENSATED`
- Verify crash-resilience — saga state persisted after each task transition

### Phase 5: DLQ & Observability (Day 6 — ~6 hours)

- Implement `DlqMonitor` in both services — listens to `.DLT` topics, logs structured JSON
- Implement `AdminController` (engine):
  - `POST /admin/simulate/task-failure?task=task-b&enabled=true` — generic toggle (enforced in worker)
  - `POST /admin/dlt/replay?topic=&key=` — DLQ message replay
  - `GET /admin/workflows?type=&status=` — workflow search
- Implement `CorrelationIdFilter` — extracts/generates `X-Correlation-Id` on every HTTP request
- Implement `WorkflowMetrics` — counters (`workflow.saga.completed`, `workflow.saga.compensated`) + gauge (`workflow.outbox.pending`)
- Propagate correlation ID through Kafka headers across engine + worker JVMs
- Configure Logback MDC pattern to include `correlationId` + `workflowType` in every log line
- Enable Virtual Threads (JDK 21) — set `spring.threads.virtual.enabled=true` in each service's `application.yml` to run Kafka consumers + the saga dispatch on virtual threads (great JDK-21 talking point)
- Verify: one correlation ID traces the full journey across both services

### Phase 6: Testing & Documentation (Day 7 — ~10 hours)

- **Integration test**: Testcontainers (Kafka + 2 PostgreSQL), verify POST → engine DB → Outbox → Kafka → worker → Kafka → engine → COMPLETED, Awaitility for async assertions
- **Unit tests**:
  - `SagaStateMachineTest` — Mockito: generic happy path + compensation (domain-agnostic)
  - `WorkerTest` — generic task handler tests
  - `IdempotencyServiceTest` — dedup logic
- **README.md**: architecture diagram (Mermaid), quick-start, API table, pattern explanations, how to add new business logic
- **DESIGN_DECISIONS.md**: Kafka vs RabbitMQ, Outbox polling vs Debezium, orchestration vs choreography, task-dispatch engine design, monorepo vs polyrepo
- **demo.sh**: full bash script — happy path, toggle failure, compensation, DLQ replay, metrics

---

## 🧰 Tech Stack

```
Java 21 (LTS)
Maven (multi-module parent POM, maven-compiler-plugin 3.13+)
Spring Boot 4.1.x (2+ independently deployable services)
Spring Kafka 4.x (Spring Framework 7.x)
PostgreSQL 16 (one database per service, Docker)
Apache Kafka 3.9 (KRaft mode; client managed by Boot)
Flyway 11.x
Jackson 3.x (tools.jackson — Boot 4 default)
Testcontainers 1.22.x
Micrometer 2.x (Actuator)
Lombok 1.18.x (managed)
JUnit 5.13 + Mockito 5.20 + AssertJ 3.27 + Awaitility 4.3 (test)
```

---

## 📊 API Endpoints (Workflow Engine — port 8080)

| Method | Path | Description |
|:---|:---|:---|
| `POST` | `/api/v1/workflows` | Submit a workflow. Body: `{ "type": "generic-workflow", "payload": {...} }` |
| `GET` | `/api/v1/workflows/{id}` | Get workflow + saga summary |
| `GET` | `/api/v1/workflows/{id}/status` | Get full saga state (completed tasks, current task, status) |
| `GET` | `/api/v1/workflows?type=generic-workflow` | List workflows by type |
| `POST` | `/api/v1/admin/simulate/task-failure?task=task-b&enabled=true` | Toggle 100% failure on any task by name (generic) |
| `POST` | `/api/v1/admin/dlt/replay?topic=&key=` | Replay a DLQ message |

---

## 📊 Kafka Topics (Generic)

| Topic | Producer | Consumer | Partitions | Purpose |
|:---|:---|:---|:---|:---|
| `workflow.started` | Engine (outbox) | Engine | 3 | New workflow submitted, starts the saga state machine |
| `task.request` | Engine | All workers | 3 | Dispatch a task — `taskType` discriminator routes to handler |
| `task.completed` | Worker | Engine | 3 | Task succeeded → advance state machine |
| `task.failed` | Worker | Engine | 3 | Task failed → trigger compensation |
| `workflow.completed` | Engine | Monitoring | 3 | Entire saga completed successfully |
| `workflow.compensated` | Engine | Monitoring | 3 | Entire saga rolled back |
| `*.DLT` | DeadLetterRecoverer | DlqMonitor | 3 | Dead letters per topic |

> **Routing note**: `task.request` is partitioned by `workflowId` for ordering; workers filter by `taskType`. One generic topic — the engine never knows a domain.

---

## 📊 Metrics (Actuator)

| Metric | Service | Type | Description |
|:---|:---|:---|:---|
| `workflow.saga.completed` | Engine | Counter | Successfully completed sagas (all types) |
| `workflow.saga.compensated` | Engine | Counter | Rolled-back sagas (all types) |
| `workflow.saga.completed{type=generic-workflow}` | Engine | Counter | Per-type counters via tags |
| `workflow.outbox.pending` | Engine | Gauge | Current outbox backlog |
| `worker.task.completed{task=task-b}` | Worker | Counter | Per-task execution counts |

---

## ⏱️ Time Estimate

| Phase | Task | Effort |
|:---|:---|---:|
| 1 | Monorepo scaffold + shared modules + Docker (Kafka, 2 PG, UI) | 5 hours |
| 2 | Engine service: entities + migrations + Outbox + WorkflowService + OutboxPoller | 8 hours |
| 3 | Engine service: state machine + task dispatch + result consumer + compensation | 8 hours |
| 4 | Worker service: generic task handler + idempotency + DLQ | 8 hours |
| 5 | DLQ monitor + correlation + metrics + admin endpoints (cross-service) | 6 hours |
| 6 | Testcontainers integration test + unit tests + README + Mermaid diagrams | 10 hours |
| **Total** | | **~45 hours** |

---

## 🎯 Files Created: ~54 files

| Layer | Count |
|:---|---:|
| Config (POMs, YAML, Docker) | 6 |
| Shared contract (events + enums + dto) | 11 |
| Worker SDK (TaskHandler, Registry, Context, Result, utils) | 7 |
| Engine service (entities, repositories, engine, messaging, api, service, common, monitoring) | 24 |
| Worker service (task, messaging, domain) | 8 |
| Flyway migrations (engine 5 + worker 1) | 6 |
| Exceptions | 4 |
| Tests | 6 |
| Docs | 2 |
