---
name: pcmedicalist-event-driven-architecture
description: Use when designing or operating CQRS and Event Sourcing in PCMedicalist services — command/query split, event store, projections, schema evolution, snapshots, sagas.
version: 1.0.0
author: PCMedicalist
license: CC-BY-4.0 (PCMedicalist attribution required)
metadata:
  hermes:
    tags: [architecture, cqrs, event-sourcing, events, projections, pcmedicalist]
    related_skills: [pcmedicalist-event-driven, pcmedicalist-saga-pattern, pcmedicalist-database-deep-dive, pcmedicalist-messaging-semantics, pcmedicalist-distributed-patterns, pcmedicalist-outbox-pattern]
---

# PCMedicalist Event-Driven Architecture — CQRS & Event Sourcing

## When to Use
- You are splitting a read/write-heavy domain (tips, payouts, agent-state, billing) and the single-model CRUD approach is causing contention or stale reads.
- You need a full audit trail / time-travel of an entity's state (compliance, debugging, "what actually happened").
- You are deploying the Outbox-relay or CDC pipeline (see `pcmedicalist-database-deep-dive` topic 30) as a first-class event source rather than a side-effect.
- You must coordinate a multi-step business process across services without distributed transactions (see `pcmedicalist-saga-pattern`).
- You are auditing the system for the two previously-uncovered checklist items: **#88 CQRS** and **#89 Event Sourcing**.

## Portfolio Reality
PCMedicalist today uses **Postgres + Prisma + Redis** on a single VPS, with an **Outbox pattern** for write-then-publish and **Redis Streams / cron** for async work. There is **no formal CQRS boundary and no event store** yet — reads and writes hit the same Prisma models. This skill defines the target architecture for when a domain matures enough to warrant it (financial ledger, audit-critical state, high read/write skew). It reuses the Outbox (atomic event capture) and the messaging-semantics discipline (at-least-once + idempotent consumers) as the transport underneath. Related: `pcmedicalist-event-driven`, `pcmedicalist-saga-pattern`, `pcmedicalist-messaging-semantics`.

## Topics Covered
1. CQRS — Command Query Responsibility Segregation
2. Event Sourcing
3. Command Model (Write Side)
4. Query Model / Projections (Read Side)
5. Event Store
6. Event Schema Evolution
7. Snapshots
8. Saga / Process Manager
9. Dual-Write Avoidance (Outbox tie-in)

Each topic: **Concept → Pattern → Validate → Fix**.

---

## 1. CQRS — Command Query Responsibility Segregation
**Concept:** Separate the **write model** (handles commands, enforces invariants, produces events) from the **read model** (optimized projections for queries). They scale, optimize, and evolve independently. You no longer force one ORM entity to serve both a `POST /tips` mutation and a `GET /creators/:id/dashboard` read.

**Pattern:**
```ts
// WRITE SIDE — commands, no read DTOs
async function handleCreateTip(cmd: CreateTipCmd) {
  // load aggregate, enforce invariants, persist events (topic 2/3)
}
// READ SIDE — projections, no domain logic
async function getCreatorDashboard(id: string) {
  return readDb.creatorDashboard.findUnique({ where: { creatorId: id } }); // pre-joined, denormalized
}
```
The read model is a **projection** built from events (topic 4), not the write table queried differently. They communicate only via the event log.

**Validate:** The write path never serves a `GET` dashboard query; the read path never validates a business rule. Load-testing writes does not slow reads (separate models / separate DBs possible). A schema change to the read model does not require a migration of the write model.

**Fix:** If a single entity serves both mutations and complex reads (N+1, heavy joins), split: extract a command handler + a projection builder. Start with a logical split (separate modules) before a physical split (separate stores).

---

## 2. Event Sourcing
**Concept:** Persist **state as a sequence of events**, not as a mutable row. Current state = fold/replay of all events for an aggregate. `TipCreated`, `TipConfirmed`, `TipDisputed` — not `UPDATE tips SET status='confirmed'`.

**Pattern:**
```ts
// append-only event store (Postgres example)
CREATE TABLE tip_events (
  id          bigserial PRIMARY KEY,
  aggregate_id uuid NOT NULL,
  version     int NOT NULL,                 -- per-aggregate sequence
  type        text NOT NULL,
  payload     jsonb NOT NULL,
  recorded_at timestamptz DEFAULT now(),
  CONSTRAINT uq_agg_version UNIQUE (aggregate_id, version)  -- optimistic concurrency
);
-- current state = replay:
SELECT payload FROM tip_events WHERE aggregate_id=$1 ORDER BY version;
```
The write model applies a command to the current folded state, emits one or more events, and appends them. State is reconstructed by reducing events (or from a snapshot + tail, topic 7).

**Validate:** Replaying the event log for an aggregate reproduces its exact current state. Two replays (fresh + from snapshot) yield identical state. No event is ever mutated or deleted (audit trail immutable). A "what happened to tip X" query returns the full event history.

**Fix:** If you store only the latest row and lose history, migrate to an event table: write new events, backfill current state as a synthetic `Created` event, build projections from the stream. Enforce per-aggregate version for optimistic concurrency (no lost updates).

---

## 3. Command Model (Write Side)
**Concept:** The write side loads an aggregate, validates the command against invariants, and emits events. It is **not** about returning data — commands return success/failure (or the new version), never a read payload.

**Pattern:**
```ts
async function dispatch(cmd: Command) {
  const agg = await load(cmd.aggregateId);          // fold events -> state
  const events = agg.execute(cmd);                  // domain logic -> events or throws
  await appendEvents(cmd.aggregateId, agg.version, events);  // atomic, version-checked
  await publishToOutbox(events);                    // see topic 9
  return { ok: true, version: agg.version + events.length };
}
```
Invariants (e.g. "a tip cannot be confirmed twice", "balance must stay ≥ 0") live **here**, not in the read model. Failures throw before any event is appended.

**Validate:** A command violating an invariant is rejected (no event appended, no projection updated). Two concurrent commands on the same aggregate → one wins, the other fails on version conflict (handled by retry / user error). The write side emits zero query DTOs.

**Fix:** If business rules are scattered in controllers or read handlers, centralize them in the aggregate's `execute`. Move validation-before-mutation into the command handler. Use the version check (topic 2 UNIQUE constraint) for optimistic concurrency.

---

## 4. Query Model / Projections (Read Side)
**Concept:** Read models are **denormalized, purpose-built projections** rebuilt from the event stream. Each read use-case gets its own optimized table/view — no shared "god entity" with 20 joins.

**Pattern:**
```ts
// projection builder consumes events, writes to read-optimized store
async function projectTipConfirmed(e: TipConfirmed) {
  await readDb.creatorDashboard.upsert({
    where: { creatorId: e.creatorId },
    create: { creatorId: e.creatorId, tipCount: 1, totalWei: e.amountWei },
    update: { tipCount: { increment: 1 }, totalWei: { increment: e.amountWei } },
  });
}
```
Projections are **rebuildable from scratch** by replaying events — so they're disposable caches, not sources of truth. Multiple projections can serve different surfaces (dashboard, leaderboard, export).

**Validate:** Deleting a projection table and replaying the event log from 0 rebuilds it exactly (proving it's derived, not source of truth). A read returns the projected value within the projection's lag SLA. Replaying produces no duplicate rows (idempotent upsert).

**Fix:** If reads hit the write table with heavy joins, build a projection. Make every projection idempotent (upsert on event id) so replay/redelivery is safe. Store projections in a read-optimized store (separate Postgres schema, or even a different DB / search index).

---

## 5. Event Store
**Concept:** The event store is the **system of record** for event-sourced aggregates — an append-only, ordered, versioned log. It is NOT a message queue (though it feeds one via Outbox/CDC).

**Pattern:**
```sql
-- append with optimistic-concurrency check in one statement
INSERT INTO tip_events (aggregate_id, version, type, payload)
SELECT $1, COALESCE(MAX(version),0)+1, $2, $3
FROM tip_events WHERE aggregate_id = $1
ON CONFLICT (aggregate_id, version) DO NOTHING
RETURNING id;   -- if 0 rows, version conflict -> retry
```
Design: append-only, per-aggregate ordering via `version`, global ordering via `id`/timestamp, indexed by `aggregate_id` and `type`. Never UPDATE/DELETE an event (use a compensating event).

**Validate:** Append is atomic + version-checked (concurrent write detected, not silently lost). Events are immutable post-append (no UPDATE path). The store can be replayed to reconstruct any aggregate. Global order is monotonic.

**Fix:** If events live in a mutable table you `UPDATE`, migrate to append-only with a version column + unique constraint. Remove any delete path (compensating events instead). Index for both per-aggregate load and global replay.

---

## 6. Event Schema Evolution
**Concept:** Events are **forever** — you cannot migrate them like a row. A `TipCreated` from 2026 must still deserialize in 2029. Schema changes must be **backward-compatible** (additive) or versioned.

**Pattern:**
```ts
// versioned event with optional new fields, never removed/renamed-in-place
type TipCreatedV1 = { type: 'TipCreated'; v: 1; creatorId: string; amountWei: string };
type TipCreatedV2 = { type: 'TipCreated'; v: 2; creatorId: string; amountWei: string; currency: string };
// consumer: read with fallback
const currency = e.currency ?? 'USDC';   // old events lack it -> default
```
Rules: add fields (optional), never remove/rename. If a breaking change is unavoidable, emit a **new event type** (`TipCreatedV2`) and let the projection handle both. Store a `schema_version` per event. Avoid serializing domain objects directly — use explicit event DTOs.

**Validate:** An old event (stored last year) deserializes and projects correctly against the current consumer (with defaults for absent fields). A new event with extra fields doesn't break the old consumer path. No event type was renamed in place (a replay would fail).

**Fix:** If you renamed a field in an event DTO, that breaks old events — add a new versioned type instead. Add a schema-registry / contract test that replays a fixture of historical events through the current projection. Default all new fields.

---

## 7. Snapshots
**Concept:** Replaying 10,000 events per aggregate on every command is slow. A **snapshot** stores the folded state at version N; replay resumes from N+1.

**Pattern:**
```sql
CREATE TABLE tip_snapshots (
  aggregate_id uuid PRIMARY KEY,
  version int NOT NULL,
  state jsonb NOT NULL,
  taken_at timestamptz DEFAULT now()
);
-- load: snapshot (if version fresh enough) + events after snapshot.version
```
Take snapshots on a threshold (every 100 events, or when replay time exceeds budget). Snapshots are **derived** (rebuildable from events) — never the source of truth.

**Validate:** Loading an aggregate uses the latest snapshot + tail events and yields the same state as a full replay. A corrupted snapshot is detected (version mismatch) and discarded (rebuild from events). Snapshot rebuild from events succeeds.

**Fix:** If command latency grows with event count, add periodic snapshots (threshold-based). Ensure load logic = snapshot-then-tail. Make snapshots rebuildable (never trust them as source of truth — if mismatch, rebuild).

---

## 8. Saga / Process Manager
**Concept:** A long-running business process spanning multiple aggregates/services (tip → confirm on-chain → payout → notify) is coordinated by a **saga**: a state machine reacting to events, issuing commands, and compensating on failure. (Deep skill: `pcmedicalist-saga-pattern`.)

**Pattern:**
```ts
// saga reacts to events, sends next command, compensates on failure
on('TipConfirmed', async (e) => {
  try { await dispatch(new BroadcastPayoutCmd(e.tipId)); }
  catch { await dispatch(new ReverseTipCmd(e.tipId)); }  // compensating
});
```
The saga is itself event-sourced or persisted as state (so it survives crashes mid-process). Each step is idempotent (see `pcmedicalist-messaging-semantics` topic 1). Never use a distributed 2PC for this — use choreography (events) or orchestration (saga) with compensation.

**Validate:** A saga interrupted mid-process (kill the worker) resumes and completes correctly (no half-applied payout, no double-payout). A failed step triggers the compensating command. Replaying the events produces the same final state.

**Fix:** If a multi-step flow is held together by synchronous calls + distributed transactions, extract it into a saga with explicit compensations. Persist saga state. Make every saga command idempotent (idempotency key + unique constraint).

---

## 9. Dual-Write Avoidance (Outbox tie-in)
**Concept:** The classic CQRS/ES failure: write the event to the store **and** publish to the queue in two steps → one succeeds, the other fails → lost event or inconsistent projection. The fix is the **Outbox pattern** (atomic capture + relay), already in `pcmedicalist-database-deep-dive` topic 30.

**Pattern:**
```sql
BEGIN;
INSERT INTO tip_events (...) VALUES (...);             -- event store (source of truth)
INSERT INTO outbox (topic, payload, aggregate_id)
  VALUES ('tip.created', '{"..."}', $1);              -- SAME transaction
COMMIT;
-- SEPARATE relay worker drains outbox -> event bus -> projections (at-least-once)
```
The event store and outbox commit together (atomic). The relay (or CDC, `pcmedicalist-database-deep-dive` topic 29) publishes to the bus; projections consume idempotently. This guarantees: if the event is stored, it will be published (eventually).

**Validate:** Force a crash after COMMIT but before relay → the outbox row remains → relay later publishes → projection updates. No lost events; no event published without being stored. Replay of the event store + outbox drain reproduces the full system state.

**Fix:** If you publish-to-queue inside the same request as the DB write (or after commit without outbox), you have a dual-write bug. Wrap event-append + outbox in one transaction. Build the relay worker. Ensure consumers are idempotent (unique constraint on event id).

---

## Validation Checklist (9 topics)
- [ ] Write model and read model are distinct; no read DTO from command handler (1)
- [ ] State persisted as immutable events; replay reproduces state (2)
- [ ] Invariants enforced in aggregate `execute`; version-checked append (3)
- [ ] Read models are rebuildable projections (delete + replay = identical) (4)
- [ ] Event store append-only, version-unique, never mutated (5)
- [ ] Event schema additive/versioned; old events still project (6)
- [ ] Snapshots derived + rebuildable; load = snapshot + tail (7)
- [ ] Sagas persisted, idempotent, have compensations (8)
- [ ] Event-append + outbox in one transaction; relay drains; consumers idempotent (9)

## Fix Recipes
- **"One entity serves reads + writes badly"** → #1: split command/query models.
- **"No history / audit trail"** → #2, #5: event-source the aggregate.
- **"Business rule violated via API"** → #3: enforce in aggregate.
- **"Dashboard query is slow N+1"** → #4: build a projection.
- **"Old event won't deserialize"** → #6: version + default new fields.
- **"Command latency grows with history"** → #7: snapshots.
- **"Multi-step flow half-completes"** → #8 + `pcmedicalist-saga-pattern`: saga + compensation.
- **"Events lost between DB and queue"** → #9 + outbox (`pcmedicalist-database-deep-dive` #30).

## Cross-References
- `pcmedicalist-event-driven` — async architecture foundation
- `pcmedicalist-saga-pattern` — topic 8 deep dive
- `pcmedicalist-database-deep-dive` — Outbox (#30), CDC (#29), unique constraints (#13) backing idempotency
- `pcmedicalist-messaging-semantics` — at-least-once (#1), idempotency, ordering (#3), partition keys (#5)
- `pcmedicalist-distributed-patterns` — consistency models (#1–2), quorums (#5) for multi-node event stores
- `pcmedicalist-platform-engineering-index` — items #88 (CQRS) and #89 (Event Sourcing) now resolved here
