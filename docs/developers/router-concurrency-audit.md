# Message router — concurrency primitive audit

## Goal

A primitive-by-primitive inventory of `crates/fc-router/src/manager.rs` and
`crates/fc-router/src/pool.rs`, tagging each shared-state primitive as
**essential**, **defensive**, or **optimization** so the question of
"keep Rust vs port to TS" can be made against concrete state rather than
gut feel.

- **Essential** — required by an architectural feature (FIFO ordering,
  dedup, circuit breaker, rate limiting, reconfig draining).
- **Defensive** — safety net for known failure modes (panic recovery,
  receipt-handle expiry, stuck-message reaper).
- **Optimization** — performance shortcut that could be a simpler primitive
  at a measurable cost (DashMap vs RwLock<HashMap>, ArcSwap vs RwLock).

The TS port doesn't need most of these. Single-threaded event loop with
`Map`/`Set` covers what we use DashMap, RwLock, Mutex, AtomicXxx, and
Arc/Weak for here. The Rust verbosity is the cost of multi-threaded
concurrency with the borrow checker enforcing soundness — most of it
disappears in TS.

---

## QueueManager (`manager.rs`)

| # | Field | Type | Tag | Notes |
|---|-------|------|-----|-------|
| 1 | `in_pipeline` | `Arc<DashMap<String, InFlightMessage>>` | essential | Dedup on SQS redelivery. Without it `filter_duplicates` can't swap in a fresh receipt handle when the same broker message reappears. |
| 2 | `app_message_to_pipeline_key` | `Arc<DashMap<String, String>>` | essential | Secondary index — app message id → pipeline key — used by callback drop to clean both maps in one shot. Could be derived but lookup is hot-path. |
| 3 | `pools` | `DashMap<String, Arc<ProcessPool>>` | essential | Active routing table. |
| 4 | `draining_pools` | `DashMap<String, Arc<ProcessPool>>` | essential | Pools removed from config that haven't finished in-flight work. Routed traffic looks at `pools`; cleanup task moves drained ones out. **Consolidation candidate**: one map with `(Arc<ProcessPool>, PoolStatus { Active, Draining })` value. |
| 5 | `consumers` | `RwLock<HashMap<String, Arc<dyn QueueConsumer>>>` | essential | Active queue consumers. |
| 6 | `draining_consumers` | `RwLock<HashMap<String, Arc<dyn QueueConsumer>>>` | essential | Same draining pattern as (4). Same consolidation candidate. |
| 7 | `pool_configs` | `RwLock<HashMap<String, PoolConfig>>` | essential | Last-applied pool configs, for diff during `sync_pools`. |
| 8 | `queue_configs` | `RwLock<HashMap<String, QueueConfig>>` | essential | Same diff role for queues. |
| 9 | `consumer_factory` | `Option<Arc<dyn ConsumerFactory>>` | essential | Used by `sync_queue_consumers` to create new consumers when a queue is hot-added. |
| 10 | `mediator_source` | `MediatorSource` (PerPool / Shared) | essential | Test seam: prod builds one mediator per pool, tests inject a shared mock. Removing this would lose the test mocking story. |
| 11 | `running` | `AtomicBool` | essential | Start/stop lifecycle. Cross-task read of "are we shutting down". |
| 12 | `shutdown_tx` | `broadcast::Sender<()>` | essential | Fan-out of shutdown signal to every spawned background task. |
| 13 | `batch_counter` | `AtomicU64` | essential | Monotonic batch id for tracing & batch-group dedup. |
| 14 | `pending_delete_broker_ids` | `Arc<Mutex<HashMap<String, Instant>>>` | **defensive** | "ACK succeeded but the receipt handle had already expired" recovery. When the same broker message id reappears, we ack immediately. **Inconsistent style** — the only `Mutex<HashMap>` in the manager; the rest are DashMap. Either move to DashMap for consistency, or document why the consolidated single-lock semantics matter here. |
| 15 | `warning_service` | `Arc<WarningService>` | essential | Plumbing. |
| 16 | `health_service` | `Option<Arc<HealthService>>` | essential | Plumbing. |
| 17 | `self_ref` | `parking_lot::RwLock<Option<Weak<Self>>>` | **defensive/optimization** | Exists because `sync_queue_consumers` is reached from call sites that hold `&self`, not `Arc<Self>`, but needs to spawn a task that takes an Arc clone. **Removable** by changing those call sites to `&Arc<Self>` or `self: Arc<Self>` — same refactor we already did for `init_self_ref`/`start`/`spawn_in_pipeline_reaper`. Would delete the field, the RwLock, the upgrade-on-use code, and the "must call `init_self_ref` before `start`" footgun. |

**Plain non-concurrent fields** (config values, not primitives):
`default_pool_code`, `max_pools`, `pool_warning_threshold`, `stall_config`.

### Patterns

- **Active-map + draining-map** (rows 3+4, 5+6) — appears twice for the
  same reason (graceful reconfig). Consolidating to a tagged-value map
  would remove two fields and make the state machine explicit instead of
  implicit-via-which-map-it's-in.
- **Inconsistent map primitive** (row 14 vs everything else) — picks
  `Mutex<HashMap>` where every other key/value store is `DashMap`. The
  rationale comment is sound (no `.await` between lock acquire and
  release, brief critical sections) but the inconsistency reads as
  "different person wrote this part". Pick one.
- **Weak self-reference** (row 17) — present because of one call site
  shape. Mechanical fix.

---

## ProcessPool (`pool.rs`)

| # | Field | Type | Tag | Notes |
|---|-------|------|-----|-------|
| 1 | `mediator` | `Arc<dyn Mediator>` | essential | Plumbing. |
| 2 | `concurrency` | `AtomicU32` | essential | Hot-swap value reflecting the live limit (config can change at runtime via `update_concurrency`). |
| 3 | `semaphore` | `Arc<Semaphore>` | essential | Pool-wide concurrency cap. Cloned into every spawned task to await a permit. |
| 4 | `group_handlers` | `Arc<DashMap<Arc<str>, parking_lot::Mutex<MessageGroupHandler>>>` | essential | Per-group FIFO queue + processing flag. The Mutex inside the DashMap value guards the VecDeque + the `processing` boolean — that's the canonical "lightweight drain task" pattern. Nested primitive is hard to read but each layer earns its keep: DashMap so groups don't contend on a single lock; Mutex on each value because handler state is brief and never `.await`s. |
| 5 | `in_flight_groups` | `DashSet<Arc<str>>` | **defensive/optimization** | Tracks "this group is currently holding a semaphore permit, with a drain task in flight." Overlaps with `group_handlers[id].lock().processing`: `processing == true` means "a drain task exists"; `in_flight_groups.contains(id)` means "and it's holding a permit right now." Used by the panic guard to release `active_workers` correctly. **Consolidation candidate**: fold into the `MessageGroupHandler` as a `holding_permit: bool` and remove the `DashSet`. |
| 6 | `failed_batch_groups` | `Arc<DashSet<BatchGroupKey>>` | essential | BlockOnError dispatch mode: when any message in a batch+group fails, every later message in the same batch+group is fast-nacked instead of delivered out of order. The set is the cascade marker. |
| 7 | `batch_group_message_count` | `Arc<DashMap<BatchGroupKey, AtomicU32>>` | essential | Reference count for the cascade marker — entry is removed from `failed_batch_groups` when count hits 0. Without it the set would leak entries forever. |
| 8 | `rate_limiter` | `Arc<RwLock<Option<Arc<RateLimiter>>>>` | essential | **The worst-reading primitive in the file**: triple-nested. The shape is "shareable handle to a lock guarding an optional shareable rate limiter." It's correct — the outer Arc shares to spawned tasks, the RwLock allows hot-swap, the Option allows "no rate limit", the inner Arc lets a snapshot survive past a swap. **Cleaner**: `arc_swap::ArcSwap<Option<Arc<RateLimiter>>>` — one primitive, lock-free read on the hot path. |
| 9 | `rate_limit_per_minute` | `Arc<RwLock<Option<u32>>>` | **optimization** | Separately tracked u32 used only to check "did the rate limit value change?" during `update_rate_limit`. Could be replaced by reading the current value off the `ArcSwap` from (8) without a second lock. Saves a field. |
| 10 | `running` | `AtomicBool` | essential | Start/stop lifecycle, same role as the manager's. |
| 11 | `queue_size` | `Arc<AtomicU32>` | essential | Queue capacity check on `submit()`, cloned into spawned tasks for decrement. |
| 12 | `active_workers` | `Arc<AtomicU32>` | essential | Metrics + capacity tracking. |
| 13 | `metrics_collector` | `Arc<PoolMetricsCollector>` | essential | Plumbing. |
| 14 | `circuit_breaker_registry` | `Arc<CircuitBreakerRegistry>` | essential | Per-endpoint CB shared across pools. |
| 15 | `warning_service` | `Arc<WarningService>` | essential | Plumbing. |

**MessageGroupHandler** (inside the `Mutex` in row 4):

| Field | Type | Tag | Notes |
|-------|------|-----|-------|
| `high_priority` | `VecDeque<PoolTask>` | essential | FIFO bucket A. |
| `regular` | `VecDeque<PoolTask>` | essential | FIFO bucket B. |
| `processing` | `bool` | essential | "Is a drain task currently active?" — the gate that prevents two drain tasks racing on the same group. |

### Patterns

- **Triple-nested hot-swap** (row 8) — most-cited example of "this is
  hard to audit". `arc_swap::ArcSwap` is the idiomatic replacement.
- **Counter sprawl** (rows 11, 12, plus 2) — three separate
  `Arc<AtomicU32>` / `AtomicU32` that all describe the same pool's
  state. **Consolidation candidate**: one `Arc<PoolCounters>` with the
  three atomics inline, cloned once into each spawned task instead of
  three Arc clones per spawn.
- **State on parallel tracks** (rows 4, 5) — handler-state vs
  in-flight-set. Overlap was documented above.
- **Side state for change detection** (row 9 alongside 8) — the only
  reason `rate_limit_per_minute` exists is to compare against itself.
  Comparison can be done on the live primitive.

---

## What this means in numbers

- **QueueManager**: 17 primitive-bearing fields.
  - Essential: 14
  - Defensive: 1 (`pending_delete_broker_ids`)
  - Defensive/optimization (removable): 2 (`draining_pools`/`draining_consumers` after consolidation pattern, `self_ref` via refactor)
- **ProcessPool**: 15 primitive-bearing fields + 3 inside the per-group
  Mutex.
  - Essential: 12
  - Optimization: 2 (`rate_limit_per_minute` once `ArcSwap` is in,
    counter consolidation removes 2 Arc-wraps but not the atomics)
  - Defensive/optimization: 1 (`in_flight_groups` foldable into the
    handler)

**Realistic post-consolidation count**: ~13 (manager) + ~12 (pool) = ~25
primitive-bearing fields, down from 32. That's not a transformation —
it's noticeable but not "now it reads in 30 min instead of 2 hours."

### Three concrete simplifications worth doing regardless of port-vs-keep

1. **`ArcSwap` for the rate limiter** (`pool.rs` row 8 + 9). One line of
   types changes from triple-nested to a single primitive; lock-free on
   the hot path. Removes one whole field. ~30 minutes.
2. **Active/draining unification** in QueueManager (rows 3+4, 5+6). One
   map keyed by code, value is `(Arc<X>, PoolStatus)`. Makes
   "is this pool draining?" a value check, not a map check. Removes two
   fields. ~1 hour.
3. **Delete `self_ref`** (manager row 17) by promoting the affected call
   sites to `&Arc<Self>` / `Arc<Self>`. Same pattern we already used for
   `init_self_ref`, `start`, `spawn_in_pipeline_reaper`. Removes the
   field, removes the "must call init_self_ref before start" footgun,
   removes the upgrade-on-use codepath. ~30 minutes.

Total: ~2 hours of focused work, removes 4 fields, eliminates the
worst-reading primitive in the file. None of these change behaviour.

### Three larger simplifications worth considering

4. **`pending_delete_broker_ids` style alignment** — move to DashMap to
   match the rest of the manager, or document why the consolidated lock
   matters. Trivial change, comprehension win.
5. **Counter consolidation** in `ProcessPool` — bundle `queue_size`,
   `active_workers` into one `PoolCounters` struct behind one Arc.
   Touches every `tokio::spawn` site in `pool.rs` (clone count drops
   from N to 1 per spawn). ~1 hour.
6. **`in_flight_groups` fold** — move the bit into `MessageGroupHandler`
   as `holding_permit: bool`. Removes a field, removes a `DashSet`,
   makes the per-group state live in one place. ~1 hour.

---

## What this audit *doesn't* tell you

- **Whether you can read the result.** It tells you what would remain to
  be read. The number going from 32 to ~25 (or ~22 with the larger pass)
  is a real but bounded improvement.
- **Whether silent failure modes lurk.** The audit confirms each
  primitive earns its keep but it cannot certify absence of bugs.
  Staging being clean is the evidence we have on that.
- **Whether TS would be easier in practice.** The TS port replaces this
  whole concurrency story with `Map` + `Set` + `Promise.all` + a single
  event loop. Auditability goes up sharply; throughput ceiling goes
  down (single-thread CPU bound, no per-worker mediation parallelism).

## Recommended next step, port-or-keep aside

Do (1), (2), (3) above as one PR (~2 hours). Re-read `manager.rs` and
`pool.rs` after. If at that point the answer to "could I find a stuck
message at 3am here" feels closer to "yes," the Rust path is viable. If
not, the audit doubles as the spec for the TS preservation work — every
"essential" entry above is a feature that must survive the port.
