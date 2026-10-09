# fold: an event-sourcing / DDD database

## The three layers (2026-10-09)

fold runs as three services, each owning one layer of the schema and talking
to the others only over gRPC. The split answers the coupling the single
daemon had grown: one redb file for the log and everything derived from it,
one grammar unit mixing identity, derivation and application, one `Shared`
where the command path waited on projection runners.

| Layer | Service (crate, binary) | Owns | Serves | Hosts |
|---|---|---|---|---|
| **domain** | database (`fold-db`, `fold-dbd`) | contexts, values, enums, events (families, versions, upcast *declarations*), aggregate identity (key, stream template, event list) | `Log` (Append, ReadStream/All/ByType, StreamHead, ListStreams, LookupIdempotencyKey, SubscribeAll, Replicate), `Cluster` (Health, Promote, Fence, RequestVote, RenewLease), `Backup`, `Schema.GetSchema` | the log only: no wasm, no derived store (CI checks `cargo tree -p fold-db`) |
| **derivation** | derivation node (`fold-derive`, `fold-derived`) | `state Ctx.Agg {..} evolve ..`, `projection Ctx.P {..}`, snapshots | `Query`, `Aggregate.GetAggregate`, `DeriveAdmin` (schema, projections, snapshots, rebuilds, health), `Derive` (GetState, Evolve, GetRow, WaitCheckpoint, Upcast: the application node's view) | the evolve, fold and upcaster exports; a `DerivedStore` |
| **application** | application node (`fold-app`, `fold-appd`) | `commands Ctx.Agg {..}`, `invariants Ctx.Agg {..}`, `invariant Ctx.Name {..}`, `process Ctx.Name {..}` with timers | `Command` (Execute, Append under the invariants), `AppAdmin` (schema, processes, snapshots, rebuilds, health with the layer check) | the handler, check and react exports; a `DerivedStore` for the process managers' tables |

**Schema.** Every file starts with `layer domain | derivation | application`
after its docs and imports files of its own layer or below (S060). A domain
file holds contexts, whose aggregates are identity only (`key`, `stream`,
`events`, local values/enums/entities). The upper layers declare against the
domain by qualified name: `state Orders.Order { fields } evolve wasm ".."
[snapshot every N]`, `projection Orders.OrderTotals { from .. fold .. table .. }`,
`commands Orders.Order { Cmd {..} [requires ..] -> wasm .., .. }`,
`invariants Orders.Order { Name -> wasm .. | Name: expr, .. }`, `invariant
Orders.MaxOpenOrders { on .. projection .. scope .. check .. }`, `process
Orders.Fulfilment { .. timers .. }`. A declaration outside its layer is S058;
S061 a root of the wrong layer for a service; S062 an unknown context, S063 an
unknown aggregate, S064 a second `state`, S065 commands or invariants for an
aggregate without a state, S066 a duplicate block. The models chain by `Arc`
and `Deref` so a service's type says what it may know: `DomainSchema`
(contexts, lookups, validation, row operations), `DerivationSchema { domain,
states, projections }`, `ApplicationSchema { derivation, commands,
invariants, processes }`; `Sources::compile()` yields the `Compiled` layer of
the root, `compile_domain/derivation/application` the layer a service needs
from any root at or above it. The diff is partitioned the same way
(`diff_domain` with the log's facts, `diff_derivation`, `diff_application`;
`Action::layer()`), and each node checks and applies its own layer at start.
The legacy single-file schema is gone; the example is
`examples/orders/{domain,derive,app}.fold`.

**Storage.** `fold-core` is the log alone: segments, the log's redb tables,
idempotency keys, epoch and votes, the schema text, and a **generation** with
a **cut** position bumped by every truncation or restore. `fold-store` is the
derived store a derivation or application node keeps (`derived.redb`:
checkpoints, `rm:` tables, instance snapshots, `meta` with the log id, the
generation and the schema text). Checkpoints and snapshots carry the id of
the last event they include, so a log that moved backwards is detected:
`SubscribeAll{from_position, last_event_id}` answers "diverged" like
`Replicate` does, a changed generation in the first `LogStatus` of a
subscription runs `reset_past(cut)` once, a changed log id rebinds the store
from scratch, and the node's health says so. Backups hold the log only
(format 2; format-1 archives restore with their derived tables skipped).

**Protocol.** Four packages in `fold-proto`: `fold.common.v1` (events,
expected versions, rows, `RunnerState`, snapshot and rebuild messages, the
schema bundle with its layer and sha256), `fold.database.v1`,
`fold.derivation.v1`, `fold.application.v1`. Position tokens
(`fold1:<log_id>:<epoch>:<position>`) are issued by the database and
checked by the derivation node against the log it is bound to. `Fold.*`
events (process timers) are accepted by `Log.Append` only with the system
token (`fold-system-token`, the shared secret; the database compares its
sha256 in constant time) that the application node holds; a database
without a secret refuses them all.

**The command path** (`fold-app`): resolve and canonicalize → layer check
and the database's role gate → per-stream lock → `Log.LookupIdempotencyKey`
→ `Derive.GetState` at least at the version this node last appended →
guards → the handler → `Derive.Evolve` for the candidate state → state
invariants → context invariants (per-scope locks, `Derive.WaitCheckpoint`,
rows through `Derive.GetRow`) → `Log.Append` with `Exact(version)` or
`NoStream`, the idempotency key and the fencing token. A version conflict
retries once from the database's version; a duplicate key is
AlreadyExecuted. Scope locks are in-process: **one application node** is
assumed for cross-stream invariants, and its health says `invariants:
single-node`. Process managers tail the database, react through
`Derive.Upcast` for old versions, dispatch through the in-process execute
with `pm:` keys, fire timers through `Log.Append` under the system token,
act only while the database is a primary, and wait for the layer check
before issuing anything.

**The layer check.** At start and every 30 s the application node fetches
the database's and the derivation node's bundles and compares them with its
imports (`diff_domain` without breaking changes; `diff_derivation` without
derivation-layer changes). `Execute` is UNAVAILABLE until the check passes
and FAILED_PRECONDITION naming the difference on a mismatch.

**The composite** (`foldd`). One process hosts the three nodes and serves
every service on one address. The database and the derivation node also
listen on a loopback port of their own, used by the node above while it
opens (the derivation node asks the database for its log id and bundle as
it opens; the application node the same) and afterwards, so the paths
inside the composite are the deployment's. Under `--data-dir` the log is
`default/` (unchanged from a standalone database), the derivation node's
store `derive/`, the application node's `app/`; `--schema` names the
application file. A system secret is generated per start unless given.
The public address is served only after the application node's layer
check has passed, and `start` returns then: a client that reaches the
composite, after a start or after the supervisor's restart for a live
restore, finds every layer ready rather than the database alone (the
loopback listeners the check itself uses serve from the moment each node
opens). The `Supervisor` restarts all three on the pinned address after a
live restore, and the two upper nodes reset past the cut on the next
status. The crate ships `foldd`, `fold-dbd`,
`fold-derived` and `fold-appd`.

**The CLI** routes by layer: `--addr` names the composite, `--db`, `--derive`
and `--app` the services of a split deployment. `exec`, `append`, `log
process`, `process *` go to the application node (`append --unguarded` to
the database, past the invariants); `query`, `log aggregate`, `projection *`,
`aggregate *` to the derivation node; `log read/all/tail`, `promote`,
`fence`, `backup`, `backups`, `restore --live` to the database; `health`
asks all three; `schema show --layer` picks the node.

**Tests.** Each service crate has a suite over its own node: `fold-db`
(appends, reads, subscriptions, replication, fencing, elections, leases,
backups), `fold-derive` over a database node (reads, tokens, aggregate
state, resets, schema changes, snapshots), `fold-app` over both (commands,
guards, concurrency, processes, timers, the refusals). `foldd`'s suite runs
the cross-layer behaviours against the composite (orders, processes, timers,
guards, concurrency, aggregates, snapshots, upcasts, schema changes, restore,
replica composites, the boundaries) and the three binaries as processes;
`fold-cli` drives `fold` against an in-process composite.

**Not in this iteration.** Database-held scope leases for several
application nodes; repointing a derivation node after a database failover
(`--database` is one URL); old `.fsnap` files without `last_event_id` are
usable only with `force`.

The rest of this document is the vertical-slice plan the project started
from and its first two iterations; where it says "the daemon" or `foldd`
did something, that work now lives in the service named above, and the
`fold.v1` protocol it describes was replaced by the four packages.

## The vertical-slice plan

### Context

A new project at `/Users/c/projects/fold` (not yet created). **fold** is a database
built on event sourcing and domain-driven design: a user creates named **logs**,
declares a domain in a schema file (bounded **contexts**, **events**, **values** (value
objects), **aggregates** containing **entities** and values, **projections**), and the
database stores the events, validates them against the schema, and runs the
projections itself as WASM modules. The name is the fold that turns a sequence of
events into state.

Decisions already made with the user:

| Decision | Choice |
|---|---|
| Name | `fold`; crates `fold-core`, `fold-schema`, `fold-wasm`, `fold-guest`, `fold-proto`, `foldd`, `fold-cli` (binary `fold`) |
| Language / layout | Rust workspace, edition 2024, `crates/`, toolchain pinned 1.98.0, MIT, nextest in CI (mirrors `/Users/c/projects/sqex`) |
| Shape | core library + daemon `foldd` + CLI `fold` |
| Wire | gRPC over HTTP/2 via tonic 0.14 (+ `tonic-prost`), system `protoc` |
| Domain definition | own small DSL (`.fold` files); command/fold logic in WASM (wasmtime, core-module ABI, JSON over linear memory) |
| Storage | own append-only segment files for the log; redb 2 for indexes, checkpoints, read models |
| Fold row access | host import `fold.get_row(table, key)`; the guest returns upsert/delete mutations |
| Stream id | derived from the aggregate's `stream "order-{order_id}"` template and enforced on append |
| Validation | strict; unknown payload fields rejected |
| Aggregate state | each aggregate declares a `state` type and an `evolve` WASM function `(state?, event) -> state`; state is **snapshotted** in redb every N events and **cached** in memory; loading = latest snapshot + replay of the events after it |
| Aggregate internals | an aggregate declares **entities** (`entity Line { id line_id: uuid, ... }`: identity within the aggregate, mutable over time, never visible outside it) and **values** (`value`: immutable records compared structurally, declared at context level to share or inside the aggregate to keep local). An entity held in a `map<K, E>` is keyed by its own id; the database enforces it |
| Cross-aggregate projections | a projection's `from` may list event families from any aggregate or context; it sees them in global log order and may join across its tables with `get_row` |
| Collection primitives | read-model columns may be `set<T>`, `list<T>` (`[T]`) or `map<K, V>`; folds mutate them with typed column ops (`set_add`, `set_remove`, `list_push`, `list_truncate`, `map_put`, `map_remove`, `add`, `set`) that the host applies atomically, so a fold rarely needs to read and rewrite a whole row |
| Invariants | rules every command must respect, declared in the schema and checked in WASM before anything is appended. **State invariants** live in an aggregate (`invariants LinesNotEmpty -> wasm ...`) and see the state the command would produce. **Context invariants** live in a context (`invariant MaxOpenOrders { on Order  projection CustomerOrders  scope customer_id  check wasm ... }`) and read a projection; the daemon serializes commands per scope value and catches the projection up to the log head first, so the rule holds under concurrency. Raw appends are checked too |
| Value rules | a value may end with `rules { Name: expr, ... }`; every rule is checked wherever an instance of the value is created: event payloads, commands, aggregate and process state, entity fields, read-model rows. Expressions compare fields (descending through nested values) with literals or each other, `len(field)`, `field matches "regex"`, `field in [...]`, combined with `and`/`or`/`not`. A violation is a validation error naming the path, the value type and the rule |
| Log backups | `Admin.BackupLog` writes one archive (`FOLDBKUP`): every index table dumped from one read transaction (which fixes the head), then the LOG identity, the schema, every segment file and the snapshot files, with a crc32 trailer. `fold restore <archive> <dir>` is offline: it writes a fresh log directory, rebuilds the index from the dump, verifies the checksum before creating the index, and opens the result once; recovery trims any record a segment carried past the archived head. `ListBackups` lists the default backups directory |
| Online restore | `Admin.RestoreLog(path)` validates the archive and hands it to the daemon's supervisor loop (`foldd::Supervisor`, what the binary runs): the daemon stops serving, moves `<data>/default` aside as `default.replaced-<time>` (kept, never deleted), restores the archive in its place, and starts again on the same address; on failure the previous log is moved back. The call returns on acceptance; `Health` then reports `log_id` and `last_restore`. An unsupervised daemon (the library's `start` alone) refuses the request |
| Scheduled backups | `foldd --backup-every 6h --backup-keep 7` (or `[backup] every = "6h", keep = 7`) runs a task that, on each tick, backs up into the log's backups directory if the head moved since the newest backup there, then prunes to `keep` (newest first; 0 keeps all). `ListBackups` reports the schedule, its last run, last head, last error and next run |
| Incremental backups | `Admin.BackupLog(incremental)` writes an archive of kind `Incremental` holding only the records from the newest backup's head to the current head (raw segment frames for `[base_head, head)`), the idempotency keys first used in that range, and the schema; its header carries `base_head` and the log id. It is not a restore point: `fold restore <inc> <dir>` refuses it, and `fold restore <inc> <dir> --apply` (`fold_core::apply_backup`) appends it onto the log at `<dir>` only if that log has the same id and its head equals `base_head`, so increments chain and a repeat or a gap is refused before anything is written. Checkpoints, read models and aggregate snapshots stay at the base; the runners catch up on start and the carried idempotency keys stop a process manager from re-issuing. `--backup-incremental` (with `--backup-full-every N`, default 24) makes the schedule write a full backup first, increments after, and a full every N backups; pruning keeps `keep` fulls and every increment that chains from a kept full. A live restore refuses an increment |
| Point-in-time restore | every restore path takes a position `to` or a time `at`: `fold restore <archive> <dir> --to N` / `--at <RFC 3339>`, `--apply --to N` (within the increment's range; a time may land anywhere) and `--live --to N` / `--at ...` (`RestoreLogRequest.to` / `at_unix_nanos`). A time keeps every batch recorded at or before it: `Log::position_after` binary-searches the positions for the first event recorded later (a batch carries one timestamp and timestamps follow append order) and `truncate_log_at` cuts there; the resolved position is what `Health.last_restore` reports. The log is restored and then cut by `fold_core::truncate_log` so its head is `N` and it holds exactly the events below `N`; `N` must be a batch boundary (an append is atomic), otherwise the error names the batch and both of its edges. The cut drops, in one index transaction, the positions, stream entries and type-index entries at or past `N`, recomputes the stream heads, drops idempotency keys first used at or past `N`, resets every projection and process whose checkpoint is past `N` (its read-model tables go; it rebuilds from scratch on start), drops aggregate snapshots at a version the stream no longer reaches, and sets the head; then it truncates the segment holding `N`, removes the later segments and the snapshot files named by a later checkpoint. A reset process replays from scratch and, its keys gone, re-issues the commands whose effects were cut |
| Read-your-writes on replicas | every write returns a **position token** (`ExecuteResponse.token`, `AppendResponse.token`): `fold1:<log_id>:<epoch>:<position>`. `Query.Get/Scan` take it as `token` (or the bare `min_position`, as before): the read side checks the token is of its own log (INVALID_ARGUMENT otherwise), then waits for the projection's checkpoint to reach the position, bounded by `wait_ms`. On a replica that means waiting for replication *and* projection, since a checkpoint cannot pass a position the log has not received; the timeout message says which of the two is behind (the projection's sampled head tells whether the position has reached this daemon at all). The token carries the log id so a client that mixes clusters is told, and the epoch for the record |
| Session consistency | every read hands back the state it saw: `GetResponse.token` (and, for `Scan`, the response's initial metadata `fold-session`) is a position token at the projection's checkpoint. A client that passes its latest token as `token` on the next read, on any member, gets a read at least that far along, so its reads never go backwards even across a primary and several replicas; a write's token and a read's token are the same kind, so one session value covers read-your-writes and monotonic reads. The CLI keeps the session in a file: `fold --session <file>` (or `FOLD_SESSION`) sends the file's token with every read and advances it with every read or write, keeping the furthest position of the log it is on and replacing it when the client moves to another log |
| Replication | `foldd --replicate-from http://primary:4141` (or `replicate_from` in the config) runs a read-only replica. At start it asks the primary's `Health` for the log id and either creates an empty log with that identity (`Log::create_with_id`) or checks the one it has (another log's, or one ahead of the primary, is refused). It then tails `Log.Replicate(from = local head)`: a server stream of `ReplicationChunk`s, each the raw segment bytes of `from..to` (about 1024 records, rounded to a batch boundary; a longer batch goes whole) plus the idempotency keys first used in that range, then live via the log's watch. The replica appends a chunk through the incremental-backup path (`Log::apply_replication_chunk`: same identity, starts at the head, committed batch by batch), so ids, timestamps, versions and CRCs are the primary's. Its `Command` service answers FAILED_PRECONDITION; its projections run on the replicated events and serve `Query`; its process managers react and fill their outboxes but do not dispatch. The tail reconnects with a backoff; `Health` reports `role`, `replicating_from`, `replica_connected`, `primary_head` and the last error. Promotion is `Admin.Promote` (`fold promote`), in place: the tail task is cancelled and awaited (so no chunk is half-applied), a `promoted` marker is written in the log directory, the role flips (`Shared::is_replica` is an atomic, read by the write side and the process runners), and every process manager gets a `Control::Drain` so the outbox entries it held are dispatched now and found already executed through the replicated keys. A restart without `--replicate-from` does the same; a restart that still says `--replicate-from` is refused by the marker, since a promoted log may be ahead of its old primary. `Health` reports `promoted_from` and the promotion note. **Automatic failover** is opt-in: `--auto-failover 30s` (`auto_failover` in the config; refused without `replicate_from`) makes the tail promote itself once the primary has been out of reach for that long without a break. "Out of reach" is a connect or stream failure, or, while the stream is open, a `Health` probe every grace/3 that fails or times out, so a primary that holds the connection but no longer answers counts as gone. The reconnect wait is capped at grace/4 so the deadline is noticed promptly. `Health` shows `auto_failover_secs` and, while it lasts, `primary_unreachable_secs` |
| Quorum | `--quorum-peers http://a:4141,http://b:4141` (`quorum_peers` in the config; needs `auto_failover`) names the other cluster members, primary included. When the grace period passes, the replica holds an **election** instead of promoting outright: it proposes an epoch (one past its own, its last vote, and any vote a peer reported), votes for itself durably (`META.voted_epoch`), and asks every peer `Admin.RequestVote` in parallel with a 3 s timeout. A voter grants once per epoch, only if it is not a primary, the epoch is newer than its own and than anything it voted for, the candidate's head is at least its own, and the primary does not answer `Health` from the voter within 1 s; a grant is persisted before it is returned. The candidate needs a majority of peers plus itself (3 of 5, 2 of 3; with 2 members no automatic failover is possible); any peer that still reaches the primary loses the round outright. A lost round is retried on the next pass with a higher proposal; `Health` shows `quorum_size` and `last_election`. No peers is a quorum of one, which is what `auto_failover` alone means, and start-up says so. A requested `Promote` ignores the quorum: it is the operator's call |
| Leader leases | `--lease 5s` (`lease` in the config; needs `quorum_peers`) makes a primary serve reads (`Query.Get/Scan`, `Log.GetAggregate/GetProcess`) only under a lease: every third of the duration it asks each peer `Admin.RenewLease(epoch, duration)`, and with a majority of peers plus itself it holds the lease until *send time* plus the duration. A peer grants unless it knows a newer epoch, voted in one, or is a primary itself; it remembers the grant and `RequestVote` denies every candidate until it ends, so no one is elected while the old primary may still be serving reads on its lease. Without a lease a read is UNAVAILABLE with the reason; a fenced daemon refuses reads with FAILED_PRECONDITION; a replica serves reads regardless (its contract is eventual consistency). Raw log reads (`ReadAll/ReadStream/SubscribeAll/Replicate`) are not gated: replicas and integrations need them, and they say what position they are at. Writes are not gated by the lease either; fencing is what stops a stale primary writing. `Health` shows `lease_secs`, `lease_held`, `lease_remaining_ms` and `lease_error`. The read gate (`state::ReadGate`: role and lease) is the one thing the Query service shares with the rest of the daemon; it holds no log |
| Fencing | the log carries an **epoch** (`META.epoch`, 0 when created, in backups; `Log::epoch/set_epoch`). A replica adopts its primary's epoch from every chunk; a promotion sets `epoch + 1`. A daemon has a role: primary, replica or **fenced**. Writes may carry a `fencing_token` (the epoch the client read from `Health`): equal passes, stale is refused, and a newer one proves a newer primary exists, so a primary that receives it **fences itself** (a `fenced` marker in the log directory, kept across restarts; role `fenced`; every write refused; process managers hold) and refuses that write. `Admin.Fence(epoch)` (`fold fence N`) does the same by request, and a promotion spawns a fencer that keeps calling it on the old primary with a backoff until it acknowledges, so an old primary that was merely slow or partitioned stops as soon as it can be reached; `Health` shows `epoch`, `fenced_by` and `old_primary_fenced`. The way back for a fenced log is to become a replica of the new primary: `replica::prepare` checks that the histories agree (the last local event's id equals the primary's at that position, and `Log.Replicate` checks the same on every connection), removes the marker, and tails; a log that took writes of its own is refused as diverged |
| Projection snapshots | `Admin.SnapshotProjection` writes every row of a projection's tables at its checkpoint from one read transaction into `<log>/snapshots/<Ctx.Projection>/<checkpoint>.fsnap` (checksummed, with the fold module's hash); `RebuildProjection` resets the tables and checkpoint and replays from scratch or from a snapshot; `ListSnapshots`/`DeleteSnapshot` manage them; `projection X { ... snapshot every N ... }` takes them automatically. Requests go through the runner's control channel so a snapshot never races a rebuild |
| Aggregate snapshots | the same RPCs accept an aggregate name: `SnapshotProjection` exports every instance snapshot (stream → version, module hash, state) to a file; `RebuildProjection` drops the instance snapshots and the cache, restores a file if given, then loads every instance of the aggregate from its events so each is re-evolved by the current module and re-snapshotted; it returns when that is done |
| Process snapshots | the same snapshot and rebuild RPCs accept a process name: the file holds its `state` and `outbox` tables. Outbox ids are derived from the triggering position (`<position>-<idx>`, rejections `<parent>-r-<idx>`), so a replay after a rebuild derives the same idempotency keys and every already-executed command is skipped rather than re-issued. `process X { ... snapshot every N }` snapshots automatically |
| Process managers | `process Name { key field  from Event [by field], ...  state {...}  react wasm ... }` declared in a context. A runner per process follows the log; for each event it declared, it loads the instance keyed by the correlating field, runs `react` (state in, state + issued commands out), and commits state, outbox and checkpoint in one transaction. Outbox entries are executed through the normal command path with an idempotency key derived from the entry id, so a crash-retry finds the command already applied. A refused command returns to the instance as a `rejected` trigger; a failed one is retried with backoff |
| Doc comments and `fmt` | `///` attaches to the next declaration, field, variant, rule, command, invariant, table or column and reaches the model (`docs: Vec<String>`; `schema check` shows the first line, `--json` all of them); `//!` at the top of a file documents the schema. `fold schema fmt [--check]` rewrites files in canonical layout keeping every `//` comment by position (an AST round-trip proptest and a comment-survival proptest guard it) |
| Enums with payloads | `enum Status { Pending, Shipped { carrier: string } }`: a variant may carry a record. JSON is externally tagged: a unit variant stays the string `"Pending"` (unchanged for every existing schema and guest), a payload variant is `{"Shipped": {"carrier": "DHL"}}`. A payload is a record like a value: context-level enums follow context-value placement, aggregate-local ones entity placement; enums with payloads join the cycle check; rules compare enums by variant name, written bare (`status == Pending`, `in [Pending, Paid]`, S053 for a name that is no variant) |
| Field defaults | `qty: uint = 1`, `status: Status = Pending` on required scalar and enum fields only (S043–S045). An absent or `null` field takes its default during validation, on every write path (events, command payloads, evolved and reaction state, rows), and on the read path for records stored before the default existed (`Schema::apply_defaults` in `to_guest_event`). A field with a default never needs an upcast op; a version whose only changes are added optional or defaulted fields has an implicit upcast |
| Imports | `import "rel.fold"` before the contexts, relative to the importing file, same path rules as wasm paths (S047); each file loaded once (cycles and diamonds fine); the root's contexts first, then each import depth-first; S010 across files names the file. Wasm paths in an imported file are rebased onto the root's directory at compile time (`sub/b.fold` + `"m.wasm"` → `"sub/m.wasm"`), so nothing downstream changes. The log stores a **bundle**: the root text verbatim for one file, else each file after a `// ---- file: <path>` line; `Sources::from_bundle` turns it back into the same files, so the stored text compiles to the same model (`GetSchema` serves the bundle; `compile(text)` refuses imports with S046). Diagnostics render as `file:line:col` |
| Event upcasting | `event E v2 {..} upcast from v1 { set f: v, rename a as b }` or `upcast from v1 wasm "m" [export "e"]` (default export `upcast_<Event>_v<N>`, guest `fold_guest::upcast!`, ABI `{abi, event: {type, from_version, to_version, payload}} → {payload} | {error}`). Fields of the same name and type carry over, target-only optional fields become `null`, source-only fields are dropped; a declarative result is checked statically against v2 (S051); every version after the first needs an explicit or implicit upcast from its predecessor (S048–S052). `foldd::upcast::to_latest` applies the chain in version order (canonicalising each step), and every consumer — projections, processes, aggregate evolve and replay, the candidate state on commit — sees the latest version; raw reads, replication and backups keep the stored version; handlers must emit the latest version (INTERNAL otherwise), raw `Append` may write any declared one (the migration path); upcaster exports are checked at start |
| Declarative guards | `invariants Name: expr` beside `Name -> wasm` (paths from the state's fields), and commands take `requires { Name: expr, .. }` or a bare `requires expr` (named `Requires`) with paths rooted at `state.` or `command.` and a `state exists` term (S054, S055). An absent *required* operand makes a comparison false, an absent optional one still vacuously true, so `state.status == Pending` on a new stream fails and `not state exists or ..` works. Declarative state invariants run in `commit()` with the wasm ones, on commands and raw appends; `requires` runs after the state is loaded, before the handler. Rejection code = the guard's name, `fold-invariant` = `Ctx.Agg.Name` / `Ctx.Agg.Cmd.Name`, message = the expression text (plus "(the stream has no state yet)") |
| Schema compatibility | `fold_schema::diff(old, new)` classifies every change as compatible, needing a rebuild, or breaking, each with an `Action` (rebuild or drop a projection/process/table, clear aggregate snapshots, drop a timer); the diff destructures every model struct exhaustively so a new field must be classified. Principle: stored data must still fit (else breaking); derived data is rebuilt; removed derived things are cleaned up; removing an event or aggregate breaks only if the log holds its events or streams (`Facts`; offline `AssumeData`). At start, a file that differs from the stored text is diffed against the log: breaking is refused with the diff and a hint unless `--force-schema` (a flag, never a config key); otherwise the actions run before any runner starts and the new bundle is stored; `Health.last_schema_change` says what happened; a stored text that no longer compiles is refused the same way. A replica refuses a schema that breaks against the primary's. `fold schema diff old new` (files or bundles) prints the classification and exits 1 on breaking |
| Process timers | `process P { .. timers A, B }` declares names; a reaction returns `timers: [{name, after_ms | at}]` and `cancel_timers: [name]` (`Reaction::set_timer/cancel_timer`, `SetTimer::after/at`); each timer is a row of the process's `timers` table (one per instance and name) written in the reaction's transaction, due at the trigger's recording plus the delay, so a replay derives the same deadline; an ended instance keeps none; an undeclared name fails the process. On the **primary** the runner fires a due timer by appending `Fold.TimerFired@v1 {process, instance, name, due_at}` to `fold-timers-<Ctx.Proc>` under the idempotency key `timer:<proc>:<instance>:<name>:<due>`; the reaction is driven by that event (`Trigger::Timer {name, due_at, fired_at}`), so replicas, rebuilds and a promoted replica derive the same state and never double-fire (a promotion's `Drain` fires what is due); a fired event whose row is gone or due at another time is ignored. `Fold` is reserved: clients cannot append to it. `ProcessStatus.pending_timers` counts what is set |
| CQRS | the API is segregated: a **Command** service (execute a declared command against an aggregate; raw `Append` as the escape hatch), a **Query** service (read models only, with a read-your-writes position token), a **Log** service (event reads and subscriptions, for integration and debugging) and an **Admin** service. Aggregate state is never a query result for application code |
| First milestone | thin vertical slice: execute a command over gRPC → handler emits events → validate → fold in WASM → query the read model via CLI, read-your-writes |

Decisions made during design (no user input needed): positions and versions 0-based;
CRC-32/IEEE via `crc32fast`; `recorded_at` unix nanos i64; payloads JSON with a
3-bit encoding tag reserved; schema immutable per log in the slice; one redb txn per
projection batch; guest SDK crate `fold-guest` shipped in-workspace; default listen
`127.0.0.1:4141`, env prefix `FOLD_`; snapshots are taken lazily on load (not on every
append) and default to `every 100`; command handlers receive `state`, not history;
commands on one stream are serialized by a per-stream lock in the daemon so
load → handle → append is atomic; collections are stored inline in the row's JSON with
a 1 MiB row limit (a wide layout keyed by element is a later addition); `add` covers
`int`, `uint` and `decimal` (decimal arithmetic via `rust_decimal`, verify at
implementation time); event-id dedupe, hot reload, separate query nodes,
membership queries that avoid fetching the row: **out of scope** (upcasting was, and
is now in: see the second DSL iteration below). The crate is not
being published, so a `fold` name on crates.io does not matter.

### CQRS, as the database enforces it

- **Write side**: a service sends `Command.Execute(aggregate, stream, command payload)`.
  The daemon validates the payload against the command's declared fields, loads the
  aggregate state (snapshot + replay + cache), runs the WASM handler, validates every
  emitted event against the schema and the aggregate's event list, and appends with
  `Exact(version)`. The response carries the new events and their positions. `Append`
  exists for migrations and tests and is the only way to write without a handler.
- **Read side**: a service reads only projections, through `Query.Get` / `Query.Scan`.
  A query may carry `min_position` (the position a command returned): the daemon waits
  until that projection's checkpoint reaches it (bounded by `wait_ms`, max 30 s) and
  otherwise answers `UNAVAILABLE` with the current checkpoint. That is the only
  consistency bridge between the two sides; everything else is eventually consistent.
- **Not a query**: aggregate state (`Log.GetAggregate`) and raw event reads live on the
  Log service so a client cannot mistake them for a read model. They are for
  integrations (other systems subscribing), debugging and tests.
- In the daemon the two sides are separate modules (`command.rs`, `query.rs`) joined
  only by the log and the projection runners, so query serving can later move to a
  replica that tails the log.

Only `redb 2.6`, `crc32fast 1.5`, `uuid 1.26`, `jiff 0.2`, `indexmap 2.14`, `wat 1`
are confirmed in the local registry. Verify `tonic`/`tonic-prost`/`prost` 0.14,
`wasmtime` 41+, `base64` 0.22 with `cargo add` at implementation time; `protoc` 34 and
`cargo-nextest` are installed; the `wasm32-unknown-unknown` target is **not** (the
toolchain file will pull it).

## Workspace

```
fold/
  Cargo.toml  rust-toolchain.toml (1.98.0, clippy, rustfmt, targets=[wasm32-unknown-unknown])
  LICENSE  README.md  .gitignore  .github/workflows/ci.yml
  crates/
    fold-schema/    DSL: lexer, parser, resolver, the three layer models, JSON validator, formatter, diff
    fold-core/      the log: segments, redb index, append/read/subscribe, generation and cut
    fold-store/     the derived store: checkpoints, rows, instance snapshots, fingerprints
    fold-host/      daemon code the upper nodes share: codecs, .fsnap files, upcasting, guest linking, runner types
    fold-wasm/      wasmtime host: engine, module cache, the guest ABI, limits
    fold-guest/     guest-side SDK
    fold-proto/     the four packages + tonic-prost-build codegen
    fold-db/        the database service
    fold-derive/    the derivation service
    fold-app/       the application service
    foldd/          the composite; binaries foldd, fold-dbd, fold-derived, fold-appd; the cross-layer suite
    fold-cli/       binary `fold`
  examples/orders/
    domain.fold  derive.fold  app.fold
    guest/          crate orders-guest, cdylib → orders_guest.wasm
```

`fold-schema` and `fold-core` do not depend on each other; the services
compose them. `fold-db` depends on neither `fold-wasm` nor `fold-store`.
Profiles copied from sqex (`lto = "thin"`, `strip`), plus
`[profile.release.package.orders-guest] opt-level = "s"`.

## 1. `fold-schema` — the DSL

**Parser**: hand-written lexer + recursive descent (≈20 productions, no precedence;
best error messages with spans; zero deps).

Grammar (commas separate fields, trailing comma ok, `//` and `/* */` comments):

```
File       = { InnerDoc } "layer" ("domain"|"derivation"|"application") { Import } { Context | TopItem } ;
TopItem    = StateDecl | ProjectionTop | CommandsDecl | InvariantsDecl | InvariantTop | ProcessTop ;
                                                  // contexts in domain files only; each TopItem in its layer's
                                                  // files, naming its context or aggregate: `state C.A {..}`,
                                                  // `projection C.P {..}`, `commands C.A {..}`, `invariants C.A {..}`,
                                                  // `invariant C.N {..}`, `process C.N {..}` (see the top section)
InnerDoc   = "//!" text ;                         // file docs, only at the top
Import     = "import" String ;                    // relative to this file; no "..", no ":"
Doc        = "///" text ;                         // attaches to the declaration, field, variant, rule,
                                                  // command, invariant, table or column that follows
Context    = {Doc} "context" Ident "{" { Value | Enum | Event | Aggregate | Projection
                                        | Invariant | Process } "}" ;
Value      = {Doc} "value" Ident "{" Fields "}" ["rules" RuleBlock] ;
RuleBlock  = "{" Rule {"," Rule} [","] "}" ;      Rule = {Doc} Ident ":" Expr ;
Enum       = {Doc} "enum" Ident "{" Variant {"," Variant} [","] "}" ;
Variant    = {Doc} Ident [ "{" Fields "}" ] ;     // a payload: a record, at least one field
Event      = {Doc} "event" Ident "v" Integer "{" Fields "}" [Upcast] ;
Upcast     = "upcast" "from" "v" Integer ( "{" [UpcastOp {"," UpcastOp} [","]] "}" | WasmRef ) ;
UpcastOp   = "set" Ident ":" UpcastValue | "rename" Ident "as" Ident ;
UpcastValue= Literal | "null" | "[" [UpcastValue {"," UpcastValue}] "]"
           | "{" [Ident ":" UpcastValue {"," Ident ":" UpcastValue}] "}" ;
Fields     = [ Field {"," Field} [","] ] ;
Field      = {Doc} Ident ":" Type ["=" Literal] ; // a default: required scalar or enum fields only
Type       = BaseType ["?"] ;
BaseType   = Scalar | TypeRef | "[" Type "]" | "list" "<" Type ">"
           | "set" "<" Scalar ">" | "map" "<" Scalar "," Type ">" ;
Scalar     = string|int|uint|decimal|bool|uuid|timestamp|bytes ;
TypeRef    = Ident ["." Ident] ;                  // Money | Shared.Money
Literal    = Number | String | "true" | "false" | Ident ;   // a bare Ident is an enum variant
WasmRef    = "wasm" String ["export" String] ;
Aggregate  = {Doc} "aggregate" Ident "{" "key" Field  "stream" String
               { Value | Enum | Entity }             // aggregate-local types
               "events" EventRef {"," EventRef}
               "state" "{" Fields "}"  "evolve" WasmRef
               ["snapshot" "every" Integer]          // default 100; 0 = never
               ["commands" Command {"," Command}]
               ["invariants" InvariantRef {"," InvariantRef}] "}" ;
Entity     = {Doc} "entity" Ident "{" "id" Field { "," Field } "}" ;
Command    = {Doc} Ident "{" Fields "}" [Requires] "->" WasmRef ;
Requires   = "requires" ( RuleBlock | Expr ) ;    // a bare Expr is the guard named `Requires`
InvariantRef = {Doc} Ident "->" WasmRef | {Doc} Ident ":" Expr ;
Invariant  = {Doc} "invariant" Ident "{" "on" Ident "projection" EventRef "scope" Ident
               "check" WasmRef "}" ;
Projection = {Doc} "projection" Ident "{" "from" EventRef {"," EventRef}   // any context
               "fold" WasmRef ["snapshot" "every" Integer] Table {Table} "}" ;
Table      = {Doc} "table" Ident "{" ["key"] Field {"," ["key"] Field} [","] "}" ;
Process    = {Doc} "process" Ident "{" "key" Field  "from" Source {"," Source}
               "state" "{" Fields "}"  "react" WasmRef ["snapshot" "every" Integer]
               ["timers" Ident {"," Ident}] "}" ;
Source     = EventRef ["by" Ident] ;
Expr       = Or ;  Or = And {"or" And} ;  And = Not {"and" Not} ;  Not = "not" Not | Cmp ;
Cmp        = Term ("<"|"<="|">"|">="|"=="|"!=") Term | Path "matches" String
           | Path "in" "[" [Literal {"," Literal}] "]" | Ident "exists" | "(" Expr ")" ;
Term       = Literal | Path | "len" "(" Path ")" ;   Path = Ident {"." Ident} ;
```

A `///` where nothing doc-bearing can follow is a syntax error; ordinary `//` comments
are kept by position when `fold schema fmt` rewrites a file, never in the model. The
context `Fold` is reserved for the daemon's own events (S057).

A projection belongs to the context it is declared in but may consume events from any
context (`Customers.CustomerRegistered`); it receives them in global log order, which
is the only ordering the database promises across streams.

**Entities and values.** A `value` is an immutable record with no identity: two
values with equal fields are the same value. A context-level value (`Shared.Money`) may
be used anywhere; an aggregate-local value only inside that aggregate and in events
and commands that belong to it. An `entity` exists only inside its aggregate: it has an
`id` field (scalar, its identity within the aggregate) and may change over time. Where
an entity type may appear: the aggregate's `state`, its other entities, its commands,
and the events listed in its `events` (as snapshots of the entity's data, qualified
`Order.Line`). Where it may not: projection tables, other aggregates, other contexts.
An entity may be held as `E`, `E?`, `list<E>` or `map<K, E>`; in a map, `K` must be
the id's type and every key must equal its entity's id, which the validator checks on
state, events and commands. Values may not contain entities. Entity and value graphs
are acyclic.

Name resolution for `X.Y`: `X` is a context, or an aggregate in the current context;
if both exist it is a diagnostic. Unqualified `Y`: the current aggregate's local
types, then the current context's.

**Log backups.** Order matters: the index is dumped first, in one transaction, so the
head is fixed; the segment files are copied afterwards and therefore contain every
record below that head (they were fsynced before the index committed). A record a
segment carries past the head is truncated by recovery on open, exactly as after a
crash between fsync and commit. Idempotency keys, checkpoints, read models and
aggregate snapshots are all index tables, so a restored daemon continues without
re-issuing process commands. Restore refuses an existing log, removes everything it
wrote on a checksum failure, and builds the index last so a damaged archive leaves
no half-built log.

**Online restore.** The swap is a restart in place rather than a hot swap of `Shared`,
so every service, runner and cache is rebuilt from the restored log and nothing can
hold a handle to the old one. Clients see one reconnect. The listen address is pinned
after the first start so an ephemeral port survives the restart.

**Scheduled backups.** The tick skips when the newest backup's head equals the current
head, so an idle daemon does not fill the directory; a failure is logged and shown in
`ListBackups` and the next tick retries. Retention counts by head then creation time.

**Incremental backups.** The increment copies bytes, not records: `frames_between`
reads each segment from the first frame at or past `since` to the end of the last
frame below `head`, so an increment is a byte range verified by the frame crcs when
it is applied. `apply` reads and checks the whole archive before touching the log,
then runs the frames through the segment scanner, checks that each record's
position is the one expected, and commits batch by batch at the `LAST_IN_BATCH`
flag through the ordinary append path (index transaction, head, watch, roll), so a
log that an apply left half-done is a log with fewer batches, never a torn one. An
increment ending inside a batch is refused. The idempotency keys travel because they
are the only index table a *future* command consults; everything else in the index is
derived from the events and the runners rebuild it. Retention is per chain: an
increment is only useful while the full it hangs from exists, which is why the
schedule prunes fulls by count and increments by their `base_head`.

**Point-in-time restore.** The cut is done index first: once `META.head` says `N`,
recovery on open would finish the segment side on its own (it removes segments past
the head and truncates the tail to it), so a crash between the two halves leaves a
log that opens correctly; the explicit segment truncation is for tidiness, and the
cut ends by opening the result to prove it. What is kept versus dropped follows one
rule: a fact derived from events is kept only if every event it depends on survives.
Stream heads and the type index are recomputed; a checkpoint at or below `N` keeps
its tables, a later one loses them; an aggregate snapshot survives only while its
version is still on the stream. Idempotency keys are the one table a *future*
command consults, so a key whose command landed past the cut must go: the process
manager that replays the surviving events will issue that command again, and this
time it is new. The batch rule exists because a position inside a batch would leave
an append the log never acknowledged as a whole; the error reports the batch's two
edges so the caller can choose a side. A time never hits that rule: an append stamps
its whole batch with one `recorded_at`, so "recorded at or before `at`" keeps or drops
a batch whole, and the search still walks back to a boundary in case a stepped clock
made two batches share an instant out of order. The resolution is a binary search
over positions (each probe one indexed read), not a scan, so it costs the same on a
log of a billion events as on one of ten; it assumes `recorded_at` is non-decreasing
in position, which holds while the wall clock does not step backwards between
appends.

**Session consistency.** Monotonic reads are read-your-writes applied to reads: the
state a read saw is a position, and a later read that waits for that position
cannot show less. So the read returns the token it would have accepted, and the
client's job is to keep the maximum. The daemon holds no session state at all,
which is what lets a client wander between members and lets any member answer:
the token carries the whole session. It is the projection's checkpoint, not the
log head, because the read model is what the client saw; a position from the log
head would make the next read wait for events the client never observed. The
server-side cost is one string per response; the client-side cost is remembering
it, which the CLI does in a file so that separate invocations form one session.

**Read-your-writes on a replica.** Nothing new had to be invented on the replica:
the projection runner there is the same one as on the primary, its checkpoint is a
position in the same global sequence, and a checkpoint cannot overtake the local
head, so waiting for "checkpoint ≥ P" on a replica is waiting for replication and
projection together. What the token adds is the part a bare number lacks: whose
sequence the number belongs to. A position is only meaningful within one log, and a
client that talks to two clusters, or to a restored copy, would otherwise get a
plausible wrong wait rather than an error. The token is opaque to clients by
contract but legible to operators, and the Query service can check it with the log
id alone, which keeps the read side's boundary where it was.

**Replication.** The replica is a copy of the log, not of the daemon: everything
derived (read models, checkpoints, aggregate and process state) is recomputed locally
by the same runners the primary runs, which keeps the replica honest about what the
schema says and lets it serve queries at its own pace. The one thing that is not
derivable from events is the idempotency table: whether a process manager's command
was executed is a fact about the primary's past, so the chunks carry the keys and the
index keeps a second copy of them by position (`IDEMPOTENCY_BY_POS`, backfilled on
open for an index from before it) so a range is a range scan rather than a pass over
every key. That is also why a replica's process managers react but hold: reacting
keeps their state current for a promotion, dispatching would append locally and fork
the log. On promotion the held commands run through the normal path and the keys
answer `AlreadyExecuted`; a command the primary executed as a no-op left no key and
runs again, which for a deterministic handler is the same no-op. A replica's log is
append-only from one source, so the chunk protocol needs only "same identity" and
"starts at my head"; a replica that is ahead of its primary has diverged (a promotion
that was later undone, say) and is refused rather than rewound. Promotion is a
two-step flip because the two sides race otherwise: the write side must not open
while a chunk is being appended, so the tail is stopped and *joined* first, and
the role is flipped only after. The held outboxes need an explicit nudge: a
process runner drains after reacting to an event, and after a failover no event
arrives from anywhere until a command is taken, so `Promote` sends `Drain` rather
than waiting for traffic. Nothing is done about the old primary: if it is still
alive and taking writes, the two logs fork from the promotion head, and that is
the operator's call to make, not the daemon's. Automatic failover sharpens that
risk, which is why it is off by default and keyed on an unbroken period of
silence rather than on any single failure: a replica partitioned from a primary
that is still serving its clients will promote itself after the grace period
and fork the log. Without a third party to arbitrate there is no way for the
replica to tell that case from a dead primary; the grace period is the only
dial, and the promotion note records that the decision was automatic so the
operator can see it afterwards. A manual `Promote` that arrives while the tail
is promoting itself returns the same outcome rather than racing it.

**Quorum.** The election borrows the two rules that make a Raft vote safe without
borrowing a log-replication protocol it does not need. A vote is a durable promise
for an epoch, so two candidates cannot both collect the same voter in the same
epoch, and a crashed voter does not forget whom it promised; a candidate behind the
voter is refused, so the elected primary is at least as complete as every voter that
elected it, and no replicated event is lost by the election. The third rule is the
one that matters for this database: a voter asks the primary itself before
answering, so a replica that alone lost its link does not find a majority among
peers who can still see the primary. The quorum counts the primary as a member: a
cluster of a primary and one replica can never elect, which is the honest answer
for two nodes, and a cluster of three elects with the one other replica agreeing.
Proposals climb past any vote a peer reports so that a candidate that lost a round
to a vanished rival is not stuck behind the rival's epoch. The elected epoch is the
proposal, not the old epoch plus one, so the fencing token a client picks up after
the election is strictly newer than anything the old primary ever issued.

**Leader leases.** Fencing makes a stale primary stop *writing* as soon as someone
tells it; nothing so far made it stop *answering*, and a primary cut off from its
cluster would happily serve read models that the rest of the world had moved past.
The lease is the time-bounded version of the quorum's agreement: a majority says
"you are still our primary for the next T", and the primary believes it only until
T, counted from before it asked, so a slow answer cannot stretch the belief. The
same promise binds the peers from the other side: a voter that granted a lease
refuses every candidate until the lease ends, which is what makes the two
mechanisms consistent. Either no new primary exists while the old one serves
reads, or the old one has stopped serving reads before a new one can be elected,
so a reader never sees two primaries answer for the same instant. The cost is
that a primary that loses its majority stops answering reads within T even when it
is in fact the only live node, and the operator chooses T with that in mind. The
usual caveat holds: this is a lease on wall-clock durations measured on both
sides, so it assumes clocks that do not drift by a meaningful fraction of T.

**Fencing.** The token does what the daemon cannot do on its own: carry the news
of a promotion across the partition that caused it. The new primary tries to
deliver it directly (the fencer), but a partitioned old primary learns of the
new epoch from the first client that reaches it carrying the new token, which is
why the token fences rather than merely being refused: refusing alone would let
the next tokenless client write. Fencing is a one-way door persisted in the log
directory, because an old primary that restarts must not forget it was fenced.
It is not a rewind: whatever the old primary wrote after the fork stays in its
log and makes it diverged, and divergence is detected by content (the id of the
last event, which is a uuid v7 minted at append) rather than by head, since two
forks can be the same length. A fenced log whose history still agrees with the
new primary's rejoins as its replica and takes the new epoch from the stream;
one that does not is refused and has to be restored from the new primary's
backup. Tokens are optional on writes so unaware clients keep working; strict
fencing needs every writer to send one.

**Projection snapshots.** A projection may say `snapshot every N` after `fold`. The file
format is `FOLDPSNP | u32 header_len | header JSON {projection, checkpoint, tables,
rows, created_at, module_hash} | records (u16 table, u32 klen, key, u32 rlen, row)… |
u16 0xFFFF | u32 crc32`. A rebuild from a snapshot made by a different fold module is
refused unless forced; a damaged file is refused by its checksum; a failed restore
leaves the projection continuing from its stored checkpoint. During a rebuild the
status is REBUILDING and queries see a partial read model until it is live again;
`min_position` waits as usual.

**Aggregate snapshots and rebuilds.** Instance snapshots live in the `SNAPSHOTS` table
and are taken lazily by a load that replays at least `snapshot every` events. Exporting
writes them as one `snapshots` table (key = stream id, row = `{version, module_hash,
state}`) at the log head. A rebuild clears the table and the LRU, restores the file if
one is named, and then walks every stream whose id matches the aggregate's template
(`Log::stream_ids`, linear in the number of streams) loading each, so an evolve
module that no longer accepts old events fails the rebuild there and then. A
projection, process and aggregate in one context share the snapshot directory
namespace by `Context.Name`, so they should not share a name.

**Process snapshots and rebuilds.** A process's tables are `state` and `outbox`. Rebuilding
replays every reaction; because outbox ids are deterministic, each replayed command's
idempotency key (`pm:<process>:<id>`) is already in the log and `execute` answers
`AlreadyExecuted`. A command that was *rejected* the first time has no key in the log,
so a replay retries it; the usual outcome is the same rejection, but if the world has
changed since, it may now succeed. That is accepted behaviour: a rebuild re-decides
only what was never decided. The runner drains the restored outbox before replaying.

**The second DSL iteration** (doc comments and `fmt`, enums with payloads, field
defaults, imports, upcasting, declarative guards, the compatibility check, timers) is
described in the decisions table above; its resolver codes run S043–S057 (listed in
`resolve.rs`). Two defects it surfaced in the daemon are fixed with tests: a command
re-dispatched under a used idempotency key ran its handler (which could reject it)
before the key was checked, and an aggregate read served a cached state that
replication had moved past (`aggregate::load` now catches up from the cache when the
stream head moved). Migration note: a daemon started on a log whose stored schema
no longer compiles under the new grammar refuses unless `--force-schema`; nothing in
the example schema became invalid.

**Process managers.** `process Name { key k: T  from A, B.C by field, ...  state { ... }  react wasm "m" [export "e"] }`
(default export `react_<Name>`). The key must be uuid, string, int or uint; every
source event must carry the correlating field (`by`, or the key's name) with the key's
type (S035–S038). A process shares the read-model namespace with its context's
projections (its `state` and `outbox` tables and its checkpoint live there), so it may
not be named like one.

**Value rules.** `value Money { amount: decimal, currency: string } rules { NonNegative:
amount >= 0, IsoCurrency: currency matches "^[A-Z]{3}$" }`. Grammar: `Rule = Ident ":"
Expr`; `Expr = Or`, `or` < `and` < `not` < comparison, parentheses group; comparison
is `Term (< | <= | > | >= | == | !=) Term`, `path matches "re"`, or `path in [lit,
...]`; `Term = literal | path | len(path)`; literals are numbers (`-2.50`), strings
and booleans. Operands are typed: numbers (int, uint, decimal, `len`), text (string,
uuid, timestamp, bytes, enums) and booleans must agree, and ordering needs numbers
(S039–S042). An absent optional operand makes a comparison hold vacuously. Rules run
after the record's fields validate, on the canonical record, innermost value first; a
record whose field failed is not judged by its own rules. Values nest in values and
entities (never entities in values), and rules apply at every level.

**Invariants.** An aggregate may end with `invariants Name -> wasm "m" [export "e"], ...`
(default export `check_<Name>`). A context may declare
`invariant Name { on Aggregate  projection [Ctx.]Projection  scope field  check wasm "m" [export "e"] }`
where `scope` is a required keyable scalar field of the aggregate's state.
Resolver codes S030–S034 cover duplicates, an unknown aggregate or projection,
and a bad scope.

Collections: `[T]` is sugar for `list<T>`. Set elements and map keys are scalars
(`bytes` excluded) so they have a canonical JSON form; map and list values may be any
type. Collections may appear in events, value objects, state and tables alike; the
column **ops** below apply to tables only. A collection column is never optional: its
empty value is the default.

Example `examples/orders/schema.fold`:

```
context Shared { value Money { amount: decimal, currency: string } }

context Customers {
  event CustomerRegistered v1 { customer_id: uuid, name: string }

  aggregate Customer {
    key customer_id: uuid
    stream "customer-{customer_id}"
    events CustomerRegistered
    state { name: string }
    evolve wasm "orders.wasm" export "evolve_customer"
    commands Register { name: string } -> wasm "orders.wasm" export "handle_register"
  }
}

context Orders {
  enum Status { Pending, Paid, Cancelled }
  event OrderPlaced v1   { order_id: uuid, customer_id: uuid, lines: [Order.Line], total: Shared.Money }
  event LineAdded v1     { order_id: uuid, line: Order.Line, total: Shared.Money }
  event OrderCancelled v1 { order_id: uuid, reason: string?, at: timestamp }

  aggregate Order {
    key order_id: uuid
    stream "order-{order_id}"

    value Discount { percent: uint, reason: string }                  // local value
    entity Line { id line_id: uuid, sku: string, qty: uint, price: Shared.Money, discount: Discount? }

    events OrderPlaced, LineAdded, OrderCancelled
    state { customer_id: uuid, status: Status, lines: map<uuid, Line>, total: Shared.Money }
    evolve wasm "orders.wasm" export "evolve_order"
    snapshot every 100
    commands
      PlaceOrder  { customer_id: uuid, lines: [Line] } -> wasm "orders.wasm" export "handle_place_order",
      AddLine     { line: Line }                       -> wasm "orders.wasm" export "handle_add_line",
      CancelOrder { reason: string? }                  -> wasm "orders.wasm" export "handle_cancel_order"
  }

  // read model per order
  projection OrderTotals {
    from OrderPlaced, OrderCancelled
    fold wasm "orders.wasm" export "project_order_totals"
    table order_totals { key order_id: uuid, total: Shared.Money, status: Status }
  }

  // read model across two aggregates in two contexts, using the collection primitives
  projection CustomerOrders {
    from Customers.CustomerRegistered, OrderPlaced, OrderCancelled
    fold wasm "orders.wasm" export "project_customer_orders"
    table customer_orders {
      key customer_id: uuid,
      name: string?,
      open_orders: set<uuid>,              // set_add on place, set_remove on cancel
      recent_orders: list<uuid>,           // list_push then list_truncate last 5
      spent_by_currency: map<string, decimal>,   // add { map_key: currency, by: amount }
      order_count: uint                    // add 1
    }
  }
}
```

`CustomerOrders` is the cross-aggregate case: `CustomerRegistered` sets `name`, each
`OrderPlaced` issues four column ops against the customer's row without reading it,
and the row is created with defaults (`name: null`, empty collections, `0`) when the
order arrives before its customer.

Modules: `span.rs`, `lexer.rs`, `ast.rs`, `parser.rs`, `types.rs`, `model.rs`
(resolved `Schema { contexts: IndexMap }`, `EventFamily { versions: BTreeMap<u16,
EventType> }`, `Aggregate { key, stream: StreamTemplate, values, enums, entities:
IndexMap<String, Entity>, events, state: Vec<Field>, evolve: WasmRef, snapshot_every:
u32, commands: IndexMap<String, Command> }`, `Entity { name, id: Field, fields:
Vec<Field> }`, `Command { name, fields: Vec<Field>, handler: WasmRef }`, `Projection {
from, fold: WasmRef, tables }`; `Type` gains `Entity(EntityId)`, `Set`, `Map`),
`resolve.rs` (all rules as numbered diagnostics `S001..`, collects all
errors), `template.rs` (`StreamTemplate::parse/render(&Value)/matches`),
`validate.rs` (`Schema::validate_event(&EventType, &Value)`,
`Schema::validate_state(&Aggregate, &Value)` and `Schema::validate_command(&Command,
&Value)`, all `-> Result<(), Vec<ValidationError>>`, strict, all errors with JSON path;
one record validator serves events, value objects, state and commands), `rows.rs`
(the column-op applier, below), `fmt.rs` (the canonical printer behind `fold schema
fmt`, comment-preserving), `source.rs` (`Sources`: files, imports, the bundle),
`diff.rs` (the compatibility classification), `upcast.rs` (declarative upcasts).

JSON mapping: `decimal` is a **string** (`"12.50"`), `uuid` hyphenated string,
`timestamp` RFC 3339 (jiff), `bytes` base64, `T?` null/absent ok, a unit enum variant as its name string and a payload variant as
a one-key object `{"Shipped": {...}}`, a defaulted field filled in when absent or null,
`list<T>` array, `set<T>` array with unique elements kept **sorted by encoded key**
(canonical, so two equal sets serialize identically), `map<K, V>` object whose keys
are the canonical string form of `K` (`"42"`, `"true"`, a uuid) and whose entries are
emitted sorted. An entity is an object like a value; inside `map<K, E>` the validator
additionally requires `key == canonical(entity.id)` and reports
`EntityKeyMismatch { path, key, id }` otherwise.

### Row operations (`rows.rs`)

`apply(table: &Table, row: Option<Value>, ops: &[ColumnOp]) -> Result<Value, RowError>`
is a pure function: it starts from the stored row or from the table's **default row**
(nulls for optionals, empty collections, `0` for numbers, and an error for any other
missing non-optional column, which `upsert` must supply), applies the ops in order,
validates the result against the table (strict), and enforces the 1 MiB limit.

| Op | Column type | Semantics |
|---|---|---|
| `set { column, value }` | any | replace the column; `null` only on `T?` |
| `add { column, map_key?, by }` | `int`, `uint`, `decimal`, or a map of those with `map_key` | numeric add; `uint` below zero → error; absent map entry starts at `0` |
| `set_add { column, value }` | `set<T>` | insert, idempotent |
| `set_remove { column, value }` | `set<T>` | remove, idempotent |
| `list_push { column, value, front? }` | `list<T>` | append (or prepend) |
| `list_remove { column, value, all? }` | `list<T>` | remove the first (or every) equal element |
| `list_truncate { column, keep, from: "front" \| "back" }` | `list<T>` | keep the first or last `keep` elements |
| `map_put { column, map_key, value }` | `map<K, V>` | insert or replace |
| `map_remove { column, map_key }` | `map<K, V>` | remove, idempotent |

Element and key values are validated against `T`/`K`/`V` before applying; an op on a
column of the wrong type is `RowError::WrongColumnType`. Tests: table-driven per op,
plus proptests that `set_add` is idempotent and order-independent, `set_remove` after
`set_add` restores the row, `list_push` then `list_truncate` keeps the right tail, and
`map_put` then `map_remove` restores the row.

Resolution rules: unique names per kind per context and per aggregate (a local type
may not shadow a context-level one); refs resolve per the `X.Y` rule above; value and
entity graphs acyclic; values contain no entities; entities appear only where the
entity rules allow; `map<K, E>` with an entity `E` has `K` = the id's type; projection
tables contain no entities; aggregate key ∈ {uuid,string,int,uint}; template
placeholders ⊆ {key}, ≥1 placeholder; every aggregate event carries the key field
with the same type; an event family belongs to ≤1 aggregate; command names unique per
aggregate; projection `from` refs resolve across contexts; table has ≥1 scalar `key`;
wasm paths relative, no `..`.

## 2. `fold-core` — log, index, read models

Vocabulary: **Log** = one data dir + one schema + one global sequence + one
`index.redb`. **Stream** = one aggregate instance, id ≤255 bytes. Dense 0-based
`GlobalPosition(u64)` and `StreamVersion(u64)`; `head()` = next position.

Envelope (`event.rs`): `RecordedEvent { id: Uuid(v7), position, stream_id,
stream_version, event_type: {context, name, version: u16}, recorded_at: i64 nanos,
payload: Bytes, metadata: Bytes, flags: u8 (bit0 LAST_IN_BATCH, bits1-3 encoding) }`.

On disk:
```
<data_dir>/<log_name>/ LOG (64 B identity) · LOCK (File::try_lock) · schema/current.fold
                       segments/<base_position:020>.seg · index.redb
```
Segment: 64-byte header (magic `FOLDSEG\0`, format, header crc, base position, log
uuid). Record: `u32 body_len, u32 crc32(body), body` with a hand-rolled fixed layout
(not serde). Roll after a batch when size ≥ `segment_max_bytes` (256 MiB default; tests
use ~1 KiB). A batch never spans segments.

**Commit protocol**: write records → fdatasync (policy `Always`/`Never`) → one redb
write txn (POSITIONS, STREAMS, STREAM_HEADS, EVENT_TYPES, META.head) → commit → `watch`
publish. The redb commit is the commit point. Recovery on open: scan the last segment
from byte 64, stop at the first short/bad-crc/wrong-position record and `set_len`
there; anything at `position >= META.head` was never acked → truncate; index ahead of
data → `Error::Corrupt`; missing `index.redb` → rebuild by scanning, truncating to the
last `LAST_IN_BATCH` record.

redb tables (`index.rs`): `META`, `POSITIONS u64 → (segment_base, offset)`,
`STREAMS (&str, u64) → u64`, `STREAM_HEADS &str → u64`, `EVENT_TYPES` multimap,
`CHECKPOINTS &str → u64 next_position`, `SNAPSHOTS (&str aggregate, &str stream_id) →
(u64 version, &[u8] state JSON)` (one row per stream, overwritten; the version is the
stream version the state includes), and one `rm:<projection>:<table>` table per
declared table (`&[u8] → &[u8]` JSON row). Key encoding (`keyenc.rs`) is
order-preserving and prefix-free: uuid 16 raw bytes, string UTF-8 + `0x00`, int
sign-flipped BE, uint BE, bool 1 byte, timestamp as int.

API (`log.rs`), synchronous; daemon wraps in `spawn_blocking`; core depends on tokio
with only `sync`:

```rust
Log::create/open/open_or_create(dir, name, OpenOptions) -> Result<Log>   // Log: Clone (Arc)
append(&self, &StreamId, ExpectedVersion{Any,NoStream,StreamExists,Exact(v)}, Vec<NewEvent>) -> Result<AppendResult{first,last,stream_version}>
read_stream(&self, &StreamId, from: StreamVersion, Direction, limit) -> Result<Vec<RecordedEvent>>
read_all(&self, from: GlobalPosition, limit) / read_all_backward / read_by_type
stream_head(&self, &StreamId) -> Result<Option<StreamVersion>>;  head(&self) -> GlobalPosition
subscribe(&self) -> Subscription   // watch::Receiver; async wait_past(pos)
read_models(&self) -> ReadModelStore   // snapshot(): get(table, key); commit(projection, next_pos, puts, deletes) in ONE txn
snapshots(&self) -> SnapshotStore      // get(aggregate, stream) -> Option<(StreamVersion, Bytes)>; put(aggregate, stream, version, state)
```

A snapshot is a cache, never a source of truth: `put` is idempotent and a stale or
missing snapshot only costs replay. `snapshots.rs` holds the store; the replay logic
lives in the daemon because it needs the WASM host.

Append runs under a writer `Mutex`: check `STREAM_HEADS` vs expected → assign ids,
positions, versions, flags → write+fsync → index txn → advance head → publish → maybe
roll. A failed step leaves bytes past head that the next append overwrites.

Errors (`thiserror 2`): `Io{path,op}`, `Corrupt{segment,offset,reason}`, `Locked`,
`WrongExpectedVersion{stream,expected,actual}`, `InvalidStreamId`, `EmptyBatch`,
`RecordTooLarge`, `PositionOutOfRange`, `Index(redb)`. Tracing spans `fold.append`,
`fold.open`, `fold.recover`, `fold.segment.roll`.

## 3. `fold-wasm` + `fold-guest` — the WASM host

wasmtime, `default-features = false, features = ["cranelift","runtime","std",
"parallel-compilation"]`; no WASI, no component model, no async. `Engine` owns an
epoch-ticker thread (10 ms). `Limits { fuel: 50M, memory_bytes: 64 MiB, epoch_ticks:
100, max_output_bytes: 4 MiB }`. `ModuleCache::load(schema_dir, rel)` resolves paths
relative to the schema file and refuses escapes. Fresh `Store` per call via
`InstancePre`. Keep the host behind a `Guest` trait so a component-model guest can be
added later.

**Guest ABI v1** — exports: `memory`, `fold_abi_version()->1`, `fold_alloc(len)`,
`fold_free(ptr,len)`, `fold_apply(ptr,len)->i64 (ptr<<32|len)` for a projection step,
`fold_evolve(ptr,len)->i64` for an aggregate evolve, `fold_handle(ptr,len)->i64` for a
command handler. The schema's `export "..."` names the entry point, defaulting to
`project_<Projection>` / `evolve_<Aggregate>` / `handle_<Command>`; the `fold_` prefix
is reserved for the ABI. Host imports in
namespace `fold`: `get_row(table_ptr,len,key_ptr,len)->i64` (reads the pre-fold redb
snapshot) and `log(level,ptr,len)` → tracing. Any other import → `UnsupportedImport`
at load.

Projection step JSON: input `{abi:1, projection, event:{stream, type, version,
position, payload, metadata}}`; output `{mutations:[...]}` or `{error:"..."}`. A
mutation is `{table, key:{...key fields...}, op}` where `op` is `upsert {row}`,
`delete`, or any column op from the `rows.rs` table (`{op:"set_add", column, value}`,
`{op:"add", column, map_key?, by}`, ...). The host groups mutations by `(table, key)`,
loads each row once from the pre-fold snapshot, runs `rows::apply`, and writes the
results in the batch transaction; `delete` wins over later ops on the same key. Keys
are encoded with `keyenc`.

Evolve JSON (pure, no host imports besides `log`): input `{abi:1, aggregate, stream,
version, state: null | {...}, event:{type, version, position, payload, metadata}}`;
output `{state:{...}}` or `{error:"..."}`. `state: null` only for the first event of a
stream. The host validates the returned state against the aggregate's declared `state`
fields (strict). Command JSON (pure): input `{abi:1, aggregate, stream, version: null |
n, state: null | {...}, command:{type, payload}}` → `{events:[{type, payload,
metadata?}]}` or `{rejected:{code, message}}`. The host checks every emitted event: its
family is in the aggregate's `events`, the payload validates, and its key field renders
to the stream being handled. A handler may emit zero events (a no-op command succeeds
with no append).

Check JSON (the fourth role, export `check_<Name>`): input `{abi:1, invariant, aggregate,
stream, key, version, projection?, scope?, state: candidate, events: [{type, version,
payload, metadata}]}` → `{ok:true}` | `{violation:{code, message}}` | `{error}`. A
context invariant may call `get_row` on its projection's tables; a state invariant
may not read rows.

React JSON (the fifth role, export `react_<Name>`): input `{abi:1, process, key, now,
state: null | {...}, trigger: {"event": Event} | {"rejected": {command: IssuedCommand,
rejected: {code, message}}}}` → `{state: null | {...}, commands: [{command:
"Ctx.Agg.Cmd", stream, payload, metadata?}]}` | `{error}`. `state: null` in the reply
ends the instance. No row reads.

`fold-wasm` exposes `ProjectionModule::apply(...)`, `AggregateModule::evolve(...)` and
`AggregateModule::handle(...)` over one shared `CoreGuest` implementation.

`fold-guest`: `projection!(step_fn)`, `aggregate!(evolve_fn)` and
`command!(handle_fn)` macros emitting the exports; `Ctx::get(table, key) ->
Result<Option<Value>, String>`; a `Row(table, key)` builder with `.upsert(row)`,
`.delete()`, `.set(col, v)`, `.add(col, by)`, `.add_in(col, map_key, by)`,
`.set_add(col, v)`, `.set_remove(col, v)`, `.push(col, v)`, `.push_front(col, v)`,
`.remove(col, v)`, `.truncate_back(col, n)`, `.truncate_front(col, n)`,
`.map_put(col, k, v)`, `.map_remove(col, k)`; `Emit::event(type, payload)` and
`Rejected::new(code, message)`. Imports behind `#[cfg(target_arch = "wasm32")]` so
the crate also builds natively for clippy.

Tests use the `wat` crate inline (no wasm target needed): echo step, echo evolve, echo
handle, infinite loop → `OutOfFuel`, `memory.grow` → `MemoryLimit`, WASI import →
`UnsupportedImport`, missing export, `get_row` returns a canned row, evolve returning
a state with an undeclared field → `StateInvalid`, handler emitting an event outside
the aggregate → `EventNotAllowed`, a step issuing `set_add` on a `uint` column →
`MutationInvalid` with the column named. One integration test runs the built
`orders_guest.wasm` through all three roles: handle `PlaceOrder` → emitted
`OrderPlaced`, evolve it into `Order` state, fold it into `customer_orders`.

## 4. `fold-proto` + `foldd`

As first built: one package and one daemon. The services are now split over
`fold.database.v1`, `fold.derivation.v1` and `fold.application.v1` (see the top
section); the write side, the runners and the read side described here live in
`fold-app`, `fold-derive` and `fold-derive` respectively, over gRPC instead of a
shared `Shared`.

`proto/fold/v1/fold.proto`, package `fold.v1`, four services so the segregation is
visible in every generated client:

| Service | RPCs |
|---|---|
| `Command` | `Execute(aggregate, stream_id, command, payload, metadata) -> {events: [RecordedEvent], first_position, last_position, version}`; `Append(stream_id, expected, events[]) -> positions` |
| `Query` | `Get(projection, table, key, min_position?, wait_ms?) -> {found, row, checkpoint}`; `Scan(projection, table, key_prefix, limit, min_position?, wait_ms?)` (stream) |
| `Log` | `ReadStream` (stream), `ReadAll` (stream), `SubscribeAll` (stream, never ends), `GetAggregate(stream_id) -> {aggregate, version, state, snapshot_version, replayed}` |
| `Admin` | `GetSchema`, `ListProjections` (name, state STARTING/CATCHING_UP/LIVE/FAILED/STOPPED, optional checkpoint, head, error, tables), `Health` |

Payloads cross as `bytes payload + string content_type` (`application/json`), never
`google.protobuf.Struct`. `build.rs` uses `tonic_prost_build` with system `protoc`
(clear error if missing). Error mapping: validation → `INVALID_ARGUMENT`; version
conflict → `FAILED_PRECONDITION`; handler rejection → `FAILED_PRECONDITION` with the
guest's `code` and `message` in details; unknown aggregate/command/projection/table →
`NOT_FOUND`; projection behind `min_position` after `wait_ms` → `UNAVAILABLE` with
the current checkpoint; projection `FAILED` → `FAILED_PRECONDITION`; absent row →
`found=false`.

`foldd` lib (`lib.rs`): `start(Options{data_dir, schema, listen, limits}) ->
Running{local_addr, shutdown()}`; binds the listener first so `:0` works. Order: load
schema → `Log::open_or_create` → engine + load every projection module (fail fast) →
spawn runners → serve with `serve_with_incoming_shutdown`. One structured `info!` line
on listen. `main.rs`: clap `--data-dir/--schema/--listen/-c config.toml`, env
`FOLD_DATA_DIR/FOLD_SCHEMA/FOLD_LISTEN/FOLDD_LOG`, tracing to stderr, SIGINT/SIGTERM
→ cancel → join runners (10 s timeout) → exit 0.

Daemon modules: `command.rs` (write side), `query.rs` (read side), `aggregate.rs`
(loader + cache), `projection.rs` (runners), `log_svc.rs`, `admin.rs`, `server.rs`
(wires the four tonic services), `shutdown.rs`.

**Write side** (`command.rs`). `Execute`: resolve the aggregate and command → render
the stream template from the request's `stream_id` (must match the aggregate) →
`validate_command` → take the per-stream lock (`StreamLocks`: a sharded
`HashMap<StreamId, Arc<Mutex<()>>>`, entries dropped when unused) → `aggregate.load`
→ `handle` in `spawn_blocking` → rejection → `FAILED_PRECONDITION`; else validate the
emitted events (family allowed, payload, key renders to this stream) →
`log.append(Exact(version) | NoStream)` → advance the cache → release the lock →
respond with the recorded events. Because the lock serializes the stream, the `Exact`
check can only fail if someone used raw `Append` concurrently; that is reported, not
retried. `Append`: parse → resolve `EventType` → `validate_event` → aggregate owning
the family → template must equal `stream_id` → `log.append` → advance the cache.

**Invariants on the write side** (`command::commit`), after the handler emitted and the
events passed validation: (1) the candidate state = loaded state evolved over the new
events; (2) each state invariant of the aggregate runs against it; (3) for each context
invariant on the aggregate, the scope value is read from the candidate state, a lock per
(invariant, scope) is taken in sorted order, the projection is waited to checkpoint ≥
head−1 (5 s, else `UNAVAILABLE`; a failed projection → `FAILED_PRECONDITION`), and the
check runs over a snapshot of that projection; (4) append, and the candidate becomes the
cached state. A violation is `FAILED_PRECONDITION` with `fold-rejection-code` and
`fold-invariant` metadata. Raw `Append` of an aggregate's events goes through the same
path; events owned by no aggregate are appended plainly.

**Process runners** (`process.rs`): per process, from its checkpoint, for each event in
`from`: correlation key = payload[by] → load `state` row → `react` → validate the new
state against the declared fields → one transaction: state put or delete, one `outbox`
row per issued command (uuid v7 id, time ordered), checkpoint = position+1 → drain the
outbox: `command::execute` with idempotency key `pm:<process>:<id>`; `Done` or
`AlreadyExecuted` deletes the row; `FAILED_PRECONDITION` carrying a rejection code
re-runs `react` with the `rejected` trigger and deletes the row in that same
transaction; anything else retries with backoff (250 ms doubling to 30 s) and shows in
`ListProcesses` as the error. Positions with nothing to react to still advance the
checkpoint. The log gained `append_idempotent` and an `IDEMPOTENCY` table for this.

**Read side** (`query.rs`): `Get`/`Scan` touch only `ReadModelStore` snapshots and the
projection status `watch`. With `min_position`, wait on the watch until `checkpoint >=
min_position`, bounded by `wait_ms` (default 5 000, max 30 000), else `UNAVAILABLE`.
No code path in `query.rs` can reach the log or the aggregate cache.

**Aggregate loader** (`aggregate.rs`): `AggregateCache { lru: Mutex<LruCache<StreamId,
Cached{version, state}>>, snapshots: SnapshotStore, modules }` sized by
`--aggregate-cache` (default 10 000 streams). `load(stream) -> Result<Loaded{version,
state, snapshot_version, replayed}>`:

1. match the stream id to an aggregate via the templates (`StreamTemplate::matches`), else `NOT_FOUND`;
2. in-memory hit → return it (the single writer keeps it current, see below);
3. else `snapshots.get` → `(v, state)` or `(none, null)`;
4. `log.read_stream(stream, from = v+1, Forward)` in pages of 256, `evolve` each event in `spawn_blocking`;
5. if `events replayed >= snapshot_every` (and `snapshot_every > 0`) → `snapshots.put(version, state)`;
6. insert into the LRU and return.

Advancing on append: after `log.append` succeeds, if the stream is in the LRU, evolve
the new events onto the cached state; an evolve error or a state that fails validation
evicts the entry (the next load replays from the snapshot) and is logged at `warn`. The
cache is per daemon process and never consulted for durability. On a schema whose
`evolve` module changes, snapshots are stale: the slice records the module's hash
alongside the snapshot and ignores a snapshot whose hash differs (one extra column in
`SNAPSHOTS`).

**Projection runner** (`projection.rs`), one tokio task per projection: read
`CHECKPOINTS` → catch-up in batches of 256 via `read_all` (each batch: snapshot, run
the step on relevant events, one txn with all mutations + checkpoint) → live via
`Subscription` `wait_past` then `read_all` again (the log is the queue; no unbounded
buffering) → any guest/schema/redb error sets `Failed{error}` with the checkpoint
unchanged and stops; nothing is skipped. Status published via `watch` for
`ListProjections`.

## 5. `fold-cli` (binary `fold`)

Subcommands mirror the services so the segregation is visible at the shell (the
routing by layer, `--db/--derive/--app` and `append --unguarded` are in the top
section):

```
fold [--addr http://127.0.0.1:4141 | $FOLD_ADDR] [--db URL] [--derive URL] [--app URL] [--json]
  init <dir> --schema <file>            offline; creates the log, copies the schema
  schema check <file>                   offline; follows imports; diagnostics as file:line:col, exit 1 on error
  schema fmt [--check] <file>...        offline; canonical layout, comments kept; --check exits 1 if a file would change
  schema diff <old> <new>               offline; compatible / rebuild / breaking per change, exit 1 on breaking
  exec <Context.Aggregate.Command> <stream> --json '{...}' [--meta '{...}']     Command.Execute; prints events + last position
  append <stream> <Context.Event[@vN]> --json '{...}' [--expect N|none|any]    Command.Append
  query get <projection> <table> <key-json> [--after P] [--wait MS]            Query.Get
  query scan <projection> <table> [--prefix] [--limit] [--after P]             Query.Scan
  log read <stream> [--from V] [--max N] | log all [--from P] | log tail [--from P]
  log aggregate <stream>                state, version, snapshot version, replayed
  projection list | health | schema show
```
Human output by default; `--json` = one object per line. Exit 0 ok, 1 server error or
rejection (code and message printed), 2 usage/connection.

## 6. Example guest and end-to-end test

`examples/orders/guest` (`orders-guest`, cdylib, workspace member) implements all
three roles: handlers `Register` (emits `CustomerRegistered`), `PlaceOrder` (rejects
`ALREADY_PLACED` if state exists, else sums the lines and emits `OrderPlaced`),
`AddLine` (rejects `NOT_PENDING`, emits `LineAdded` with the new total) and
`CancelOrder` (rejects `NOT_PENDING`); evolves `Customer` and `Order` state, keeping
`lines` as a map keyed by `line_id` so a repeated id replaces the entity; folds
`order_totals` with `upsert` and the cross-aggregate `customer_orders` purely with
column ops (`set`, `set_add`/`set_remove`, `push` + `truncate_back(5)`, `add_in`,
`add`), so the example shows both styles. Tests obtain the `.wasm` via
`common::build_orders_guest()` which runs `$CARGO build -p orders-guest --target
wasm32-unknown-unknown --release --target-dir target/guest` (separate target dir avoids
the outer cargo lock; repeat builds are no-ops). The `.wasm` is not committed.

`crates/foldd/tests/suite/e2e_orders.rs` (sqex single-binary layout): tempdir with
schema + wasm → `foldd::start` on `127.0.0.1:0` → `Command.Execute Register` on
`customer-C` → `Execute PlaceOrder` on `order-A` and `order-B` for customer C, keeping
the last position → `Query.Get CustomerOrders customer_orders {customer_id: C}` with
`min_position = last` and no polling: the read-your-writes wait is the thing under
test → asserts `name`, `open_orders == {A, B}` (as a sorted array), `recent_orders ==
[A, B]`, `spent_by_currency == {"EUR": "40.00"}`, `order_count == 2` → `Execute
CancelOrder` on `order-A` → `Get` again with the new position → `open_orders == {B}`,
`recent_orders` unchanged (it is history), map unchanged → place six more orders →
`recent_orders` holds the last five → negatives:
`PlaceOrder` again on `order-A` → `FAILED_PRECONDITION` with code `ALREADY_PLACED`;
`Execute` with a payload missing `lines` → `INVALID_ARGUMENT`; `Append` with wrong
`exact` → `FAILED_PRECONDITION`; `Append` with a stream-id mismatch →
`INVALID_ARGUMENT`; `Get` with `min_position = head + 1_000` and `wait_ms = 100` →
`UNAVAILABLE`; missing key → `found=false` → shutdown → restart on the same dir →
checkpoint unchanged and totals not doubled (exactly-once across restarts). A second
test registers the customer **after** the orders and asserts the row's `name` is filled
in while `open_orders` is kept: the cross-aggregate fold handles either arrival order.

`crates/foldd/tests/suite/e2e_aggregate.rs`, with a test schema variant using
`snapshot every 2`: `Execute PlaceOrder` on `order-A` with one line → `Log.GetAggregate`
returns version 0, `Pending`, `lines` a one-entry map keyed by that line's id,
`replayed = 0` (the command path populated the cache), no snapshot → `Execute AddLine`
with a new line → `lines` has two entries and `total` grew → `Execute AddLine` again
with the **same** `line_id` but `qty + 1` → still two entries, that entry updated
(identity, not duplication) → `Execute CancelOrder` → `Cancelled` → restart the daemon →
`GetAggregate`: replays 2 events (`replayed = 2`) and writes a snapshot at version 1
→ `Append` a further `OrderCancelled`-compatible event directly → restart →
`GetAggregate` shows `snapshot_version = 1, replayed = 1` and the correct state. This
proves snapshot + replay, cache population by commands and raw appends, and that a
snapshot is never trusted past its version. Negatives: a guest whose evolve returns
an undeclared field → `INTERNAL` and no snapshot written; an evolve that files a line
under a map key that is not its `line_id` → `INTERNAL` with `EntityKeyMismatch`; a
handler whose emitted event belongs to another aggregate → `INTERNAL` with
`EventNotAllowed` and nothing appended (stream head unchanged); an `AddLine` command
whose line lacks `line_id` → `INVALID_ARGUMENT`.

`crates/foldd/tests/suite/concurrency.rs`: 16 tasks each `Execute` a command that
appends to the same stream with no rejection logic → every call succeeds, versions are
dense, the final state counts 16: the per-stream lock, not the client, provides the
serialization.

## 7. CI (`.github/workflows/ci.yml`, from sqex)

`fmt` job as sqex; `test` job: `apt-get install protobuf-compiler`,
`dtolnay/rust-toolchain@stable` (honors the toolchain file incl. wasm target),
`taiki-e/install-action@nextest`, `Swatinem/rust-cache`, warm `cargo build -p
orders-guest --target wasm32-unknown-unknown --release --target-dir target/guest`,
`cargo nextest run --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`.

## 8. Implementation order (each step gated on `cargo nextest run -p <crate>` and clippy `-D warnings`)

1. **Skeleton**: `git init`, workspace `Cargo.toml`, toolchain, LICENSE, `.gitignore`, ci.yml, empty crates. Check: `cargo build --workspace`.
2. **fold-schema**: span+lexer → ast+parser (incl. `set<>`, `map<>`, `list<>`, aggregate-local `value`/`enum`/`entity`) → types/model/template → resolve (one failing fixture per diagnostic, incl. entity-in-projection, value-containing-entity, map-key-type and `X.Y` ambiguity) → validate (table-driven JSON incl. canonical set/map forms and `EntityKeyMismatch`) → `rows.rs` column ops with proptests → fmt + proptest round-trip. Check: `schema.fold` compiles; strict unknown-field rejection proven; every op has a wrong-type negative.
3. **fold-core**: ids/error/options → envelope codec → `segment.rs` (framing + torn-write matrix at segment level) → `dir.rs` (lock, numeric sort) → `index.rs` → `log.rs` + `recover.rs` (concurrency: 8 threads × 100 `Exact` rounds, exactly one winner per version; roll test; full torn-write matrix incl. "fsynced but uncommitted" and index rebuild) → `subscribe.rs` → `keyenc.rs` proptest + `readmodel.rs` atomic checkpoint → `snapshots.rs` (put/get, overwrite, reopen). Check: all of the above plus `examples/append_read.rs`.
4. **fold-proto**: proto + build.rs. Check: prost round-trip test.
5. **fold-wasm + fold-guest + orders-guest**: WAT tests for apply, evolve and handle, then the real-guest tests. Check: `.wasm` emitted; handle → evolve → fold chain passes.
6. **foldd lib: Log + Admin services and `Command.Append`** (template enforcement + validation), shutdown. Check: suite test appends and reads back on `:0`; shutdown < 1 s.
7. **Aggregate loader + `Log.GetAggregate` + `Command.Execute`** (LRU, snapshot store, replay, per-stream lock, emitted-event checks, module hash). Check: `e2e_aggregate.rs` and `concurrency.rs` pass.
8. **Projection runner + `Query` service** (mutation grouping + `rows::apply` in the batch txn, Get/Scan with `min_position` wait, ListProjections). Check: `e2e_orders.rs` passes including the collection assertions, the arrival-order variant and restart.
9. **foldd main** (clap, env, config, tracing, signals). Check: manual start, `fold health`, SIGINT prints "foldd stopped".
10. **fold-cli** all subcommands and `--json`; `assert_cmd` tests for `schema check` and `init`. Check: README quickstart runs end to end against a live daemon: `exec Register`, `exec PlaceOrder`, `query get --after`, `log aggregate`.
11. First commit per gated step; push when CI is green on the whole slice.

Second DSL iteration (2026-10-09), each step gated and pushed with CI green: doc
comments and the comment-keeping formatter → `fold schema fmt` and the first CLI tests
→ `Literal::Variant`, enums with payloads, field defaults → upcasting (schema + guest
ABI) → declarative guards → imports and the bundle → timers syntax and the reserved
`Fold` context → the diff engine → defaults and upcasts on the daemon's paths →
guards end to end → imports in the CLI and daemon → the compatibility check at start
and `fold schema diff` → the timers runtime → this write-up.

## Verification

- `cargo nextest run --workspace` and `cargo clippy --workspace --all-targets -- -D warnings` green locally and in CI.
- Torn-write, index-rebuild and concurrency tests prove the log; the WAT tests prove the sandbox limits; the `rows.rs` proptests prove the collection ops; `e2e_orders.rs` proves command → events → cross-aggregate projection with set/list/map columns → read-your-writes query, and exactly-once across restart; `e2e_aggregate.rs` proves snapshot + replay and the cache; `concurrency.rs` proves the per-stream lock.
- Manual quickstart from the README: `fold init`, `foldd`, `fold exec`, `fold query get --after`, `fold log aggregate`, `fold log tail`.
- CQRS boundary check: `query.rs` imports neither `fold_core::Log` nor the aggregate cache; a unit test asserts the `Query` service type holds only a `ReadModelStore` and the status watches.
- Negative controls are part of every gate: each validation rule and each error mapping has a test that fails without it.
