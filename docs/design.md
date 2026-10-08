# fold: an event-sourcing / DDD database — vertical-slice plan

## Context

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
| Replication | `foldd --replicate-from http://primary:4141` (or `replicate_from` in the config) runs a read-only replica. At start it asks the primary's `Health` for the log id and either creates an empty log with that identity (`Log::create_with_id`) or checks the one it has (another log's, or one ahead of the primary, is refused). It then tails `Log.Replicate(from = local head)`: a server stream of `ReplicationChunk`s, each the raw segment bytes of `from..to` (about 1024 records, rounded to a batch boundary; a longer batch goes whole) plus the idempotency keys first used in that range, then live via the log's watch. The replica appends a chunk through the incremental-backup path (`Log::apply_replication_chunk`: same identity, starts at the head, committed batch by batch), so ids, timestamps, versions and CRCs are the primary's. Its `Command` service answers FAILED_PRECONDITION; its projections run on the replicated events and serve `Query`; its process managers react and fill their outboxes but do not dispatch. The tail reconnects with a backoff; `Health` reports `role`, `replicating_from`, `replica_connected`, `primary_head` and the last error. Promotion is a restart without `--replicate-from`: the held outbox entries are then dispatched and found already executed through the replicated keys |
| Projection snapshots | `Admin.SnapshotProjection` writes every row of a projection's tables at its checkpoint from one read transaction into `<log>/snapshots/<Ctx.Projection>/<checkpoint>.fsnap` (checksummed, with the fold module's hash); `RebuildProjection` resets the tables and checkpoint and replays from scratch or from a snapshot; `ListSnapshots`/`DeleteSnapshot` manage them; `projection X { ... snapshot every N ... }` takes them automatically. Requests go through the runner's control channel so a snapshot never races a rebuild |
| Aggregate snapshots | the same RPCs accept an aggregate name: `SnapshotProjection` exports every instance snapshot (stream → version, module hash, state) to a file; `RebuildProjection` drops the instance snapshots and the cache, restores a file if given, then loads every instance of the aggregate from its events so each is re-evolved by the current module and re-snapshotted; it returns when that is done |
| Process snapshots | the same snapshot and rebuild RPCs accept a process name: the file holds its `state` and `outbox` tables. Outbox ids are derived from the triggering position (`<position>-<idx>`, rejections `<parent>-r-<idx>`), so a replay after a rebuild derives the same idempotency keys and every already-executed command is skipped rather than re-issued. `process X { ... snapshot every N }` snapshots automatically |
| Process managers | `process Name { key field  from Event [by field], ...  state {...}  react wasm ... }` declared in a context. A runner per process follows the log; for each event it declared, it loads the instance keyed by the correlating field, runs `react` (state in, state + issued commands out), and commits state, outbox and checkpoint in one transaction. Outbox entries are executed through the normal command path with an idempotency key derived from the entry id, so a crash-retry finds the command already applied. A refused command returns to the instance as a `rejected` trigger; a failed one is retried with backoff |
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
implementation time); event-id dedupe, upcasting, hot reload, separate query nodes,
membership queries that avoid fetching the row: **out of scope**. The crate is not
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
    fold-schema/    DSL: lexer, parser, resolver, JSON validator, formatter
    fold-core/      segmented log, redb index, append/read/subscribe, read-model store
    fold-wasm/      wasmtime host: engine, module cache, projection ABI, limits
    fold-guest/     guest-side SDK (projection! macro, Ctx::get, Mutation)
    fold-proto/     fold.proto + tonic-prost-build codegen
    foldd/          daemon lib + bin: server, projection runner, shutdown
    fold-cli/       binary `fold`
  examples/orders/
    schema.fold
    guest/          crate orders-guest, cdylib → orders_guest.wasm
```

`fold-schema` and `fold-core` do not depend on each other; `foldd` composes them.
Profiles copied from sqex (`lto = "thin"`, `strip`), plus
`[profile.release.package.orders-guest] opt-level = "s"`.

## 1. `fold-schema` — the DSL

**Parser**: hand-written lexer + recursive descent (≈20 productions, no precedence;
best error messages with spans; zero deps).

Grammar (commas separate fields, trailing comma ok, `//` and `/* */` comments):

```
File       = { Context } ;
Context    = "context" Ident "{" { Value | Enum | Event | Aggregate | Projection } "}" ;
Value      = "value" Ident "{" Fields "}" ;
Enum       = "enum" Ident "{" Ident { "," Ident } "}" ;
Event      = "event" Ident "v" Integer "{" Fields "}" ;
Field      = Ident ":" Type ;      Type = BaseType ["?"] ;
BaseType   = Scalar | TypeRef | "[" Type "]" | "list" "<" Type ">"
           | "set" "<" Scalar ">" | "map" "<" Scalar "," Type ">" ;
Scalar     = string|int|uint|decimal|bool|uuid|timestamp|bytes ;
TypeRef    = Ident ["." Ident] ;   // Money | Shared.Money
Aggregate  = "aggregate" Ident "{" "key" Field  "stream" String
               { Value | Enum | Entity }             // aggregate-local types
               "events" EventRef {"," EventRef}
               "state" "{" Fields "}"  "evolve" "wasm" String ["export" String]
               ["snapshot" "every" Integer]          // default 100; 0 = never
               ["commands" Command {"," Command}] "}" ;
Entity     = "entity" Ident "{" "id" Field { "," Field } "}" ;
Command    = Ident "{" Fields "}" "->" "wasm" String ["export" String] ;
Projection = "projection" Ident "{" "from" EventRef {"," EventRef}   // any context, any aggregate
               "fold" "wasm" String ["export" String]  Table {Table} "}" ;
Table      = "table" Ident "{" ["key"] Field {"," ["key"] Field} "}" ;
```

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
that was later undone, say) and is refused rather than rewound.

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
(the column-op applier, below), `fmt.rs` (canonical printer, used by proptest).

JSON mapping: `decimal` is a **string** (`"12.50"`), `uuid` hyphenated string,
`timestamp` RFC 3339 (jiff), `bytes` base64, `T?` null/absent ok, enum as string,
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

Subcommands mirror the four services so the segregation is visible at the shell:

```
fold [--addr http://127.0.0.1:4141 | $FOLD_ADDR] [--json]
  init <dir> --schema <file>            offline; creates the log, copies the schema
  schema check <file>                   offline; diagnostics, exit 1 on error
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

## Verification

- `cargo nextest run --workspace` and `cargo clippy --workspace --all-targets -- -D warnings` green locally and in CI.
- Torn-write, index-rebuild and concurrency tests prove the log; the WAT tests prove the sandbox limits; the `rows.rs` proptests prove the collection ops; `e2e_orders.rs` proves command → events → cross-aggregate projection with set/list/map columns → read-your-writes query, and exactly-once across restart; `e2e_aggregate.rs` proves snapshot + replay and the cache; `concurrency.rs` proves the per-stream lock.
- Manual quickstart from the README: `fold init`, `foldd`, `fold exec`, `fold query get --after`, `fold log aggregate`, `fold log tail`.
- CQRS boundary check: `query.rs` imports neither `fold_core::Log` nor the aggregate cache; a unit test asserts the `Query` service type holds only a `ReadModelStore` and the status watches.
- Negative controls are part of every gate: each validation rule and each error mapping has a test that fails without it.
