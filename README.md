# fold

An event-sourcing / domain-driven-design database. You declare a domain in
schema files (bounded contexts, events, values, aggregates with entities;
then state and projections), write the evolve functions and projection
folds as WASM modules, write your commands, invariants and process managers
in Rust against the `fold-app` SDK, and fold stores the events, validates
them, keeps aggregate state, runs the projections, and runs your
application.

fold is three layers, the two lower ones generic services driven by schema
files, the top one your application:

| Layer | Who | Owns | Serves |
|---|---|---|---|
| **database** | `fold-dbd` (schema-driven) | the `domain` layer: contexts, events, aggregate identity, streams | `Log` (append, reads, subscriptions, replication), `Cluster` (health, promotion, fencing, elections, leases), `Backup`, `Schema` |
| **derivation** | `fold-derived` (schema-driven) | the `derivation` layer: aggregate `state` and `evolve`, projections, snapshots | `Query` (read models, read-your-writes tokens), `Aggregate` (instance state), `DeriveAdmin`, and `Derive` for the application |
| **application** | your Rust program on `fold-app` (`orders-app` is the example) | commands, state invariants, projection-driven invariants, process managers and their timers, registered in Rust | `Command` (execute, guarded append), `AppAdmin` |

The database stores and streams events only: it hosts no WASM and keeps
nothing derived, so it is the one thing to replicate and back up. The
derivation node tails it and derives state and rows; the application reads
that state, runs its handlers and invariants, and appends to the database
with expected versions. `foldd` runs the two schema-driven nodes in one
process on one address, and an application can embed both (`orders-app
--embed`), which is how development, a single machine and the test suite
run; the layers still talk to each other over gRPC on loopback, so the code
paths are the deployment's.

The API is segregated along CQRS lines: a **Command** service executes
registered commands (and appends raw events under the invariants as an
escape hatch), a **Query** service reads projections with a read-your-writes
position token, the **Log** service exposes events for integration and
debugging, and each layer's admin service reports what it runs, its runners
and its health.

Values carry their own rules (`value Money { ... } rules { NonNegative: amount >= 0 }`)
and are checked wherever an instance is created, however deeply nested in an
event, a command's emitted event, an entity or a read model.

The schema language documents itself: `///` comments attach to declarations,
fields, enum variants, rules and tables and reach the model (`fold schema
check` shows them), and `fold schema fmt` rewrites files in a canonical
layout keeping every comment. Every file names its layer (`layer domain`,
`layer derivation`) and imports the files of its own layer and the one below
(`import "domain.fold"`); the derivation layer declares against the domain
by qualified name (`state Orders.Order { .. }`, `projection
Orders.OrderTotals { .. }`). Enums may carry payloads (`enum Status {
Pending, Shipped { carrier: string } }`, stored as `{"Shipped": {...}}`);
and fields may have defaults (`qty: uint = 1`), filled in wherever a record
is written and for records stored before the default existed, which is what
makes adding a field to an event a compatible change.

Events evolve: `event OrderCancelled v2 { ... note: string } upcast from v1
{ set note: "legacy" }` (or `upcast from v1 wasm "m"` for an upcaster in the
guest) tells the readers how an old record reads as the new version, and
every consumer, from projections to aggregate replay to your process
managers, sees the latest version while the log keeps what was recorded. A
schema that changed since the data was written is diffed at start, each
node for its layer: the database refuses a breaking domain change with the
reason unless `--force-schema`, the derivation node applies the compatible
changes of its layer (a new projection fills from history, a changed one
rebuilds, a dropped table is dropped), and `fold schema diff old.fold
new.fold` tells you in advance. The application has no schema file: at
start it fetches the domain from the database and the derivation layer from
the derivation node and checks every registration against them (an
aggregate the domain does not declare, a process source without the key, an
invariant reading a projection the derivation node does not run, each
refuses the start by name), and it keeps checking while it runs, refusing
commands until they fit.

Invariants are Rust code enforced before anything is appended: an
aggregate's **state invariants** see the state a command would produce; a
**projection-driven invariant** reads a read model, and the application
serializes commands per scope value and waits for the projection to catch up
first, so a rule like "at most five open orders per customer" holds under
concurrency (one application node is assumed for that; its health says so).
The database's own `Log.Append` (`fold append --unguarded`) is the one way
past the invariants; it still validates against the domain.

The whole log can be backed up online (`fold backup`) into one checksummed
archive and restored offline (`fold restore`) into a fresh directory; the
database can do it on a schedule (`--backup-every 6h --backup-keep 7`) and
can restore a backup into itself while running (`fold restore --live`),
keeping the previous log aside. An incremental backup (`fold backup --incremental`,
or `--backup-incremental`) holds only the records since the newest backup;
`fold restore <inc> <dir> --apply` appends it onto a restored full backup.
Any restore can stop at a point in time with `--to <position>` or
`--at <RFC 3339 timestamp>`: the log comes back holding exactly the events
below that position, or every batch recorded at or before that instant. The
log then carries a new **generation** and the cut position, and every
derivation node and application drops what it derived past the cut and
rebuilds it; a node whose store belongs to another log starts over. Their
health says what was reset.

Every write returns a position token; a query on any derivation node, one
tailing a replica included, that carries it (`fold query get ... --token <t>`)
answers only once that node has projected the write, so a client that writes
through one path and reads through another still reads its own writes. Every
read returns a token too, and a client that keeps passing its latest one
(`fold --session <file>` does this for you) never reads an older state than
it already saw, whichever node answers.

A second database can run as a read-only **replica** of the first
(`fold-dbd --replicate-from http://primary:4141`, or `foldd --replicate-from`
for a whole replica composite): it tails the primary's log as raw records;
its derivation node serves queries over the replicated events, an
application on it refuses commands and holds its process managers' commands,
and it becomes a primary either in place (`fold promote`, a failover without
a restart) or when restarted without the flag. With `--auto-failover 30s` it
promotes itself once the primary has been out of reach for that long; this
is off by default, since a replica cut off from a primary that is still
serving others would fork the log. With `--quorum-peers` naming the other
members, it promotes itself only after winning an election: a majority of
the cluster must agree the primary is gone, each member votes once per
epoch, and only for a candidate at least as far along as itself. With
`--lease 5s` the primary also serves reads only under a lease the majority
keeps renewing, so a primary cut off from the cluster stops answering stale
reads within five seconds, and a fenced primary refuses reads outright.
Every promotion starts a new **epoch** (`fold health` shows it); the new
primary fences the old one (`fold fence`), and a write carrying a newer epoch
as its `--fencing-token` fences any old primary it reaches, so it stops
taking writes for good. A fenced database rejoins as a replica of the new
primary if their histories agree.

Projections can be snapshotted at a checkpoint and rebuilt from scratch or
from a snapshot (`fold projection snapshot`, `fold projection rebuild`), and a
projection may take its own snapshots with `snapshot every N`. The same works
for process managers (a process rebuild never re-issues a command it already
executed) and for aggregates, whose rebuild re-derives every instance from its
events with the current evolve module.

Process managers react to events across aggregates and contexts, keep state
per correlation key, and issue commands through the same path a client uses;
state, issued commands and checkpoint commit together and each command is
executed with an idempotency key, so a crash never doubles a command. A
process may also set **timers** (`.timers(["ShipmentOverdue"])` on the
registration, a `SetTimer` in the reaction): the application of the primary
fires a due timer as a `Fold.TimerFired` event appended to the database under
a system token only it holds, so replicas, rebuilds and a promoted replica
all see it fire exactly once, and a timer set before a restart still fires
after it. The application remembers what it registered beside its process
tables: a process whose key or sources changed starts over, one no longer
registered has its tables dropped.

See [docs/design.md](docs/design.md) for the design.

## Layout

| Crate | What it is |
|---|---|
| `fold-schema` | the `.fold` schema language: parser, resolver, the domain and derivation models, JSON validation, row operations, the diff |
| `fold-core` | the append-only segmented log and its redb index |
| `fold-store` | the derived store a derivation node or an application keeps beside the log: checkpoints, rows, instance snapshots, fingerprints |
| `fold-host` | daemon code the derivation node and the application share: codecs, snapshot files, upcasting, guest linking, runner types |
| `fold-wasm` | the wasmtime host for evolve, projection and upcast modules |
| `fold-guest` | the SDK a WASM module is written with |
| `fold-proto` | the gRPC contracts (tonic): `fold.common.v1`, `fold.database.v1`, `fold.derivation.v1`, `fold.application.v1` |
| `fold-db` | the database service |
| `fold-derive` | the derivation service |
| `fold-app` | the application SDK and runtime: register commands, invariants and process managers in Rust; serve them over gRPC or call them in-process |
| `foldd` | the composite of the two services, the embed API for applications, and the binaries `foldd`, `fold-dbd`, `fold-derived` |
| `fold-cli` | the `fold` command-line client |
| `examples/orders` | a two-file schema, a guest with the evolves, folds and an upcaster, and the orders application (`orders-app`) in Rust |

## Quickstart

Requirements: the pinned Rust toolchain (installed on first `cargo` call,
including the `wasm32-unknown-unknown` target) and `protoc`
(`brew install protobuf` or `apt-get install protobuf-compiler`).

```bash
cargo build --release -p orders-app -p foldd -p fold-cli
cargo build -p orders-guest --target wasm32-unknown-unknown --release --target-dir target/guest
cp target/guest/wasm32-unknown-unknown/release/orders_guest.wasm examples/orders/orders.wasm
```

Create a log from the example schema (its derivation file; the domain comes
through its import) and start the orders application with the database and
the derivation node embedded:

```bash
target/release/fold init ./orders-db --schema examples/orders/derive.fold
target/release/orders-app --embed --data-dir ./orders-db/data --schema examples/orders/derive.fold
```

In another shell, write through commands and read through projections:

```bash
fold exec Customers.Customer.Register customer-c0000000-0000-0000-0000-000000000001 -d '{"name":"Ada"}'
fold exec Orders.Order.PlaceOrder order-a0000000-0000-0000-0000-000000000001 -d '{
  "customer_id": "c0000000-0000-0000-0000-000000000001",
  "lines": [{"line_id":"10000000-0000-0000-0000-000000000001","sku":"SKU-1","qty":2,
             "price":{"amount":"7.50","currency":"EUR"}}]}'
```

The second command prints the position it appended at. Pass it to a query so
the query waits until the projection has applied it:

```bash
fold query get Orders.CustomerOrders customer_orders '{"customer_id":"c0000000-0000-0000-0000-000000000001"}' --after 1
fold query get Orders.CustomerOrders customer_orders '{"customer_id": "<uuid>"}' --token <token from exec>   # read-your-writes on any node
fold log aggregate order-a0000000-0000-0000-0000-000000000001
fold log process Orders.Fulfilment '"a0000000-0000-0000-0000-000000000001"'
fold health                                   # the three layers
fold projection list
fold projection snapshot Orders.CustomerOrders
fold projection rebuild Orders.CustomerOrders --from <snapshot id>
fold aggregate snapshot Orders.Order
fold aggregate rebuild Orders.Order
fold process list
fold process snapshot Orders.Fulfilment
fold process rebuild Orders.Fulfilment --from <snapshot id>
fold append customer-<uuid> Customers.CustomerRegistered -d '{...}' --expect none   # under the invariants
fold append customer-<uuid> Customers.CustomerRegistered -d '{...}' --unguarded     # the database directly
fold backup
fold backup --incremental                      # records since the newest backup
fold restore <full.fbak> ./restored-db         # offline, database stopped
fold restore <inc.fbak> ./restored-db --apply  # then each increment, in order
fold restore <archive.fbak> --live             # into the running database
fold restore <archive.fbak> ./restored-db --to 1200   # point in time: positions below 1200
fold restore <archive.fbak> --live --at 2026-10-08T14:30:00Z   # or a timestamp, live
foldd --data-dir ./replica --schema examples/orders/derive.fold --listen 127.0.0.1:4142 \
      --replicate-from http://127.0.0.1:4141     # a read-only replica of the two nodes
fold --addr http://127.0.0.1:4142 promote     # failover: the replica becomes the primary
foldd ... --replicate-from http://127.0.0.1:4141 --auto-failover 30s   # or by itself
foldd ... --auto-failover 30s --quorum-peers http://127.0.0.1:4141,http://127.0.0.1:4143   # with a majority
foldd ... --quorum-peers http://127.0.0.1:4142,http://127.0.0.1:4143 --lease 5s   # primary: reads under a lease
fold exec Orders.Order.PlaceOrder order-<uuid> -d '{...}' --fencing-token 1   # refused by a stale primary
fold --addr http://127.0.0.1:4141 fence 1     # tell an old primary a newer epoch exists
fold schema show --layer domain               # the bundle a node runs; --layer application prints what the app registered
fold schema fmt --check examples/orders/derive.fold   # canonical layout, comments kept
fold schema diff examples/orders/derive.fold new.fold # compatible, rebuild or breaking; exit 1 on breaking
foldd ... --force-schema                      # adopt a breaking schema change anyway
fold log tail
```

`fold --json ...` prints one JSON object per line for scripting. `foldd -c
./orders-db/foldd.toml` runs the two schema-driven nodes without an
application, for an application that runs on its own.

### Three processes

The same deployment as three processes, each with its own data directory;
the application and the database share a secret for the process timers:

```bash
fold-dbd     --data-dir ./db      --schema examples/orders/domain.fold --listen 127.0.0.1:4141 --system-secret "$SECRET"
fold-derived --data-dir ./derive  --schema examples/orders/derive.fold --database http://127.0.0.1:4141 --listen 127.0.0.1:4142
orders-app   --data-dir ./app     --database http://127.0.0.1:4141 --derivation http://127.0.0.1:4142 \
             --listen 127.0.0.1:4143 --system-secret "$SECRET"
fold --db http://127.0.0.1:4141 --derive http://127.0.0.1:4142 --app http://127.0.0.1:4143 health
```

`fold` sends each command to the layer that owns it: `--addr` (or
`FOLD_ADDR`) names the composite, and `--db`, `--derive` and `--app`
(`FOLD_DB_ADDR`, `FOLD_DERIVE_ADDR`, `FOLD_APP_ADDR`) override it per layer.
Several derivation nodes may tail one database; more than one application
node is not yet supported for cross-stream invariants.

## Writing an application

An application is a Rust program on `fold-app`. It registers what it does
against the domain by name, with its own types for commands and state;
`serve` runs it against a database and a derivation node, and
`foldd::start_with_app` embeds both:

```rust
use fold_app::{App, CmdCtx, ContextInvariant, Emit, Fail, InvCtx, IssuedCommand, PendingEvent,
               ProcCtx, Process, Reaction, Rejected, Rows, SetTimer, Trigger, json};

#[derive(serde::Deserialize)]
struct PlaceOrder { customer_id: uuid::Uuid, lines: Vec<Line> }

let app = App::new()
    .aggregate::<OrderState>("Orders.Order", |a| a
        .command("PlaceOrder", |cx: &CmdCtx, state: Option<OrderState>, cmd: PlaceOrder| {
            if state.is_some() {
                return Err(Rejected::new("ALREADY_PLACED", "this order was already placed").into());
            }
            Ok(vec![Emit::event("Orders.OrderPlaced", json!({ "order_id": cx.key, /* … */ }))])
        })
        // sees the state every command (and guarded append) would leave behind
        .invariant("LinesNotEmpty", |_cx: &InvCtx, state: &OrderState, _ev: &[PendingEvent]| {
            if state.status == "Pending" && state.lines.is_empty() {
                return Err(Rejected::new("EMPTY_ORDER", "a pending order must keep at least one line"));
            }
            Ok(())
        }))
    // reads a projection, serialized per customer and caught up first
    .invariant(ContextInvariant::new("Orders.MaxOpenOrders")
        .on("Orders.Order").projection("Orders.CustomerOrders").scope("customer_id")
        .check::<OrderState>(|cx: &InvCtx, rows: &dyn Rows, _state, events| {
            if !events.iter().any(|e| e.is("Orders.OrderPlaced")) { return Ok(()); }
            let open = rows.get("customer_orders", &json!({ "customer_id": cx.scope }))?
                .and_then(|row| row["open_orders"].as_array().map(Vec::len)).unwrap_or(0);
            if open >= 5 { return Err(Rejected::new("MAX_OPEN_ORDERS", "five open orders already").into()); }
            Ok(())
        }))
    .process(Process::new("Orders.Fulfilment")
        .key("order_id")
        .from("Orders.OrderPlaced").from("Orders.OrderCancelled")
        .from_by("Shipping.ShipmentPrepared", "order_id")
        .timers(["ShipmentOverdue"])
        .react::<FulfilmentState>(|cx: &ProcCtx, state, trigger: &Trigger| match trigger {
            Trigger::Event(ev) if ev.is("Orders.OrderPlaced") => Ok(Reaction::keep(FulfilmentState { /* … */ })
                .issue(IssuedCommand::new("Shipping.Shipment.Prepare", format!("shipment-{}", cx.key.as_str().unwrap()),
                                          json!({ "order_id": cx.key, "customer_id": ev.payload["customer_id"] })))
                .set_timer(SetTimer::after("ShipmentOverdue", 86_400_000))),
            Trigger::Rejected { rejected, .. } => { /* a command this instance issued was refused */ Ok(Reaction::unchanged(state)) }
            Trigger::Timer { name, .. } if name == "ShipmentOverdue" => Ok(Reaction::keep(state.unwrap())
                .issue(IssuedCommand::new("Orders.Order.CancelOrder", format!("order-{}", cx.key.as_str().unwrap()),
                                          json!({ "reason": "shipment overdue" })))),
            _ => Ok(Reaction::unchanged(state)),
        }));

let running = fold_app::serve(app, fold_app::Options::new("./app", "http://127.0.0.1:4141", "http://127.0.0.1:4142", "127.0.0.1:4143".parse()?)).await?;
```

A payload that does not fit the command's type is `INVALID_ARGUMENT`; a
state that does not fit the application's type, a `Fail::Error`, or a
panic in application code is `INTERNAL` and the node keeps serving. A
`Rejected` is a business decision and reaches the client as
`FAILED_PRECONDITION` with its code. `examples/orders/app/src/lib.rs` is the
complete version; `running.handle()` executes commands in-process over the
same path.

## Writing a guest

A guest is a Rust `cdylib` built for `wasm32-unknown-unknown` that depends on
`fold-guest`: the evolve functions, projection folds and upcasters the
derivation node runs. Declare the ABI plumbing once, then one export per
schema entry point:

```rust
fold_guest::module!();

fold_guest::aggregate!(evolve_order = |state: Option<Value>, ev: &Event| { /* fold one event */ });

fold_guest::upcast!(upcast_order_cancelled_v2 = |ev: &UpcastEvent| {
    let mut payload = ev.payload.clone();          // the v1 payload in, the v2 payload out
    payload["note"] = json!(format!("wasm:{}", payload["reason"].as_str().unwrap_or("-")));
    Ok(payload)
});

fold_guest::projection!(project_customer_orders = |cx: &Ctx, ev: &Event| {
    let row = Row::new("customer_orders", json!({ "customer_id": ev.payload["customer_id"] }));
    Ok(vec![row.set_add("open_orders", ev.payload["order_id"].clone()), row.add("order_count", 1)])
});
```

`examples/orders/guest/src/lib.rs` is the complete version.

## Developing

```bash
cargo nextest run --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Each service crate has its own suite over its own node (`fold-db` with the
log, replication, fencing and backups; `fold-derive` over a database node;
`fold-app` over both with the orders application served in-process), and
`foldd`'s suite runs the three layers together as the composite with the
application embedded and as three processes. The end-to-end tests build
the example guest themselves (into `target/guest`, so they never contend
with the outer cargo's lock).
