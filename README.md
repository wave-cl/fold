# fold

An event-sourcing / domain-driven-design database. You declare a domain in
schema files (bounded contexts, events, values, aggregates with entities;
then state and projections; then commands, invariants and process managers),
write the command handlers, evolve functions and projection folds as WASM
modules, and fold stores the events, validates them, keeps aggregate state,
runs the projections, and runs your commands.

fold is three services, each owning one layer of the schema:

| Service | Binary | Owns | Serves |
|---|---|---|---|
| **database** | `fold-dbd` | the `domain` layer: contexts, events, aggregate identity, streams | `Log` (append, reads, subscriptions, replication), `Cluster` (health, promotion, fencing, elections, leases), `Backup`, `Schema` |
| **derivation** | `fold-derived` | the `derivation` layer: aggregate `state` and `evolve`, projections, snapshots | `Query` (read models, read-your-writes tokens), `Aggregate` (instance state), `DeriveAdmin`, and `Derive` for the application node |
| **application** | `fold-appd` | the `application` layer: `commands`, `invariants`, process managers and their timers | `Command` (execute, guarded append), `AppAdmin` |

The database stores and streams events only: it hosts no WASM and keeps
nothing derived, so it is the one thing to replicate and back up. The
derivation node tails it and derives state and rows; the application node
reads that state, runs handlers and invariants, and appends to the database
with expected versions. `foldd` runs all three in one process on one
address, which is how development, a single machine and the test suite run;
the nodes still talk to each other over gRPC on loopback, so the code paths
are the deployment's.

The API is segregated along CQRS lines: a **Command** service executes
declared commands (and appends raw events under the invariants as an escape
hatch), a **Query** service reads projections with a read-your-writes
position token, the **Log** service exposes events for integration and
debugging, and each node's admin service reports its schema, its runners and
its health.

Values carry their own rules (`value Money { ... } rules { NonNegative: amount >= 0 }`)
and are checked wherever an instance is created, however deeply nested in an
event, a command, an entity or a read model.

The schema language documents itself: `///` comments attach to declarations,
fields, enum variants, rules, commands, invariants and tables and reach the
model (`fold schema check` shows them), and `fold schema fmt` rewrites files in
a canonical layout keeping every comment. Every file names its layer
(`layer domain`, `layer derivation`, `layer application`) and imports the
files of its own layer and the ones below (`import "domain.fold"`); the
upper layers declare against the domain by qualified name
(`state Orders.Order { .. }`, `projection Orders.OrderTotals { .. }`,
`commands Orders.Order { .. }`, `process Orders.Fulfilment { .. }`). Enums may
carry payloads (`enum Status { Pending, Shipped { carrier: string } }`,
stored as `{"Shipped": {...}}`); and fields may have defaults
(`qty: uint = 1`), filled in wherever a record is written and for records
stored before the default existed, which is what makes adding a field to an
event a compatible change.

Events evolve: `event OrderCancelled v2 { ... note: string } upcast from v1
{ set note: "legacy" }` (or `upcast from v1 wasm "m"` for an upcaster in the
guest) tells the readers how an old record reads as the new version, and
every consumer, from projections to aggregate replay, sees the latest version
while the log keeps what was recorded. A schema that changed since the data
was written is diffed at start, each node for its layer: the database refuses
a breaking domain change with the reason unless `--force-schema`, the
derivation and application nodes apply the compatible changes of theirs (a
new projection fills from history, a changed one rebuilds, a dropped table is
dropped), and `fold schema diff old.fold new.fold` tells you in advance. The
application node checks at start that the database and the derivation node
run the layers it imports, and refuses commands until they do.

Invariants are declared in the schema and enforced before anything is
appended: an aggregate's **state invariants** see the state a command would
produce; a context's **projection-driven invariants** read a read model, and
the application node serializes commands per scope value and waits for the
projection to catch up first, so a rule like "at most five open orders per
customer" holds under concurrency (one application node is assumed for that;
its health says so). Both state invariants and command guards can be written
in the schema instead of WASM: `invariants Orders.Order { MaxLines: len(lines) <= 10 }`
and `CancelOrder { .. } requires { Open: state.status == Pending }` reject
with the guard's name, and `requires not state exists` is how a command
insists on a fresh stream. The database's own `Log.Append`
(`fold append --unguarded`) is the one way past the invariants; it still
validates against the domain.

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
derivation and application node drops what it derived past the cut and
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
its derivation node serves queries over the replicated events, its
application node refuses commands and holds its process managers' commands,
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
process may also set **timers** (`timers ShipmentOverdue` in the schema, a
`SetTimer` in the reaction): the application node of the primary fires a due
timer as a `Fold.TimerFired` event appended to the database under a system
token only it holds, so replicas, rebuilds and a promoted replica all see it
fire exactly once, and a timer set before a restart still fires after it.

See [docs/design.md](docs/design.md) for the design.

## Layout

| Crate | What it is |
|---|---|
| `fold-schema` | the `.fold` schema language: parser, resolver, the three layer models, JSON validation, row operations, the diff |
| `fold-core` | the append-only segmented log and its redb index |
| `fold-store` | the derived store a derivation or application node keeps beside the log: checkpoints, rows, instance snapshots, fingerprints |
| `fold-host` | daemon code the derivation and application nodes share: codecs, snapshot files, upcasting, guest linking, runner types |
| `fold-wasm` | the wasmtime host for projection, evolve, handler, check, react and upcast modules |
| `fold-guest` | the SDK a WASM module is written with |
| `fold-proto` | the gRPC contracts (tonic): `fold.common.v1`, `fold.database.v1`, `fold.derivation.v1`, `fold.application.v1` |
| `fold-db` | the database service |
| `fold-derive` | the derivation service |
| `fold-app` | the application service |
| `foldd` | the composite, and the four binaries `foldd`, `fold-dbd`, `fold-derived`, `fold-appd` |
| `fold-cli` | the `fold` command-line client |
| `examples/orders` | a three-file schema and a guest covering every feature |

## Quickstart

Requirements: the pinned Rust toolchain (installed on first `cargo` call,
including the `wasm32-unknown-unknown` target) and `protoc`
(`brew install protobuf` or `apt-get install protobuf-compiler`).

```bash
cargo build --release -p foldd -p fold-cli
cargo build -p orders-guest --target wasm32-unknown-unknown --release --target-dir target/guest
cp target/guest/wasm32-unknown-unknown/release/orders_guest.wasm examples/orders/orders.wasm
```

Create a log from the example schema (its application file; the derivation
and domain files come through its imports) and start the composite:

```bash
target/release/fold init ./orders-db --schema examples/orders/app.fold
target/release/foldd -c ./orders-db/foldd.toml
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
foldd --data-dir ./replica --schema examples/orders/app.fold --listen 127.0.0.1:4142 \
      --replicate-from http://127.0.0.1:4141     # a read-only replica composite
fold --addr http://127.0.0.1:4142 promote     # failover: the replica becomes the primary
foldd ... --replicate-from http://127.0.0.1:4141 --auto-failover 30s   # or by itself
foldd ... --auto-failover 30s --quorum-peers http://127.0.0.1:4141,http://127.0.0.1:4143   # with a majority
foldd ... --quorum-peers http://127.0.0.1:4142,http://127.0.0.1:4143 --lease 5s   # primary: reads under a lease
fold exec Orders.Order.PlaceOrder order-<uuid> -d '{...}' --fencing-token 1   # refused by a stale primary
fold --addr http://127.0.0.1:4141 fence 1     # tell an old primary a newer epoch exists
fold schema show --layer domain               # the bundle a node runs
fold schema fmt --check examples/orders/app.fold   # canonical layout, comments kept
fold schema diff examples/orders/app.fold new.fold # compatible, rebuild or breaking; exit 1 on breaking
foldd ... --force-schema                      # adopt a breaking schema change anyway
fold log tail
```

`fold --json ...` prints one JSON object per line for scripting.

### Three services

The same deployment as three processes, each with its own data directory
and schema file (each imports the layers below it); the application node
and the database share a secret for the process timers:

```bash
fold-dbd     --data-dir ./db      --schema examples/orders/domain.fold --listen 127.0.0.1:4141 --system-secret "$SECRET"
fold-derived --data-dir ./derive  --schema examples/orders/derive.fold --database http://127.0.0.1:4141 --listen 127.0.0.1:4142
fold-appd    --data-dir ./app     --schema examples/orders/app.fold    --database http://127.0.0.1:4141 \
             --derivation http://127.0.0.1:4142 --listen 127.0.0.1:4143 --system-secret "$SECRET"
fold --db http://127.0.0.1:4141 --derive http://127.0.0.1:4142 --app http://127.0.0.1:4143 health
```

`fold` sends each command to the layer that owns it: `--addr` (or
`FOLD_ADDR`) names the composite, and `--db`, `--derive` and `--app`
(`FOLD_DB_ADDR`, `FOLD_DERIVE_ADDR`, `FOLD_APP_ADDR`) override it per layer.
Several derivation nodes may tail one database; more than one application
node is not yet supported for cross-stream invariants.

## Writing a guest

A guest is a Rust `cdylib` built for `wasm32-unknown-unknown` that depends on
`fold-guest`. Declare the ABI plumbing once, then one export per schema entry
point:

```rust
fold_guest::module!();

fold_guest::command!(handle_place_order = |cx: &CmdCtx, state: Option<Value>, cmd: &Command| {
    if state.is_some() {
        return Err(Rejected::new("ALREADY_PLACED", "this order was already placed").into());
    }
    Ok(vec![Emit::event("Orders.OrderPlaced", json!({ "order_id": cx.key, /* … */ }))])
});

fold_guest::aggregate!(evolve_order = |state: Option<Value>, ev: &Event| { /* fold one event */ });

fold_guest::invariant!(check_lines_not_empty = |_cx: &InvCtx, _rows: &Ctx, state: &Value, _ev: &[PendingEvent]| {
    if state["status"] == "Pending" && state["lines"].as_object().is_none_or(|l| l.is_empty()) {
        return Err(Rejected::new("EMPTY_ORDER", "a pending order must keep at least one line").into());
    }
    Ok(())
});

fold_guest::process!(react_fulfilment = |cx: &ProcCtx, state: Option<Value>, trigger: &Trigger| {
    match trigger {
        Trigger::Event(ev) if ev.is("Orders.OrderPlaced") => Ok(Reaction::keep(json!({ /* … */ }))
            .issue(IssuedCommand::new("Shipping.Shipment.Prepare", format!("shipment-{}", cx.key.as_str().unwrap()),
                                      json!({ "order_id": cx.key, "customer_id": ev.payload["customer_id"] })))),
        Trigger::Rejected { rejected, .. } => { /* a command this instance issued was refused */ Ok(Reaction::unchanged(state)) }
        Trigger::Timer { name, .. } if name == "ShipmentOverdue" => Ok(Reaction::keep(state.unwrap())
            .issue(IssuedCommand::new("Orders.Order.CancelOrder", format!("order-{}", cx.key.as_str().unwrap()),
                                      json!({ "reason": "shipment overdue" })))),
        _ => Ok(Reaction::unchanged(state)),
    }
    // A reaction sets a timer with `.set_timer(SetTimer::after("ShipmentOverdue", ms))`
    // and cancels it with `.cancel_timer("ShipmentOverdue")`.
});

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

`examples/orders/guest/src/lib.rs` is the complete version. One module may
serve every role; each node links only the exports of its layer (the
derivation node `evolve`, `fold` and the upcasters, the application node the
handlers, the invariant checks and `react`).

## Developing

```bash
cargo nextest run --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Each service crate has its own suite over its own node (`fold-db` with the
log, replication, fencing and backups; `fold-derive` over a database node;
`fold-app` over both), and `foldd`'s suite runs the three together as the
composite and as three processes. The end-to-end tests build the example
guest themselves (into `target/guest`, so they never contend with the outer
cargo's lock).
