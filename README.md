# fold

An event-sourcing / domain-driven-design database. You declare a domain in a
schema file (bounded contexts, events, values, aggregates with entities, and
projections), write the command handlers, evolve functions and projection
folds as WASM modules, and `foldd` stores the events, validates them, keeps
aggregate state, and runs the projections.

The API is segregated along CQRS lines: a **Command** service executes
declared commands (and appends raw events as an escape hatch), a **Query**
service reads projections with a read-your-writes position token, a **Log**
service exposes events and aggregate state for integration and debugging, and
an **Admin** service reports schema, projection status and health.

Values carry their own rules (`value Money { ... } rules { NonNegative: amount >= 0 }`)
and are checked wherever an instance is created, however deeply nested in an
event, a command, an entity or a read model.

The schema language documents itself: `///` comments attach to declarations,
fields, enum variants, rules, commands, invariants and tables and reach the
model (`fold schema check` shows them), and `fold schema fmt` rewrites files in
a canonical layout keeping every comment. A schema may be split over files
with `import "shared.fold"`; enums may carry payloads
(`enum Status { Pending, Shipped { carrier: string } }`, stored as
`{"Shipped": {...}}`); and fields may have defaults (`qty: uint = 1`), filled
in wherever a record is written and for records stored before the default
existed, which is what makes adding a field to an event a compatible change.

Events evolve: `event OrderCancelled v2 { ... note: string } upcast from v1
{ set note: "legacy" }` (or `upcast from v1 wasm "m"` for an upcaster in the
guest) tells the daemon how an old record reads as the new version, and every
consumer, from projections to aggregate replay, sees the latest version while
the log keeps what was recorded. A schema that changed since the log was
written is diffed against the log at start: compatible changes are applied
(a new projection fills from history, a changed one rebuilds, a dropped table
is dropped), breaking ones are refused with the reason unless
`foldd --force-schema`, and `fold schema diff old.fold new.fold` tells you in
advance.

Invariants are declared in the schema and enforced before anything is
appended: an aggregate's **state invariants** see the state a command would
produce; a context's **projection-driven invariants** read a read model, and
the daemon serializes commands per scope value and catches the projection up
first, so a rule like "at most five open orders per customer" holds under
concurrency. Both state invariants and command guards can be written in the
schema instead of WASM: `invariants MaxLines: len(lines) <= 10` and
`CancelOrder { .. } requires { Open: state.status == Pending }` reject with
the guard's name, and `requires not state exists` is how a command insists
on a fresh stream.

The whole log can be backed up online (`fold backup`) into one checksummed
archive and restored offline (`fold restore`) into a fresh directory; the
daemon can do it on a schedule (`foldd --backup-every 6h --backup-keep 7`) and
can restore a backup into itself while running (`fold restore --live`), keeping
the previous log aside. An incremental backup (`fold backup --incremental`, or
`foldd --backup-incremental`) holds only the records since the newest backup;
`fold restore <inc> <dir> --apply` appends it onto a restored full backup.
Any restore can stop at a point in time with `--to <position>` or
`--at <RFC 3339 timestamp>`: the log comes back holding exactly the events
below that position, or every batch recorded at or before that instant, with
the read models, checkpoints and snapshots that looked past it dropped so
they rebuild.

Every write returns a position token; a query on any member, replica
included, that carries it (`fold query get ... --token <t>`) answers only
once that member has replicated and projected the write, so a client that
writes to the primary and reads from a replica still reads its own writes.
Every read returns a token too, and a client that keeps passing its latest
one (`fold --session <file>` does this for you) never reads an older state
than it already saw, whichever member answers.

A second daemon can run as a read-only **replica** of the first
(`foldd --replicate-from http://primary:4141`): it tails the primary's log
as raw records, runs the same projections and process managers over them,
serves queries, refuses commands, and becomes a primary either in place
(`fold promote`, a failover without a restart) or when restarted without the
flag. With `--auto-failover 30s` it promotes itself once the primary has been
out of reach for that long; this is off by default, since a replica cut off
from a primary that is still serving others would fork the log. With
`--quorum-peers` naming the other members, it promotes itself only after
winning an election: a majority of the cluster must agree the primary is
gone, each member votes once per epoch, and only for a candidate at least
as far along as itself. With `--lease 5s` the primary also serves reads only
under a lease the majority keeps renewing, so a primary cut off from the
cluster stops answering stale reads within five seconds, and a fenced
primary refuses reads outright. Every
promotion starts a new **epoch** (`fold health` shows it); the new primary
fences the old one (`fold fence`), and a write carrying a newer epoch as its
`--fencing-token` fences any old primary it reaches, so it stops taking
writes for good. A fenced daemon rejoins as a replica of the new primary if
their histories agree.

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
`SetTimer` in the reaction): the primary fires a due timer as a
`Fold.TimerFired` event in the log, so replicas, rebuilds and a promoted
replica all see it fire exactly once, and a timer set before a restart
still fires after it.

See [docs/design.md](docs/design.md) for the design.

## Layout

| Crate | What it is |
|---|---|
| `fold-schema` | the `.fold` schema language: parser, resolver, JSON validation, row operations |
| `fold-core` | the append-only segmented log, redb indexes, read-model and snapshot stores |
| `fold-wasm` | the wasmtime host for projection, evolve and command-handler modules |
| `fold-guest` | the SDK a WASM module is written with |
| `fold-proto` | the `fold.v1` gRPC contract (tonic) |
| `foldd` | the daemon |
| `fold-cli` | the `fold` command-line client |
| `examples/orders` | a schema and guest covering every feature of the slice |

## Quickstart

Requirements: the pinned Rust toolchain (installed on first `cargo` call,
including the `wasm32-unknown-unknown` target) and `protoc`
(`brew install protobuf` or `apt-get install protobuf-compiler`).

```bash
cargo build --release -p foldd -p fold-cli
cargo build -p orders-guest --target wasm32-unknown-unknown --release --target-dir target/guest
cp target/guest/wasm32-unknown-unknown/release/orders_guest.wasm examples/orders/orders.wasm
```

Create a log from the example schema and start the daemon:

```bash
target/release/fold init ./orders-db --schema examples/orders/schema.fold
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
fold --addr http://127.0.0.1:4142 query get Orders.CustomerOrders customer_orders '{"customer_id": "<uuid>"}' --token <token from exec>   # read-your-writes on a replica
fold log aggregate order-a0000000-0000-0000-0000-000000000001
fold log process Orders.Fulfilment '"a0000000-0000-0000-0000-000000000001"'
fold projection list
fold projection snapshot Orders.CustomerOrders
fold projection rebuild Orders.CustomerOrders --from <snapshot id>
fold aggregate snapshot Orders.Order
fold aggregate rebuild Orders.Order
fold backup
fold backup --incremental                      # records since the newest backup
fold restore <full.fbak> ./restored-db         # offline, daemon stopped
fold restore <inc.fbak> ./restored-db --apply  # then each increment, in order
fold restore <archive.fbak> --live             # into the running daemon
fold restore <archive.fbak> ./restored-db --to 1200   # point in time: positions below 1200
fold restore <archive.fbak> --live --at 2026-10-08T14:30:00Z   # or a timestamp, live
foldd --data-dir ./replica --schema orders.fold --listen 127.0.0.1:4142 \
      --replicate-from http://127.0.0.1:4141     # a read-only replica
fold --addr http://127.0.0.1:4142 promote     # failover: the replica becomes the primary
foldd ... --replicate-from http://127.0.0.1:4141 --auto-failover 30s   # or by itself
foldd ... --auto-failover 30s --quorum-peers http://127.0.0.1:4141,http://127.0.0.1:4143   # with a majority
foldd ... --quorum-peers http://127.0.0.1:4142,http://127.0.0.1:4143 --lease 5s   # primary: reads under a lease
fold exec Orders.Order.PlaceOrder order-<uuid> -d '{...}' --fencing-token 1   # refused by a stale primary
fold --addr http://127.0.0.1:4141 fence 1     # tell an old primary a newer epoch exists
fold process list
fold process snapshot Orders.Fulfilment
fold process rebuild Orders.Fulfilment --from <snapshot id>
fold schema fmt --check examples/orders/schema.fold   # canonical layout, comments kept
fold schema diff examples/orders/schema.fold new.fold # compatible, rebuild or breaking; exit 1 on breaking
foldd ... --force-schema                      # adopt a breaking schema change anyway
fold log tail
```

`fold --json ...` prints one JSON object per line for scripting.

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

`examples/orders/guest/src/lib.rs` is the complete version.

## Developing

```bash
cargo nextest run --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

The end-to-end tests build the example guest themselves (into
`target/guest`, so they never contend with the outer cargo's lock).
