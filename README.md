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
fold log aggregate order-a0000000-0000-0000-0000-000000000001
fold projection list
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
