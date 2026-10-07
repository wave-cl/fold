# fold

An event-sourcing / domain-driven-design database. You declare a domain in a
schema file (bounded contexts, events, values, aggregates with entities, and
projections), write the command handlers, evolve functions and projection folds
as WASM modules, and `foldd` stores the events, validates them, keeps aggregate
state, and runs the projections.

Work in progress; see the crates under `crates/`.
