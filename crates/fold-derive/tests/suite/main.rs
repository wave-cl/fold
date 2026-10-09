//! The derivation service's end-to-end suite: each test starts a database
//! node and a derivation node over the orders example (domain and
//! derivation files, the example guest), appends events to the database
//! and reads state and rows from the derivation node. There is no
//! application here.

mod common;
mod derive;
mod lifecycle;
mod reads;
