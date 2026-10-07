//! Create a log in a temporary directory, append a few events to two
//! streams, and read them back by stream, by position and by type.
//!
//! ```sh
//! cargo run -p fold-core --example append_read
//! ```

use bytes::Bytes;
use fold_core::{
    Direction, EventType, ExpectedVersion, GlobalPosition, Log, NewEvent, OpenOptions, StreamId,
    StreamVersion,
};

fn main() -> fold_core::Result<()> {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = Log::create(dir.path(), "orders", OpenOptions::default())?;
    println!("created {}", log.path().display());

    let placed = EventType::new("Orders", "OrderPlaced", 1);
    let cancelled = EventType::new("Orders", "OrderCancelled", 1);
    let order_a = StreamId::new("order-a")?;
    let order_b = StreamId::new("order-b")?;

    let r = log.append(
        &order_a,
        ExpectedVersion::NoStream,
        vec![NewEvent::new(
            placed.clone(),
            Bytes::from_static(br#"{"total":"40.00"}"#),
        )],
    )?;
    println!(
        "order-a placed at position {} version {}",
        r.first, r.stream_version
    );

    let r = log.append(
        &order_b,
        ExpectedVersion::NoStream,
        vec![NewEvent::new(
            placed.clone(),
            Bytes::from_static(br#"{"total":"12.50"}"#),
        )],
    )?;
    println!(
        "order-b placed at position {} version {}",
        r.first, r.stream_version
    );

    let r = log.append(
        &order_a,
        ExpectedVersion::Exact(StreamVersion(0)),
        vec![NewEvent::new(
            cancelled,
            Bytes::from_static(br#"{"reason":"changed mind"}"#),
        )],
    )?;
    println!(
        "order-a cancelled at position {} version {}",
        r.first, r.stream_version
    );

    // A stale expectation is refused and writes nothing.
    let refused = log.append(
        &order_a,
        ExpectedVersion::Exact(StreamVersion(0)),
        vec![NewEvent::new(placed, Bytes::new())],
    );
    println!("stale append: {}", refused.unwrap_err());

    println!("\nstream order-a:");
    for e in log.read_stream(&order_a, StreamVersion(0), Direction::Forward, 100)? {
        println!(
            "  v{} @{} {} {}",
            e.stream_version,
            e.position,
            e.event_type,
            String::from_utf8_lossy(&e.payload)
        );
    }

    println!("\nall events (head = {}):", log.head());
    for e in log.read_all(GlobalPosition(0), 100)? {
        println!("  @{} {} {}", e.position, e.stream_id, e.event_type);
    }

    println!("\nOrders.OrderPlaced:");
    for e in log.read_by_type("Orders.OrderPlaced", GlobalPosition(0), 100)? {
        println!("  @{} {}", e.position, e.stream_id);
    }

    Ok(())
}
