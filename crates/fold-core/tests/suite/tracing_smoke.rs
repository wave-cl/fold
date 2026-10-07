//! The spans the plan names are emitted: `fold.append`, `fold.open`,
//! `fold.recover`, `fold.segment.roll`.

use std::sync::{Arc, Mutex};

use fold_core::{ExpectedVersion, Log, OpenOptions};
use tracing::span;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::{Layer, Registry};

use crate::common::*;

#[derive(Default, Clone)]
struct Spans(Arc<Mutex<Vec<String>>>);

impl<S: tracing::Subscriber> Layer<S> for Spans {
    fn on_new_span(&self, attrs: &span::Attributes<'_>, _id: &span::Id, _ctx: Context<'_, S>) {
        self.0
            .lock()
            .unwrap()
            .push(attrs.metadata().name().to_string());
    }
}

impl Spans {
    fn names(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
    fn count(&self, name: &str) -> usize {
        self.names().iter().filter(|n| n.as_str() == name).count()
    }
}

#[test]
fn append_open_recover_and_roll_spans_are_emitted() {
    let spans = Spans::default();
    let subscriber = Registry::default().with(spans.clone());
    let _guard = tracing::subscriber::set_default(subscriber);

    let d = tmp();
    let opts = OpenOptions::default().segment_max_bytes(200);
    let log = create_with(d.path(), opts.clone());
    assert_eq!(spans.count("fold.open"), 1, "create is an open");
    assert_eq!(spans.count("fold.recover"), 0, "create recovers nothing");
    assert_eq!(spans.count("fold.append"), 0);

    log.append(&sid("s"), ExpectedVersion::Any, vec![ev("E", "0")])
        .unwrap();
    assert_eq!(spans.count("fold.append"), 1);
    let rolls = spans.count("fold.segment.roll");
    for _ in 0..4 {
        log.append(&sid("s"), ExpectedVersion::Any, vec![ev("E", "more")])
            .unwrap();
    }
    assert_eq!(spans.count("fold.append"), 5);
    assert!(
        spans.count("fold.segment.roll") > rolls,
        "a 200-byte segment rolls"
    );
    // a refused append still opens the span: the span wraps the attempt
    let _ = log.append(&sid("s"), ExpectedVersion::NoStream, vec![ev("E", "x")]);
    assert_eq!(spans.count("fold.append"), 6);
    drop(log);

    let _log = Log::open(d.path(), NAME, opts).unwrap();
    assert_eq!(spans.count("fold.open"), 2);
    assert_eq!(spans.count("fold.recover"), 1);
}
