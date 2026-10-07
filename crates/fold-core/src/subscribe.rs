use tokio::sync::watch;

use crate::error::Closed;
use crate::ids::GlobalPosition;

/// Follows the log head. Created by `Log::subscribe`; works without a tokio
/// runtime for `current`, needs one only to await `wait_past`.
#[derive(Debug, Clone)]
pub struct Subscription {
    rx: watch::Receiver<u64>,
}

impl Subscription {
    pub(crate) fn new(rx: watch::Receiver<u64>) -> Self {
        Subscription { rx }
    }

    /// The head as last published: the position the next append will take.
    pub fn current(&self) -> GlobalPosition {
        GlobalPosition(*self.rx.borrow())
    }

    /// Resolves with the head once `head > pos`, i.e. once position `pos`
    /// has been committed. Returns at once if it already has. `Err(Closed)`
    /// when every `Log` handle has been dropped.
    pub async fn wait_past(&mut self, pos: GlobalPosition) -> Result<GlobalPosition, Closed> {
        loop {
            let head = *self.rx.borrow_and_update();
            if head > pos.0 {
                return Ok(GlobalPosition(head));
            }
            self.rx.changed().await.map_err(|_| Closed)?;
        }
    }
}
