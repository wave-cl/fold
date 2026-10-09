//! The system token: what lets the application service append `Fold.*`
//! events (process timers) to the database. Both sides derive it from one
//! shared secret; the database compares the sha256 of what a request
//! carries with the sha256 of its own, in constant time.

use sha2::{Digest, Sha256};
use tonic::Status;

/// The database's side of the secret.
#[derive(Clone, Debug)]
pub struct SystemToken {
    /// sha256 of the secret; `None` when the database has none.
    hash: Option<[u8; 32]>,
}

impl SystemToken {
    pub fn from_secret(secret: Option<&str>) -> Self {
        SystemToken {
            hash: secret.map(digest),
        }
    }

    pub fn is_configured(&self) -> bool {
        self.hash.is_some()
    }

    /// Whether `metadata` carries the token of this database's secret.
    pub fn accepts(&self, metadata: &tonic::metadata::MetadataMap) -> bool {
        let Some(mine) = &self.hash else {
            return false;
        };
        let Some(given) = metadata
            .get(fold_proto::SYSTEM_TOKEN_HEADER)
            .and_then(|v| v.to_str().ok())
        else {
            return false;
        };
        constant_time_eq(&digest(given), mine)
    }

    /// The refusal for a `Fold.*` append without the token.
    pub fn refusal(&self, type_ref: &str) -> Status {
        if self.is_configured() {
            Status::permission_denied(format!(
                "{type_ref} is a system event; appending it needs the system token in the `{}` header",
                fold_proto::SYSTEM_TOKEN_HEADER
            ))
        } else {
            Status::permission_denied(format!(
                "{type_ref} is a system event, and this database has no system secret; start it with one for the application service to append timers"
            ))
        }
    }
}

/// The header value for `secret`, as the application service sends it.
pub fn header_value(secret: &str) -> String {
    secret.to_string()
}

fn digest(s: &str) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    h.finalize().into()
}

fn constant_time_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_token_is_the_secret_and_nothing_else() {
        let t = SystemToken::from_secret(Some("s3cret"));
        let mut md = tonic::metadata::MetadataMap::new();
        assert!(!t.accepts(&md), "no header");
        md.insert(
            fold_proto::SYSTEM_TOKEN_HEADER,
            header_value("other").parse().unwrap(),
        );
        assert!(!t.accepts(&md), "wrong secret");
        md.insert(
            fold_proto::SYSTEM_TOKEN_HEADER,
            header_value("s3cret").parse().unwrap(),
        );
        assert!(t.accepts(&md));
        let none = SystemToken::from_secret(None);
        assert!(!none.accepts(&md), "no secret accepts nothing");
        assert!(
            none.refusal("Fold.TimerFired")
                .message()
                .contains("no system secret")
        );
    }
}
