//! Position tokens: `fold1:<log_id>:<epoch>:<position>`. What a write
//! returns and what a read on any node hands back for read-your-writes;
//! every service parses them the same way.

use std::fmt;

/// Why a token was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenError {
    /// Not four `:`-separated parts.
    Malformed,
    /// A tag other than `fold1`.
    UnknownFormat(String),
    /// The log id is not a uuid.
    BadLogId,
    /// The token is of another log.
    OtherLog { token: uuid::Uuid, ours: uuid::Uuid },
    /// The position is not a number.
    BadPosition,
}

impl fmt::Display for TokenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TokenError::Malformed => write!(
                f,
                "token must be fold1:<log_id>:<epoch>:<position>, as a write returned it"
            ),
            TokenError::UnknownFormat(tag) => write!(f, "unknown token format {tag:?}"),
            TokenError::BadLogId => write!(f, "token: the log id is not a uuid"),
            TokenError::OtherLog { token, ours } => {
                write!(f, "token is of log {token}; this node serves log {ours}")
            }
            TokenError::BadPosition => write!(f, "token: the position is not a number"),
        }
    }
}

impl std::error::Error for TokenError {}

/// A position token for `position` of `log_id` at `epoch`.
pub fn position_token(log_id: uuid::Uuid, epoch: u64, position: u64) -> String {
    format!("fold1:{log_id}:{epoch}:{position}")
}

/// Parses a position token, checking it is of `log_id`. Returns the
/// position.
pub fn parse_position_token(token: &str, log_id: uuid::Uuid) -> Result<u64, TokenError> {
    let parts: Vec<&str> = token.split(':').collect();
    let [tag, id, _epoch, position] = parts.as_slice() else {
        return Err(TokenError::Malformed);
    };
    if *tag != "fold1" {
        return Err(TokenError::UnknownFormat((*tag).to_string()));
    }
    let id: uuid::Uuid = id.parse().map_err(|_| TokenError::BadLogId)?;
    if id != log_id {
        return Err(TokenError::OtherLog {
            token: id,
            ours: log_id,
        });
    }
    position.parse().map_err(|_| TokenError::BadPosition)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_token_round_trips_and_names_its_log() {
        let id = uuid::Uuid::from_u128(7);
        let t = position_token(id, 3, 42);
        assert_eq!(t, format!("fold1:{id}:3:42"));
        assert_eq!(parse_position_token(&t, id), Ok(42));
        let other = uuid::Uuid::from_u128(8);
        assert!(matches!(
            parse_position_token(&t, other),
            Err(TokenError::OtherLog { .. })
        ));
        assert_eq!(parse_position_token("x", id), Err(TokenError::Malformed));
        assert_eq!(
            parse_position_token("fold2:a:b:c", id),
            Err(TokenError::UnknownFormat("fold2".into()))
        );
        assert_eq!(
            parse_position_token("fold1:nope:1:2", id),
            Err(TokenError::BadLogId)
        );
        assert_eq!(
            parse_position_token(&format!("fold1:{id}:1:x"), id),
            Err(TokenError::BadPosition)
        );
    }
}
