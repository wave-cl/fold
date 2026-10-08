//! A client session: the latest position token seen, kept in a file, so
//! that every read carries it and every read or write advances it. With
//! `--session <file>` (or `FOLD_SESSION`) a client's reads never go
//! backwards, whichever member answers them.

use std::path::{Path, PathBuf};

use anyhow::Context as _;

#[derive(Debug)]
pub struct Session {
    path: PathBuf,
    token: Option<String>,
}

/// `(log_id, position)` of a token, or `None` if it is not one.
fn parse(token: &str) -> Option<(&str, u64)> {
    let mut parts = token.split(':');
    let tag = parts.next()?;
    let log_id = parts.next()?;
    let _epoch = parts.next()?;
    let position: u64 = parts.next()?.parse().ok()?;
    if tag != "fold1" || parts.next().is_some() {
        return None;
    }
    Some((log_id, position))
}

impl Session {
    /// Loads the session at `path`; a missing file is an empty session.
    pub fn load(path: &Path) -> anyhow::Result<Session> {
        let token = match std::fs::read_to_string(path) {
            Ok(text) => {
                let t = text.trim().to_string();
                if t.is_empty() {
                    None
                } else {
                    anyhow::ensure!(
                        parse(&t).is_some(),
                        "{} does not hold a position token: {t:?}",
                        path.display()
                    );
                    Some(t)
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e).with_context(|| format!("cannot read {}", path.display())),
        };
        Ok(Session {
            path: path.to_path_buf(),
            token,
        })
    }

    /// The token to send with the next read, if any.
    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    /// Takes `token` into the session if it is further along: a token of
    /// another log replaces the session (the client moved cluster), an
    /// older position of the same log is ignored.
    pub fn advance(&mut self, token: &str) -> anyhow::Result<()> {
        let Some((log_id, position)) = parse(token) else {
            if token.is_empty() {
                return Ok(());
            }
            anyhow::bail!("the server returned something that is not a position token: {token:?}");
        };
        let keep = match self.token.as_deref().and_then(parse) {
            Some((had_log, had_pos)) if had_log == log_id => position > had_pos,
            _ => true,
        };
        if keep {
            self.token = Some(token.to_string());
            std::fs::write(&self.path, format!("{token}\n"))
                .with_context(|| format!("cannot write {}", self.path.display()))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_session_keeps_the_furthest_token_of_its_log() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session");
        let mut s = Session::load(&path).unwrap();
        assert_eq!(s.token(), None);
        s.advance("fold1:a:0:10").unwrap();
        s.advance("fold1:a:0:7").unwrap();
        assert_eq!(
            s.token(),
            Some("fold1:a:0:10"),
            "an older position is ignored"
        );
        s.advance("fold1:a:1:11").unwrap();
        assert_eq!(s.token(), Some("fold1:a:1:11"));
        s.advance("").unwrap();
        assert_eq!(s.token(), Some("fold1:a:1:11"), "no token, no change");
        assert!(s.advance("junk").is_err());
        // Persisted, and reloaded as it was.
        let s2 = Session::load(&path).unwrap();
        assert_eq!(s2.token(), Some("fold1:a:1:11"));
        // Another log replaces the session.
        let mut s2 = s2;
        s2.advance("fold1:b:0:1").unwrap();
        assert_eq!(s2.token(), Some("fold1:b:0:1"));
        // A file holding junk is refused rather than silently emptied.
        std::fs::write(&path, "not a token\n").unwrap();
        assert!(Session::load(&path).is_err());
    }
}
