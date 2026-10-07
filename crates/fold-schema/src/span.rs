//! Byte-offset spans into the schema source and line/column rendering.

/// A half-open byte range `[start, end)` into the source text.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    pub fn new(start: usize, end: usize) -> Self {
        Span { start, end }
    }

    /// The smallest span covering both `self` and `other`.
    pub fn join(self, other: Span) -> Span {
        Span {
            start: self.start.min(other.start),
            end: self.end.max(other.end),
        }
    }

    /// 1-based `(line, column)` of the span's start in `src`.
    pub fn line_col(&self, src: &str) -> (usize, usize) {
        line_col(src, self.start)
    }
}

/// 1-based `(line, column)` of byte `offset` in `src`. Columns count
/// characters, not bytes. An offset past the end points just after the text.
pub fn line_col(src: &str, offset: usize) -> (usize, usize) {
    let offset = offset.min(src.len());
    let before = &src[..offset];
    let line = before.matches('\n').count() + 1;
    let line_start = before.rfind('\n').map_or(0, |i| i + 1);
    let col = before[line_start..].chars().count() + 1;
    (line, col)
}

/// The full source line containing byte `offset`, without its newline.
pub fn source_line(src: &str, offset: usize) -> &str {
    let offset = offset.min(src.len());
    let line_start = src[..offset].rfind('\n').map_or(0, |i| i + 1);
    let line_end = src[offset..].find('\n').map_or(src.len(), |i| offset + i);
    src[line_start..line_end].trim_end_matches('\r')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn line_col_counts_from_one() {
        let src = "ab\ncd\n\nef";
        assert_eq!(line_col(src, 0), (1, 1));
        assert_eq!(line_col(src, 1), (1, 2));
        assert_eq!(line_col(src, 3), (2, 1));
        assert_eq!(line_col(src, 7), (4, 1));
        assert_eq!(line_col(src, 9), (4, 3));
        assert_eq!(line_col(src, 100), (4, 3));
    }

    #[test]
    fn source_line_strips_newline() {
        let src = "first\nsecond\r\nthird";
        assert_eq!(source_line(src, 0), "first");
        assert_eq!(source_line(src, 8), "second");
        assert_eq!(source_line(src, 15), "third");
    }

    #[test]
    fn join_covers_both() {
        assert_eq!(Span::new(3, 5).join(Span::new(1, 4)), Span::new(1, 5));
    }
}
