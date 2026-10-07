//! Order-preserving, prefix-free key encoding for read-model tables.
//!
//! Every scalar encodes so that `a < b` iff `encode(a) < encode(b)` under
//! byte-wise comparison, and no encoding is a proper prefix of another
//! encoding of the same type. Concatenating parts with
//! [`encode_parts`] therefore preserves tuple order and lets a prefix scan
//! over the leading columns work.
//!
//! | type | encoding |
//! |---|---|
//! | uuid | 16 raw bytes |
//! | string | UTF-8 bytes + `0x00` (interior NUL rejected) |
//! | int (i64) | 8 bytes big-endian with the sign bit flipped |
//! | uint (u64) | 8 bytes big-endian |
//! | bool | one byte, `0` or `1` |
//! | timestamp | as int (nanoseconds) |

use uuid::Uuid;

use crate::error::{Error, Result};

/// One column of a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyPart<'a> {
    Uuid(Uuid),
    Str(&'a str),
    I64(i64),
    U64(u64),
    Bool(bool),
}

pub fn encode_uuid(u: Uuid, out: &mut Vec<u8>) {
    out.extend_from_slice(u.as_bytes());
}

/// UTF-8 bytes followed by a `0x00` terminator. Rejects interior NUL since
/// that would break both ordering and prefix-freedom.
pub fn encode_str(s: &str, out: &mut Vec<u8>) -> Result<()> {
    if s.bytes().any(|b| b == 0) {
        return Err(Error::InvalidKey("string key contains NUL".into()));
    }
    out.extend_from_slice(s.as_bytes());
    out.push(0);
    Ok(())
}

/// Big-endian with the sign bit flipped so negatives sort below positives.
pub fn encode_i64(v: i64, out: &mut Vec<u8>) {
    out.extend_from_slice(&((v as u64) ^ (1u64 << 63)).to_be_bytes());
}

pub fn encode_u64(v: u64, out: &mut Vec<u8>) {
    out.extend_from_slice(&v.to_be_bytes());
}

pub fn encode_bool(v: bool, out: &mut Vec<u8>) {
    out.push(u8::from(v));
}

/// Appends one part.
pub fn encode_part(part: &KeyPart<'_>, out: &mut Vec<u8>) -> Result<()> {
    match part {
        KeyPart::Uuid(u) => encode_uuid(*u, out),
        KeyPart::Str(s) => encode_str(s, out)?,
        KeyPart::I64(v) => encode_i64(*v, out),
        KeyPart::U64(v) => encode_u64(*v, out),
        KeyPart::Bool(b) => encode_bool(*b, out),
    }
    Ok(())
}

/// Concatenates the parts into one key.
pub fn encode_parts(parts: &[KeyPart<'_>]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    for p in parts {
        encode_part(p, &mut out)?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::cmp::Ordering;

    fn enc(p: &KeyPart<'_>) -> Vec<u8> {
        encode_parts(std::slice::from_ref(p)).unwrap()
    }

    #[test]
    fn interior_nul_rejected() {
        assert!(matches!(
            encode_parts(&[KeyPart::Str("a\0b")]),
            Err(Error::InvalidKey(_))
        ));
        assert_eq!(encode_parts(&[KeyPart::Str("ab")]).unwrap(), b"ab\0");
    }

    #[test]
    fn i64_sign_handling_is_the_point() {
        // Without the sign flip, -1 (0xff..) would sort above 1.
        assert!(enc(&KeyPart::I64(-1)) < enc(&KeyPart::I64(0)));
        assert!(enc(&KeyPart::I64(i64::MIN)) < enc(&KeyPart::I64(-1)));
        assert!(enc(&KeyPart::I64(0)) < enc(&KeyPart::I64(i64::MAX)));
    }

    #[test]
    fn strings_are_prefix_free() {
        // "ab" is a prefix of "abc" as text; the encodings must not be.
        let a = enc(&KeyPart::Str("ab"));
        let b = enc(&KeyPart::Str("abc"));
        assert!(!b.starts_with(&a));
        assert!(a < b);
    }

    proptest! {
        #[test]
        fn i64_order_preserved(a: i64, b: i64) {
            prop_assert_eq!(a.cmp(&b), enc(&KeyPart::I64(a)).cmp(&enc(&KeyPart::I64(b))));
        }

        #[test]
        fn u64_order_preserved(a: u64, b: u64) {
            prop_assert_eq!(a.cmp(&b), enc(&KeyPart::U64(a)).cmp(&enc(&KeyPart::U64(b))));
        }

        #[test]
        fn uuid_order_preserved(a: u128, b: u128) {
            let (ua, ub) = (Uuid::from_u128(a), Uuid::from_u128(b));
            prop_assert_eq!(ua.cmp(&ub), enc(&KeyPart::Uuid(ua)).cmp(&enc(&KeyPart::Uuid(ub))));
        }

        #[test]
        fn bool_order_preserved(a: bool, b: bool) {
            prop_assert_eq!(a.cmp(&b), enc(&KeyPart::Bool(a)).cmp(&enc(&KeyPart::Bool(b))));
        }

        #[test]
        fn str_order_preserved_and_prefix_free(a in "[^\\x00]{0,12}", b in "[^\\x00]{0,12}") {
            let (ea, eb) = (enc(&KeyPart::Str(&a)), enc(&KeyPart::Str(&b)));
            prop_assert_eq!(a.as_bytes().cmp(b.as_bytes()), ea.cmp(&eb));
            if a != b {
                prop_assert!(!ea.starts_with(&eb) && !eb.starts_with(&ea));
            }
        }

        /// Concatenation preserves lexicographic tuple order, which only
        /// holds because each part is prefix-free.
        #[test]
        fn parts_preserve_tuple_order(
            s1 in "[^\\x00]{0,6}", i1: i64, u1 in any::<u128>(),
            s2 in "[^\\x00]{0,6}", i2: i64, u2 in any::<u128>(),
        ) {
            let t1 = (s1.as_bytes(), i1, u1);
            let t2 = (s2.as_bytes(), i2, u2);
            let k1 = encode_parts(&[KeyPart::Str(&s1), KeyPart::I64(i1), KeyPart::Uuid(Uuid::from_u128(u1))]).unwrap();
            let k2 = encode_parts(&[KeyPart::Str(&s2), KeyPart::I64(i2), KeyPart::Uuid(Uuid::from_u128(u2))]).unwrap();
            prop_assert_eq!(t1.cmp(&t2), k1.cmp(&k2));
            if t1.cmp(&t2) == Ordering::Equal {
                prop_assert_eq!(k1, k2);
            }
        }
    }
}
