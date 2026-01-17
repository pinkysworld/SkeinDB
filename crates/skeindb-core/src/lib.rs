//! SkeinDB Core (scaffold)
//!
//! This crate will host:
//! - on-disk formats (WAL, segments)
//! - MVCC engine
//! - deduplicated ValueStore
//! - compaction + GC
//!
//! For now it contains only basic encoding helpers used across the project.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("unexpected EOF")]
    UnexpectedEof,

    #[error("invalid varint")]
    InvalidVarint,
}

/// Encode a u64 as unsigned LEB128 (VarU).
pub fn encode_varu(mut value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            break;
        } else {
            out.push(byte | 0x80);
        }
    }
    out
}

/// Decode a u64 from unsigned LEB128 (VarU).
pub fn decode_varu(input: &mut &[u8]) -> Result<u64, CoreError> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;

    for _ in 0..10 {
        let Some((&b, rest)) = input.split_first() else {
            return Err(CoreError::UnexpectedEof);
        };
        *input = rest;

        let payload = (b & 0x7F) as u64;
        result |= payload << shift;

        if (b & 0x80) == 0 {
            return Ok(result);
        }
        shift += 7;
    }

    Err(CoreError::InvalidVarint)
}

/// CRC32C of a byte slice.
pub fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// Compute ValueID = BLAKE3-128 (first 16 bytes)
pub fn value_id(bytes: &[u8]) -> [u8; 16] {
    let h = blake3::hash(bytes);
    let mut out = [0u8; 16];
    out.copy_from_slice(&h.as_bytes()[0..16]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varu_roundtrip_small() {
        for v in 0u64..10000 {
            let enc = encode_varu(v);
            let mut slice: &[u8] = &enc;
            let dec = decode_varu(&mut slice).unwrap();
            assert_eq!(v, dec);
            assert!(slice.is_empty());
        }
    }

    #[test]
    fn varu_roundtrip_large() {
        let values = [0u64, 1, 127, 128, 255, 16384, u32::MAX as u64, u64::MAX];
        for v in values {
            let enc = encode_varu(v);
            let mut slice: &[u8] = &enc;
            let dec = decode_varu(&mut slice).unwrap();
            assert_eq!(v, dec);
        }
    }

    #[test]
    fn value_id_is_stable() {
        let a = value_id(b"hello");
        let b = value_id(b"hello");
        let c = value_id(b"world");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
