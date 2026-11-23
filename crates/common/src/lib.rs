use std::io;

#[derive(Debug)]
pub enum DbError {
    Io(io::Error),
    InvalidMagic,
    UnsupportedVersion(u16),
    CorruptedData(&'static str),
    ChecksumMismatch {
        expected: u32,
        actual: u32,
    },
    NodeNotFound(u64),
    ParentNotFound(u64),
    CannotDeleteRoot,
    HasChildren(u64),
    InvalidParentLink,
    CycleDetected,
    SchemaAlreadyExists(u16),
    SchemaNotFound(u16),
    InvalidParentType {
        child_type: u16,
        expected_parent: Option<u16>,
        actual_parent: Option<u16>,
    },
    ColumnCountMismatch {
        expected: usize,
        actual: usize,
    },
    TypeMismatch {
        column: String,
        expected: &'static str,
    },
    NullConstraintViolation {
        column: String,
    },
    FixedLengthExceeded {
        column: String,
        max: usize,
        actual: usize,
    },
    InlineCapacityExceeded {
        used: usize,
        max: usize,
    },
    UniqueConstraintViolation {
        column: String,
    },
    KeyTooLarge {
        max: usize,
        actual: usize,
    },
    ReadOnlyReplica,
    ReplicationLag {
        requested_after_lsn: u64,
        oldest_available_lsn: u64,
    },
    InvalidUtf8,
    LockPoisoned,
}

impl From<io::Error> for DbError {
    fn from(err: io::Error) -> Self {
        DbError::Io(err)
    }
}

/// Pure-Rust IEEE 802.3 CRC32 checksum implementation.
pub fn crc32(data: &[u8]) -> u32 {
    const TABLE: [u32; 256] = generate_crc32_table();
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in data {
        let idx = ((crc ^ (byte as u32)) & 0xFF) as usize;
        crc = (crc >> 8) ^ TABLE[idx];
    }
    !crc
}

const fn generate_crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            if c & 1 != 0 {
                c = 0xEDB8_8320 ^ (c >> 1);
            } else {
                c >>= 1;
            }
            k += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc32_standard_vector_and_bit_flip() {
        // Standard IEEE 802.3 CRC32 check value for "123456789"
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0x0000_0000);

        let original = b"AliceDB_storage_integrity_check";
        let mut tampered = *original;
        tampered[10] ^= 0x01; // Flip a single bit
        assert_ne!(crc32(original), crc32(&tampered));
    }
}
