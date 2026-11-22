#[derive(Debug)]
pub enum HdbError {
    SchemaAlreadyExists(u16),
    SchemaNotFound(u16),
    InvalidParentType {
        expected: Option<u16>,
        actual: Option<u16>,
    },
    CyclicMove {
        node_id: u64,
        target_parent_id: u64,
    },
    InlineCapacityExceeded {
        required: usize,
        max: usize,
    },
    TooManyColumns(usize),
    ColumnCountMismatch {
        expected: usize,
        actual: usize,
    },
    TypeMismatch {
        col_index: usize,
    },
    NullConstraintViolation {
        col_index: usize,
    },
    FixedLengthExceeded {
        col_index: usize,
        max: usize,
        actual: usize,
    },
    InvalidUtf8,
    NodeNotFound(u64),
    HasChildren(u64),
    ReadOnlyReplica,
    ReplicationLag {
        requested_lsn: u64,
        oldest_available_lsn: u64,
    },
    LockPoisoned,
    Io(std::io::Error),
    CorruptedData(&'static str),
}

impl From<std::io::Error> for HdbError {
    fn from(err: std::io::Error) -> Self {
        HdbError::Io(err)
    }
}
