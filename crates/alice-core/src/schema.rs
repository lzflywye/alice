use crate::error::HdbError;

pub const MAX_COLUMNS: usize = 64; // Fits within a single 64-bit null bitmap

/// Exact 128-bit base-10 decimal representation: `unscaled * 10^(-scale)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal {
    pub unscaled: i128,
    pub scale: u8,
}

impl Decimal {
    pub fn new(unscaled: i128, scale: u8) -> Self {
        Self { unscaled, scale }
    }

    /// Normalizes trailing base-10 zeros so equivalent values (e.g., 1.50 and 1.5) share the same key.
    pub fn normalize(mut self) -> Self {
        if self.unscaled == 0 {
            self.scale = 0;
            return self;
        }
        while self.scale > 0 && self.unscaled % 10 == 0 {
            self.unscaled /= 10;
            self.scale -= 1;
        }
        self
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Bool = 1,
    I64 = 2,
    F64 = 3,
    FixedString(u16) = 4,
    FixedBytes(u16) = 5,
    VarString = 6,
    VarBytes = 7,
    Decimal = 8,
}

impl DataType {
    pub fn type_tag(&self) -> u8 {
        match self {
            DataType::Bool => 1,
            DataType::I64 => 2,
            DataType::F64 => 3,
            DataType::FixedString(_) => 4,
            DataType::FixedBytes(_) => 5,
            DataType::VarString => 6,
            DataType::VarBytes => 7,
            DataType::Decimal => 8,
        }
    }

    pub fn type_param(&self) -> u16 {
        match self {
            DataType::FixedString(n) | DataType::FixedBytes(n) => *n,
            _ => 0,
        }
    }

    pub fn from_raw(tag: u8, param: u16) -> Result<Self, HdbError> {
        match tag {
            1 => Ok(DataType::Bool),
            2 => Ok(DataType::I64),
            3 => Ok(DataType::F64),
            4 => Ok(DataType::FixedString(param)),
            5 => Ok(DataType::FixedBytes(param)),
            6 => Ok(DataType::VarString),
            7 => Ok(DataType::VarBytes),
            8 => Ok(DataType::Decimal),
            _ => Err(HdbError::CorruptedData("Unknown DataType tag")),
        }
    }

    /// Returns the exact number of bytes consumed in the inline payload area.
    pub fn inline_size(&self) -> usize {
        match self {
            DataType::Bool => 1,
            DataType::I64 | DataType::F64 => 8,
            DataType::FixedString(n) => 2 + (*n as usize),
            DataType::FixedBytes(n) => *n as usize,
            DataType::VarString | DataType::VarBytes => 12,
            DataType::Decimal => 17, // 1B scale + 16B i128
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub indexed: bool,
    pub unique: bool,
}

impl ColumnDef {
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
            indexed: false,
            unique: false,
        }
    }

    pub fn with_index(mut self, unique: bool) -> Self {
        self.indexed = true;
        self.unique = unique;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentSchema {
    pub node_type: u16,
    pub name: String,
    pub parent_type: Option<u16>,
    pub columns: Vec<ColumnDef>,
}

impl SegmentSchema {
    /// Validates column count, indexable types, and inline payload capacity.
    pub fn validate(&self, max_inline_cap: usize) -> Result<(), HdbError> {
        if self.columns.len() > MAX_COLUMNS {
            return Err(HdbError::TooManyColumns(self.columns.len()));
        }
        for (i, col) in self.columns.iter().enumerate() {
            if (col.indexed || col.unique) && matches!(col.data_type, DataType::F64) {
                return Err(HdbError::UnsupportedIndexType { col_index: i });
            }
        }
        let required: usize = 8 + self
            .columns
            .iter()
            .map(|c| c.data_type.inline_size())
            .sum::<usize>();
        if required > max_inline_cap {
            return Err(HdbError::InlineCapacityExceeded {
                required,
                max: max_inline_cap,
            });
        }
        Ok(())
    }

    pub fn find_column(&self, name: &str) -> Option<(usize, &ColumnDef)> {
        self.columns
            .iter()
            .enumerate()
            .find(|(_, c)| c.name == name)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Decimal(Decimal),
}
