use crate::error::HdbError;

pub const MAX_COLUMNS: usize = 64; // Fits within a single 64-bit null bitmap

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
            _ => Err(HdbError::CorruptedData("Unknown DataType tag in catalog")),
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
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentSchema {
    pub node_type: u16,
    pub name: String,
    pub parent_type: Option<u16>,
    pub columns: Vec<ColumnDef>,
}

impl SegmentSchema {
    /// Validates that column count and inline byte size fit within NodeSlot limits.
    pub fn validate(&self, max_inline_cap: usize) -> Result<(), HdbError> {
        if self.columns.len() > MAX_COLUMNS {
            return Err(HdbError::TooManyColumns(self.columns.len()));
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
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
}
