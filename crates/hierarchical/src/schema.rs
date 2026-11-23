use common::DbError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Bool,
    I64,
    F64,
    Decimal,
    FixedString(u16),
    FixedBytes(u16),
    VarString,
    VarBytes,
}

impl DataType {
    pub fn type_tag(&self) -> u8 {
        match self {
            DataType::Bool => 1,
            DataType::I64 => 2,
            DataType::F64 => 3,
            DataType::Decimal => 4,
            DataType::FixedString(_) => 5,
            DataType::FixedBytes(_) => 6,
            DataType::VarString => 7,
            DataType::VarBytes => 8,
        }
    }

    pub fn param(&self) -> u16 {
        match self {
            DataType::FixedString(n) | DataType::FixedBytes(n) => *n,
            _ => 0,
        }
    }

    pub fn from_tag(tag: u8, param: u16) -> Result<Self, DbError> {
        match tag {
            1 => Ok(DataType::Bool),
            2 => Ok(DataType::I64),
            3 => Ok(DataType::F64),
            4 => Ok(DataType::Decimal),
            5 => Ok(DataType::FixedString(param)),
            6 => Ok(DataType::FixedBytes(param)),
            7 => Ok(DataType::VarString),
            8 => Ok(DataType::VarBytes),
            _ => Err(DbError::CorruptedData("Unknown column DataType tag")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Decimal {
    pub unscaled: i128,
    pub scale: u8,
}

impl Decimal {
    pub fn new(unscaled: i128, scale: u8) -> Self {
        Self { unscaled, scale }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Decimal(Decimal),
    String(String),
    Bytes(Vec<u8>),
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
    pub fn validate_values(&self, values: &[Value]) -> Result<(), DbError> {
        if values.len() != self.columns.len() {
            return Err(DbError::ColumnCountMismatch {
                expected: self.columns.len(),
                actual: values.len(),
            });
        }

        for (col, val) in self.columns.iter().zip(values.iter()) {
            if matches!(val, Value::Null) {
                if !col.nullable {
                    return Err(DbError::NullConstraintViolation {
                        column: col.name.clone(),
                    });
                }
                continue;
            }

            match (&col.data_type, val) {
                (DataType::Bool, Value::Bool(_))
                | (DataType::I64, Value::I64(_))
                | (DataType::F64, Value::F64(_))
                | (DataType::Decimal, Value::Decimal(_))
                | (DataType::VarString, Value::String(_))
                | (DataType::VarBytes, Value::Bytes(_)) => {}
                (DataType::FixedString(max), Value::String(s)) => {
                    if s.len() > *max as usize {
                        return Err(DbError::FixedLengthExceeded {
                            column: col.name.clone(),
                            max: *max as usize,
                            actual: s.len(),
                        });
                    }
                }
                (DataType::FixedBytes(max), Value::Bytes(b)) => {
                    if b.len() > *max as usize {
                        return Err(DbError::FixedLengthExceeded {
                            column: col.name.clone(),
                            max: *max as usize,
                            actual: b.len(),
                        });
                    }
                }
                (dt, _) => {
                    return Err(DbError::TypeMismatch {
                        column: col.name.clone(),
                        expected: type_label(*dt),
                    });
                }
            }
        }
        Ok(())
    }
}

fn type_label(dt: DataType) -> &'static str {
    match dt {
        DataType::Bool => "Bool",
        DataType::I64 => "I64",
        DataType::F64 => "F64",
        DataType::Decimal => "Decimal",
        DataType::FixedString(_) => "FixedString",
        DataType::FixedBytes(_) => "FixedBytes",
        DataType::VarString => "VarString",
        DataType::VarBytes => "VarBytes",
    }
}
