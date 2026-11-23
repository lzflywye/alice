use crate::error::HdbError;
use crate::schema::{Decimal, SegmentSchema, Value};
use std::collections::{BTreeSet, HashMap};

/// Hashable key representation for secondary index lookups (excludes F64 and Null).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IndexKey {
    Bool(bool),
    I64(i64),
    String(String),
    Bytes(Vec<u8>),
    Decimal(Decimal),
}

impl IndexKey {
    pub fn from_value(val: &Value) -> Option<Self> {
        match val {
            Value::Null | Value::F64(_) => None,
            Value::Bool(b) => Some(IndexKey::Bool(*b)),
            Value::I64(v) => Some(IndexKey::I64(*v)),
            Value::String(s) => Some(IndexKey::String(s.clone())),
            Value::Bytes(b) => Some(IndexKey::Bytes(b.clone())),
            Value::Decimal(d) => Some(IndexKey::Decimal(d.normalize())),
        }
    }
}

/// In-memory secondary index manager keyed by (node_type, col_index, IndexKey).
#[derive(Default)]
pub struct SecondaryIndex {
    entries: HashMap<(u16, usize), HashMap<IndexKey, BTreeSet<u64>>>,
}

impl SecondaryIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Validates unique constraints before committing a mutation.
    pub fn check_unique(
        &self,
        schema: &SegmentSchema,
        values: &[Value],
        exclude_node_id: Option<u64>,
    ) -> Result<(), HdbError> {
        for (col_idx, (col, val)) in schema.columns.iter().zip(values.iter()).enumerate() {
            if !col.unique {
                continue;
            }
            let Some(key) = IndexKey::from_value(val) else {
                continue;
            };
            if let Some(col_map) = self.entries.get(&(schema.node_type, col_idx))
                && let Some(node_ids) = col_map.get(&key)
            {
                let conflict = node_ids.iter().any(|&id| Some(id) != exclude_node_id);
                if conflict {
                    return Err(HdbError::UniqueConstraintViolation {
                        node_type: schema.node_type,
                        col_index: col_idx,
                    });
                }
            }
        }
        Ok(())
    }

    pub fn insert_node(&mut self, schema: &SegmentSchema, node_id: u64, values: &[Value]) {
        for (col_idx, (col, val)) in schema.columns.iter().zip(values.iter()).enumerate() {
            if !(col.indexed || col.unique) {
                continue;
            }
            if let Some(key) = IndexKey::from_value(val) {
                self.entries
                    .entry((schema.node_type, col_idx))
                    .or_default()
                    .entry(key)
                    .or_default()
                    .insert(node_id);
            }
        }
    }

    pub fn remove_node(&mut self, schema: &SegmentSchema, node_id: u64, values: &[Value]) {
        for (col_idx, (col, val)) in schema.columns.iter().zip(values.iter()).enumerate() {
            if !(col.indexed || col.unique) {
                continue;
            }
            if let Some(key) = IndexKey::from_value(val)
                && let Some(col_map) = self.entries.get_mut(&(schema.node_type, col_idx))
                && let Some(set) = col_map.get_mut(&key)
            {
                set.remove(&node_id);
                if set.is_empty() {
                    col_map.remove(&key);
                }
            }
        }
    }

    pub fn lookup(&self, node_type: u16, col_index: usize, val: &Value) -> Vec<u64> {
        let Some(key) = IndexKey::from_value(val) else {
            return Vec::new();
        };
        self.entries
            .get(&(node_type, col_index))
            .and_then(|col_map| col_map.get(&key))
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }
}
