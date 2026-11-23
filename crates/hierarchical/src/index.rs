use crate::schema::{Decimal, SegmentSchema, Value};
use common::DbError;
use std::collections::{BTreeMap, BTreeSet};
use storage_slotted::NodeId;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum IndexKey {
    Null,
    Bool(bool),
    I64(i64),
    F64Bits(u64),
    Decimal(Decimal),
    String(String),
    Bytes(Vec<u8>),
}

impl IndexKey {
    pub fn from_value(v: &Value) -> Self {
        match v {
            Value::Null => IndexKey::Null,
            Value::Bool(b) => IndexKey::Bool(*b),
            Value::I64(n) => IndexKey::I64(*n),
            Value::F64(f) => {
                let bits = if *f == 0.0 {
                    0.0f64.to_bits()
                } else {
                    f.to_bits()
                };
                IndexKey::F64Bits(bits)
            }
            Value::Decimal(d) => IndexKey::Decimal(*d),
            Value::String(s) => IndexKey::String(s.clone()),
            Value::Bytes(b) => IndexKey::Bytes(b.clone()),
        }
    }
}

#[derive(Default)]
pub struct SecondaryIndex {
    tables: BTreeMap<(u16, String), BTreeMap<IndexKey, BTreeSet<NodeId>>>,
}

impl SecondaryIndex {
    pub fn clear(&mut self) {
        self.tables.clear();
    }

    pub fn check_unique(
        &self,
        schema: &SegmentSchema,
        values: &[Value],
        exclude_node: Option<NodeId>,
    ) -> Result<(), DbError> {
        for (col, val) in schema.columns.iter().zip(values.iter()) {
            if !col.unique || matches!(val, Value::Null) {
                continue;
            }
            let key = IndexKey::from_value(val);
            if let Some(map) = self.tables.get(&(schema.node_type, col.name.clone()))
                && let Some(ids) = map.get(&key) {
                    let conflict = match exclude_node {
                        Some(ex) => ids.iter().any(|&id| id != ex),
                        None => !ids.is_empty(),
                    };
                    if conflict {
                        return Err(DbError::UniqueConstraintViolation {
                            column: col.name.clone(),
                        });
                    }
                }
        }
        Ok(())
    }

    pub fn insert_record(&mut self, schema: &SegmentSchema, node_id: NodeId, values: &[Value]) {
        for (col, val) in schema.columns.iter().zip(values.iter()) {
            if !col.indexed || matches!(val, Value::Null) {
                continue;
            }
            let key = IndexKey::from_value(val);
            self.tables
                .entry((schema.node_type, col.name.clone()))
                .or_default()
                .entry(key)
                .or_default()
                .insert(node_id);
        }
    }

    pub fn remove_record(&mut self, schema: &SegmentSchema, node_id: NodeId, values: &[Value]) {
        for (col, val) in schema.columns.iter().zip(values.iter()) {
            if !col.indexed || matches!(val, Value::Null) {
                continue;
            }
            let key = IndexKey::from_value(val);
            if let Some(map) = self.tables.get_mut(&(schema.node_type, col.name.clone()))
                && let Some(set) = map.get_mut(&key) {
                    set.remove(&node_id);
                    if set.is_empty() {
                        map.remove(&key);
                    }
                }
        }
    }

    pub fn lookup(&self, node_type: u16, col_name: &str, value: &Value) -> Vec<NodeId> {
        let key = IndexKey::from_value(value);
        self.tables
            .get(&(node_type, col_name.to_string()))
            .and_then(|map| map.get(&key))
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }
}
