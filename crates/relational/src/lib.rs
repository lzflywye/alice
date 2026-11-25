pub mod codec;
pub mod numeric;
pub mod sql;

use codec::{SqlType, SqlValue, build_btree_key, build_table_prefix, decode_row, encode_row};
use common::DbError;
use numeric::RoundingMode;
use std::collections::HashMap;
use std::path::Path;
use std::sync::RwLock;
use storage_btree::BTreeEngine;

use crate::codec::{decode_table_schema, encode_table_schema};

#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub sql_type: SqlType,
    pub nullable: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub table_id: u32,
    pub name: String,
    pub columns: Vec<ColumnSchema>,
}

impl TableSchema {
    pub fn pk_index(&self) -> Option<usize> {
        self.columns.iter().position(|c| c.primary_key)
    }

    pub fn col_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone)]
pub struct Predicate {
    pub column: String,
    pub op: CmpOp,
    pub rhs: SqlValue,
}

/// Lightweight Relational Database Engine backed by `storage-btree`.
pub struct RelationalDb {
    btree: BTreeEngine,
    tables: RwLock<HashMap<String, TableSchema>>,
    next_table_id: RwLock<u32>,
    default_rounding: RoundingMode,
}

impl RelationalDb {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, DbError> {
        let btree = BTreeEngine::open(dir)?;
        let db = Self {
            btree,
            tables: RwLock::new(HashMap::new()),
            next_table_id: RwLock::new(1),
            default_rounding: RoundingMode::HalfEven,
        };
        db.load_persisted_schemas()?;
        Ok(db)
    }

    pub fn create_table(
        &self,
        name: impl Into<String>,
        columns: Vec<ColumnSchema>,
    ) -> Result<TableSchema, DbError> {
        let name = name.into();
        let mut tables = self.tables.write().map_err(|_| DbError::LockPoisoned)?;
        if tables.contains_key(&name) {
            return Err(DbError::CorruptedData("Table already exists"));
        }
        if columns.iter().filter(|c| c.primary_key).count() != 1 {
            return Err(DbError::CorruptedData(
                "Table must define exactly one PRIMARY KEY column",
            ));
        }

        let mut next_id = self
            .next_table_id
            .write()
            .map_err(|_| DbError::LockPoisoned)?;
        let table_id = *next_id;
        *next_id += 1;

        let schema = TableSchema {
            table_id,
            name: name.clone(),
            columns,
        };
        tables.insert(name, schema.clone());
        self.persist_schema(&schema)?;
        Ok(schema)
    }

    pub fn insert(&self, table_name: &str, raw_values: &[SqlValue]) -> Result<(), DbError> {
        let schema = {
            let tables = self.tables.read().map_err(|_| DbError::LockPoisoned)?;
            tables
                .get(table_name)
                .cloned()
                .ok_or(DbError::CorruptedData("Table not found"))?
        };

        if raw_values.len() != schema.columns.len() {
            return Err(DbError::ColumnCountMismatch {
                expected: schema.columns.len(),
                actual: raw_values.len(),
            });
        }

        let mut coerced = Vec::with_capacity(raw_values.len());
        for (col, val) in schema.columns.iter().zip(raw_values.iter()) {
            if matches!(val, SqlValue::Null) && !col.nullable {
                return Err(DbError::NullConstraintViolation {
                    column: col.name.clone(),
                });
            }
            coerced.push(val.coerce_to(col.sql_type, self.default_rounding)?);
        }

        let pk_idx = schema.pk_index().unwrap();
        let pk_val = &coerced[pk_idx];
        if matches!(pk_val, SqlValue::Null) {
            return Err(DbError::NullConstraintViolation {
                column: schema.columns[pk_idx].name.clone(),
            });
        }

        let btree_key = build_btree_key(schema.table_id, 0, std::slice::from_ref(pk_val));
        if self.btree.get(&btree_key)?.is_some() {
            return Err(DbError::UniqueConstraintViolation {
                column: schema.columns[pk_idx].name.clone(),
            });
        }

        let row_bytes = encode_row(&coerced);
        self.btree.put(&btree_key, &row_bytes)
    }

    /// Point lookup by Primary Key (`O(log N)` B+Tree lookup).
    pub fn get_by_pk(
        &self,
        table_name: &str,
        pk_value: &SqlValue,
    ) -> Result<Option<Vec<SqlValue>>, DbError> {
        let schema = {
            let tables = self.tables.read().map_err(|_| DbError::LockPoisoned)?;
            tables
                .get(table_name)
                .cloned()
                .ok_or(DbError::CorruptedData("Table not found"))?
        };
        let pk_col = &schema.columns[schema.pk_index().unwrap()];
        let coerced_pk = pk_value.coerce_to(pk_col.sql_type, self.default_rounding)?;
        let key = build_btree_key(schema.table_id, 0, &[coerced_pk]);
        match self.btree.get(&key)? {
            Some(raw) => Ok(Some(decode_row(&raw)?)),
            None => Ok(None),
        }
    }

    /// Scans rows matching optional `Predicate` filters.
    pub fn select(
        &self,
        table_name: &str,
        filters: &[Predicate],
    ) -> Result<Vec<Vec<SqlValue>>, DbError> {
        let schema = {
            let tables = self.tables.read().map_err(|_| DbError::LockPoisoned)?;
            tables
                .get(table_name)
                .cloned()
                .ok_or(DbError::CorruptedData("Table not found"))?
        };

        let (start, end) = build_table_prefix(schema.table_id, 0);
        let raw_entries = self.btree.scan_range(Some(&start), Some(&end))?;
        let mut out = Vec::new();

        for (_, row_bytes) in raw_entries {
            let row = decode_row(&row_bytes)?;
            if eval_predicates(&schema, &row, filters, self.default_rounding)? {
                out.push(row);
            }
        }
        Ok(out)
    }

    /// Inner Equi-Join between `left_table` and `right_table` on `left_col == right_col`.
    pub fn inner_join(
        &self,
        left_table: &str,
        right_table: &str,
        left_col: &str,
        right_col: &str,
    ) -> Result<Vec<Vec<SqlValue>>, DbError> {
        let (l_schema, r_schema) = {
            let tables = self.tables.read().map_err(|_| DbError::LockPoisoned)?;
            let l = tables
                .get(left_table)
                .cloned()
                .ok_or(DbError::CorruptedData("Left table not found"))?;
            let r = tables
                .get(right_table)
                .cloned()
                .ok_or(DbError::CorruptedData("Right table not found"))?;
            (l, r)
        };

        let l_idx = l_schema
            .col_index(left_col)
            .ok_or(DbError::CorruptedData("Left join column not found"))?;
        let r_idx = r_schema
            .col_index(right_col)
            .ok_or(DbError::CorruptedData("Right join column not found"))?;

        let left_rows = self.select(left_table, &[])?;
        let right_rows = self.select(right_table, &[])?;

        let mut joined = Vec::new();
        for l_row in &left_rows {
            if matches!(l_row[l_idx], SqlValue::Null) {
                continue;
            }
            for r_row in &right_rows {
                if l_row[l_idx] == r_row[r_idx] {
                    let mut combined = Vec::with_capacity(l_row.len() + r_row.len());
                    combined.extend(l_row.iter().cloned());
                    combined.extend(r_row.iter().cloned());
                    joined.push(combined);
                }
            }
        }
        Ok(joined)
    }

    pub fn delete_by_pk(&self, table_name: &str, pk_value: &SqlValue) -> Result<(), DbError> {
        let schema = {
            let tables = self.tables.read().map_err(|_| DbError::LockPoisoned)?;
            tables
                .get(table_name)
                .cloned()
                .ok_or(DbError::CorruptedData("Table not found"))?
        };
        let pk_col = &schema.columns[schema.pk_index().unwrap()];
        let coerced_pk = pk_value.coerce_to(pk_col.sql_type, self.default_rounding)?;
        let key = build_btree_key(schema.table_id, 0, &[coerced_pk]);
        self.btree.delete(&key)
    }

    pub fn list_tables(&self) -> Result<Vec<TableSchema>, DbError> {
        let tables = self.tables.read().map_err(|_| DbError::LockPoisoned)?;
        let mut list: Vec<TableSchema> = tables.values().cloned().collect();
        list.sort_by_key(|t| t.table_id);
        Ok(list)
    }

    fn persist_schema(&self, schema: &TableSchema) -> Result<(), DbError> {
        let key = build_btree_key(0, 0, &[SqlValue::Text(schema.name.clone())]);
        let encoded = encode_table_schema(schema);
        self.btree.put(&key, &encoded)
    }

    fn load_persisted_schemas(&self) -> Result<(), DbError> {
        let (start, end) = build_table_prefix(0, 0);
        let entries = self.btree.scan_range(Some(&start), Some(&end))?;
        let mut tables = self.tables.write().map_err(|_| DbError::LockPoisoned)?;
        let mut next_id = self
            .next_table_id
            .write()
            .map_err(|_| DbError::LockPoisoned)?;

        for (_, val_bytes) in entries {
            let schema = decode_table_schema(&val_bytes)?;
            if schema.table_id >= *next_id {
                *next_id = schema.table_id + 1;
            }
            tables.insert(schema.name.clone(), schema);
        }
        Ok(())
    }
}

fn eval_predicates(
    schema: &TableSchema,
    row: &[SqlValue],
    filters: &[Predicate],
    mode: RoundingMode,
) -> Result<bool, DbError> {
    for pred in filters {
        let col_idx = schema
            .col_index(&pred.column)
            .ok_or(DbError::CorruptedData("Filter column not found"))?;
        let col_type = schema.columns[col_idx].sql_type;
        let lhs = &row[col_idx];
        let rhs = pred.rhs.coerce_to(col_type, mode)?;

        let matched = match (lhs, &rhs) {
            (SqlValue::Null, _) | (_, SqlValue::Null) => false,
            (SqlValue::Bool(a), SqlValue::Bool(b)) => compare_ord(a, b, pred.op),
            (SqlValue::Int(a), SqlValue::Int(b)) => compare_ord(a, b, pred.op),
            (SqlValue::Float(a), SqlValue::Float(b)) => {
                a.partial_cmp(b).is_some_and(|ord| match_ord(ord, pred.op))
            }
            (SqlValue::Numeric(a), SqlValue::Numeric(b)) => compare_ord(a, b, pred.op),
            (SqlValue::Text(a), SqlValue::Text(b)) => compare_ord(a, b, pred.op),
            (SqlValue::Blob(a), SqlValue::Blob(b)) => compare_ord(a, b, pred.op),
            (SqlValue::Date(a), SqlValue::Date(b)) => compare_ord(a, b, pred.op),
            (SqlValue::Timestamp(a), SqlValue::Timestamp(b)) => compare_ord(a, b, pred.op),
            _ => false,
        };
        if !matched {
            return Ok(false);
        }
    }
    Ok(true)
}

fn compare_ord<T: Ord>(a: &T, b: &T, op: CmpOp) -> bool {
    match_ord(a.cmp(b), op)
}

fn match_ord(ord: std::cmp::Ordering, op: CmpOp) -> bool {
    match op {
        CmpOp::Eq => ord.is_eq(),
        CmpOp::Ne => ord.is_ne(),
        CmpOp::Lt => ord.is_lt(),
        CmpOp::Le => ord.is_le(),
        CmpOp::Gt => ord.is_gt(),
        CmpOp::Ge => ord.is_ge(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::numeric::Numeric;
    use std::fs;

    #[test]
    fn test_numeric_rounding_modes_and_arithmetic() {
        let n1 = Numeric::parse("2.345").unwrap();
        assert_eq!(
            n1.rescale(2, RoundingMode::HalfEven).unwrap().to_string(),
            "2.34"
        );
        let n2 = Numeric::parse("2.355").unwrap();
        assert_eq!(
            n2.rescale(2, RoundingMode::HalfEven).unwrap().to_string(),
            "2.36"
        );
        assert_eq!(
            n1.rescale(2, RoundingMode::HalfUp).unwrap().to_string(),
            "2.35"
        );
        assert_eq!(
            Numeric::parse("-2.399")
                .unwrap()
                .rescale(2, RoundingMode::Down)
                .unwrap()
                .to_string(),
            "-2.39"
        );
        assert_eq!(
            Numeric::parse("-2.301")
                .unwrap()
                .rescale(2, RoundingMode::Up)
                .unwrap()
                .to_string(),
            "-2.31"
        );

        let price = Numeric::parse("19.99").unwrap();
        let tax_rate = Numeric::parse("0.10").unwrap();
        let tax = price
            .checked_mul(tax_rate)
            .unwrap()
            .rescale(2, RoundingMode::HalfEven)
            .unwrap();
        assert_eq!(tax.to_string(), "2.00");
    }

    #[test]
    fn test_relational_tables_numeric_filter_and_inner_join() {
        let dir = std::env::temp_dir().join(format!("rel_db_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        let db = RelationalDb::open(&dir).unwrap();

        db.create_table(
            "departments",
            vec![
                ColumnSchema {
                    name: "id".into(),
                    sql_type: SqlType::Int,
                    nullable: false,
                    primary_key: true,
                },
                ColumnSchema {
                    name: "dept_name".into(),
                    sql_type: SqlType::Text,
                    nullable: false,
                    primary_key: false,
                },
            ],
        )
        .unwrap();

        db.create_table(
            "employees",
            vec![
                ColumnSchema {
                    name: "emp_id".into(),
                    sql_type: SqlType::Int,
                    nullable: false,
                    primary_key: true,
                },
                ColumnSchema {
                    name: "dept_id".into(),
                    sql_type: SqlType::Int,
                    nullable: false,
                    primary_key: false,
                },
                ColumnSchema {
                    name: "salary".into(),
                    sql_type: SqlType::Numeric {
                        precision: 12,
                        scale: 2,
                    },
                    nullable: false,
                    primary_key: false,
                },
            ],
        )
        .unwrap();

        db.insert(
            "departments",
            &[SqlValue::Int(10), SqlValue::Text("Platform".into())],
        )
        .unwrap();
        db.insert(
            "departments",
            &[SqlValue::Int(20), SqlValue::Text("Security".into())],
        )
        .unwrap();

        // Insert with automatic HalfEven rounding to NUMERIC(12, 2)
        db.insert(
            "employees",
            &[
                SqlValue::Int(1),
                SqlValue::Int(10),
                SqlValue::Numeric(Numeric::parse("750000.125").unwrap()), // -> 750000.12
            ],
        )
        .unwrap();
        db.insert(
            "employees",
            &[
                SqlValue::Int(2),
                SqlValue::Int(20),
                SqlValue::Numeric(Numeric::parse("920000.50").unwrap()),
            ],
        )
        .unwrap();

        let high_earners = db
            .select(
                "employees",
                &[Predicate {
                    column: "salary".into(),
                    op: CmpOp::Gt,
                    rhs: SqlValue::Numeric(Numeric::parse("800000.00").unwrap()),
                }],
            )
            .unwrap();
        assert_eq!(high_earners.len(), 1);
        assert_eq!(high_earners[0][0], SqlValue::Int(2));

        let joined = db
            .inner_join("employees", "departments", "dept_id", "id")
            .unwrap();
        assert_eq!(joined.len(), 2);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_sql_parser_ddl_dml_join_and_compact_numeric_packing() {
        let dir = std::env::temp_dir().join(format!("rel_sql_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        let db = RelationalDb::open(&dir).unwrap();

        db.execute_sql(
            "CREATE TABLE accounts (
                id INT PRIMARY KEY,
                owner TEXT NOT NULL,
                balance NUMERIC(12, 2),
                opened_on DATE
            )",
        )
        .unwrap();

        db.execute_sql("INSERT INTO accounts VALUES (1, 'Alice', 1000.125, '2026-11-01')")
            .unwrap();
        db.execute_sql("INSERT INTO accounts VALUES (2, 'Bob', 2500.75, '2026-11-15')")
            .unwrap();
        db.execute_sql("INSERT INTO accounts VALUES (3, 'Charlie', 500.00, '2026-11-26')")
            .unwrap();

        let res = db
            .execute_sql(
                "SELECT owner, balance FROM accounts WHERE balance >= 1000.00 ORDER BY balance DESC LIMIT 2",
            )
            .unwrap();

        match res {
            sql::QueryResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["owner", "balance"]);
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][0], SqlValue::Text("Bob".into()));
                // 1000.125 rounded with HalfEven -> 1000.12
                assert_eq!(rows[1][0], SqlValue::Text("Alice".into()));
                assert_eq!(
                    rows[1][1],
                    SqlValue::Numeric(Numeric::parse("1000.12").unwrap())
                );
            }
            other => panic!("Expected Rows, got {other:?}"),
        }

        // Verify compact 5B / 9B / 17B Numeric encoding sizes
        let small_num = encode_row(&[SqlValue::Numeric(Numeric::parse("12.34").unwrap())]);
        let med_num = encode_row(&[SqlValue::Numeric(
            Numeric::parse("123456789012.34").unwrap(),
        )]);
        let big_num = encode_row(&[SqlValue::Numeric(
            Numeric::parse("123456789012345678901234.56").unwrap(),
        )]);
        assert_eq!(small_num.len(), 2 + 1 + 5); // 5B Numeric
        assert_eq!(med_num.len(), 2 + 1 + 9); // 9B Numeric
        assert_eq!(big_num.len(), 2 + 1 + 17); // 17B Numeric

        let _ = fs::remove_dir_all(&dir);
    }
}
