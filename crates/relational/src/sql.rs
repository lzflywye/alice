use crate::codec::{SqlType, SqlValue};
use crate::numeric::Numeric;
use crate::{CmpOp, ColumnSchema, Predicate, RelationalDb};
use common::DbError;

#[derive(Debug, Clone, PartialEq)]
pub enum QueryResult {
    AffectedRows(usize),
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<SqlValue>>,
    },
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Number(String),
    StringLit(String),
    LParen,
    RParen,
    Comma,
    Semicolon,
    Star,
    Dot,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Eof,
}

fn tokenize(input: &str) -> Result<Vec<Token>, DbError> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let ch = chars[i];
        if ch.is_whitespace() {
            i += 1;
            continue;
        }
        match ch {
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            ';' => {
                tokens.push(Token::Semicolon);
                i += 1;
            }
            '*' => {
                tokens.push(Token::Star);
                i += 1;
            }
            '.' => {
                tokens.push(Token::Dot);
                i += 1;
            }
            '=' => {
                tokens.push(Token::Eq);
                i += 1;
            }
            '!' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                tokens.push(Token::Ne);
                i += 2;
            }
            '<' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::Le);
                    i += 2;
                } else if i + 1 < chars.len() && chars[i + 1] == '>' {
                    tokens.push(Token::Ne);
                    i += 2;
                } else {
                    tokens.push(Token::Lt);
                    i += 1;
                }
            }
            '>' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::Ge);
                    i += 2;
                } else {
                    tokens.push(Token::Gt);
                    i += 1;
                }
            }
            '\'' => {
                i += 1;
                let mut lit = String::new();
                while i < chars.len() {
                    if chars[i] == '\'' {
                        if i + 1 < chars.len() && chars[i + 1] == '\'' {
                            lit.push('\'');
                            i += 2;
                        } else {
                            break;
                        }
                    } else {
                        lit.push(chars[i]);
                        i += 1;
                    }
                }
                if i >= chars.len() || chars[i] != '\'' {
                    return Err(DbError::CorruptedData("Unterminated SQL string literal"));
                }
                i += 1;
                tokens.push(Token::StringLit(lit));
            }
            '-' if i + 1 < chars.len() && chars[i + 1].is_ascii_digit() => {
                let start = i;
                i += 1;
                while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                    i += 1;
                }
                tokens.push(Token::Number(chars[start..i].iter().collect()));
            }
            c if c.is_ascii_digit() => {
                let start = i;
                while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                    i += 1;
                }
                tokens.push(Token::Number(chars[start..i].iter().collect()));
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let start = i;
                while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                tokens.push(Token::Ident(chars[start..i].iter().collect()));
            }
            _ => return Err(DbError::CorruptedData("Unexpected character in SQL")),
        }
    }

    tokens.push(Token::Eof);
    Ok(tokens)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn next(&mut self) -> Token {
        let tok = self.peek().clone();
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
        tok
    }

    fn eat_keyword(&mut self, kw: &str) -> bool {
        if let Token::Ident(s) = self.peek()
            && s.eq_ignore_ascii_case(kw)
        {
            self.next();
            return true;
        }
        false
    }

    fn expect_keyword(&mut self, kw: &str) -> Result<(), DbError> {
        if self.eat_keyword(kw) {
            Ok(())
        } else {
            Err(DbError::CorruptedData("Expected SQL keyword"))
        }
    }

    fn expect_ident(&mut self) -> Result<String, DbError> {
        match self.next() {
            Token::Ident(s) => Ok(s),
            _ => Err(DbError::CorruptedData("Expected SQL identifier")),
        }
    }

    fn expect_token(&mut self, expected: &Token) -> Result<(), DbError> {
        if self.peek() == expected {
            self.next();
            Ok(())
        } else {
            Err(DbError::CorruptedData("Unexpected SQL token"))
        }
    }

    fn parse_sql_type(&mut self) -> Result<SqlType, DbError> {
        let type_name = self.expect_ident()?.to_ascii_uppercase();
        match type_name.as_str() {
            "BOOL" | "BOOLEAN" => Ok(SqlType::Bool),
            "INT" | "INTEGER" | "BIGINT" => Ok(SqlType::Int),
            "FLOAT" | "DOUBLE" | "REAL" => Ok(SqlType::Float),
            "TEXT" | "VARCHAR" | "CHAR" => {
                if self.peek() == &Token::LParen {
                    self.next();
                    let _ = self.next();
                    self.expect_token(&Token::RParen)?;
                }
                Ok(SqlType::Text)
            }
            "BLOB" | "BYTEA" => Ok(SqlType::Blob),
            "DATE" => Ok(SqlType::Date),
            "TIMESTAMP" => Ok(SqlType::Timestamp),
            "NUMERIC" | "DECIMAL" => {
                if self.peek() == &Token::LParen {
                    self.next();
                    let p = match self.next() {
                        Token::Number(n) => n
                            .parse::<u8>()
                            .map_err(|_| DbError::CorruptedData("Invalid NUMERIC precision"))?,
                        _ => return Err(DbError::CorruptedData("Expected NUMERIC precision")),
                    };
                    let s = if self.peek() == &Token::Comma {
                        self.next();
                        match self.next() {
                            Token::Number(n) => n
                                .parse::<u8>()
                                .map_err(|_| DbError::CorruptedData("Invalid NUMERIC scale"))?,
                            _ => return Err(DbError::CorruptedData("Expected NUMERIC scale")),
                        }
                    } else {
                        0
                    };
                    self.expect_token(&Token::RParen)?;
                    Ok(SqlType::Numeric {
                        precision: p,
                        scale: s,
                    })
                } else {
                    Ok(SqlType::Numeric {
                        precision: 18,
                        scale: 0,
                    })
                }
            }
            _ => Err(DbError::CorruptedData("Unsupported SQL column type")),
        }
    }

    fn parse_literal(&mut self) -> Result<SqlValue, DbError> {
        match self.next() {
            Token::Ident(s) if s.eq_ignore_ascii_case("NULL") => Ok(SqlValue::Null),
            Token::Ident(s) if s.eq_ignore_ascii_case("TRUE") => Ok(SqlValue::Bool(true)),
            Token::Ident(s) if s.eq_ignore_ascii_case("FALSE") => Ok(SqlValue::Bool(false)),
            Token::StringLit(s) => Ok(SqlValue::Text(s)),
            Token::Number(n) => {
                if n.contains('.') {
                    Ok(SqlValue::Numeric(Numeric::parse(&n)?))
                } else {
                    let int_val: i64 = n
                        .parse()
                        .map_err(|_| DbError::CorruptedData("Invalid integer literal"))?;
                    Ok(SqlValue::Int(int_val))
                }
            }
            _ => Err(DbError::CorruptedData("Expected SQL literal value")),
        }
    }

    fn parse_cmp_op(&mut self) -> Result<CmpOp, DbError> {
        match self.next() {
            Token::Eq => Ok(CmpOp::Eq),
            Token::Ne => Ok(CmpOp::Ne),
            Token::Lt => Ok(CmpOp::Lt),
            Token::Le => Ok(CmpOp::Le),
            Token::Gt => Ok(CmpOp::Gt),
            Token::Ge => Ok(CmpOp::Ge),
            _ => Err(DbError::CorruptedData("Expected comparison operator")),
        }
    }

    fn parse_where_clause(&mut self) -> Result<Vec<Predicate>, DbError> {
        if !self.eat_keyword("WHERE") {
            return Ok(Vec::new());
        }
        let mut preds = Vec::new();
        loop {
            let mut col = self.expect_ident()?;
            if self.peek() == &Token::Dot {
                self.next();
                col = self.expect_ident()?;
            }
            let op = self.parse_cmp_op()?;
            let rhs = self.parse_literal()?;
            preds.push(Predicate {
                column: col,
                op,
                rhs,
            });
            if !self.eat_keyword("AND") {
                break;
            }
        }
        Ok(preds)
    }
}

impl RelationalDb {
    /// Executes a SQL statement (`CREATE TABLE`, `INSERT`, `SELECT`, `DELETE`).
    pub fn execute_sql(&self, sql: &str) -> Result<QueryResult, DbError> {
        let tokens = tokenize(sql)?;
        let mut p = Parser::new(tokens);

        if p.eat_keyword("CREATE") {
            p.expect_keyword("TABLE")?;
            let table_name = p.expect_ident()?;
            p.expect_token(&Token::LParen)?;

            let mut columns = Vec::new();
            loop {
                let col_name = p.expect_ident()?;
                let sql_type = p.parse_sql_type()?;
                let mut nullable = true;
                let mut primary_key = false;

                loop {
                    if p.eat_keyword("PRIMARY") {
                        p.expect_keyword("KEY")?;
                        primary_key = true;
                        nullable = false;
                    } else if p.eat_keyword("NOT") {
                        p.expect_keyword("NULL")?;
                        nullable = false;
                    } else if p.eat_keyword("NULL") {
                        nullable = true;
                    } else {
                        break;
                    }
                }

                columns.push(ColumnSchema {
                    name: col_name,
                    sql_type,
                    nullable,
                    primary_key,
                });

                if p.peek() == &Token::Comma {
                    p.next();
                } else {
                    break;
                }
            }
            p.expect_token(&Token::RParen)?;
            let _ = p.peek() == &Token::Semicolon;

            self.create_table(table_name, columns)?;
            Ok(QueryResult::AffectedRows(0))
        } else if p.eat_keyword("INSERT") {
            p.expect_keyword("INTO")?;
            let table_name = p.expect_ident()?;
            p.expect_keyword("VALUES")?;
            p.expect_token(&Token::LParen)?;

            let mut values = Vec::new();
            if p.peek() != &Token::RParen {
                loop {
                    values.push(p.parse_literal()?);
                    if p.peek() == &Token::Comma {
                        p.next();
                    } else {
                        break;
                    }
                }
            }
            p.expect_token(&Token::RParen)?;
            self.insert(&table_name, &values)?;
            Ok(QueryResult::AffectedRows(1))
        } else if p.eat_keyword("SELECT") {
            let mut proj_cols: Option<Vec<String>> = None;
            if p.peek() == &Token::Star {
                p.next();
            } else {
                let mut list = Vec::new();
                loop {
                    let mut col = p.expect_ident()?;
                    if p.peek() == &Token::Dot {
                        self.next_ident_after_dot(&mut p, &mut col)?;
                    }
                    list.push(col);
                    if p.peek() == &Token::Comma {
                        p.next();
                    } else {
                        break;
                    }
                }
                proj_cols = Some(list);
            }

            p.expect_keyword("FROM")?;
            let table_name = p.expect_ident()?;

            // Optional JOIN clause: `JOIN right_table ON left_col = right_col`
            let join_spec = if p.eat_keyword("JOIN") {
                let right_table = p.expect_ident()?;
                p.expect_keyword("ON")?;
                let mut l_col = p.expect_ident()?;
                if p.peek() == &Token::Dot {
                    self.next_ident_after_dot(&mut p, &mut l_col)?;
                }
                p.expect_token(&Token::Eq)?;
                let mut r_col = p.expect_ident()?;
                if p.peek() == &Token::Dot {
                    self.next_ident_after_dot(&mut p, &mut r_col)?;
                }
                Some((right_table, l_col, r_col))
            } else {
                None
            };

            let filters = p.parse_where_clause()?;

            let order_by = if p.eat_keyword("ORDER") {
                p.expect_keyword("BY")?;
                let mut col = p.expect_ident()?;
                if p.peek() == &Token::Dot {
                    self.next_ident_after_dot(&mut p, &mut col)?;
                }
                let desc = if p.eat_keyword("DESC") {
                    true
                } else {
                    let _ = p.eat_keyword("ASC");
                    false
                };
                Some((col, desc))
            } else {
                None
            };

            let limit = if p.eat_keyword("LIMIT") {
                match p.next() {
                    Token::Number(n) => Some(
                        n.parse::<usize>()
                            .map_err(|_| DbError::CorruptedData("Invalid LIMIT"))?,
                    ),
                    _ => return Err(DbError::CorruptedData("Expected LIMIT count")),
                }
            } else {
                None
            };

            let (mut all_col_names, mut rows) = if let Some((right_table, l_col, r_col)) = join_spec
            {
                let l_schema = self.get_table_schema(&table_name)?;
                let r_schema = self.get_table_schema(&right_table)?;
                let mut names: Vec<String> =
                    l_schema.columns.iter().map(|c| c.name.clone()).collect();
                names.extend(r_schema.columns.iter().map(|c| c.name.clone()));
                let joined = self.inner_join(&table_name, &right_table, &l_col, &r_col)?;
                (names, joined)
            } else {
                let schema = self.get_table_schema(&table_name)?;
                let names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
                let selected = self.select(&table_name, &filters)?;
                (names, selected)
            };

            if let Some((order_col, desc)) = order_by
                && let Some(ord_idx) = all_col_names.iter().position(|c| c == &order_col)
            {
                rows.sort_by(|a, b| {
                    let cmp = compare_sql_values(&a[ord_idx], &b[ord_idx]);
                    if desc { cmp.reverse() } else { cmp }
                });
            }

            if let Some(lim) = limit {
                rows.truncate(lim);
            }

            if let Some(requested) = proj_cols {
                let mut indices = Vec::with_capacity(requested.len());
                for req_c in &requested {
                    let idx = all_col_names
                        .iter()
                        .position(|c| c == req_c)
                        .ok_or(DbError::CorruptedData("Projected column not found"))?;
                    indices.push(idx);
                }
                all_col_names = requested;
                rows = rows
                    .into_iter()
                    .map(|r| indices.iter().map(|&i| r[i].clone()).collect())
                    .collect();
            }

            Ok(QueryResult::Rows {
                columns: all_col_names,
                rows,
            })
        } else if p.eat_keyword("DELETE") {
            p.expect_keyword("FROM")?;
            let table_name = p.expect_ident()?;
            let filters = p.parse_where_clause()?;
            let schema = self.get_table_schema(&table_name)?;
            let pk_idx = schema.pk_index().unwrap();
            let matching = self.select(&table_name, &filters)?;
            let count = matching.len();
            for row in matching {
                self.delete_by_pk(&table_name, &row[pk_idx])?;
            }
            Ok(QueryResult::AffectedRows(count))
        } else {
            Err(DbError::CorruptedData("Unsupported SQL statement"))
        }
    }

    fn next_ident_after_dot(&self, p: &mut Parser, col: &mut String) -> Result<(), DbError> {
        p.next();
        *col = p.expect_ident()?;
        Ok(())
    }

    fn get_table_schema(&self, name: &str) -> Result<crate::TableSchema, DbError> {
        let tables = self.tables.read().map_err(|_| DbError::LockPoisoned)?;
        tables
            .get(name)
            .cloned()
            .ok_or(DbError::CorruptedData("Table not found"))
    }
}

fn compare_sql_values(a: &SqlValue, b: &SqlValue) -> std::cmp::Ordering {
    match (a, b) {
        (SqlValue::Int(x), SqlValue::Int(y)) => x.cmp(y),
        (SqlValue::Numeric(x), SqlValue::Numeric(y)) => x.cmp(y),
        (SqlValue::Text(x), SqlValue::Text(y)) => x.cmp(y),
        (SqlValue::Date(x), SqlValue::Date(y)) => x.cmp(y),
        (SqlValue::Timestamp(x), SqlValue::Timestamp(y)) => x.cmp(y),
        (SqlValue::Bool(x), SqlValue::Bool(y)) => x.cmp(y),
        (SqlValue::Float(x), SqlValue::Float(y)) => {
            x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        _ => std::cmp::Ordering::Equal,
    }
}
