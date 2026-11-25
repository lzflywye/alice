use relational::RelationalDb;
use relational::codec::SqlValue;
use relational::sql::QueryResult;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

fn format_value(v: &SqlValue) -> String {
    match v {
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Bool(b) => b.to_string(),
        SqlValue::Int(n) => n.to_string(),
        SqlValue::Float(f) => f.to_string(),
        SqlValue::Numeric(num) => num.to_string(),
        SqlValue::Text(s) => s.clone(),
        SqlValue::Blob(b) => format!(
            "0x{}",
            b.iter().map(|x| format!("{x:02x}")).collect::<String>()
        ),
        SqlValue::Date(days) => format!("DATE({days})"),
        SqlValue::Timestamp(us) => format!("TS({us})"),
    }
}

fn print_table(columns: &[String], rows: &[Vec<SqlValue>]) {
    let mut widths: Vec<usize> = columns.iter().map(String::len).collect();
    let str_rows: Vec<Vec<String>> = rows
        .iter()
        .map(|r| {
            r.iter()
                .enumerate()
                .map(|(i, v)| {
                    let s = format_value(v);
                    if i < widths.len() {
                        widths[i] = widths[i].max(s.len());
                    }
                    s
                })
                .collect()
        })
        .collect();

    let sep: String = widths
        .iter()
        .map(|w| "-".repeat(*w + 2))
        .collect::<Vec<_>>()
        .join("+");
    let sep_line = format!("+{sep}+");

    println!("{sep_line}");
    let header: String = columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!(" {:<width$} ", c, width = widths[i]))
        .collect::<Vec<_>>()
        .join("|");
    println!("|{header}|");
    println!("{sep_line}");

    for r in &str_rows {
        let line: String = r
            .iter()
            .enumerate()
            .map(|(i, cell)| format!(" {:<width$} ", cell, width = widths[i]))
            .collect::<Vec<_>>()
            .join("|");
        println!("|{line}|");
    }
    println!("{sep_line}");
    println!("({} rows)\n", rows.len());
}

fn main() {
    let data_dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "./alice_rdb_data".to_string());
    let db = RelationalDb::open(&data_dir).expect("Failed to open RelationalDb");

    println!("AliceDB Relational SQL REPL (data_dir: {data_dir})");
    println!(
        "Commands: SQL statements (CREATE TABLE / INSERT / SELECT / DELETE), .tables, .exit\n"
    );

    let mut rl = DefaultEditor::new().expect("Failed to init line editor");
    loop {
        match rl.readline("sql> ") {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(trimmed);

                if trimmed.eq_ignore_ascii_case(".exit") || trimmed.eq_ignore_ascii_case("\\q") {
                    break;
                }
                if trimmed.eq_ignore_ascii_case(".tables")
                    || trimmed.eq_ignore_ascii_case("SHOW TABLES")
                {
                    match db.list_tables() {
                        Ok(tables) => {
                            for t in tables {
                                let cols: Vec<String> = t
                                    .columns
                                    .iter()
                                    .map(|c| {
                                        if c.primary_key {
                                            format!("{} {:?} PK", c.name, c.sql_type)
                                        } else {
                                            format!("{} {:?}", c.name, c.sql_type)
                                        }
                                    })
                                    .collect();
                                println!("  - {} ({})", t.name, cols.join(", "));
                            }
                            println!();
                        }
                        Err(e) => println!("Error: {e:?}\n"),
                    }
                    continue;
                }

                let stmt = trimmed.trim_end_matches(';');
                match db.execute_sql(stmt) {
                    Ok(QueryResult::AffectedRows(n)) => println!("OK ({n} rows affected)\n"),
                    Ok(QueryResult::Rows { columns, rows }) => print_table(&columns, &rows),
                    Err(e) => println!("SQL Error: {e:?}\n"),
                }
            }
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => break,
            Err(err) => {
                println!("Readline error: {err:?}");
                break;
            }
        }
    }
}
