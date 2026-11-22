use alice_core::db::{Database, NodeView};
use alice_core::schema::{ColumnDef, DataType, Decimal, SegmentSchema, Value};
use alice_server::auth::AuthRegistry;
use alice_server::protocol::{Request, Response};
use alice_server::server::{AliceClient, AliceServer};
use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::sync::Arc;
use std::time::Duration;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        print_usage();
        return;
    }

    match args[1].as_str() {
        "server" => {
            let dir = args.get(2).map(String::as_str).unwrap_or("./alice_data");
            let addr = args.get(3).map(String::as_str).unwrap_or("127.0.0.1:7432");
            let mut primary: Option<String> = None;
            let mut auth_pair: Option<(String, String)> = None;

            let mut i = 4;
            while i < args.len() {
                match args[i].as_str() {
                    "--auth" => {
                        if let Some(cred) = args.get(i + 1) {
                            auth_pair = parse_cred(cred);
                        }
                        i += 2;
                    }
                    "--primary" => {
                        primary = args.get(i + 1).cloned();
                        i += 2;
                    }
                    other => {
                        primary = Some(other.to_string());
                        i += 1;
                    }
                }
            }

            let db = Arc::new(Database::open(dir).expect("Failed to open database"));
            let auth = Arc::new(AuthRegistry::new());
            if let Some((ref u, ref p)) = auth_pair {
                auth.add_user(u, p).expect("Failed to register SCRAM user");
            }

            let srv = AliceServer::with_auth(Arc::clone(&db), auth);
            let bound = srv.start_listener(addr).expect("Failed to bind listener");

            if let Some(primary_addr) = primary {
                println!("AliceDB Standby listening on {bound}, replicating from {primary_addr}");
                srv.start_standby_replication_with_auth(
                    primary_addr,
                    Duration::from_millis(50),
                    auth_pair,
                );
            } else {
                println!("AliceDB Active listening on {bound} (data: {dir})");
            }

            loop {
                std::thread::sleep(Duration::from_secs(3600));
            }
        }
        "cli" => {
            let addr = args.get(2).map(String::as_str).unwrap_or("127.0.0.1:7432");
            let auth_pair = if args.get(3).map(String::as_str) == Some("--auth") {
                args.get(4).and_then(|s| parse_cred(s))
            } else {
                None
            };
            run_repl(addr, auth_pair);
        }
        _ => print_usage(),
    }
}

fn parse_cred(s: &str) -> Option<(String, String)> {
    let (u, p) = s.split_once(':')?;
    Some((u.to_string(), p.to_string()))
}

fn print_usage() {
    println!("Usage:");
    println!("  alice-cli server <data_dir> <bind_addr> [--primary <addr>] [--auth <user:pass>]");
    println!("  alice-cli cli <server_addr> [--auth <user:pass>]");
}

struct ReplState {
    cwd: u64,
    schemas: HashMap<u16, SegmentSchema>,
}

impl ReplState {
    fn new() -> Self {
        Self {
            cwd: 0,
            schemas: HashMap::new(),
        }
    }

    fn refresh_schemas(&mut self, client: &mut AliceClient) {
        if let Ok(Response::OkCatalog(list)) = client.call(Request::SyncCatalog) {
            self.schemas = list.into_iter().map(|s| (s.node_type, s)).collect();
        }
    }

    fn type_label(&self, node_type: u16) -> String {
        self.schemas
            .get(&node_type)
            .map(|s| format!("{}({})", s.name, node_type))
            .unwrap_or_else(|| format!("Type({})", node_type))
    }

    fn prompt(&self) -> String {
        if self.cwd == 0 {
            "alice:/> ".to_string()
        } else {
            format!("alice:/{}> ", self.cwd)
        }
    }
}

fn run_repl(addr: &str, auth_pair: Option<(String, String)>) {
    let mut client = match auth_pair {
        Some((user, pass)) => AliceClient::connect_with_auth(addr, &user, &pass)
            .expect("Failed to connect/authenticate to AliceDB server"),
        None => AliceClient::connect(addr).expect("Failed to connect to AliceDB server"),
    };

    let mut state = ReplState::new();
    state.refresh_schemas(&mut client);

    println!("Connected to {addr}. Type 'help' for commands.");
    print!("{}", state.prompt());
    let _ = io::stdout().flush();

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            print!("{}", state.prompt());
            let _ = io::stdout().flush();
            continue;
        }

        match parts[0] {
            "quit" | "exit" => break,
            "help" => print_help(),
            "ping" => println!("=> {:?}", client.call(Request::Ping)),
            "schemas" => {
                state.refresh_schemas(&mut client);
                print_schema_tree(&state.schemas);
            }
            "demo_schema" => {
                let r1 = client.call(Request::DefineSegment(SegmentSchema {
                    node_type: 1,
                    name: "Dept".into(),
                    parent_type: None,
                    columns: vec![
                        ColumnDef::new("code", DataType::FixedString(16), false).with_index(true),
                        ColumnDef::new("name", DataType::VarString, false),
                    ],
                }));
                let r2 = client.call(Request::DefineSegment(SegmentSchema {
                    node_type: 2,
                    name: "Emp".into(),
                    parent_type: Some(1),
                    columns: vec![
                        ColumnDef::new("emp_no", DataType::I64, false).with_index(true),
                        ColumnDef::new("salary", DataType::Decimal, false),
                    ],
                }));
                println!("Define Dept: {:?}", r1);
                println!("Define Emp:  {:?}", r2);
                state.refresh_schemas(&mut client);
            }
            "define" => match parse_define_command(&parts[1..]) {
                Ok(schema) => {
                    let resp = client.call(Request::DefineSegment(schema));
                    println!("=> {:?}", resp);
                    state.refresh_schemas(&mut client);
                }
                Err(msg) => println!("define error: {msg}"),
            },
            "pwd" => handle_pwd(&mut client, &state),
            "cd" => {
                let target = parts.get(1).copied().unwrap_or("/");
                handle_cd(&mut client, &mut state, target);
            }
            "ls" => handle_ls(&mut client, &state),
            "tree" => {
                let max_depth = parts.get(1).and_then(|d| d.parse().ok());
                handle_tree(&mut client, &state, max_depth);
            }
            "insert" if parts.len() >= 3 => {
                let node_type: u16 = parts[1].parse().unwrap_or(1);
                let values: Vec<Value> = parts[2..].iter().map(|s| parse_cli_value(s)).collect();
                let resp = client.call(Request::Insert {
                    parent_id: state.cwd,
                    node_type,
                    values,
                });
                println!("=> {:?}", resp);
            }
            "insert_at" if parts.len() >= 4 => {
                let parent_id: u64 = parts[1].parse().unwrap_or(0);
                let node_type: u16 = parts[2].parse().unwrap_or(1);
                let values: Vec<Value> = parts[3..].iter().map(|s| parse_cli_value(s)).collect();
                let resp = client.call(Request::Insert {
                    parent_id,
                    node_type,
                    values,
                });
                println!("=> {:?}", resp);
            }
            "update" if parts.len() >= 3 => {
                let node_id: u64 = parts[1].parse().unwrap_or(0);
                let values: Vec<Value> = parts[2..].iter().map(|s| parse_cli_value(s)).collect();
                println!("=> {:?}", client.call(Request::Update { node_id, values }));
            }
            "get" => {
                let node_id = parts
                    .get(1)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(state.cwd);
                println!("=> {:?}", client.call(Request::Get { node_id }));
            }
            "find" if parts.len() == 4 => {
                let node_type: u16 = parts[1].parse().unwrap_or(1);
                let col_name = parts[2].to_string();
                let value = parse_cli_value(parts[3]);
                println!(
                    "=> {:?}",
                    client.call(Request::FindByIndex {
                        node_type,
                        col_name,
                        value,
                    })
                );
            }
            "delete" if parts.len() == 2 => {
                let node_id: u64 = parts[1].parse().unwrap_or(0);
                let resp = client.call(Request::Delete { node_id });
                if matches!(resp, Ok(Response::OkEmpty)) && state.cwd == node_id {
                    state.cwd = 0;
                }
                println!("=> {:?}", resp);
            }
            "vacuum" => println!("=> {:?}", client.call(Request::Vacuum)),
            "promote" => println!("=> {:?}", client.call(Request::Promote)),
            _ => println!("Unknown or invalid command. Type 'help'."),
        }

        print!("{}", state.prompt());
        let _ = io::stdout().flush();
    }
}

fn print_help() {
    println!("Navigation & Inspection:");
    println!("  schemas                              Show registered segment hierarchy");
    println!("  pwd                                  Show current node path from root");
    println!("  cd <node_id | .. | />                Change current parent node");
    println!("  ls                                   List direct children under current node");
    println!("  tree [max_depth]                     Render subtree under current node");
    println!("Data Manipulation:");
    println!("  demo_schema                          Register sample Dept(1) -> Emp(2) schemas");
    println!("  define <type> <name> <parent|root> <col:type[:opts]>...");
    println!("  insert <node_type> <v1> [v2...]      Insert child under current node (cwd)");
    println!("  insert_at <parent_id> <type> <v...>  Insert child under explicit parent_id");
    println!("  update <node_id> <v1> [v2...]        Update node values");
    println!("  get [node_id]                        Fetch node record (defaults to cwd)");
    println!("  find <node_type> <col> <value>       Lookup NodeIds via secondary index");
    println!("  delete <node_id>                     Delete leaf node");
    println!("Cluster & Maintenance:");
    println!("  vacuum                               Compact nodes.db and heap.db");
    println!("  promote                              Promote Standby server to Active");
    println!("  ping / quit");
}

fn handle_pwd(client: &mut AliceClient, state: &ReplState) {
    if state.cwd == 0 {
        println!("/ (root)");
        return;
    }
    let mut chain = Vec::new();
    let mut curr = state.cwd;
    while curr != 0 {
        match client.call(Request::WalkSubtree {
            root_node_id: curr,
            max_depth: Some(0),
        }) {
            Ok(Response::OkNodeViews(mut views)) if !views.is_empty() => {
                let v = views.remove(0);
                chain.push(format!("{} [{}]", v.node_id, state.type_label(v.node_type)));
                curr = v.parent_id;
            }
            _ => break,
        }
    }
    chain.reverse();
    println!("/{}", chain.join(" / "));
}

fn handle_cd(client: &mut AliceClient, state: &mut ReplState, target: &str) {
    match target {
        "/" | "0" => {
            state.cwd = 0;
        }
        ".." => {
            if state.cwd == 0 {
                return;
            }
            match client.call(Request::WalkSubtree {
                root_node_id: state.cwd,
                max_depth: Some(0),
            }) {
                Ok(Response::OkNodeViews(views)) if !views.is_empty() => {
                    state.cwd = views[0].parent_id;
                }
                _ => state.cwd = 0,
            }
        }
        id_str => match id_str.parse::<u64>() {
            Ok(id) => match client.call(Request::Get { node_id: id }) {
                Ok(Response::OkRecord { .. }) => state.cwd = id,
                Ok(Response::Err(e)) => println!("cd failed: {e}"),
                Err(e) => println!("cd failed: {e:?}"),
                _ => {}
            },
            Err(_) => println!("Invalid node ID: {id_str}"),
        },
    }
}

fn handle_ls(client: &mut AliceClient, state: &ReplState) {
    if state.cwd == 0 {
        println!("(At virtual root '/': use 'find <type> <col> <val>' or 'cd <node_id>')");
        return;
    }
    match client.call(Request::WalkSubtree {
        root_node_id: state.cwd,
        max_depth: Some(1),
    }) {
        Ok(Response::OkNodeViews(views)) => {
            let children: Vec<&NodeView> = views.iter().filter(|v| v.depth == 1).collect();
            if children.is_empty() {
                println!("(empty)");
                return;
            }
            for child in children {
                println!(
                    "  [{}] {} -> {}",
                    child.node_id,
                    state.type_label(child.node_type),
                    format_values(&child.values)
                );
            }
        }
        Ok(Response::Err(e)) => println!("ls error: {e}"),
        Err(e) => println!("ls error: {e:?}"),
        _ => {}
    }
}

fn handle_tree(client: &mut AliceClient, state: &ReplState, max_depth: Option<usize>) {
    if state.cwd == 0 {
        println!("(At virtual root '/': 'cd <node_id>' first to inspect a subtree)");
        return;
    }
    match client.call(Request::WalkSubtree {
        root_node_id: state.cwd,
        max_depth,
    }) {
        Ok(Response::OkNodeViews(views)) => {
            for v in &views {
                let indent = "  ".repeat(v.depth);
                let branch = if v.depth == 0 { "" } else { "└── " };
                println!(
                    "{indent}{branch}[{}] {} : {}",
                    v.node_id,
                    state.type_label(v.node_type),
                    format_values(&v.values)
                );
            }
        }
        Ok(Response::Err(e)) => println!("tree error: {e}"),
        Err(e) => println!("tree error: {e:?}"),
        _ => {}
    }
}

fn print_schema_tree(schemas: &HashMap<u16, SegmentSchema>) {
    if schemas.is_empty() {
        println!("(No segment schemas registered)");
        return;
    }
    let mut list: Vec<&SegmentSchema> = schemas.values().collect();
    list.sort_by_key(|s| s.node_type);

    fn render_node(parent: Option<u16>, depth: usize, all: &[&SegmentSchema]) {
        for s in all.iter().filter(|s| s.parent_type == parent) {
            let indent = "  ".repeat(depth);
            let branch = if depth == 0 { "" } else { "└── " };
            let cols: Vec<String> = s
                .columns
                .iter()
                .map(|c| {
                    let idx_tag = if c.unique {
                        ", unique"
                    } else if c.indexed {
                        ", idx"
                    } else {
                        ""
                    };
                    format!("{}:{:?}{}", c.name, c.data_type, idx_tag)
                })
                .collect();
            println!(
                "{indent}{branch}[{}] {} ({})",
                s.node_type,
                s.name,
                cols.join(", ")
            );
            render_node(Some(s.node_type), depth + 1, all);
        }
    }

    render_node(None, 0, &list);
}

fn format_values(values: &[Value]) -> String {
    let parts: Vec<String> = values
        .iter()
        .map(|v| match v {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::I64(n) => n.to_string(),
            Value::F64(f) => f.to_string(),
            Value::String(s) => format!("\"{s}\""),
            Value::Bytes(b) => format!("0x({} bytes)", b.len()),
            Value::Decimal(d) => format_decimal(*d),
        })
        .collect();
    format!("[{}]", parts.join(", "))
}

fn format_decimal(d: Decimal) -> String {
    if d.scale == 0 {
        return d.unscaled.to_string();
    }
    let sign = if d.unscaled < 0 { "-" } else { "" };
    let abs = d.unscaled.unsigned_abs();
    let div = 10u128.pow(d.scale as u32);
    let int_part = abs / div;
    let frac_part = abs % div;
    format!(
        "{sign}{int_part}.{:0width$}",
        frac_part,
        width = d.scale as usize
    )
}

fn parse_cli_value(token: &str) -> Value {
    if token.eq_ignore_ascii_case("null") {
        Value::Null
    } else if let Ok(v) = token.parse::<i64>() {
        Value::I64(v)
    } else if let Some((int_part, frac_part)) = token.split_once('.') {
        if let (Ok(a), Ok(b)) = (int_part.parse::<i128>(), frac_part.parse::<i128>()) {
            let scale = frac_part.len() as u8;
            let sign = if int_part.starts_with('-') { -1 } else { 1 };
            let unscaled = a * 10i128.pow(scale as u32) + sign * b;
            return Value::Decimal(Decimal::new(unscaled, scale));
        }
        Value::String(token.to_string())
    } else {
        Value::String(token.to_string())
    }
}

fn parse_define_command(args: &[&str]) -> Result<SegmentSchema, String> {
    if args.len() < 4 {
        return Err(
            "Usage: define <node_type> <name> <parent_type|root> <col:type[:null|idx|unique]>..."
                .into(),
        );
    }

    let node_type: u16 = args[0]
        .parse()
        .map_err(|_| format!("Invalid node_type: {}", args[0]))?;
    let name = args[1].to_string();
    let parent_type = if args[2].eq_ignore_ascii_case("root") || args[2] == "0" {
        None
    } else {
        let p: u16 = args[2]
            .parse()
            .map_err(|_| format!("Invalid parent_type: {}", args[2]))?;
        Some(p)
    };

    let mut columns = Vec::with_capacity(args.len() - 3);
    for spec in &args[3..] {
        columns.push(parse_column_spec(spec)?);
    }

    Ok(SegmentSchema {
        node_type,
        name,
        parent_type,
        columns,
    })
}

fn parse_column_spec(spec: &str) -> Result<ColumnDef, String> {
    let mut parts = spec.split(':');
    let col_name = parts
        .next()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("Missing column name in '{spec}'"))?;
    let type_str = parts
        .next()
        .ok_or_else(|| format!("Missing data type in '{spec}'"))?;

    let data_type = parse_data_type(type_str)?;
    let mut nullable = false;
    let mut indexed = false;
    let mut unique = false;

    for opt in parts {
        match opt.to_ascii_lowercase().as_str() {
            "null" | "nullable" => nullable = true,
            "idx" | "index" => indexed = true,
            "unique" | "uniq" => {
                indexed = true;
                unique = true;
            }
            _ => return Err(format!("Unknown column option '{opt}' in '{spec}'")),
        }
    }

    Ok(ColumnDef {
        name: col_name.to_string(),
        data_type,
        nullable,
        indexed,
        unique,
    })
}

fn parse_data_type(s: &str) -> Result<DataType, String> {
    let lower = s.to_ascii_lowercase();
    match lower.as_str() {
        "bool" => Ok(DataType::Bool),
        "i64" | "int" => Ok(DataType::I64),
        "f64" | "float" => Ok(DataType::F64),
        "decimal" | "dec" => Ok(DataType::Decimal),
        "varstr" | "string" => Ok(DataType::VarString),
        "varbytes" | "bytes" => Ok(DataType::VarBytes),
        _ => {
            if let Some(inner) = lower.strip_prefix("str(").and_then(|r| r.strip_suffix(')')) {
                let len: u16 = inner
                    .parse()
                    .map_err(|_| format!("Invalid FixedString length: '{s}'"))?;
                Ok(DataType::FixedString(len))
            } else if let Some(inner) = lower
                .strip_prefix("bytes(")
                .and_then(|r| r.strip_suffix(')'))
            {
                let len: u16 = inner
                    .parse()
                    .map_err(|_| format!("Invalid FixedBytes length: '{s}'"))?;
                Ok(DataType::FixedBytes(len))
            } else {
                Err(format!("Unsupported data type: '{s}'"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_and_format_cli_values() {
        assert_eq!(parse_cli_value("null"), Value::Null);
        assert_eq!(parse_cli_value("42"), Value::I64(42));
        let dec = Decimal::new(-905, 2);
        assert_eq!(parse_cli_value("-9.05"), Value::Decimal(dec));
        assert_eq!(format_decimal(dec), "-9.05");
        assert_eq!(
            parse_cli_value("ENG-01"),
            Value::String("ENG-01".to_string())
        );
    }
}
