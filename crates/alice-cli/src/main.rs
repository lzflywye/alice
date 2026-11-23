use alice_core::db::{Database, NodeView};
use alice_core::schema::{ColumnDef, DataType, Decimal, SegmentSchema, Value};
use alice_server::auth::AuthRegistry;
use alice_server::config::AliceConfig;
use alice_server::protocol::{Request, Response};
use alice_server::server::{AliceClient, AliceServer};
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        print_usage();
        return;
    }

    match args[1].as_str() {
        "init-config" => {
            let out_path = args.get(2).map(String::as_str).unwrap_or("alice.toml");
            fs::write(out_path, AliceConfig::sample_toml()).expect("Failed to write sample config");
            println!("Generated sample config at {out_path}");
        }
        "server" => {
            let mut cfg = AliceConfig::default();
            let mut positional = Vec::new();
            let mut i = 2;

            while i < args.len() {
                match args[i].as_str() {
                    "--config" => {
                        let path = args.get(i + 1).expect("Missing path after --config");
                        cfg =
                            AliceConfig::load_from_file(path).expect("Failed to load config file");
                        i += 2;
                    }
                    "--auth" => {
                        if let Some((u, p)) = args.get(i + 1).and_then(|s| parse_cred(s)) {
                            cfg.auth.users.push(alice_server::config::UserEntry {
                                username: u,
                                password: p,
                            });
                        }
                        i += 2;
                    }
                    "--primary" => {
                        cfg.replication.primary_addr = args.get(i + 1).cloned();
                        i += 2;
                    }
                    other => {
                        positional.push(other.to_string());
                        i += 1;
                    }
                }
            }

            if let Some(dir) = positional.first() {
                cfg.storage.data_dir = dir.clone();
            }
            if let Some(addr) = positional.get(1) {
                cfg.server.bind_addr = addr.clone();
            }
            if let Some(primary) = positional.get(2) {
                cfg.replication.primary_addr = Some(primary.clone());
            }

            let segment_bytes = cfg.storage.wal_segment_mb * 1024 * 1024;
            let db = Arc::new(
                Database::open_with_segment_size(&cfg.storage.data_dir, segment_bytes)
                    .expect("Failed to open database"),
            );

            let auth = Arc::new(AuthRegistry::new());
            for user in &cfg.auth.users {
                auth.add_user(&user.username, &user.password)
                    .expect("Failed to register SCRAM user");
            }

            let srv = AliceServer::with_config(Arc::clone(&db), auth, cfg.server.clone());
            let bound = srv
                .start_listener(&cfg.server.bind_addr)
                .expect("Failed to bind listener");

            if let Some(ref primary_addr) = cfg.replication.primary_addr {
                let repl_cred = cfg
                    .auth
                    .users
                    .first()
                    .map(|u| (u.username.clone(), u.password.clone()));
                println!(
                    "AliceDB Standby listening on {bound}, replicating from {primary_addr} (max_conns: {})",
                    cfg.server.max_connections
                );
                srv.start_standby_replication_with_auth(
                    primary_addr.clone(),
                    Duration::from_millis(cfg.replication.poll_interval_ms),
                    repl_cred,
                );
            } else {
                println!(
                    "AliceDB Active listening on {bound} (data: {}, max_conns: {})",
                    cfg.storage.data_dir, cfg.server.max_connections
                );
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
    println!("  alice-cli init-config [out_path]");
    println!(
        "  alice-cli server [--config alice.toml] [data_dir] [bind_addr] [--primary <addr>] [--auth <user:pass>]"
    );
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

    fn resolve_node_type(&self, token: &str) -> Option<u16> {
        if let Ok(id) = token.parse::<u16>() {
            return Some(id);
        }
        self.schemas
            .values()
            .find(|s| s.name.eq_ignore_ascii_case(token))
            .map(|s| s.node_type)
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

    let mut rl = DefaultEditor::new().expect("Failed to initialize line editor");
    println!("Connected to {addr}. Use arrow keys for cursor/history. Type 'help' for commands.");

    loop {
        let line = match rl.readline(&state.prompt()) {
            Ok(l) => l,
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => break,
            Err(err) => {
                println!("Readline error: {err}");
                break;
            }
        };

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let _ = rl.add_history_entry(trimmed);

        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        match parts[0] {
            "quit" | "exit" => break,
            "help" => print_help(),
            "ping" => println!("=> {:?}", client.call(Request::Ping)),
            "schemas" => {
                state.refresh_schemas(&mut client);
                print_schema_tree(&state.schemas);
            }
            "define" => match parse_define_command(&parts[1..], &state) {
                Ok(schema) => {
                    let resp = client.call(Request::DefineSegment(schema));
                    println!("=> {:?}", resp);
                    state.refresh_schemas(&mut client);
                }
                Err(msg) => println!("define error: {msg}"),
            },
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
            "insert" if parts.len() >= 3 => match state.resolve_node_type(parts[1]) {
                Some(node_type) => {
                    let values: Vec<Value> =
                        parts[2..].iter().map(|s| parse_cli_value(s)).collect();
                    let resp = client.call(Request::Insert {
                        parent_id: state.cwd,
                        node_type,
                        values,
                    });
                    println!("=> {:?}", resp);
                }
                None => println!("Unknown segment type: {}", parts[1]),
            },
            "insert_at" if parts.len() >= 4 => {
                let parent_id: u64 = parts[1].parse().unwrap_or(0);
                match state.resolve_node_type(parts[2]) {
                    Some(node_type) => {
                        let values: Vec<Value> =
                            parts[3..].iter().map(|s| parse_cli_value(s)).collect();
                        let resp = client.call(Request::Insert {
                            parent_id,
                            node_type,
                            values,
                        });
                        println!("=> {:?}", resp);
                    }
                    None => println!("Unknown segment type: {}", parts[2]),
                }
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
            "find" if parts.len() == 4 => match state.resolve_node_type(parts[1]) {
                Some(node_type) => {
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
                None => println!("Unknown segment type: {}", parts[1]),
            },
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
    }
}

fn print_help() {
    println!("Schema & Navigation:");
    println!("  schemas                                    Show registered segment hierarchy");
    println!("  define <type> <name> <parent|root> <c:t>.. Define custom segment schema");
    println!("  demo_schema                                Register sample Dept(1) -> Emp(2)");
    println!("  pwd                                        Show current node path from root");
    println!("  cd <node_id | label | .. | />              Change current parent node");
    println!("  ls                                         List direct children under cwd");
    println!("  tree [max_depth]                           Render subtree under cwd");
    println!("Data Manipulation:");
    println!("  insert <type|name> <v1> [v2...]            Insert child under cwd");
    println!("  insert_at <parent_id> <type|name> <v...>   Insert child under explicit parent");
    println!("  update <node_id> <v1> [v2...]              Update node values");
    println!("  get [node_id]                              Fetch node record (defaults to cwd)");
    println!("  find <type|name> <col> <value>             Lookup NodeIds via secondary index");
    println!("  delete <node_id>                           Delete leaf node");
    println!("Cluster & Maintenance:");
    println!("  vacuum                                     Compact nodes.db and heap.db");
    println!("  promote                                    Promote Standby server to Active");
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
        token => {
            if let Ok(id) = token.parse::<u64>() {
                match client.call(Request::Get { node_id: id }) {
                    Ok(Response::OkRecord { .. }) => {
                        state.cwd = id;
                        return;
                    }
                    Ok(Response::Err(e)) => {
                        println!("cd failed: {e}");
                        return;
                    }
                    _ => {}
                }
            }

            // Label-based lookup: search children if inside a node, or indexed first column at root
            let val = parse_cli_value(token);
            if state.cwd != 0 {
                if let Ok(Response::OkNodeViews(views)) = client.call(Request::WalkSubtree {
                    root_node_id: state.cwd,
                    max_depth: Some(1),
                }) && let Some(child) = views
                    .iter()
                    .find(|v| v.depth == 1 && v.values.first() == Some(&val))
                {
                    state.cwd = child.node_id;
                    return;
                }
            } else {
                for schema in state.schemas.values().filter(|s| s.parent_type.is_none()) {
                    if let Some(col) = schema.columns.first().filter(|c| c.indexed || c.unique)
                        && let Ok(Response::OkNodeIds(ids)) = client.call(Request::FindByIndex {
                            node_type: schema.node_type,
                            col_name: col.name.clone(),
                            value: val.clone(),
                        })
                        && let Some(&id) = ids.first()
                    {
                        state.cwd = id;
                        return;
                    }
                }
            }
            println!("Node not found: {token}");
        }
    }
}

fn handle_ls(client: &mut AliceClient, state: &ReplState) {
    if state.cwd == 0 {
        println!("(At virtual root '/': use 'cd <node_id | label>' or 'find <type> <col> <val>')");
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
        println!("(At virtual root '/': 'cd <node_id | label>' first to inspect a subtree)");
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

fn parse_define_command(args: &[&str], state: &ReplState) -> Result<SegmentSchema, String> {
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
        let p = state
            .resolve_node_type(args[2])
            .ok_or_else(|| format!("Unknown parent_type: {}", args[2]))?;
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
