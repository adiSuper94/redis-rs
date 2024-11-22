use crate::redis_commands::Command;
use crate::redis_db::RedisDB;
use anyhow::Context;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::io;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

#[derive(Copy, Clone)]
pub enum Role {
    Primary,
    Replica,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Primary => write!(f, "master"),
            Role::Replica => write!(f, "slave"),
        }
    }
}

pub struct Redis {
    db: Arc<Mutex<HashMap<String, String>>>,
    exp: Arc<Mutex<HashMap<String, SystemTime>>>,
    config: Arc<Mutex<HashMap<String, String>>>,
    role: Role,
    port: String,
    replid: Option<String>,
    repl_offset: Option<usize>,
    master_host: Option<String>,
    master_port: Option<String>,
    master_stream: Option<Arc<Mutex<TcpStream>>>,
    replica_stream: Option<Arc<Mutex<TcpStream>>>,
}

pub struct RedisCliArgs {
    pub dir: Option<String>,
    pub file_name: Option<String>,
    pub port: String,
    pub master_host: Option<String>,
    pub master_port: Option<String>,
    pub role: Role,
}

impl Redis {
    pub async fn new(cli_args: RedisCliArgs) -> Self {
        let mut instance = Redis {
            db: Arc::new(Mutex::new(HashMap::new())),
            exp: Arc::new(Mutex::new(HashMap::new())),
            config: Arc::new(Mutex::new(HashMap::new())),
            repl_offset: Some(0),
            port: cli_args.port,
            replid: match cli_args.role {
                Role::Primary => Some("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string()),
                Role::Replica => None,
            },
            role: cli_args.role,
            master_host: cli_args.master_host,
            master_port: cli_args.master_port,
            master_stream: None,
            replica_stream: None,
        };
        if let Some(dir) = cli_args.dir {
            if let Some(file_name) = cli_args.file_name {
                let mut config = instance.config.lock().await;
                config.insert("dir".to_string(), dir.clone());
                config.insert("file_name".to_string(), file_name.clone());
                let mut redis_db = RedisDB::new(dir, file_name);
                match redis_db.read_rdb() {
                    Ok((kivals, exp_map)) => {
                        let mut db = instance.db.lock().await;
                        let mut exp = instance.exp.lock().await;
                        for (key, value) in kivals {
                            match exp_map.get(&key) {
                                Some(exp_time) => {
                                    println!(
                                        "key: {:?}, val: {:?}, exp_time: {:?}, cuurent_time: {:?}",
                                        key,
                                        value,
                                        exp_time,
                                        SystemTime::now()
                                    );
                                    if exp_time > &SystemTime::now() {
                                        db.insert(key.clone(), value);
                                        exp.insert(key.clone(), *exp_time);
                                    }
                                }
                                None => {
                                    db.insert(key.clone(), value);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("Error reading RDB file: {:?}", e);
                    }
                }
            };
        };
        match &instance.role {
            Role::Primary => {}
            Role::Replica => instance.handshake_with_master().await,
        }
        instance
    }

    pub fn clone(&self) -> Self {
        let mut clone = Redis {
            db: Arc::clone(&self.db),
            exp: Arc::clone(&self.exp),
            config: Arc::clone(&self.config),
            role: self.role.clone(),
            repl_offset: self.repl_offset.clone(),
            replid: self.replid.clone(),
            master_host: self.master_host.clone(),
            master_port: self.master_port.clone(),
            port: self.port.clone(),
            master_stream: None,
            replica_stream: None,
        };
        if let Some(master_stream) = &self.master_stream {
            clone.master_stream = Some(Arc::clone(master_stream));
        };
        clone
    }

    async fn get(&mut self, key: &str) -> Option<String> {
        let mut exp = self.exp.lock().await;
        let mut db = self.db.lock().await;
        if let Some(exp) = exp.get(key).cloned() {
            if exp < std::time::SystemTime::now() {
                db.remove(key);
            }
        }

        if let None = db.get(key) {
            exp.remove(key);
        }
        return db.get(key).cloned();
    }

    async fn set(&mut self, key: String, value: String, exp: &Option<SystemTime>) {
        let mut db = self.db.lock().await;
        db.insert(key.clone(), value);
        if let Some(exp) = exp {
            self.exp.lock().await.insert(key, exp.clone());
        }
    }

    async fn handshake_with_master(&mut self) {
        if let None = &self.master_port {
            println!("master port is not set. This instance must be the master, so will not init handshake");
            return;
        }
        let master_port = self.master_port.clone().unwrap();
        if let None = &self.master_host {
            println!("master host is not set, This instance must be the master, so will not init handshake. But since master_port is set to {}, there may be some issue", master_port);
            return;
        }
        let master_host = self.master_host.clone().unwrap();
        let stream = TcpStream::connect(format!("{}:{}", master_host, master_port)).await;
        if let Err(e) = stream {
            println!("error while connecting to master for handshake:{}", e);
            return;
        }
        let stream = stream.unwrap();
        let ping = Command::Ping;
        let msg = ping.serialize();
        write(&stream, msg.as_bytes()).await;
        let mut buf = [0; 512];
        if let Err(e) = stream.readable().await {
            println!(
                "error while waiting for stream to be readable after sending handshake(PING): {}",
                e
            );
            return;
        }
        loop {
            match stream.try_read(&mut buf) {
                Ok(_) => break,
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        continue;
                    }
                    println!(
                        "Error while reading handshake(PING) response from master: {}",
                        e
                    );
                    return;
                }
            }
        }
        let pong = String::from_utf8_lossy(&buf).trim().to_string();
        if pong.eq("$4\r\nPONG\r\n") {
            println!("Pong did not match: {}", pong);
        }
        let replconf1 = Command::ReplConf("listening-port".to_string(), self.port.clone());
        let msg = replconf1.serialize();
        write(&stream, msg.as_bytes()).await;
        println!("sent listening port");
        if let Err(e) = stream.readable().await {
            println!(
                "error while waiting for stream to become readable after sending handshake(REPLCONF 1): {}",
                e
            );
            return;
        }
        loop {
            match stream.try_read(&mut buf) {
                Ok(_) => break,
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        continue;
                    }
                    println!(
                        "Error while reading handshake(REPLCONF 1) response from master: {}",
                        e
                    );
                    return;
                }
            }
        }
        let replconf2 = Command::ReplConf("capa".to_string(), "psync2".to_string());
        println!("created capa");
        let msg = replconf2.serialize();
        write(&stream, msg.as_bytes()).await;
        println!("sent capa");
        if let Err(e) = stream.readable().await {
            println!(
                "error while waiting for stream to become readable after sending handshake(REPLCONF 2): {}",
                e
            );
            return;
        }
        loop {
            match stream.try_read(&mut buf) {
                Ok(_) => break,
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        continue;
                    }
                    println!(
                        "error while reading handshake(REPLCONF 2) response from master: {}",
                        e
                    );
                    return;
                }
            }
        }
        let psync = Command::Psync("?".to_string(), "-1".to_string());
        let msg = psync.serialize();
        write(&stream, msg.as_bytes()).await;
        self.master_stream = Some(Arc::new(Mutex::new(stream)));
    }

    pub async fn execute(&mut self, command: Command, stream: Arc<Mutex<TcpStream>>) {
        let mut replicate = false;
        let mut full_replicate = false;
        let resp = match &command {
            Command::Echo(echo) => format!("${}\r\n{}\r\n", echo.len(), echo),
            Command::Ping => format!("$4\r\nPONG\r\n"),
            Command::Get(key) => {
                if let Some(value) = self.get(key).await {
                    format!("${}\r\n{}\r\n", value.len(), value)
                } else {
                    format!("$-1\r\n")
                }
            }
            Command::Set(key, val, exp) => {
                self.set(key.to_string(), val.to_string(), exp).await;
                replicate = true;
                format!("+OK\r\n")
            }
            Command::ConfigGet(key) => {
                if let Some(value) = self.config.lock().await.get(key) {
                    format!(
                        "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                        key.len(),
                        key,
                        value.len(),
                        value
                    )
                } else {
                    format!("$-1\r\n")
                }
            }
            Command::Keys(_pattern) => {
                let key_count = self.db.lock().await.keys().count();
                let res = self.db.lock().await.keys().fold(String::new(), |acc, key| {
                    format!("{}${}\r\n{}\r\n", acc, key.len(), key)
                });
                format!("*{}\r\n{}", key_count, res)
            }
            Command::Info(section) => {
                if section == "all" || section == "replication" || section == "REPLICATION" {
                    let info = format!("# Replication \r\nrole:{}\r\n", self.role);
                    let info = if let Some(master_replid) = &self.replid {
                        format!("{}master_replid:{}\r\n", info, master_replid)
                    } else {
                        info
                    };
                    let info = if let Some(master_repl_offset) = &self.repl_offset {
                        format!("{}master_repl_offset:{}\r\n", info, master_repl_offset)
                    } else {
                        info
                    };
                    format!("${}\r\n{}\r\n", info.len(), info)
                } else {
                    format!("$-1\r\n")
                }
            }
            Command::ReplConf(_, _) => format!("+OK\r\n"),
            Command::Psync(_repl_id, _offset) => match self.role {
                Role::Primary => {
                    let master_repl_offset = self.repl_offset.clone().unwrap();
                    let master_replid = self.replid.clone().unwrap();
                    full_replicate = true;
                    self.replica_stream = Some(Arc::clone(&stream));
                    format!("+FULLRESYNC {} {}\r\n", master_replid, master_repl_offset)
                }
                Role::Replica => format!("$-1\r\n"),
            },
        };

        let stream = stream.lock().await;
        println!("hi");
        write(&stream, resp.as_bytes()).await;
        if replicate {
            self.replicate(command).await;
        }
        if full_replicate {
            println!("hi 2");
            self.send_emtpy_rdb(&stream).await;
        }
    }

    async fn replicate(&self, cmd: Command) {
        if let Some(master_stream) = &self.master_stream {
            let clone_stream = Arc::clone(master_stream);
            tokio::spawn(async move {
                let stream = clone_stream.lock().await;
                if let Ok(()) = stream.readable().await {
                    write(&stream, cmd.serialize().as_bytes()).await;
                }
            });
        }
    }

    async fn send_emtpy_rdb(&mut self, stream: &TcpStream) {
        let decode_bytes = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
            .context("Error while decoding hex").unwrap();
        match &self.role {
            Role::Primary => {
                    write(&stream, format!("${}\r\n", decode_bytes.len()).as_bytes()).await;
                    write(&stream, &decode_bytes).await;
            }
            Role::Replica => {}
        }
    }
}

async fn write(stream: &TcpStream, bytes: &[u8]) {
    let mut offset = 0;
    loop {
        stream.writable().await.unwrap();
        if let Ok(n) = stream.try_write(&bytes) {
            offset += n;
            if offset >= bytes.len() {
                break;
            }
        }
    }
}
