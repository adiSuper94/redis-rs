use anyhow::Context as _;

use crate::redis_commands::Command;
use crate::redis_db::RedisDB;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

pub struct Redis {
    db: Arc<Mutex<HashMap<String, String>>>,
    exp: Arc<Mutex<HashMap<String, SystemTime>>>,
    config: Arc<Mutex<HashMap<String, String>>>,
    role: String,
    port: String,
    master_replid: Option<String>,
    master_repl_offset: Option<usize>,
    master_host: Option<String>,
    master_port: Option<String>,
}

pub struct RedisCliArgs {
    pub dir: Option<String>,
    pub file_name: Option<String>,
    pub port: String,
    pub master_host: Option<String>,
    pub master_port: Option<String>,
    pub role: String,
}

impl Redis {
    pub fn new(cli_args: RedisCliArgs) -> Self {
        let instance = Redis {
            db: Arc::new(Mutex::new(HashMap::new())),
            exp: Arc::new(Mutex::new(HashMap::new())),
            config: Arc::new(Mutex::new(HashMap::new())),
            role: cli_args.role,
            master_repl_offset: Some(0),
            port: cli_args.port,
            master_replid: if cli_args.master_host.is_some() {
                None
            } else {
                Some("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string())
            },
            master_host: cli_args.master_host,
            master_port: cli_args.master_port,
        };
        if let Some(dir) = cli_args.dir {
            instance
                .config
                .lock()
                .unwrap()
                .insert("dir".to_string(), dir.clone());
            if let Some(file_name) = cli_args.file_name {
                instance
                    .config
                    .lock()
                    .unwrap()
                    .insert("file_name".to_string(), file_name.clone());
                let mut redis_db = RedisDB::new(dir, file_name);
                match redis_db.read_rdb() {
                    Ok((kivals, exp_map)) => {
                        let mut db = instance.db.lock().unwrap();
                        let mut exp = instance.exp.lock().unwrap();
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
        if instance.role == "slave" {
            instance.handshake_with_master();
        }
        instance
    }
    pub fn clone(&self) -> Self {
        Redis {
            db: Arc::clone(&self.db),
            exp: Arc::clone(&self.exp),
            config: Arc::clone(&self.config),
            role: self.role.clone(),
            master_repl_offset: self.master_repl_offset.clone(),
            master_replid: self.master_replid.clone(),
            master_host: self.master_host.clone(),
            master_port: self.master_port.clone(),
            port: self.port.clone(),
        }
    }

    fn get(&mut self, key: &str) -> Option<String> {
        if let Some(exp) = self.exp.lock().unwrap().get(key).cloned() {
            if exp < std::time::SystemTime::now() {
                self.db.lock().unwrap().remove(key);
            }
        }

        if let None = self.db.lock().unwrap().get(key) {
            self.exp.lock().unwrap().remove(key);
        }
        return self.db.lock().unwrap().get(key).cloned();
    }

    fn set(&mut self, key: String, value: String, exp: &Option<SystemTime>) {
        self.db.lock().unwrap().insert(key.clone(), value);
        if let Some(exp) = exp {
            self.exp.lock().unwrap().insert(key, exp.clone());
        }
    }

    fn handshake_with_master(&self) {
        if let None = &self.master_port {
            println!("master port is not set. This instance must be the master, so will not init handshake");
        }
        let master_port = self.master_port.clone().unwrap();
        if let None = &self.master_host {
            println!("master host is not set, This instance must be the master, so will not init handshake. But since master_port is set to {}, there may be some issue", master_port);
            return;
        }
        let master_host = self.master_host.clone().unwrap();
        let stream = TcpStream::connect(format!("{}:{}", master_host, master_port));
        if let Err(e) = stream {
            println!("error while connecting to master for handshake:{}", e);
            return;
        }
        let mut stream = stream.unwrap();
        let ping = Command::Ping;
        let msg = ping.serialize();
        let _ = stream.write_all(msg.as_bytes());
        let mut buf = [0; 512];
        if let Err(e) = stream.read(&mut buf) {
            println!(
                "error while reading handshake(PING) response from master: {}",
                e
            );
            return;
        }
        let pong = String::from_utf8_lossy(&buf).trim().to_string();
        if pong.eq("$4\r\nPONG\r\n") {
            println!("Pong did not match: {}", pong);
        }
        let replconf1 = Command::ReplConf("listening-port".to_string(), self.port.clone());
        let msg = replconf1.serialize();
        let _ = stream.write_all(msg.as_bytes());
        if let Err(e) = stream.read(&mut buf) {
            println!(
                "error while reading handshake(REPLCONF 1) response from master: {}",
                e
            );
            return;
        }
        let replconf2 = Command::ReplConf("capa".to_string(), "psync2".to_string());
        let msg = replconf2.serialize();
        let _ = stream.write_all(msg.as_bytes());
        if let Err(e) = stream.read(&mut buf) {
            println!(
                "error while reading handshake(REPLCONF 2) response from master: {}",
                e
            );
            return;
        }
        let psync = Command::Psync("?".to_string(), "-1".to_string());
        let msg = psync.serialize();
        let _ = stream.write_all(msg.as_bytes());
    }

    pub fn execute(&mut self, command: &Command) -> Vec<String> {
        match &command {
            Command::Echo(echo) => [format!("${}\r\n{}\r\n", echo.len(), echo)].to_vec(),
            Command::Ping => [format!("$4\r\nPONG\r\n")].to_vec(),
            Command::Get(key) => {
                if let Some(value) = self.get(key) {
                    [format!("${}\r\n{}\r\n", value.len(), value)].to_vec()
                } else {
                    [format!("$-1\r\n")].to_vec()
                }
            }
            Command::Set(key, val, exp) => {
                self.set(key.to_string(), val.to_string(), exp);
                [format!("+OK\r\n")].to_vec()
            }
            Command::ConfigGet(key) => {
                if let Some(value) = self.config.lock().unwrap().get(key) {
                    [format!(
                        "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                        key.len(),
                        key,
                        value.len(),
                        value
                    )]
                    .to_vec()
                } else {
                    [format!("$-1\r\n")].to_vec()
                }
            }
            Command::Keys(_pattern) => {
                let key_count = self.db.lock().unwrap().keys().count();
                let res = self
                    .db
                    .lock()
                    .unwrap()
                    .keys()
                    .fold(String::new(), |acc, key| {
                        format!("{}${}\r\n{}\r\n", acc, key.len(), key)
                    });
                [format!("*{}\r\n{}", key_count, res)].to_vec()
            }
            Command::Info(section) => {
                if section == "all" || section == "replication" || section == "REPLICATION" {
                    let info = format!("# Replication \r\nrole:{}\r\n", self.role);
                    let info = if let Some(master_replid) = &self.master_replid {
                        format!("{}master_replid:{}\r\n", info, master_replid)
                    } else {
                        info
                    };
                    let info = if let Some(master_repl_offset) = &self.master_repl_offset {
                        format!("{}master_repl_offset:{}\r\n", info, master_repl_offset)
                    } else {
                        info
                    };
                    [format!("${}\r\n{}\r\n", info.len(), info)].to_vec()
                } else {
                    [format!("$-1\r\n")].to_vec()
                }
            }
            Command::ReplConf(_, _) => [format!("+OK\r\n")].to_vec(),
            Command::Psync(_repl_id, _offset) => {
                if let None = self.master_replid {
                    return [format!("$-1\r\n")].to_vec();
                }
                let master_replid = self.master_replid.clone().unwrap();
                if let None = self.master_repl_offset {
                    return [format!("$-1\r\n")].to_vec();
                }
                let decode_bytes = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2").context("Error while decoding hex").unwrap();
                let empty_rdb = String::from_utf8_lossy(&decode_bytes).to_string();
                println!("{}", empty_rdb);
                let master_repl_offset = self.master_repl_offset.clone().unwrap();
                [
                    format!("+FULLRESYNC {} {}\r\n", master_replid, master_repl_offset),
                    format!("${}\r\n{}", empty_rdb.len(), empty_rdb),
                ]
                .to_vec()
            }
        }
    }
}
