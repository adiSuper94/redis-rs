use crate::redis_commands::Command;
use crate::redis_db::RedisDB;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

pub struct Redis {
    db: Arc<Mutex<HashMap<String, String>>>,
    exp: Arc<Mutex<HashMap<String, SystemTime>>>,
    config: Arc<Mutex<HashMap<String, String>>>,
    role: String,
    master_replid: Option<String>,
    master_repl_offset: Option<usize>,
}

impl Redis {
    pub fn new(dir: Option<String>, file_name: Option<String>, primary: bool) -> Self {
        let instance = Redis {
            db: Arc::new(Mutex::new(HashMap::new())),
            exp: Arc::new(Mutex::new(HashMap::new())),
            config: Arc::new(Mutex::new(HashMap::new())),
            role: if primary {
                "master".to_string()
            } else {
                "slave".to_string()
            },
            master_repl_offset: Some(0),
            master_replid: if primary {
                Some("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string())
            } else {
                None
            },
        };
        if let Some(dir) = dir {
            instance
                .config
                .lock()
                .unwrap()
                .insert("dir".to_string(), dir.clone());
            if let Some(file_name) = file_name {
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

    pub fn execute(&mut self, command: &Command) -> String {
        match &command {
            Command::Echo(echo) => format!("${}\r\n{}\r\n", echo.len(), echo),
            Command::Ping => format!("$4\r\nPONG\r\n"),
            Command::Get(key) => {
                if let Some(value) = self.get(key) {
                    format!("${}\r\n{}\r\n", value.len(), value)
                } else {
                    format!("$-1\r\n")
                }
            }
            Command::Set(key, val, exp) => {
                self.set(key.to_string(), val.to_string(), exp);
                format!("+OK\r\n")
            }
            Command::ConfigGet(key) => {
                if let Some(value) = self.config.lock().unwrap().get(key) {
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
                let key_count = self.db.lock().unwrap().keys().count();
                let res = self
                    .db
                    .lock()
                    .unwrap()
                    .keys()
                    .fold(String::new(), |acc, key| {
                        format!("{}${}\r\n{}\r\n", acc, key.len(), key)
                    });
                format!("*{}\r\n{}", key_count, res)
            }
            Command::Info(section) => {
                if section == "all" || section == "replication" {
                    let info = if self.role == "master"{
                     format!("# Replication \r\nrole:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\n", self.role, self.master_replid.unwrap(), self.master_repl_offset.unwrap())
                    } else{
                     format!("# Replication \r\nrole:{}\r\n", self.role)
                    };
                    format!("${}\r\n{}\r\n", info.len(), info)
                } else {
                    format!("$-1\r\n")
                }
            }
        }
    }
}
