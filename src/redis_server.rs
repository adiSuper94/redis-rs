use crate::redis_commands::Command;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

pub struct Redis {
    db: Arc<Mutex<HashMap<String, String>>>,
    exp: Arc<Mutex<HashMap<String, SystemTime>>>,
    config: Arc<Mutex<HashMap<String, String>>>,
}

impl Redis {
    pub fn new(dir: Option<String>, file_name: Option<String>) -> Self {
        let instance = Redis {
            db: Arc::new(Mutex::new(HashMap::new())),
            exp: Arc::new(Mutex::new(HashMap::new())),
            config: Arc::new(Mutex::new(HashMap::new())),
        };
        if let Some(dir) = dir {
            instance
                .config
                .lock()
                .unwrap()
                .insert("dir".to_string(), dir);
        };
        if let Some(file_name) = file_name {
            instance
                .config
                .lock()
                .unwrap()
                .insert("file_name".to_string(), file_name);
        };
        instance
    }
    pub fn clone(&self) -> Self {
        Redis {
            db: Arc::clone(&self.db),
            exp: Arc::clone(&self.exp),
            config: Arc::clone(&self.config),
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
        }
    }
}
