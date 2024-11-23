use std::{iter::Peekable, slice::Iter, str::Split, time::SystemTime};

#[derive(Clone)]
pub enum Command {
    Echo(String),
    Ping,
    Get(String),
    Set(String, String, Option<SystemTime>),
    ConfigGet(String),
    Keys(String),
    Info(String),
    ReplConf(String, String),
    Psync(String, String),
}

impl Command {
    pub fn deserialize(req: &str) -> Vec<Self> {
        let req = RedisDataType::deserialize(req);
        match req {
            RedisDataType::Array(arr) => {
                let mut arr_iter: Peekable<Iter<'_, RedisDataType>> = arr.iter().peekable();
                return Self::parse_req(&mut arr_iter);
            }
            _ => {
                panic!("Invalid data type")
            }
        }
    }

    pub fn serialize(&self) -> String {
        match self {
            Command::Echo(echo) => {
                format!("*2\r\n$4\r\nECHO\r\n${}\r\n{}\r\n", echo.len(), echo)
            }
            Command::Ping => {
                format!("*1\r\n$4\r\nPING\r\n")
            }
            Command::Get(_) => todo!(),
            Command::Set(_, _, _system_time) => todo!(),
            Command::ConfigGet(_) => todo!(),
            Command::Keys(_) => todo!(),
            Command::Info(_) => todo!(),
            Command::ReplConf(key, val) => format!(
                "*3\r\n$8\r\nREPLCONF\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                key.len(),
                key,
                val.len(),
                val
            ),
            Command::Psync(repl_id, offset) => format!(
                "*3\r\n$5\r\nPSYNC\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                repl_id.len(),
                repl_id,
                offset.len(),
                offset
            ),
        }
    }

    fn parse_req(data_stream: &mut Peekable<Iter<'_, RedisDataType>>) -> Vec<Command> {
        let mut commands: Vec<Command> = Vec::new();
        while let Some(item) = data_stream.next() {
            match &item {
                RedisDataType::SimpleString(str) | RedisDataType::BulkString(str) => {
                    if str == "PING" || str == "ping" {
                        commands.push(Command::Ping);
                    } else if str == "ECHO" || str == "echo" {
                        let message = Self::get_next_string(data_stream).unwrap();
                        commands.push(Command::Echo(message));
                    } else if str == "GET" || str == "get" {
                        let key = Self::get_next_string(data_stream).unwrap();
                        commands.push(Command::Get(key));
                    } else if str == "SET" || str == "set" {
                        let key = Self::get_next_string(data_stream).unwrap();
                        let value = Self::get_next_string(data_stream).unwrap();
                        let mut exp: Option<SystemTime> = None;
                        if let Some(next_str) = Self::peek_next_string(data_stream) {
                            if next_str == "PX" || next_str == "px" {
                                let _ = Self::get_next_string(data_stream).unwrap();
                                let px = Self::get_next_string(data_stream).unwrap();
                                let duration = px.parse::<u64>().unwrap();
                                exp = std::time::SystemTime::now()
                                    .checked_add(std::time::Duration::from_millis(duration as u64));
                            }
                        }
                        commands.push(Command::Set(key, value, exp));
                    } else if str == "CONFIG" || str == "config" {
                        let cmd = Self::get_next_string(data_stream).unwrap();
                        if cmd == "GET" || cmd == "get" {
                            let key = Self::get_next_string(data_stream).unwrap();
                            commands.push(Command::ConfigGet(key));
                        }
                    } else if str == "KEYS" || str == "keys" {
                        let pattern = Self::get_next_string(data_stream).unwrap();
                        commands.push(Command::Keys(pattern));
                    } else if str == "INFO" || str == "info" {
                        if let Some(section) = Self::peek_next_string(data_stream) {
                            if section == "replication" || section == "REPLICATION" {
                                commands.push(Command::Info(section));
                            } else {
                                commands.push(Command::Info("all".to_string()));
                            }
                        } else {
                            commands.push(Command::Info("all".to_string()));
                        }
                    } else if str == "REPLCONF" || str == "replconf" {
                        let key = Self::get_next_string(data_stream).unwrap();
                        let val = Self::get_next_string(data_stream).unwrap();
                        commands.push(Command::ReplConf(key, val));
                    } else if str == "PSYNC" || str == "psync" {
                        let key = Self::get_next_string(data_stream).unwrap();
                        let val = Self::get_next_string(data_stream).unwrap();
                        commands.push(Command::Psync(key, val));
                    }
                }
                RedisDataType::Array(arr) => {
                    let mut arr_iter = arr.iter().peekable();
                    let mut arr_resp = Self::parse_req(&mut arr_iter);
                    commands.append(&mut arr_resp);
                }
            }
        }
        return commands;
    }

    fn peek_next_string(data_stream: &mut Peekable<Iter<'_, RedisDataType>>) -> Option<String> {
        if let Some(message) = data_stream.peek() {
            match message {
                RedisDataType::SimpleString(msg) => Some(msg.to_string()),
                RedisDataType::BulkString(msg) => Some(msg.to_string()),
                RedisDataType::Array(_) => None,
            }
        } else {
            None
        }
    }

    fn get_next_string(data_stream: &mut Peekable<Iter<'_, RedisDataType>>) -> Option<String> {
        if let Some(message) = data_stream.next() {
            match message {
                RedisDataType::SimpleString(msg) => Some(msg.to_string()),
                RedisDataType::BulkString(msg) => Some(msg.to_string()),
                RedisDataType::Array(_) => None,
            }
        } else {
            None
        }
    }
}

#[derive(Debug)]
enum RedisDataType {
    SimpleString(String),
    BulkString(String),
    Array(Vec<RedisDataType>),
}

impl RedisDataType {
    #[allow(dead_code)]
    fn serialize(&self) -> String {
        match self {
            RedisDataType::SimpleString(str) => format!("+{}\r\n", str),
            RedisDataType::BulkString(str) => format!("${}\r\n{}\r\n", str.len(), str),
            RedisDataType::Array(arr) => {
                let mut serialized_arr = format!("*{}\r\n", arr.len());
                for item in arr {
                    serialized_arr.push_str(&item.serialize());
                }
                serialized_arr
            }
        }
    }

    fn deserialize(data: &str) -> Self {
        let mut tokens = data.split("\r\n");
        Self::parse_req(None, &mut tokens).pop().unwrap()
    }

    fn parse_req(arr_len: Option<usize>, tokens: &mut Split<'_, &str>) -> Vec<RedisDataType> {
        let mut redis_data_stream: Vec<RedisDataType> = Vec::new();
        let mut count = 0;
        while let Some(token) = tokens.next() {
            if let Some(first_byte) = token.chars().next() {
                if first_byte == '+' {
                    let simple_string = (&token[1..]).to_string();
                    redis_data_stream.push(RedisDataType::SimpleString(simple_string));
                } else if first_byte == '*' {
                    if let Ok(array_len) = token[1..].parse::<usize>() {
                        let array = Self::parse_req(Some(array_len), tokens);
                        redis_data_stream.push(RedisDataType::Array(array));
                    }
                } else if first_byte == '$' {
                    if let Ok(bulk_str_len) = token[1..].parse::<usize>() {
                        if let Some(bulk_str) = tokens.next() {
                            let bulk_string = bulk_str.to_string();
                            assert_eq!(bulk_string.len(), bulk_str_len);
                            redis_data_stream.push(RedisDataType::BulkString(bulk_string));
                        }
                    }
                }
            }
            count += 1;
            if let Some(n) = arr_len {
                if count == n {
                    return redis_data_stream;
                }
            }
        }
        redis_data_stream
    }
}
