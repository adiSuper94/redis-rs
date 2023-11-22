use std::{slice::Iter, str::Split};

pub enum Command {
    ECHO(String),
    PING,
}

impl Command {
    pub fn execute(&self) -> String {
        match self {
            Command::ECHO(echo) => format!("${}\r\n{}\r\n", echo.len(), echo),
            Command::PING => format!("$4\r\nPONG\r\n"),
        }
    }

    pub fn deserialize(req: &str) -> Vec<Self> {
        let req = RedisDataType::deserialize(req);
        match req {
            RedisDataType::Array(arr) => {
                let mut arr_iter = arr.iter();
                return Self::process_req(&mut arr_iter);
            }
            _ => {
                panic!("Invalid data type")
            }
        }
    }

    fn process_req(data_stream: &mut Iter<'_, RedisDataType>) -> Vec<Command> {
        let mut commands: Vec<Command> = Vec::new();
        while let Some(item) = data_stream.next() {
            match &item {
                RedisDataType::SimpleString(str) | RedisDataType::BulkString(str) => {
                    if str == "PING" || str == "ping" {
                        commands.push(Command::PING);
                    } else if str == "ECHO" || str == "echo" {
                        let message = Self::get_next_string(data_stream).unwrap();
                        commands.push(Command::ECHO(message));
                    }
                }
                RedisDataType::Array(arr) => {
                    let mut arr_iter = arr.iter();
                    let mut arr_resp = Self::process_req(&mut arr_iter);
                    commands.append(&mut arr_resp);
                }
            }
        }
        return commands;
    }

    fn get_next_string(data_stream: &mut Iter<'_, RedisDataType>) -> Option<String> {
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
