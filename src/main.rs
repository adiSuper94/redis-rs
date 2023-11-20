use std::{slice::Iter, str::Split};

use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        if let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                handle_stream(stream).await;
            });
        }
    }
}

async fn handle_stream(stream: TcpStream) {
    loop {
        if let Err(_) = stream.readable().await {
            continue;
        }
        let mut buf = [0; 512];
        match stream.try_read(&mut buf) {
            Ok(n) => {
                if n == 0 {
                    break;
                }
            }
            Err(e) => {
                println!("failed to read from socket; err = {:?}", e);
                continue;
            }
        }
        let req = String::from_utf8_lossy(&buf).to_string();
        // println!("received: {}", req);
        let mut tokens = req.split("\r\n");
        let data_stream = parse_req(None, &mut tokens);
        // println!("data_stream: {:?}", data_stream);
        let mut ds_iter = data_stream.iter();
        let resps = process_req(&mut ds_iter);
        for resp in resps {
            stream.writable().await.unwrap();
            stream.try_write(resp.as_bytes()).unwrap();
        }
    }
}

// enum Command {
//     ECHO(String),
//     PING,
// }

#[derive(Debug)]
enum RedisDataTypes {
    SimpleString(String),
    BulkString(String),
    Array(Vec<RedisDataTypes>),
}

fn parse_req(arr_len: Option<usize>, tokens: &mut Split<'_, &str>) -> Vec<RedisDataTypes> {
    let mut redis_data_stream: Vec<RedisDataTypes> = Vec::new();
    let mut count = 0;
    while let Some(token) = tokens.next() {
        if let Some(first_byte) = token.chars().next() {
            if first_byte == '+' {
                let simple_string = (&token[1..]).to_string();
                // println!("simple string: {}", simple_string);
                redis_data_stream.push(RedisDataTypes::SimpleString(simple_string));
            } else if first_byte == '*' {
                if let Ok(array_len) = token[1..].parse::<usize>() {
                    let array = parse_req(Some(array_len), tokens);
                    redis_data_stream.push(RedisDataTypes::Array(array));
                }
            } else if first_byte == '$' {
                // println!("bulk string: {}", token);
                if let Ok(bulk_str_len) = token[1..].parse::<usize>() {
                    // println!("bulk string len: {}", bulk_str_len);
                    if let Some(bulk_str) = tokens.next() {
                        // println!("bulk string: {}", bulk_str);
                        let bulk_string = bulk_str.to_string();
                        assert_eq!(bulk_string.len(), bulk_str_len);
                        redis_data_stream.push(RedisDataTypes::BulkString(bulk_string));
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

fn process_req(data_stream: &mut Iter<'_, RedisDataTypes>) -> Vec<String> {
    let mut resps: Vec<String> = Vec::new();
    while let Some(item) = data_stream.next() {
        match &item {
            RedisDataTypes::SimpleString(str) => {
                if str == "PING" || str == "ping" {
                    resps.push("+PONG\r\n".to_string())
                } else if str == "ECHO" || str == "echo" {
                    let echo = format!(
                        "+{}\r\n",
                        get_next_string(data_stream).unwrap_or("".to_string())
                    );
                    resps.push(echo);
                }
            }
            RedisDataTypes::BulkString(str) => {
                if str == "PING" || str == "ping" {
                    resps.push("+PONG\r\n".to_string())
                } else if str == "ECHO" || str == "echo" {
                    let echo = format!(
                        "+{}\r\n",
                        get_next_string(data_stream).unwrap_or("".to_string())
                    );
                    resps.push(echo);
                }
            }
            RedisDataTypes::Array(arr) => {
                let mut arr_iter = arr.iter();
                let mut arr_resp = process_req(&mut arr_iter);
                resps.append(&mut arr_resp);
            }
        }
    }
    // println!("resps: {:?}", resps);
    return resps;
}

fn get_next_string(data_stream: &mut Iter<'_, RedisDataTypes>) -> Option<String> {
    if let Some(message) = data_stream.next() {
        match message {
            RedisDataTypes::SimpleString(msg) => Some(msg.to_string()),
            RedisDataTypes::BulkString(msg) => Some(msg.to_string()),
            RedisDataTypes::Array(_) => None,
        }
    } else {
        None
    }
}
