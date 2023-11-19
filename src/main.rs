use std::{
    io::{Read, Write},
    net::TcpListener,
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => loop {
                let mut req = [0; 512];
                stream.read(&mut req).unwrap();
                let string = String::from_utf8_lossy(&req).to_string();
                println!("request: {}", string);
                string.split("\r\n").for_each(|s| {
                    if s.to_lowercase() == "ping" {
                        let _ = stream.write_all(b"+PONG\r\n").unwrap();
                    }
                });
            },
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
