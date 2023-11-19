use std::{
    io::{Read, Write},
    net::TcpListener,
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut req: Vec<u8> = Vec::new();
                stream.read_to_end(&mut req).unwrap();
                let string = String::from_utf8(req).unwrap();
                string.split("\r\n").for_each(|s| {
                    if s == "PING" {
                        let _ = stream.write_all(b"+PONG\r\n").unwrap();
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
