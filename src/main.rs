pub mod redis_server;

use redis_server::{Command, Redis};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let redis_server = Redis::new();
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        let redis_server_clone = redis_server.clone();
        if let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                handle_stream(stream, redis_server_clone).await;
            });
        }
    }
}

async fn handle_stream(stream: TcpStream, mut redis_server: Redis) {
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
            Err(_e) => {
                continue;
            }
        }
        let req = String::from_utf8_lossy(&buf).to_string();
        let commands = Command::deserialize(&req);
        for command in commands {
            let resp = redis_server.execute(&command);
            stream.writable().await.unwrap();
            stream.try_write(resp.as_bytes()).unwrap();
        }
    }
}
