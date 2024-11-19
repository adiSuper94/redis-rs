pub mod redis_commands;
pub mod redis_db;
pub mod redis_server;

use redis_commands::Command;
use redis_server::Redis;
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let (dir, file_name, port) = parse_cli_args();
    let redis_server = Redis::new(dir, file_name);
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    loop {
        let redis_server_clone = redis_server.clone();
        if let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                handle_stream(stream, redis_server_clone).await;
            });
        }
    }
}

fn parse_cli_args() -> (Option<String>, Option<String>, String) {
    let args: Vec<String> = std::env::args().collect();
    let mut opts = getopts::Options::new();
    opts.optopt("d", "dir", "set persistence directory", "DIR");
    opts.optopt("f", "dbfilename", "set persistence filename", "FILENAME");
    opts.optopt("p", "port", "set port number for redis to run on", "PORT");
    let cli_opts = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => panic!("{}", f.to_string()),
    };
    let dir = cli_opts.opt_str("d");
    let file_name = cli_opts.opt_str("f");
    let port = if let Some(port) = cli_opts.opt_str("p") {
        port
    } else {
        "6379".to_string()
    };
    (dir, file_name, port)
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
