pub mod redis_commands;
pub mod redis_db;
pub mod redis_server;
use std::sync::Arc;

use redis_commands::Command;
use redis_server::{Redis, RedisCliArgs, Role};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

#[tokio::main]
async fn main() {
    let cli_args = parse_cli_args();
    let port = cli_args.port.clone();
    let mut redis_server = Redis::new(cli_args).await;
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    loop {
        let redis_server_clone = redis_server.clone();
        if let Ok((stream, _)) = listener.accept().await {
            redis_server = handle_stream(stream, redis_server_clone).await;
        }
    }
}

fn parse_cli_args() -> RedisCliArgs {
    let args: Vec<String> = std::env::args().collect();
    let mut opts = getopts::Options::new();
    opts.optopt("d", "dir", "set persistence directory", "DIR");
    opts.optopt("f", "dbfilename", "set persistence filename", "FILENAME");
    opts.optopt("p", "port", "set port number for redis to run on", "PORT");
    opts.optopt("r", "replicaof", "set master url", "REPLICAOF");
    let cli_opts = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => panic!("{}", f.to_string()),
    };
    let dir = cli_opts.opt_str("d");
    let file_name = cli_opts.opt_str("f");
    let replica_of = cli_opts.opt_str("r");
    let port = if let Some(port) = cli_opts.opt_str("p") {
        port
    } else {
        "6379".to_string()
    };
    let mut args = RedisCliArgs {
        dir,
        file_name,
        port,
        master_host: None,
        master_port: None,
        role: Role::Primary,
    };
    if let Some(replica_of) = replica_of {
        let replica_of: Vec<&str> = replica_of.split(" ").collect();
        if replica_of.len() != 2 {
            panic!("Invalid replicaof argument");
        }
        args.master_host = Some(replica_of[0].to_string());
        args.master_port = Some(replica_of[1].to_string());
        args.role = Role::Replica
    }
    args
}

async fn handle_stream(stream: TcpStream, mut redis_server: Redis) -> Redis{
    let stream = Arc::new(Mutex::new(stream));
    //loop {
        if let Err(_) = stream.lock().await.readable().await {
            return redis_server;
        }
        let mut buf = [0; 512];
        match stream.lock().await.try_read(&mut buf) {
            Ok(n) => {
                if n == 0 {
                    return redis_server;
                }
            }
            Err(_e) => {
                return redis_server;
            }
        }
        let req = String::from_utf8_lossy(&buf).to_string();
        let commands = Command::deserialize(&req);
        for command in commands {
            redis_server = redis_server.execute(command, Arc::clone(&stream)).await;
        }
    //}
    redis_server
}

//async fn write(stream: &TcpStream, bytes: &[u8]) {
//    let mut offset = 0;
//    loop {
//        stream.writable().await.unwrap();
//        if let Ok(n) = stream.try_write(&bytes) {
//            offset += n;
//            if offset >= bytes.len() {
//                break;
//            }
//        }
//    }
//}
