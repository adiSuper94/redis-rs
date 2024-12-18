pub mod redis_commands;
pub mod redis_db;
pub mod redis_server;

use std::sync::Arc;

use redis_commands::Command;
use redis_server::{Redis, RedisCliArgs, Role};
use tokio::{net::{TcpListener, TcpStream}, sync::broadcast::{self, Sender}};

#[tokio::main]
async fn main() {
    let cli_args = parse_cli_args();
    let port = cli_args.port.clone();
    let redis_server = Redis::new(cli_args).await;
    let (tx, _rx) = broadcast::channel::<Command>(8);
    let sender = Arc::new(tx);
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    loop {
        let redis_server_clone = redis_server.clone();
        let sender = Arc::clone(&sender);
        if let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                handle_stream(stream, redis_server_clone, sender).await;
            });
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

async fn handle_stream(stream: TcpStream, mut redis_server: Redis, sender: Arc::<Sender<Command>>) {
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
            redis_server.execute(command, &stream, Arc::clone(&sender)).await;
        }
    }
}
