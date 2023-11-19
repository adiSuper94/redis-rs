use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            handle_stream(stream).await;
        });
    }
}

async fn handle_stream(stream: TcpStream) {
    loop {
        stream.readable().await.unwrap();
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
        let s = std::str::from_utf8(&buf).unwrap();
        let x = s.split("\r\n");
        for i in x {
            if i.to_lowercase() == "ping" {
                stream.writable().await.unwrap();
                stream.try_write("+PONG\r\n".as_bytes()).unwrap();
            }
        }
    }
}
