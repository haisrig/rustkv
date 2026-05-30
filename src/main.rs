use std::io::Read;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Mutex};
use tokio::time::{interval, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};


#[derive(Debug)]
enum Command {
    Set(String, String, Option<String>, Option<String>),
    Get(String),
    Del(String),
    Publish(String, String),
    Subscribe(String),
    Ping,
    Info,
    Quit,
}

struct Entry {
    value: String,
    expires_at: Option<Instant>
}

type Store = Arc<Mutex<HashMap<String, Entry>>>;
type PubSub = Arc<Mutex<HashMap<String, broadcast::Sender<String>>>>;

fn parse_command(input: &str) -> Result<Command, String> {
    let tokens = input.trim().split("\r\n")
        .filter(|s| !s.starts_with("*"))
        .filter(|s| !s.starts_with("$"))
        .collect::<Vec<_>>();

    println!("{:?}", tokens);
    let (cmd, args) = match tokens.split_first() {
        Some((cmd, args)) => (cmd.to_lowercase(), args),
        None => return Err("invalid command".to_string()),
    };

    match (cmd.as_str(), args) {
        ("set", [key, val, ex, secs]) => Ok(Command::Set(key.to_string(), val.to_string(), Some(ex.to_string()), Some(secs.to_string()))),
        ("set", [key, val]) => Ok(Command::Set(key.to_string(), val.to_string(), None, None)),
        ("get", [key]) => Ok(Command::Get(key.to_string())),
        ("del", [key]) => Ok(Command::Del(key.to_string())),
        ("publish", [channel, msg]) => Ok(Command::Publish(channel.to_string(), msg.to_string())),
        ("subscribe", [channel]) => Ok(Command::Subscribe(channel.to_string())),
        ("ping", _) => Ok(Command::Ping),
        ("info", _) => Ok(Command::Info),
        ("quit", _) => Ok(Command::Quit),
        _ => Err(format!("Unknown command: {:?}", cmd)),
    }
}

async fn handle_subscribe(channel: String, pub_sub: PubSub, stream: &mut TcpStream) {
    let mut rx = {
        let mut pubsub = pub_sub.lock().await;
        let tx = pubsub.entry(channel.clone())
            .or_insert_with(|| broadcast::channel(16).0);
        tx.subscribe()
    };
    let confirm = format!(
        "*3\r\n$9\r\nsubscribe\r\n${}\r\n{}\r\n:1\r\n",
        channel.len(), channel
    );
    if stream.write_all(confirm.as_bytes()).await.is_err() {
        return;
    }
    loop {
        match rx.recv().await {
            Ok(message) => {
                println!("Received message: {}", message);
                let response = format!(
                    "*3\r\n$7\r\nmessage\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    channel.len(), channel,
                    message.len(), message
                );
                println!("Sending response: {:?}", response);
                if stream.write_all(response.as_bytes()).await.is_err() {
                    return;
                }
            },
            Err(broadcast::error::RecvError::Lagged(n)) => {
                println!("Lagged n {}", n);
            },
            Err(_) => return,
        }
    }
}

async fn handle_client(mut stream: TcpStream, store: Store, pubsub: PubSub,
                    aof: Arc<Mutex<File>>) {
    println!("New connection from: {}", stream.peer_addr().unwrap());
    let mut buffer = [0u8; 1024];
    loop {
        let size = stream.read(&mut buffer).await;
        let n = match size {
            Ok(0) => return,
            Ok(n) => n,
            Err(_) => return,
        };
        let input = String::from_utf8_lossy(&buffer[..n]);
        let pub_sub_clone = pubsub.clone();
        if let Ok(Command::Subscribe(channel)) = parse_command(&input) {
            handle_subscribe(channel, pub_sub_clone, &mut stream).await;
            return;
        }

        let response = match parse_command(&input) {
            Ok(Command::Set(key, value, ex, duration)) => {
                let mut st = store.lock().await;
                let mut entry = Entry {value: value.clone() , expires_at: None};

                if let Some(op) = ex {
                    if op == "ex" {
                        if let Some(duration) = duration {
                            entry.expires_at = Some(Instant::now() + Duration::from_secs(duration.parse().unwrap()));
                        }
                    }
                }
                st.insert(key.clone(), entry);
                drop(st);
                let mut file = aof.lock().await;
                match file.write_all(format!("SET {} {}\n", key, value).as_bytes()).await {
                    Ok(_) => println!("Wrote to file: {}", key),
                    Err(err) => println!("Error writing to file: {}", err)
                }
                file.flush().await.expect("Couldn't flush to file");
                "+OK\r\n".to_string()
            }
            Ok(Command::Get(key)) => {
                let st = store.lock().await;
                let value = st.get(&key);
                match value {
                    Some(val) => {
                        if let Some(expires_at) = val.expires_at {
                            if expires_at > Instant::now() {
                                format!("+{}\r\n", val.value.clone())
                            } else {
                                "$-1\r\n".to_string()
                            }
                        }   else {
                            format!("+{}\r\n", val.value)
                        }
                    },
                    None => "$-1\r\n".to_string(),
                }

            }
            Ok(Command::Del(key)) => {
                let mut st =  store.lock().await;
                st.remove(&key);
                let mut file = aof.lock().await;
                file.write_all(format!("DEL {}\n", key).as_bytes()).await.expect("Couldn't write to file");
                file.flush().await.expect("Couldn't flush to file");
                "+OK\r\n".to_string()
            }
            Ok(Command::Publish(channel, msg)) => {
                let pubsub = pubsub.lock().await;
                match pubsub.get(&channel) {
                    Some(tx) => {
                        let count = tx.send(msg.clone()).unwrap_or(0);
                        format!("+{}\r\n", count)
                    }
                    None => ":0\r\n".to_string()
                }
            }
            Ok(Command::Subscribe(_channel)) => unreachable!(),
            Ok(Command::Ping) => {
                "+PONG\r\n".to_string()
            }
            Ok(Command::Info) => {
                "+\r\n".to_string()
            }
            Ok(Command::Quit) => {
                let _ = stream.write_all(b"+OK\r\n").await;
                return;
            }
            Err(e) => {
                format!("-ERR {}\r\n", e)
            }
        };
        if let Err(_) = stream.write_all(response.as_bytes()).await {
            return;
        }
    }
}

async fn handle_key_expiry(store: Store) {
    let mut timer = interval(Duration::from_secs(10));
    loop {
        timer.tick().await;
        println!("Running Key Expiry: {:?}", chrono::Local::now());
        let mut store_map = store.lock().await;
        store_map.retain(|_, v|  {
            if let Some(expires_at) = v.expires_at {
                return expires_at > Instant::now();
            }
            true
        });
    }
}


async fn replay_aof(store: Store, path: &str) {
    let file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(e) => {
            println!("No AOF file found");
            return;
        }
    };
    let mut count = 1;
    let mut lines = BufReader::new(file).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let tokens = line.trim().split(" ").collect::<Vec<_>>();
        match tokens.as_slice() {
            ["SET", key, value] => {
                store.lock().await.insert(key.to_string(), Entry {value: value.to_string(), expires_at: None});
                count += 1;
            }
            ["DEL", key] => {
                store.lock().await.remove(*key);
                count += 1;
            }
            _ => {}
        }
    }
    println!("AOF replay complete — {} commands restored", count);
}
#[tokio::main]
async fn main() {
    let store: Store = Arc::new(Mutex::new(HashMap::<String, Entry>::new()));
    replay_aof(store.clone(), "backup.txt").await;
    let pubsub: PubSub = Arc::new(Mutex::new(HashMap::new()));
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("backup.txt")
        .await.expect("Failed to open backup.txt");
    let file_ref = Arc::new(Mutex::new(file));

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("Listening on: {}", listener.local_addr().unwrap());
    let store_clone = store.clone();
    tokio::spawn(async move {
        handle_key_expiry(store_clone).await;
    });
    loop {
        let (stream, addr) = listener.accept().await.unwrap();
        let store = store.clone();
        let pub_sub = pubsub.clone();
        let file_clone = file_ref.clone();
        println!("Accepted connection from: {}", addr);
        tokio::spawn(async move {
           handle_client(stream, store, pub_sub, file_clone).await;
        });
    }
}
