
mod table;

use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write, BufRead};
use std::net::{TcpStream, TcpListener};

fn client_cli () -> io::Result<()> {
    let mut buf = String::new();
    let stdin = io::stdin();

    let mut stream = TcpStream::connect("127.0.0.1:3000")?;

    while stdin.read_line(&mut buf).is_ok() {
        client_handle_line(buf.as_str(), &mut stream)?;
        buf.clear();
    }
    Ok(())
}

fn client_args (arg: String) -> io::Result<()> {
    let lines: Vec<&str> = arg.split(";").map(|s| s.trim()).collect();
    let mut stream = TcpStream::connect("127.0.0.1:3000")?;
    for l in lines {
        client_handle_line(l, &mut stream)?;
    }
    stream.shutdown(std::net::Shutdown::Both)?;
    Ok(())
}

fn client_handle_line(line: &str, stream: &mut TcpStream) -> io::Result<()> {
    stream.write_all(line.as_bytes())?;
    stream.flush()?;

    let mut response_reader = io::BufReader::new(stream);
    let response_bytes: Vec<u8> = response_reader.fill_buf()?.to_vec();

    String::from_utf8(response_bytes)
        .map(|msg| println!("{}\n", msg))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Received string could not be parsed as UTF8"))?;


    Ok(())
}

struct ServerHandle {
    stream: TcpStream,
}

impl ServerHandle {
    fn new () -> ServerHandle {
        ServerHandle { stream: TcpStream::connect("120.0.0.1:3000").unwrap() }
    }
    fn start_listening (&mut self) -> io::Result<()> {
        let mut reader = io::BufReader::new(&mut self.stream);
        let received: Vec<u8> = reader.fill_buf()?.to_vec();
        reader.consume(received.len());

        String::from_utf8(received)
            .map(|msg| println!("{}", msg))
            .map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "Received string could not be parsed as UTF8")
            })
    }
}

#[derive(PartialEq, Eq)]
enum CommandResult {
    Exit,
    Value(Option<String>),
    Error(String)
}

fn parse_command (command: &str, map: &mut HashMap<String, i32>) -> CommandResult {
    let tokens: Vec<&str> = command
        .trim()
        .split_whitespace()
        .collect();

    if tokens.len() == 0 { return CommandResult::Exit }

    dbg!(&tokens);

    match tokens[0].to_lowercase().as_str() {
        "inc" => {
            if tokens.len() != 2 {
                return CommandResult::Error("inc command requires a single key value".to_string())
            }
            let key = tokens[1];
            let v = map.get(key);
            if let None = v { return CommandResult::Error("value not found".to_string()) } 
            map.insert(key.to_string(), v.unwrap() + 1);

            CommandResult::Value(None)
        },

        "dec" => {
            if tokens.len() != 2 {
                return CommandResult::Error("dec command requires a single key value".to_string())
            }
            let key = tokens[1];
            let v = map.get(key);
            if let None = v { return CommandResult::Error("value not found".to_string()) } 
            map.insert(key.to_string(), v.unwrap() - 1);

            CommandResult::Value(None)
        },

        "get" => {
            if tokens.len() != 2 {
                return CommandResult::Error("get command requires a single key value".to_string())
            }
            let key = tokens[1];
            let v = map.get(key);

            CommandResult::Value(Some(v.map(|i| i.to_string()).unwrap_or("value not found".to_string())))
        },

        "set" => {
            if tokens.len() != 3 {
                return CommandResult::Error("set command requires a key and a value argument".to_string())
            }
            
            let key = tokens[1];
            let value = str::parse(tokens[2]).map_err(|_| "Could not parse value to an integer".to_string());

            if let Err(e) = value { return CommandResult::Error(e) }

            map.insert(key.to_owned(), value.unwrap());

            CommandResult::Value(None)
        },

        "keys" => {
            if tokens.len() != 1 {
                return CommandResult::Error("keys command takes no arguments".into());
            }

            let r = map.keys()
                .into_iter()
                .map(|s| s.to_owned())
                .collect::<Vec<_>>().join("\n");

            if map.len() > 0 {
                CommandResult::Value(Some(r))
            } else {
                CommandResult::Value(Some(" ".into()))
            }
        }

        _ => CommandResult::Error(format!("Invalid command {}", tokens[0]))
    }
}

fn read_from_stream (stream: &mut TcpStream, map: &mut HashMap<String, i32>) -> io::Result<()> {
    let mut reader = io::BufReader::new(stream.try_clone()?);

    loop {
        println!("reading data from stream");
        let received: Vec<u8> = reader.fill_buf()?.to_vec();
        reader.consume(received.len());
        println!("read {} bytes from the stream", received.len());

        let command = String::from_utf8(received)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Received command could not be parsed as valid UTF-8"))?;

        dbg!(command.as_str());

        let command_result = parse_command(command.as_str(), map);

        if CommandResult::Exit == command_result {
            break
        }

        let rr = match command_result {
            CommandResult::Error(s) => Some(s),
            CommandResult::Value(v) => Some(v.unwrap_or(" ".to_string())),
            CommandResult::Exit => None
        };

        dbg!(&rr);

        match rr {
            Some(s) => {
                stream.write(s.as_bytes())?;
            },
            None => {
                break
            }
        }
    }

    Ok(())
}


fn do_kv() -> io::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    let t = if args.len() > 1 { args[1].to_owned() } else { "".to_owned() };
    
    return match t.as_str() {
        "server" => {
            let listener = TcpListener::bind("127.0.0.1:3000")?;
            println!("Server listening on {}", listener.local_addr()?);

            let mut map = HashMap::<String, i32>::new();

            for stream in listener.incoming() {
                println!("New connection!");
                read_from_stream(&mut stream?, &mut map)?;
            }

            Ok(())
        },

        "client-cli" => client_cli(),
        "client-args" => client_args(args[2].to_owned()),
        _ => {
            println!("Unknown command option {}", t);
            Ok(())
        }
    };
}