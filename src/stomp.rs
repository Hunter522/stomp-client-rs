//! STOMP module

// Basic architecture:
// This is a library that provides a very simple STOMP client implementation.
//
// Threads:
// - TCP receive thread
//   - handles receiving data from TCP socket and parsing the STOMP frames
//   - handles sending STOMP frames to Session thread
// - Session thread
//   - handles receiving session STOMP frames
//   - handles talking back to STOMP server through TCP socket
//   - delegates all non-session STOMP frames (i.e. normal msgs) to subscription callbacks

// TCP receive thread communicates to Session thread using the Rust channel feature. This additionally
// provides us a FIFO queue so that the TCP receive thread does not have its IO blocked.

// Shared data:
// - TCP stream (TcpRecvThread, SessionThread, MainThread)
// - 



/*
STOMP 1.2 Session BNF




NULL                = <US-ASCII null (octet 0)>
LF                  = <US-ASCII line feed (aka newline) (octet 10)>
CR                  = <US-ASCII carriage return (octet 13)>
EOL                 = [CR] LF 
OCTET               = <any 8-bit sequence of data>

frame-stream        = 1*frame

frame               = command EOL
                      *( header EOL )
                      EOL
                      *OCTET
                      NULL
                      *( EOL )

command             = client-command | server-command

client-command      = "SEND"
                      | "SUBSCRIBE"
                      | "UNSUBSCRIBE"
                      | "BEGIN"
                      | "COMMIT"
                      | "ABORT"
                      | "ACK"
                      | "NACK"
                      | "DISCONNECT"
                      | "CONNECT"
                      | "STOMP"

server-command      = "CONNECTED"
                      | "MESSAGE"
                      | "RECEIPT"
                      | "ERROR"

header              = header-name ":" header-value
header-name         = 1*<any OCTET except CR or LF or ":">
header-value        = *<any OCTET except CR or LF or ":">
*/


use std::collections::HashMap;
use std::str::FromStr;
use std::thread;
use std::io::prelude::*;
use std::net::{SocketAddrV4, TcpStream};
use std::io;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::time::Duration;

// constants
pub const NULL_CHAR: char = '\0';
pub const EOL_CHAR:  char = '\n';



/// STOMP Command
#[derive(Debug)]
pub enum Command {
    // client commands
    SEND,
    SUBSCRIBE,
    UNSUBSCRIBE,
    BEGIN,
    COMMIT,
    ABORT,
    ACK,
    NACK,
    DISCONNECT,
    CONNECT,
    STOMP,

    // server commands
    CONNECTED,
    MESSAGE,
    RECEIPT,
    ERROR,
}

/// STOMP headers (alias for std::HashMap<String, String>)
pub type Headers = HashMap<String, String>;


/// STOMP Frame
#[derive(Debug)]
pub struct Frame {
    command: Command,
    headers: Headers,
    body   : Vec<u8>,
}

impl Frame {
    // convenience functions to construct different frame types
    pub fn heartbeat() -> Frame {
        return Frame {
            command: Command::MESSAGE,
            headers: HashMap::new(),
            body: Vec::new()
        };
    }

    pub fn as_bytes(&self) -> &[u8] {
        //TODO: return command + headers + body + NULL + EOL
        let foo = "yes";
        return foo.as_bytes();
    }
}

impl FromStr for Frame {
        type Err = ();

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            //TODO: Implement decoding of frame here...
            let frame = Frame { command: Command::ERROR, headers: HashMap::new(), body: Vec::new() };
            return Ok(frame);
        }
}

/// STOMP frame handler
pub trait FrameHandler {
    fn on_message(&self, frame: &Frame, context: &Client);
}

/// impl for Fn 
impl <T> FrameHandler for T where T: Fn(&Frame, &Client) {
    fn on_message(&self, frame: &Frame, context: &Client) {
        self(frame, context);
    }
}





/// STOMP subscription
struct Subscription {
    destination: String,
    frame_handler: Box<FrameHandler + Send + Sync>,
} 






/// STOMP session state enum
#[derive(Debug)]
pub enum SessionState {
    UNSYNC,
    CONNECTED,
    DISCONNECTED,
}






/// STOMP session
/// This struct maintains the state of a STOMP session
pub struct Session {
    state : SessionState,
    heartbeats : (i32, i32),
    subscriptions: HashMap<String, Subscription>,   // Destination, Subscription
}

impl Session {
    fn new() -> Session {
        return Session { state: SessionState::UNSYNC,
                         heartbeats: (0,0),
                         subscriptions: HashMap::new() };
    }
}





/// STOMP TCP client
pub struct Client {
    addr:                  String,
    session:               Arc<Mutex<Session>>,
    session_thread:        thread::JoinHandle<()>,
    tcp_stream:            Arc<Mutex<TcpStream>>,
    tcp_stream_thread:     thread::JoinHandle<()>,
    heartbeat_thread:      thread::JoinHandle<()>,
    run_flag:              Arc<AtomicBool>
}

impl Client {

    /// Connects the TCP client
    pub fn connect(options: &ClientBuilder) -> io::Result<Client> {
        
        // create TcpStream
        // start a new thread and start doing blocking reads

        print!("Connecting to {}...", options.addr);
        let stream = match TcpStream::connect(&*options.addr) {
            Ok(stream) => stream,
            Err(error) => return Err(error),
        };
        print!("Connected to {} ", options.addr);
        
        //TODO: create shared ptr (std::Arc) on session and stream so it can be passed into
        // the read thread
        // let data = Arc::new(Mutex::new(vec![1, 2, 3]));
        let session = Session::new();
        let session_arc = Arc::new(Mutex::new(session));
        let stream_arc = Arc::new(Mutex::new(stream));
        let run_flag_arc = Arc::new(AtomicBool::new(true));

        // clone all shared data to be moved into read thread
        let session_clone = session_arc.clone();
        let stream = stream_arc.clone();
        let run_flag = run_flag_arc.clone();


        // instead of passing shared mutable state...just create a channel
        // instead that passes the parsed Frame to the Session consumer thread
        // will still need to share the tcp socket

        // create channels s.t. read thread and session thread can communicate
        let (tx, rx) = channel();
        let (connecting_tx, connecting_rx) = channel();
        let (heartbeat_tx, heartbeat_rx) = channel();

        // start session thread
        let session_thread = thread::spawn(move || {
            // this should essentially block on rx.recv() waiting for Frames to come in
            // if the Frame is a sessions-specific frame and not just a message, then
            // handle it here, else forward it to the appropriate subscription frame handler

            while run_flag.load(Ordering::SeqCst) {
                let frame: Frame = rx.recv().unwrap();

                match frame.command {
                    Command::CONNECTED => {
                        // notify connecting thread that we've connected
                        connecting_tx.send(()).unwrap();

                        // start heartbeating (if necessary)
                        heartbeat_tx.send(()).unwrap();
                    }
                    
                    Command::MESSAGE => {

                    }

                    Command::RECEIPT => {
                        unimplemented!();
                    }

                    Command::ERROR => {
                        unimplemented!();
                    }

                    _ => {
                        // forward to subscription
                    }
                }
                
            }
        });


        let run_flag = run_flag_arc.clone();
        // let read_tx = tx.clone();

        // start read thread
        let stream_thread = thread::spawn(move || {
            print!("STOMP TCP client thread started...");

            // let foo = session_test_clone;
            // print!("{:?}", session_clone.lock().unwrap().state);
            // create read buffer

            // while running
            //  try reading into read buffer
            //  keep reading until we see EOF
            //  decode frame
            //  if good frame
            //    depending on session state then do something

            let mut read_buf : [u8; 4098] = [0; 4098];
            let mut msg_buf : Vec<u8> = Vec::new();

            while run_flag.load(Ordering::SeqCst) {
                let mut stream = stream.lock().unwrap();
                let bytes_read = stream.read(&mut read_buf).unwrap();

                //TODO: Do parsing here (read until NULL is read which is frame delimiter)
                for i in 0..bytes_read {
                    msg_buf.push(read_buf[i]);
                    if read_buf[i] == NULL_CHAR as u8 {
                        // reached end of msg

                        // convert to string
                        let msg_str = String::from_utf8(msg_buf.clone()).unwrap();
                        
                        // decode string into Frame
                        match Frame::from_str(&msg_str) {
                            Ok(frame) => {
                                // notify frame handler that matches desintation
                                // this eventually should be done asyncrhonously
                                // so as to not block network IO in cases of long-running consumers

                                // let mut session = session.lock().unwrap();
                                tx.send(frame).unwrap();
                            },
                            Err(e) => print!("{:?}", e),
                        }
                        
                        msg_buf.clear()
                    }   
                }
            }

        });


        let run_flag = run_flag_arc.clone();
        let stream = stream_arc.clone();
        // start heartbeat thread
        let heartbeat_thread = thread::spawn(move || {
            // don't start sending heartbeats just yet...
            // wait until we've connected first

            let connected = heartbeat_rx.recv().unwrap();

            while run_flag.load(Ordering::SeqCst) {
                // emit heartbeats at negotiated rate
                let mut stream = stream.lock().unwrap();
                stream.write(Frame::heartbeat().as_bytes());                   
                thread::sleep(Duration::from_millis(1000));
            }
        });

        // use either condition variable or synchronization point or channels
        // attempt to connect to STOMP server

        // STOMP msg sequence:

        // send CONNECT or STOMP (1.2only) frame
        // expect CONNECTED frame
        match connecting_rx.recv_timeout(Duration::from_secs(5)) {
            Ok(_) => { }
            Err(e) => return Err(io::Error::new(io::ErrorKind::TimedOut, 
                                 "Timed out when connecting to STOMP server")),
        };

        // start heartbeating (if enabled) (NOTE: this is done automatically by the session thread)



        return Ok(Client { addr: options.addr.clone(),
                           session: session_arc.clone(),
                           session_thread: session_thread,
                           tcp_stream: stream_arc.clone(),
                           tcp_stream_thread: stream_thread,
                           heartbeat_thread: heartbeat_thread,
                           run_flag: run_flag_arc.clone()
                           });
    }

    /// Disconnects the TCP client
    pub fn disconnect(&mut self) {
        //TODO: set run flag, join on thread, then shutdown socket
    }

    /// Subscribes to a STOMP destination and calls the passed frame handler
    pub fn subscribe<F>(&mut self, destination: &str, frame_handler: F) 
        where F: FrameHandler + Send + Sync + 'static {
            let subscription = Subscription { destination: destination.to_string(), 
                                              frame_handler: Box::new(frame_handler) };
            self.session.lock().unwrap().subscriptions.insert(destination.to_string(), subscription);
            // TODO: send subscribe message
    }

    /// Unsubscribes from a STOMP destination
    pub fn unsubscribe(&mut self, destination: &str) {

    }

    /// Publishes a STOMP Frame to a STOMP destination
    pub fn publish(&self, destination: &str, frame: &Frame) {
        
    }
}




/// Builder for Clients
pub struct ClientBuilder {
    addr: String,
    heartbeats: Option<(i32, i32)>,
    credentials: Option<(String, String)>,
    custom_headers: Option<Headers>
}

impl ClientBuilder {
    pub fn new(addr: &str) -> ClientBuilder {
        return ClientBuilder { addr: addr.to_string(), heartbeats: None, credentials: None, custom_headers: None };
    }

    pub fn with_heartbeats(&mut self, heartbeats: (i32, i32)) -> &mut ClientBuilder {
        self.heartbeats = Some(heartbeats);
        return self;
    }

    pub fn with_credentials(&mut self, credentials: (String, String)) -> &mut ClientBuilder {
        self.credentials = Some(credentials);
        return self;
    }

    pub fn with_custom_headers(&mut self, custom_headers: Headers) -> &mut ClientBuilder {
        self.custom_headers = Some(custom_headers);
        return self;
    }

    pub fn connect(&self) -> io::Result<Client> {
        return Client::connect(&self);
    }
}







// struct Msg {
//     msg: char,
// }

// trait MsgHandler {
//     fn onMessage(&self, msg : Msg);
// }

// impl <F> MsgHandler for F where F: Fn(Msg) {
//     fn onMessage(&self, msg : Msg) {
//         self(msg);
//     }
// }

// struct MySub {
//     // callback: &'a Fn(i32)
//     callback: Box<MsgHandler+Send+Sync>
// }

// struct MyStruct {
//     v: Vec<MySub>
// }

// fn doSomething() {
//     // // create my struct
//     let mut foo = MyStruct { v: Vec::new() };

//     let msg_handler = |msg: Msg| {

//     };

//     let sub = MySub { callback: Box::new(msg_handler) };

//     foo.v.push(sub);


//     // // create arc so we can share between threads
//     let foo_arc : Arc<Mutex<MyStruct>> = Arc::new(Mutex::new(foo));
//     let foo_clone = foo_arc.clone();



//     // let v : Vec<Arc<MsgHandler>> = Vec::new();
//     // let h : HashMap<String, MsgHandler> 

//     let t = thread::spawn(move || {
//         let foo_2 = foo_clone.lock().unwrap();
//     });
// }







///////////////
// Unit tests
///////////////
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}
