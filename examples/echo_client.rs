extern crate stomp_client;
use std::time;
use stomp_client::stomp;

fn main() {
    print!("Hello world!");
    
    let host = "127.0.0.1:61618";
    let destination = "/topic/foo";    
    
    // create and connect a STOMP client
    // this will launch a new thread
    let mut client = match stomp::ClientBuilder::new(host).connect() {
        Ok(client) => client,
        Err(error) => panic!("Failed to connect to STOMP server{}", error)
    };

    // subscribe to topic and echo messages received back to topic
    client.subscribe(destination, move |frame: &stomp::Frame, context: &stomp::Client| {
        print!("Received message: {:?}", frame);

        // here we use provided context (Client) to echo msg back to destination
        context.publish(destination, &frame);   
    });

    // sit on main thread and do nothing
    loop {
        std::thread::sleep(time::Duration::from_millis(250));
    }
}