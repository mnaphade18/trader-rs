#[macro_use] extern crate rocket;

use std::time::Duration;

use futures::StreamExt;
use rocket::{response::stream::{EventStream, Event}, Shutdown};
use services::pump;

anchor_lang::declare_program!(pump_amm);

mod services;

#[get("/")]
fn index() -> &'static str {
    "Hello, world!"
}

#[get("/<token>")]
async fn stream_token(mut close: Shutdown, token: &str) -> EventStream![Event + '_] {
    println!("Resgistering token: {}", token);
    let resp = EventStream!{
        let mut rx = Box::pin(pump::stream_token(token).await);
        loop {
            tokio::select! {
                _ = &mut close => {
                    println!("Client stopped");
                    break; 
                }
                Some(m) = rx.next() => {
                    match serde_json::to_string(&m) {
                        Ok(s) => {
                            let r = yield Event::data(s);
                            println!("Message stream write status, {:?}", r);
                        }
                        Err(e) => {
                            println!("Failed to encode account message: {:?}, {:?}", m, e);
                        }
                    }

                }
            }
        }
        println!("Client stopped");
    };

    resp.heartbeat(Some(Duration::from_secs(5)))
}

#[launch]
fn rocket() -> _ {
    rocket::build().mount("/", routes![index, stream_token])
}
