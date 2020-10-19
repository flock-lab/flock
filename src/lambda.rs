#[macro_use]
extern crate lambda_runtime as lambda;
#[macro_use]
extern crate log;
extern crate simple_logger;
#[cfg(feature = "serde_derive")]
#[allow(unused_imports)]
#[macro_use]
extern crate serde;

use lambda::error::HandlerError;
use std::error::Error;

use log::LevelFilter;
use simple_logger::SimpleLogger;

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Clone)]
struct CustomEvent {
    #[serde(rename = "firstName")]
    first_name: String,
}

#[derive(Serialize, Clone)]
struct CustomOutput {
    message: String,
}

fn my_handler(e: CustomEvent, c: lambda::Context) -> Result<CustomOutput, HandlerError> {
    if e.first_name == "" {
        error!("Empty first name in request {}", c.aws_request_id);
        return Err(HandlerError::from("Empty first name"));
    }

    Ok(CustomOutput {
        message: format!("Hello, {}!", e.first_name),
    })
}

fn main() -> Result<(), Box<dyn Error>> {
    SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .init()
        .unwrap();
    lambda!(my_handler);

    Ok(())
}
