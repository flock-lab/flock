use lambda::{handler_fn, Context};
use serde_json::{json, Value};

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

async fn handler(event: Value, _: Context) -> Result<Value, Error> {
    let message = event["input"].as_str().unwrap();
    let event =
        json!({ "input": format!("{}. {}", message, "Execution Succeeded. Congratulations!") });
    Ok(event)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    lambda::run(handler_fn(handler)).await?;
    Ok(())
}
