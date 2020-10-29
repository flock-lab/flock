use lambda::{handler_fn, Context};
use serde_json::Value;

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

async fn handler(event: Value, _: Context) -> Result<Value, Error> {
    Ok(event)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    lambda::run(handler_fn(handler)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn handler_handles() {
        let event = json!({
            "answer": 42
        });

        assert_eq!(
            handler(event.clone(), Context::default())
                .await
                .expect("expected Ok(_) value"),
            event
        )
    }
}
