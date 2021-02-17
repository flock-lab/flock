// Copyright (c) 2020-2021, UMD Database Group. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use aws_lambda_events::event::kinesis::KinesisEvent;
use lambda::{handler_fn, Context};
use std::str::from_utf8;

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    lambda::run(handler_fn(handler)).await?;
    Ok(())
}

async fn handler(event: KinesisEvent, _: Context) -> Result<(), Error> {
    for record in event.records {
        println!(
            "{}",
            from_utf8(&record.kinesis.data.0)
                .map(|s| s.to_owned())
                .unwrap_or_else(|err| format!("expected utf8 data: {}", err))
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn handler_handles() {
        let event: KinesisEvent = serde_json::from_slice(include_bytes!("./example-event.json"))
            .expect("invalid kinesis event");

        assert!(handler(event, Context::default()).await.is_ok())
    }
}
