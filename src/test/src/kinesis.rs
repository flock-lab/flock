// Copyright 2020 UMD Database Group. All Rights Reserved.
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

use crate::DataRecord;
use base64::{decode, encode};
use chrono::{DateTime, TimeZone, Utc};
use fake::{Dummy, Fake, Faker};
use rand::Rng;
use serde::de::{Deserializer, Error as DeError};
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};

#[derive(Dummy, Debug, Clone, PartialEq, Deserialize, Serialize)]
pub(crate) struct KinesisEvent {
    #[serde(rename = "Records")]
    #[dummy(faker = "(Faker, 1000)")]
    pub records: Vec<KinesisEventRecord>,
}

#[derive(Dummy, Debug, Clone, PartialEq, Deserialize, Serialize)]
pub(crate) struct KinesisEventRecord {
    #[serde(deserialize_with = "deserialize_lambda_string")]
    #[serde(default)]
    #[serde(rename = "awsRegion")]
    pub aws_region:          Option<String>,
    #[serde(deserialize_with = "deserialize_lambda_string")]
    #[serde(default)]
    #[serde(rename = "eventID")]
    pub event_id:            Option<String>,
    #[serde(deserialize_with = "deserialize_lambda_string")]
    #[serde(default)]
    #[serde(rename = "eventName")]
    pub event_name:          Option<String>,
    #[serde(deserialize_with = "deserialize_lambda_string")]
    #[serde(default)]
    #[serde(rename = "eventSource")]
    pub event_source:        Option<String>,
    #[serde(deserialize_with = "deserialize_lambda_string")]
    #[serde(default)]
    #[serde(rename = "eventSourceARN")]
    pub event_source_arn:    Option<String>,
    #[serde(deserialize_with = "deserialize_lambda_string")]
    #[serde(default)]
    #[serde(rename = "eventVersion")]
    pub event_version:       Option<String>,
    #[serde(deserialize_with = "deserialize_lambda_string")]
    #[serde(default)]
    #[serde(rename = "invokeIdentityArn")]
    pub invoke_identity_arn: Option<String>,
    pub kinesis:             KinesisRecord,
}

#[derive(Dummy, Debug, Clone, PartialEq, Deserialize, Serialize)]
pub(crate) struct KinesisRecord {
    #[serde(rename = "approximateArrivalTimestamp")]
    pub approximate_arrival_timestamp: SecondTimestamp,
    pub data:                          Base64Data,
    #[serde(rename = "encryptionType")]
    pub encryption_type:               Option<String>,
    #[serde(deserialize_with = "deserialize_lambda_string")]
    #[serde(default)]
    #[serde(rename = "partitionKey")]
    pub partition_key:                 Option<String>,
    #[serde(deserialize_with = "deserialize_lambda_string")]
    #[serde(default)]
    #[serde(rename = "sequenceNumber")]
    pub sequence_number:               Option<String>,
    #[serde(deserialize_with = "deserialize_lambda_string")]
    #[serde(default)]
    #[serde(rename = "kinesisSchemaVersion")]
    pub kinesis_schema_version:        Option<String>,
}

/// Deserializes `Option<String>`, mapping JSON `null` or the empty string `""`
/// to `None`.
#[cfg(not(feature = "string-null-empty"))]
fn deserialize_lambda_string<'de, D>(
    deserializer: D,
) -> core::result::Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    match Option::deserialize(deserializer)? {
        Some(s) => {
            if s == "" {
                Ok(None)
            } else {
                Ok(Some(s))
            }
        }
        None => Ok(None),
    }
}

/// Binary data encoded in base64.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub(crate) struct Base64Data(
    #[serde(deserialize_with = "deserialize_base64")]
    #[serde(serialize_with = "serialize_base64")]
    pub Vec<u8>,
);

impl Dummy<Faker> for Base64Data {
    fn dummy_with_rng<R: Rng + ?Sized>(_: &Faker, _: &mut R) -> Self {
        let record: DataRecord = Faker.fake();
        Base64Data(serde_json::to_string(&record).unwrap().into_bytes())
    }
}

impl Deref for Base64Data {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Base64Data {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

fn deserialize_base64<'de, D>(deserializer: D) -> core::result::Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = String::deserialize(deserializer)?;
    decode(&s).map_err(DeError::custom)
}

fn serialize_base64<S>(value: &[u8], serializer: S) -> core::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&encode(value))
}

/// Timestamp with second precision.
#[derive(Dummy, Debug, Clone, PartialEq, Deserialize, Serialize)]
pub(crate) struct SecondTimestamp(
    #[serde(deserialize_with = "deserialize_seconds")]
    #[serde(serialize_with = "serialize_seconds")]
    pub DateTime<Utc>,
);

impl Deref for SecondTimestamp {
    type Target = DateTime<Utc>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for SecondTimestamp {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

fn serialize_seconds<S>(
    date: &DateTime<Utc>,
    serializer: S,
) -> core::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let seconds = date.timestamp().abs();
    let milliseconds = date.timestamp_subsec_millis();
    let combined = format!("{}.{}", seconds, milliseconds);
    serializer.serialize_f64(combined.parse().unwrap())
}

#[allow(dead_code)]
fn deserialize_seconds<'de, D>(deserializer: D) -> core::result::Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let (whole, frac) = normalize_timestamp(deserializer)?;
    let seconds = whole;
    let nanos = frac * 1_000_000;
    Ok(Utc.timestamp(seconds as i64, nanos as u32))
}

fn normalize_timestamp<'de, D>(deserializer: D) -> core::result::Result<(u64, u64), D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrNumber {
        String(String),
        Float(f64),
        Int(u64),
    }

    let input: f64 = match StringOrNumber::deserialize(deserializer)? {
        StringOrNumber::String(s) => s.parse::<f64>().map_err(DeError::custom)?,
        StringOrNumber::Float(f) => f,
        StringOrNumber::Int(i) => i as f64,
    };

    // We need to do this due to floating point issues.
    let input_as_string = format!("{}", input);
    let parts: core::result::Result<Vec<u64>, _> = input_as_string
        .split('.')
        .map(|x| x.parse::<u64>().map_err(DeError::custom))
        .collect();
    let parts = parts?;
    if parts.len() > 1 {
        Ok((parts[0], parts[1]))
    } else {
        Ok((parts[0], 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_batches_eq;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::json;
    use runtime::prelude::*;
    use std::io::BufReader;
    use std::sync::Arc;

    #[tokio::test]
    async fn fake_data() -> Result<()> {
        let event: KinesisEvent = Faker.fake();
        assert_eq!(event.records.len(), 1000);

        let event = KinesisEvent {
            records: fake::vec![KinesisEventRecord; 4],
        };
        assert_eq!(event.records.len(), 4);

        Ok(())
    }

    #[tokio::test]
    async fn arrow_json_reader() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
            Field::new("c", DataType::Boolean, false),
            Field::new("d", DataType::Utf8, false),
        ]);

        let records: &[u8] = include_str!("../data/basic.txt").as_bytes();
        let mut json = json::Reader::new(BufReader::new(records), Arc::new(schema), 1024, None);
        let batch = json.next().unwrap().unwrap();

        let expected = vec![
            "+-----------------+------+-------+------+",
            "| a               | b    | c     | d    |",
            "+-----------------+------+-------+------+",
            "| 1               | 2    | false | 4    |",
            "| -10             | -3.5 | true  | 4    |",
            "| 2               | 0.6  | false | text |",
            "| 1               | 2    | false | 4    |",
            "| 7               | -3.5 | true  | 4    |",
            "| 1               | 0.6  | false | text |",
            "| 1               | 2    | false | 4    |",
            "| 5               | -3.5 | true  | 4    |",
            "| 1               | 0.6  | false | text |",
            "| 1               | 2    | false | 4    |",
            "| 1               | -3.5 | true  | 4    |",
            "| 100000000000000 | 0.6  | false | text |",
            "+-----------------+------+-------+------+",
        ];

        assert_batches_eq!(&expected, &[batch]);

        Ok(())
    }
}
