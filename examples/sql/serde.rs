// Copyright (c) 2021 UMD Database Group. All rights reserved.
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

use serde::de::{self, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use std::fmt;

struct A {
    other_struct: B,
}

struct B {
    value: usize,
}

impl Serialize for A {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 1 is the number of fields in the struct.
        let mut state = serializer.serialize_struct("A", 1)?;
        state.serialize_field("other_struct", &self.other_struct)?;
        state.end()
    }
}

impl Serialize for B {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 1 is the number of fields in the struct.
        let mut state = serializer.serialize_struct("B", 1)?;
        state.serialize_field("value", &self.value)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for A {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field {
            OtherStruct,
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("other_struct")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "other_struct" => Ok(Field::OtherStruct),
                            _ => Err(de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct AVisitor;

        impl<'de> Visitor<'de> for AVisitor {
            type Value = A;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct A")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<A, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let value = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                Ok(A {
                    other_struct: value,
                })
            }

            fn visit_map<V>(self, mut map: V) -> Result<A, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut value = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::OtherStruct => {
                            if value.is_some() {
                                return Err(de::Error::duplicate_field("other_struct"));
                            }
                            value = Some(map.next_value()?);
                        }
                    }
                }
                let value = value.ok_or_else(|| de::Error::missing_field("other_struct"))?;
                Ok(A {
                    other_struct: value,
                })
            }
        }

        const FIELDS: &[&str] = &["other_struct"];
        deserializer.deserialize_struct("A", FIELDS, AVisitor)
    }
}

impl<'de> Deserialize<'de> for B {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field {
            Value,
        }

        // This part could also be generated independently by:
        //
        //    #[derive(Deserialize)]
        //    #[serde(field_identifier, rename_all = "lowercase")]
        //    enum Field { Value }
        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("value")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "value" => Ok(Field::Value),
                            _ => Err(de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct BVisitor;

        impl<'de> Visitor<'de> for BVisitor {
            type Value = B;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct B")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<B, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let value = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                Ok(B { value })
            }

            fn visit_map<V>(self, mut map: V) -> Result<B, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut value = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Value => {
                            if value.is_some() {
                                return Err(de::Error::duplicate_field("value"));
                            }
                            value = Some(map.next_value()?);
                        }
                    }
                }
                let value = value.ok_or_else(|| de::Error::missing_field("value"))?;
                Ok(B { value })
            }
        }

        const FIELDS: &[&str] = &["value"];
        deserializer.deserialize_struct("B", FIELDS, BVisitor)
    }
}

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

#[tokio::main]
async fn main() -> Result<(), Error> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;
    use std::fmt::Debug;
    use std::sync::Arc;

    #[tokio::test]
    async fn simple_nested_struct() -> Result<(), Error> {
        #[derive(Deserialize, Serialize)]
        struct C {
            #[serde(with = "serde_with::json::nested")]
            other_struct: D,
        }

        #[derive(Deserialize, Serialize)]
        struct D {
            value: usize,
        }

        let v: C = serde_json::from_str(r#"{"other_struct":"{\"value\":5}"}"#).unwrap();
        assert_eq!(5, v.other_struct.value);

        let x = C {
            other_struct: D { value: 10 },
        };
        assert_eq!(
            r#"{"other_struct":"{\"value\":10}"}"#,
            serde_json::to_string(&x).unwrap()
        );
        Ok(())
    }

    #[tokio::test]
    async fn simple_trait() -> Result<(), Error> {
        trait Event {
            fn as_any(&self) -> &dyn Any;
        }

        impl<'a> Serialize for dyn Event + 'a {
            fn serialize<S>(&self, _: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                unimplemented!()
            }
        }

        impl<'de> Deserialize<'de> for Box<dyn Event> {
            fn deserialize<D>(_: D) -> std::result::Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                unimplemented!()
            }
        }

        #[derive(Deserialize, Serialize)]
        struct C {
            #[serde(with = "serde_with::json::nested")]
            other_struct: D,
        }

        impl Event for C {
            fn as_any(&self) -> &dyn Any {
                self
            }
        }

        #[derive(Deserialize, Serialize)]
        struct D {
            value: usize,
        }

        impl Event for D {
            fn as_any(&self) -> &dyn Any {
                self
            }
        }

        let x: Box<dyn Event> = Box::new(C {
            other_struct: D { value: 10 },
        });

        if let Some(c) = x.as_any().downcast_ref::<C>() {
            assert_eq!(
                r#"{"other_struct":"{\"value\":10}"}"#,
                serde_json::to_string(&c).unwrap()
            );
        } else if let Some(d) = x.as_any().downcast_ref::<D>() {
            assert_eq!(
                r#"{"other_struct":"{\"value\":10}"}"#,
                serde_json::to_string(&d).unwrap()
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn derialize_tagged_trait() -> Result<(), Error> {
        #[typetag::deserialize(tag = "plan")]
        trait ExecutionPlan: Debug {
            fn as_any(&self) -> &dyn Any;
            fn execute(&self) -> &str;
        }

        #[derive(Debug, Deserialize)]
        struct FilterExec {
            value: usize,
        }

        #[derive(Debug, Deserialize)]
        struct ProjectionExec {
            input: Arc<dyn ExecutionPlan>,
        }

        #[typetag::deserialize(name = "filter")]
        impl ExecutionPlan for FilterExec {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn execute(&self) -> &str {
                "filter op"
            }
        }

        #[typetag::deserialize(name = "projection")]
        impl ExecutionPlan for ProjectionExec {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn execute(&self) -> &str {
                "projection op"
            }
        }

        let f = r#"
        {
          "plan": "filter",
          "value": 10
        }
        "#;
        let f: Box<dyn ExecutionPlan> = serde_json::from_str(f).unwrap();
        assert_eq!(r#"filter op"#, f.execute());

        let f = r#"
        {
          "plan": "projection",
          "input":
          {
            "plan": "filter",
            "value": 10
          }
        }
        "#;
        let f: Box<dyn ExecutionPlan> = serde_json::from_str(f).unwrap();
        assert_eq!(r#"projection op"#, f.execute());

        if let Some(project) = f.as_any().downcast_ref::<ProjectionExec>() {
            assert_eq!(r#"filter op"#, project.input.execute());
            if let Some(input) = project.input.as_any().downcast_ref::<FilterExec>() {
                assert_eq!(10, input.value);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn custom_serde_nested_struct() -> Result<(), Error> {
        let v: A = serde_json::from_str(r#"{"other_struct":{"value":5}}"#).unwrap();
        assert_eq!(5, v.other_struct.value);

        let x = A {
            other_struct: B { value: 10 },
        };
        let s_x = serde_json::to_string(&x).unwrap();
        assert_eq!(r#"{"other_struct":{"value":10}}"#, s_x);
        let d_x: A = serde_json::from_str(&s_x).unwrap();
        assert_eq!(10, d_x.other_struct.value);

        Ok(())
    }
}
