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
#![feature(unboxed_closures)]

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
    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, Schema};
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
    async fn serde_tagged_trait() -> Result<(), Error> {
        #[typetag::serde(tag = "plan")]
        trait ExecutionPlan: Debug {
            fn as_any(&self) -> &dyn Any;
            fn execute(&self) -> &str;
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct FilterExec {
            value: usize,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct ProjectionExec {
            input: Vec<Arc<dyn ExecutionPlan>>,
        }

        #[typetag::serde(name = "filter")]
        impl ExecutionPlan for FilterExec {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn execute(&self) -> &str {
                "filter op"
            }
        }

        #[typetag::serde(name = "projection")]
        impl ExecutionPlan for ProjectionExec {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn execute(&self) -> &str {
                "projection op"
            }
        }

        // Deserializaton
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
            "input": [
              {
                "plan": "filter",
                "value": 10
              },
              {
                "plan": "filter",
                "value": 11
              },
              {
                "plan": "projection",
                "input": [
                  {
                    "plan": "filter",
                    "value": 12
                  }
                ]
              }
            ]
        }
        "#;
        let f: Box<dyn ExecutionPlan> = serde_json::from_str(f).unwrap();
        assert_eq!(r#"projection op"#, f.execute());

        if let Some(project) = f.as_any().downcast_ref::<ProjectionExec>() {
            assert_eq!(r#"filter op"#, project.input[0].execute());
            if let Some(input) = project.input[0].as_any().downcast_ref::<FilterExec>() {
                assert_eq!(10, input.value);
            }
            assert_eq!(r#"filter op"#, project.input[1].execute());
            if let Some(input) = project.input[1].as_any().downcast_ref::<FilterExec>() {
                assert_eq!(11, input.value);
            }
            assert_eq!(r#"projection op"#, project.input[2].execute());
            if let Some(project) = project.input[2].as_any().downcast_ref::<ProjectionExec>() {
                assert_eq!(r#"filter op"#, project.input[0].execute());
                if let Some(input) = project.input[0].as_any().downcast_ref::<FilterExec>() {
                    assert_eq!(12, input.value);
                };
            }
        }

        // Serialization
        let plan1: Arc<dyn ExecutionPlan> = Arc::new(FilterExec { value: 10 });
        let plan2: Arc<dyn ExecutionPlan> = Arc::new(FilterExec { value: 11 });
        let plan3: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec {
            input: vec![Arc::new(FilterExec { value: 12 })],
        });

        let f: Box<dyn ExecutionPlan> = Box::new(ProjectionExec {
            input: vec![plan1, plan2, plan3],
        });
        assert_eq!(
            r#"{"plan":"projection","input":[{"plan":"filter","value":10},{"plan":"filter","value":11},{"plan":"projection","input":[{"plan":"filter","value":12}]}]}"#,
            serde_json::to_string(&f).unwrap()
        );

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

    // This example shows how to serialize and deserialize closure functions.
    //
    // This technology serializes the closure function based on the binary,
    // to ensure that the deserialization can find the corresponding function.
    // However, the AWS Lambda functions generated by the query engine have
    // different physical plans embedded in them, making their binaries completely
    // different.
    // FIXME: Therefore, the implementation is extremely risky.
    #[tokio::test]
    async fn simple_serde_closure() -> Result<(), Error> {
        use serde_closure::Fn;
        use serde_traitobject as st;

        #[derive(Serialize, Deserialize)]
        struct Abc {
            a: String,
            b: usize,
            c: Arc<dyn st::Fn(usize) -> usize>,
        }

        let my_struct = Abc {
            a: String::from("abc"),
            b: 10,
            c: Arc::new(Fn!(|x: usize| x + 10)),
        };

        let erased: st::Box<dyn st::Any> = st::Box::new(my_struct);

        let serialized = serde_json::to_string(&erased).unwrap();
        let deserialized: st::Box<dyn st::Any> = serde_json::from_str(&serialized).unwrap();
        let downcast: Box<Abc> = Box::<dyn Any>::downcast(deserialized.into_any()).unwrap();

        assert_eq!(r#"abc"#, downcast.a);
        assert_eq!(10, downcast.b);
        assert_eq!(20, (downcast.c)(downcast.b));

        Ok(())
    }

    #[tokio::test]
    async fn serde_closure() -> Result<(), Error> {
        use serde_closure::Fn;
        use serde_traitobject as st;

        #[typetag::serde(tag = "plan")]
        trait ExecutionPlan: Debug {
            fn as_any(&self) -> &dyn Any;
            fn execute(&self) -> &str;
        }

        #[derive(Serialize, Deserialize)]
        struct FilterExec {
            value: usize,
            minus: Arc<dyn st::Fn(usize) -> usize>,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct ProjectionExec {
            input: Vec<Arc<dyn ExecutionPlan>>,
        }

        impl fmt::Debug for FilterExec {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct("FilterExec")
                    .field("value", &self.value)
                    .field("minus", &"Fn { ... }")
                    .finish()
            }
        }

        #[typetag::serde(name = "filter")]
        impl ExecutionPlan for FilterExec {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn execute(&self) -> &str {
                "filter op"
            }
        }

        #[typetag::serde(name = "projection")]
        impl ExecutionPlan for ProjectionExec {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn execute(&self) -> &str {
                "projection op"
            }
        }

        // Serialization
        let plan1: Arc<dyn ExecutionPlan> = Arc::new(FilterExec {
            value: 10,
            minus: Arc::new(Fn!(|x: usize| x - 1)),
        });
        let plan2: Arc<dyn ExecutionPlan> = Arc::new(FilterExec {
            value: 11,
            minus: Arc::new(Fn!(|x: usize| x - 3)),
        });
        let plan3: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec {
            input: vec![Arc::new(FilterExec {
                value: 12,
                minus: Arc::new(Fn!(|x: usize| x - 6)),
            })],
        });

        let plan: Box<dyn ExecutionPlan> = Box::new(ProjectionExec {
            input: vec![plan1, plan2, plan3],
        });

        let serialized = serde_json::to_string_pretty(&plan).unwrap();

        // Deserializaton
        let deserialized: Box<dyn ExecutionPlan> = serde_json::from_str(&serialized).unwrap();
        assert_eq!(r#"projection op"#, deserialized.execute());

        if let Some(project) = deserialized.as_any().downcast_ref::<ProjectionExec>() {
            assert_eq!(r#"filter op"#, project.input[0].execute());
            if let Some(input) = project.input[0].as_any().downcast_ref::<FilterExec>() {
                assert_eq!(10, input.value);
                assert_eq!(9, (input.minus)(input.value));
            }
            assert_eq!(r#"filter op"#, project.input[1].execute());
            if let Some(input) = project.input[1].as_any().downcast_ref::<FilterExec>() {
                assert_eq!(11, input.value);
                assert_eq!(8, (input.minus)(input.value));
            }
            assert_eq!(r#"projection op"#, project.input[2].execute());
            if let Some(project) = project.input[2].as_any().downcast_ref::<ProjectionExec>() {
                assert_eq!(r#"filter op"#, project.input[0].execute());
                if let Some(input) = project.input[0].as_any().downcast_ref::<FilterExec>() {
                    assert_eq!(12, input.value);
                    assert_eq!(6, (input.minus)(input.value));
                };
            }
        }

        Ok(())
    }

    // This example uses a serializable structure (enum) to replace the closure
    // function in the struct, and after successful deserialization, the target
    // closure function is found through the mapping relationship.
    #[tokio::test]
    async fn serde_closure_mapping() -> Result<(), Error> {
        type ScalarFunc = dyn Fn(usize) -> Result<usize, Error>;
        fn dec(a: usize) -> Result<usize, Error> {
            Ok(a - 1)
        }

        #[derive(Serialize, Deserialize)]
        enum MathExpression {
            Dec,
        }

        impl MathExpression {
            fn as_ref(&self) -> Box<Arc<ScalarFunc>> {
                match self {
                    MathExpression::Dec => Box::new(Arc::new(dec)),
                }
            }
        }

        macro_rules! math_unary_function {
            ($FUNC:ident, $EXPR:expr) => {
                $FUNC.func.as_ref()($EXPR)?
            };
        }

        #[derive(Serialize, Deserialize)]
        struct Abc {
            name: String,
            // use enum to replace ScalarFunc
            func: MathExpression,
        }

        let abc = Abc {
            name: String::from("dec_one"),
            func: MathExpression::Dec,
        };
        let cba = serde_json::to_string_pretty(&abc).unwrap();
        let abc: Abc = serde_json::from_str(&cba).unwrap();
        assert_eq!("dec_one", abc.name);
        assert_eq!(0, abc.func.as_ref()(1)?);
        assert_eq!(0, math_unary_function!(abc, 1));
        assert_eq!(5, math_unary_function!(abc, 6));

        Ok(())
    }

    #[tokio::test]
    async fn fetch_record_batch() -> Result<(), Error> {
        use arrow::json;
        use arrow::json::reader::infer_json_schema;
        use arrow::record_batch::RecordBatch;
        use std::io::BufReader;

        let data = r#"
        {"a":1, "b":2.0, "c":false, "d":"4"}
        {"a":-10, "b":-3.5, "c":true, "d":"4"}
        {"a":2, "b":0.6, "c":false, "d":"text"}
        {"a":1, "b":2.0, "c":false, "d":"4"}
        {"a":7, "b":-3.5, "c":true, "d":"4"}
        {"a":1, "b":0.6, "c":false, "d":"text"}
        {"a":1, "b":2.0, "c":false, "d":"4"}
        {"a":5, "b":-3.5, "c":true, "d":"4"}
        {"a":1, "b":0.6, "c":false, "d":"text"}
        {"a":1, "b":2.0, "c":false, "d":"4"}
        {"a":1, "b":-3.5, "c":true, "d":"4"}
        {"a":100000000000000, "b":0.6, "c":false, "d":"text"}
        "#;
        let mut reader = BufReader::new(data.as_bytes());
        let inferred_schema = infer_json_schema(&mut reader, Some(2)).unwrap();
        let mut json = json::Reader::new(reader, inferred_schema, 12, None);

        let batch: RecordBatch = json.next().unwrap().unwrap();
        assert_eq!(
            r#"a: Int64, b: Float64, c: Boolean, d: Utf8"#,
            format!("{}", batch.schema())
        );

        Ok(())
    }

    // This test shows an example of conversion between FlightData and RecordBatch
    #[tokio::test]
    async fn recordbatch_flightdata_conversion() -> Result<(), Error> {
        use arrow::ipc::writer::IpcWriteOptions;
        use arrow::record_batch::RecordBatch;
        use arrow_flight::utils::{flight_data_from_arrow_batch, flight_data_to_arrow_batch};

        #[derive(Deserialize, Serialize)]
        // Partial FlightData defination
        pub struct FlightDataRef {
            #[serde(with = "serde_bytes")]
            pub data_header: std::vec::Vec<u8>,
            #[serde(with = "serde_bytes")]
            pub data_body:   std::vec::Vec<u8>,
        }

        // lambda 1
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int64, false),
            Field::new("c2", DataType::Float64, false),
            Field::new("c3", DataType::Utf8, false),
        ]));
        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![90, 100, 91, 101, 92, 102, 93, 103])),
                Arc::new(Float64Array::from(vec![
                    92.1, 93.2, 95.3, 96.4, 98.5, 99.6, 100.7, 101.8,
                ])),
                Arc::new(StringArray::from(vec![
                    "a", "a", "a", "b", "b", "b", "c", "c",
                ])),
            ],
        )?;

        let options = IpcWriteOptions::default();
        let flight_data = &flight_data_from_arrow_batch(&record_batch, &options)[0];

        let flight_data_ref = FlightDataRef {
            data_header: flight_data.data_header.clone(),
            data_body:   flight_data.data_body.clone(),
        };
        let json = serde_json::to_string(&flight_data_ref).unwrap();
        assert_eq!(
            r#"{"data_header":[16,0,0,0,12,0,26,0,24,0,23,0,4,0,8,0,12,0,0,0,32,0,0,0,200,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,3,0,10,0,24,0,12,0,8,0,4,0,10,0,0,0,76,0,0,0,16,0,0,0,8,0,0,0,0,0,0,0,0,0,0,0,3,0,0,0,8,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,8,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,8,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,7,0,0,0,0,0,0,0,0,0,0,0,8,0,0,0,0,0,0,0,8,0,0,0,0,0,0,0,64,0,0,0,0,0,0,0,72,0,0,0,0,0,0,0,8,0,0,0,0,0,0,0,80,0,0,0,0,0,0,0,64,0,0,0,0,0,0,0,144,0,0,0,0,0,0,0,8,0,0,0,0,0,0,0,152,0,0,0,0,0,0,0,40,0,0,0,0,0,0,0,192,0,0,0,0,0,0,0,8,0,0,0,0,0,0,0],"data_body":[255,0,0,0,0,0,0,0,90,0,0,0,0,0,0,0,100,0,0,0,0,0,0,0,91,0,0,0,0,0,0,0,101,0,0,0,0,0,0,0,92,0,0,0,0,0,0,0,102,0,0,0,0,0,0,0,93,0,0,0,0,0,0,0,103,0,0,0,0,0,0,0,255,0,0,0,0,0,0,0,102,102,102,102,102,6,87,64,205,204,204,204,204,76,87,64,51,51,51,51,51,211,87,64,154,153,153,153,153,25,88,64,0,0,0,0,0,160,88,64,102,102,102,102,102,230,88,64,205,204,204,204,204,44,89,64,51,51,51,51,51,115,89,64,255,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,2,0,0,0,3,0,0,0,4,0,0,0,5,0,0,0,6,0,0,0,7,0,0,0,8,0,0,0,0,0,0,0,97,97,97,98,98,98,99,99]}"#,
            json
        );

        // lambda 2
        let fake_flight_data: FlightDataRef = serde_json::from_str(&json).unwrap();
        let flight_data = arrow_flight::FlightData {
            flight_descriptor: None,
            app_metadata:      vec![],
            data_header:       fake_flight_data.data_header,
            data_body:         fake_flight_data.data_body,
        };

        let arrow_batch = flight_data_to_arrow_batch(&flight_data, schema).unwrap()?;
        assert_eq!(8, arrow_batch.num_rows());
        assert_eq!(3, arrow_batch.num_columns());
        assert_eq!(
            r#"c1: Int64, c2: Float64, c3: Utf8"#,
            format!("{}", arrow_batch.schema())
        );
        assert_eq!(
            "PrimitiveArray<Int64>\n[\n  90,\n  100,\n  91,\n  101,\n  92,\n  102,\n  93,\n  103,\n]",
            format!("{:?}", arrow_batch.column(0)));
        assert_eq!(
            "PrimitiveArray<Float64>\n[\n  92.1,\n  93.2,\n  95.3,\n  96.4,\n  98.5,\n  99.6,\n  100.7,\n  101.8,\n]",
            format!("{:?}", arrow_batch.column(1)));
        assert_eq!(
            "StringArray\n[\n  \"a\",\n  \"a\",\n  \"a\",\n  \"b\",\n  \"b\",\n  \"b\",\n  \"c\",\n  \"c\",\n]",
            format!("{:?}", arrow_batch.column(2)));

        Ok(())
    }
}
