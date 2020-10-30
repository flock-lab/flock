// Copyright 2020 UMD Database Group. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

/**
 * A basic Kinesis Data Analytics for Java application with Kinesis data streams as source and sink.
 */
public class BasicStreamingJob {
  private static final String region = "us-east-1";
  private static final String inputStreamName = "ExampleInputStream";
  private static final String outputStreamName = "ExampleOutputStream";

  private static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env) {
    Properties inputProperties = new Properties();
    inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
    inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

    return env.addSource(
        new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties));
  }

  private static DataStream<String> createSourceFromApplicationProperties(
      StreamExecutionEnvironment env) throws IOException {
    Map<String, Properties> applicationProperties =
        KinesisAnalyticsRuntime.getApplicationProperties();
    return env.addSource(
        new FlinkKinesisConsumer<>(
            inputStreamName,
            new SimpleStringSchema(),
            applicationProperties.get("ConsumerConfigProperties")));
  }

  private static FlinkKinesisProducer<String> createSinkFromStaticConfig() {
    Properties outputProperties = new Properties();
    outputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
    outputProperties.setProperty("AggregationEnabled", "false");

    FlinkKinesisProducer<String> sink =
        new FlinkKinesisProducer<>(new SimpleStringSchema(), outputProperties);
    sink.setDefaultStream(outputStreamName);
    sink.setDefaultPartition("0");
    return sink;
  }

  private static FlinkKinesisProducer<String> createSinkFromApplicationProperties()
      throws IOException {
    Map<String, Properties> applicationProperties =
        KinesisAnalyticsRuntime.getApplicationProperties();
    FlinkKinesisProducer<String> sink =
        new FlinkKinesisProducer<>(
            new SimpleStringSchema(), applicationProperties.get("ProducerConfigProperties"));

    sink.setDefaultStream(outputStreamName);
    sink.setDefaultPartition("0");
    return sink;
  }

  public static void main(String[] args) throws Exception {
    // set up the streaming execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    /* if you would like to use runtime configuration properties, uncomment the lines below
     * DataStream<String> input = createSourceFromApplicationProperties(env);
     */
    DataStream<String> input = createSourceFromStaticConfig(env);

    /* if you would like to use runtime configuration properties, uncomment the lines below
     * input.addSink(createSinkFromApplicationProperties())
     */
    input.addSink(createSinkFromStaticConfig());

    env.execute("Flink Streaming Java API Skeleton");
  }
}
