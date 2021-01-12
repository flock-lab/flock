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

package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

/**
 * A basic Kinesis Data Analytics for Java application with Kinesis data streams as source and sink.
 */
public class BasicStreamingJoinJob {
  private static final String region = "us-east-1";
  private static final String inputStream1Name = "stream1";
  private static final String inputStream2Name = "stream2";
  private static final String outputStreamName = "joinResults";
  private static final String s3SinkPath = "s3a://gangliao-meme/data";
  private static final long windowSize = 500L;
  private static final long rate = 3L;
  private static final long delay = 50L;

  private static DataStream<String> createSourceFromStaticConfig(
      StreamExecutionEnvironment env, String streamName) {
    Properties inputProperties = new Properties();
    inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
    inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

    // Leverage JsonNodeDeserializationSchema to convert incoming JSON to generic ObjectNode
    return env.addSource(
        new FlinkKinesisConsumer<>(streamName, new SimpleStringSchema(), inputProperties));
  }

  private static DataStream<String> createSourceFromApplicationProperties(
      StreamExecutionEnvironment env, String streamName) throws IOException {
    Map<String, Properties> applicationProperties =
        KinesisAnalyticsRuntime.getApplicationProperties();
    return env.addSource(
        new FlinkKinesisConsumer<>(
            streamName,
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

  private static StreamingFileSink<String> createS3SinkFromStaticConfig() {

    final StreamingFileSink<String> sink =
        StreamingFileSink.forRowFormat(
                new Path(s3SinkPath), new SimpleStringEncoder<String>("UTF-8"))
            .build();
    return sink;
  }

  public static void main(String[] args) throws Exception {
    // set up the streaming execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    /* if you would like to use runtime configuration properties, uncomment the lines below
     * DataStream<String> input = createSourceFromApplicationProperties(env);
     */
    // DataStream<String> stream1 = createSourceFromStaticConfig(env, inputStream1Name);
    // DataStream<String> stream2 = createSourceFromStaticConfig(env, inputStream2Name);
    // ObjectMapper jsonParser = new ObjectMapper();

    // IterativeStream<String> iter1 = stream1.iterate();
    // DataStream<Tuple4<Integer, Integer, Integer, Integer>> input1 =
    //     iter1.map(
    //         new MapFunction<String, Tuple4<Integer, Integer, Integer, Integer>>() {
    //           @Override
    //           public Tuple4<Integer, Integer, Integer, Integer> map(String value) throws
    // Exception {
    //             JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
    //             return new Tuple4<>(
    //                 jsonNode.get("attr_1").asInt(),
    //                 jsonNode.get("attr_2").asInt(),
    //                 jsonNode.get("attr_3").asInt(),
    //                 jsonNode.get("attr_4").asInt());
    //           }
    //         });

    // IterativeStream<String> iter2 = stream2.iterate();
    // DataStream<Tuple4<Integer, Integer, Integer, Integer>> input2 =
    //     iter2.map(
    //         new MapFunction<String, Tuple4<Integer, Integer, Integer, Integer>>() {
    //           @Override
    //           public Tuple4<Integer, Integer, Integer, Integer> map(String value) throws
    // Exception {
    //             JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
    //             return new Tuple4<>(
    //                 jsonNode.get("attr_1").asInt(),
    //                 jsonNode.get("attr_5").asInt(),
    //                 jsonNode.get("attr_6").asInt(),
    //                 jsonNode.get("attr_7").asInt());
    //           }
    //         });

    DataStream<Tuple5<Integer, Integer, Integer, Integer, Long>> stream1 =
        env.addSource(new StreamDataSource1()).name("Stream1");
    DataStream<Tuple5<Integer, Integer, Integer, Integer, Long>> stream2 =
        env.addSource(new StreamDataSource2()).name("Stream2");

    DataStream<Tuple5<Integer, Integer, Integer, Integer, Long>> input1 =
        stream1.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor<
                Tuple5<Integer, Integer, Integer, Integer, Long>>(Time.milliseconds(delay)) {
              @Override
              public long extractTimestamp(
                  Tuple5<Integer, Integer, Integer, Integer, Long> element) {
                return element.f4;
              }
            });

    DataStream<Tuple5<Integer, Integer, Integer, Integer, Long>> input2 =
        stream2.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor<
                Tuple5<Integer, Integer, Integer, Integer, Long>>(Time.milliseconds(delay)) {
              @Override
              public long extractTimestamp(
                  Tuple5<Integer, Integer, Integer, Integer, Long> element) {
                return element.f4;
              }
            });

    /* if you would like to use runtime configuration properties, uncomment the lines below
     * input.addSink(createSinkFromApplicationProperties())
     */

    // run the actual window join program
    DataStream<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> joinedStream =
        runWindowJoin(input1, input2, windowSize);

    // print the results with a single thread, rather than in parallel
    // joinedStream.print().setParallelism(1);

    DataStream<String> output =
        joinedStream.map(
            new MapFunction<
                Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>, String>() {
              @Override
              public String map(
                  Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> value)
                  throws Exception {
                return value.toString();
              }
            });

    output.addSink(createSinkFromStaticConfig());

    // execute program
    env.execute("Windowed Join Example");
  }

  public static DataStream<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>>
      runWindowJoin(
          DataStream<Tuple5<Integer, Integer, Integer, Integer, Long>> stream1,
          DataStream<Tuple5<Integer, Integer, Integer, Integer, Long>> stream2,
          long windowSize) {

    return stream1
        .join(stream2)
        .where(new NameKeySelector())
        .equalTo(new NameKeySelector())
        .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
        .apply(
            new JoinFunction<
                Tuple5<Integer, Integer, Integer, Integer, Long>,
                Tuple5<Integer, Integer, Integer, Integer, Long>,
                Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {

              @Override
              public Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> join(
                  Tuple5<Integer, Integer, Integer, Integer, Long> first,
                  Tuple5<Integer, Integer, Integer, Integer, Long> second) {
                return new Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>(
                    first.f0, first.f1, first.f2, first.f3, second.f1, second.f2, second.f3);
              }
            });
  }

  private static class NameKeySelector
      implements KeySelector<Tuple5<Integer, Integer, Integer, Integer, Long>, Integer> {
    @Override
    public Integer getKey(Tuple5<Integer, Integer, Integer, Integer, Long> value) {
      return value.f0;
    }
  }

  /**
   * This {@link WatermarkStrategy} assigns the current system time as the event-time timestamp. In
   * a real use case you should use proper timestamps and an appropriate {@link WatermarkStrategy}.
   */
  // private static class IngestionTimeWatermarkStrategy<T> implements WatermarkStrategy<T> {

  //   private IngestionTimeWatermarkStrategy() {}

  //   public static <T> IngestionTimeWatermarkStrategy<T> create() {
  //     return new IngestionTimeWatermarkStrategy<>();
  //   }

  //   @Override
  //   public WatermarkGenerator<T> createWatermarkGenerator(
  //       WatermarkGeneratorSupplier.Context context) {
  //     return new AscendingTimestampsWatermarks<>();
  //   }

  //   @Override
  //   public TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context
  // context) {
  //     return (event, timestamp) -> System.currentTimeMillis();
  //   }
  // }
}
