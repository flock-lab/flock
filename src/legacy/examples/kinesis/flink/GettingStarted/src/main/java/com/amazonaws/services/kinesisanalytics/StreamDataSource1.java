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

package com.amazonaws.services.kinesisanalytics;

import java.util.Random;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;

public class StreamDataSource1
    extends RichParallelSourceFunction<Tuple5<Integer, Integer, Integer, Integer, Long>> {
  private volatile boolean running = true;

  private int getRandomNumberUsingInts(int min, int max) {
    Random random = new Random();
    return random.ints(min, max).findFirst().getAsInt();
  }

  @Override
  public void run(SourceContext<Tuple5<Integer, Integer, Integer, Integer, Long>> ctx)
      throws InterruptedException {

    long t = 1000000050000L;

    for (int i = 0; i < 2500; ++i) {
      Tuple5 tuple =
          Tuple5.of(
              getRandomNumberUsingInts(90, 92),
              getRandomNumberUsingInts(90, 120),
              getRandomNumberUsingInts(90, 120),
              getRandomNumberUsingInts(90, 120),
              t);
      if (running) ctx.collect(tuple);
    }
  }

  @Override
  public void cancel() {
    running = false;
  }
}
