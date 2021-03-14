// Copyright (c) 2020-2021 Gang Liao. All rights reserved.
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

package com.amazonaws.kda.flink.benchmarking.util;

import com.amazonaws.kda.flink.benchmarking.model.Event;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.stream.Collectors;

public class KDSProducerUtil {

  /**
   * This method creates sample InteractionIds
   *
   * @param numInteractions
   * @return
   */
  public static List<String> createInteractions(int numInteractions) {
    List<String> interactionList = new ArrayList<String>();
    for (int i = 0; i < numInteractions; i++) {
      String interactionId = UUID.randomUUID().toString();
      // System.out.printf("Interaction_id: %s \n", interactionId);
      interactionList.add(interactionId);
    }
    return interactionList;
  }

  public static Iterator<String> createRotatableInteractions(int numInteractions) {
    List<String> interactionList = new ArrayList<String>();
    for (int i = 0; i < numInteractions; i++) {
      String interactionId = UUID.randomUUID().toString();
      System.out.printf("Interaction_id: %s \n", interactionId);
      interactionList.add(interactionId);
    }
    return Iterables.cycle(interactionList).iterator();
  }

  /**
   * This method create a list of sample events
   *
   * @param eventList
   * @param numEvents
   * @param interactionId
   * @return
   */
  public static List<String> createEvents(List<String> eventList, int numEvents, String sessionId) {
    // clear the eventList and build it up again!
    eventList.clear();
    long createTime = System.currentTimeMillis();

    for (int i = 1; i < numEvents + 1; i++) {
      Event event = new Event();
      event.setAttr_1("my_attr_1_" + i);
      event.setAttr_2("my_attr_2_" + i);
      event.setAttr_3("my_attr_3_" + i);
      event.setAttr_4("my_attr_4_" + i);
      event.setAttr_5("my_attr_5_" + i);
      event.setAttr_6(sessionId);
      event.setAttr_7(createTime);
      event.setSession_id(sessionId);
      event.setTimestamp(createTime);
      eventList.add(new Gson().toJson(event));
    }
    return eventList;
  }

  /**
   * This method demonstrates writing a single messages to Kinesis Data Stream using PutRecord API.
   *
   * <p>Partition key is needed and it can be an empty string. When both Partition Key and explicit
   * Hash Key are set, explicit Hash Key takes precedence. Calling hashKeyIterator.next() provides a
   * Hash Key belongs to a shard.
   *
   * <p>Retry logic: PutRecord throws ProvisionedThroughputExceededException when a stream is
   * throttled. The retry logic used here handles the exception and re-writes the failed record.
   *
   * @param record
   * @param streamName
   * @param kinesis
   */
  public static void writeSingleMessageToKinesis(
      String record, String streamName, AmazonKinesis kinesis, String startingHashKey) {
    PutRecordRequest putRecReq = new PutRecordRequest();
    try {
      putRecReq.setStreamName(streamName);
      putRecReq.setData(ByteBuffer.wrap(record.getBytes()));
      putRecReq.setExplicitHashKey(startingHashKey);
      putRecReq.setPartitionKey("reqiredButHasNoEffect-when-setExplicitHashKey-isUsed");
      kinesis.putRecord(putRecReq);
    } catch (ProvisionedThroughputExceededException exception) {
      try {
        System.out.println("ERROR: Throughput Exception Thrown.");
        exception.printStackTrace();
        System.out.println("Retrying after a short delay.");
        Thread.sleep(100);
        kinesis.putRecord(putRecReq);
      } catch (ProvisionedThroughputExceededException e) {
        e.printStackTrace();
        System.out.println(
            "Kinesis Put operation failed after re-try due to second consecutive "
                + "ProvisionedThroughputExceededException");
      } catch (Exception e) {
        e.printStackTrace();
        System.out.println("Exception thrown while writing a record to Kinesis.");
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Exception thrown while writing a record to Kinesis.");
    }
  }

  public static void writeMessagesToKinesis(
      AmazonKinesis kinesis,
      String streamName,
      List<String> recordList,
      Iterator<String> hashKeyIterator) {
    PutRecordsRequest putRecsReq = new PutRecordsRequest();
    List<PutRecordsRequestEntry> putRecReqEntryList = new ArrayList<PutRecordsRequestEntry>();
    PutRecordsResult putRecsRes = new PutRecordsResult();
    // Make sure you write messages in a batch of 500 messages
    List<List<String>> listofSmallerLists = Lists.partition(recordList, 500);
    for (List<String> smallerList : listofSmallerLists) {
      putRecReqEntryList.clear();
      for (String message : smallerList) {
        PutRecordsRequestEntry putRecsReqEntry = new PutRecordsRequestEntry();
        putRecsReqEntry.setData(ByteBuffer.wrap(message.getBytes()));
        putRecsReqEntry.setPartitionKey("reqiredButHasNoEffect-when-setExplicitHashKey-isUsed");
        putRecsReqEntry.setExplicitHashKey(hashKeyIterator.next());
        putRecReqEntryList.add(putRecsReqEntry);
      }
      try {
        putRecsReq.setStreamName(streamName);
        putRecsReq.setRecords(putRecReqEntryList);
        putRecsRes = kinesis.putRecords(putRecsReq);
        while (putRecsRes.getFailedRecordCount() > 0) {
          System.out.println("Processing rejected records");
          // TODO: For simplicity, the backoff implemented as a constant 100ms sleep
          // For production-grade, consider using CoralRetry's Exponential Jittered
          // Backoff retry strategy
          // Ref:
          // https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
          Thread.sleep(100);
          final List<PutRecordsRequestEntry> failedRecordsList =
              new ArrayList<PutRecordsRequestEntry>();
          final List<PutRecordsResultEntry> putRecsResEntryList = putRecsRes.getRecords();
          for (int i = 0; i < putRecsResEntryList.size(); i++) {
            final PutRecordsRequestEntry putRecordReqEntry = putRecReqEntryList.get(i);
            final PutRecordsResultEntry putRecordsResEntry = putRecsResEntryList.get(i);
            if (putRecordsResEntry.getErrorCode() != null) {
              failedRecordsList.add(putRecordReqEntry);
            }
          }
          putRecReqEntryList = failedRecordsList;
          putRecsReq.setRecords(putRecReqEntryList);
          putRecsRes = kinesis.putRecords(putRecsReq);
        } // end of while loop
        System.out.println("Number of messages written: " + smallerList.size());
      } catch (Exception e) {
        System.out.println("Exception in Kinesis Batch Insert: " + e.getMessage());
      }
    }
  }

  public static List<String> tokenizeStrings(String str, String separator) {
    List<String> tokenList =
        Collections.list(new StringTokenizer(str, separator)).stream()
            .map(token -> (String) token)
            .collect(Collectors.toList());
    return tokenList;
  }
}
