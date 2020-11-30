// Copyright (c) 2020 UMD Database Group. All rights reserved.
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

package com.amazonaws.kda.flink.benchmarking;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.kda.flink.benchmarking.util.DDBUtil;
import com.amazonaws.kda.flink.benchmarking.util.KDSProducerUtil;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.google.common.collect.Iterables;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.PersistJobDataAfterExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author Ravi Itha, Amazon Web Services, Inc. */
@PersistJobDataAfterExecution
public class KinesisProducerForFlinkSessionWindow implements Job {

  Logger logger = LoggerFactory.getLogger(KinesisProducerForFlinkSessionWindow.class);

  public void execute(JobExecutionContext context) throws JobExecutionException {

    String dynamoDBLocalURI = null;
    // Get job specific settings
    JobKey key = context.getJobDetail().getKey();
    JobDataMap dataMap = context.getJobDetail().getJobDataMap();
    // System.out.println("Job Key: " + key.getName());
    // System.out.println("\nFire Instance Id: " + context.getFireInstanceId());

    String jobId = dataMap.getString("jobId");
    String jobName = dataMap.getString("jobName");
    boolean isUsingDynamoDBLocal = dataMap.getBoolean("isUsingDynamoDBLocal");
    if (isUsingDynamoDBLocal) dynamoDBLocalURI = dataMap.getString("dynamoDBLocalURI");
    String childJobSummaryDDBTblName = dataMap.getString("childJobSummaryDDBTblName");
    String region = dataMap.getString("region");
    String interactionsIds = dataMap.getString("interactionsIds");
    String stringSeparator = dataMap.getString("stringSeparator");
    String targetKinesisStreams = dataMap.getString("targetKinesisStreams");
    String startingHashKeys = dataMap.getString("startingHashKeys");
    int batchSize = dataMap.getInt("batchSize");

    List<String> eventList = new ArrayList<String>();
    AmazonKinesis kinesis = AmazonKinesisClientBuilder.standard().withRegion(region).build();
    List<String> interactionList =
        KDSProducerUtil.tokenizeStrings(interactionsIds, stringSeparator);
    List<String> targetKinesisStreamsList =
        KDSProducerUtil.tokenizeStrings(targetKinesisStreams, stringSeparator);
    List<String> startingHashKeyList =
        KDSProducerUtil.tokenizeStrings(startingHashKeys, stringSeparator);
    Iterator<String> hashKeysIterator = Iterables.cycle(startingHashKeyList).iterator();

    AmazonDynamoDB dynamoDB = null;
    if (isUsingDynamoDBLocal)
      dynamoDB =
          AmazonDynamoDBClientBuilder.standard()
              .withEndpointConfiguration(
                  new AwsClientBuilder.EndpointConfiguration(dynamoDBLocalURI, region))
              .build();
    else dynamoDB = AmazonDynamoDBClientBuilder.standard().withRegion(region).build();

    DynamoDB dynamoDBClient = new DynamoDB(dynamoDB);

    for (String interactionId : interactionList) {
      eventList = KDSProducerUtil.createEvents(eventList, batchSize, interactionId);
      for (String targetStream : targetKinesisStreamsList) {
        KDSProducerUtil.writeMessagesToKinesis(kinesis, targetStream, eventList, hashKeysIterator);
        DDBUtil.insertChildJobDetailedStatus(
            dynamoDBClient,
            targetStream,
            jobId,
            context.getFireInstanceId(),
            targetStream,
            interactionId,
            batchSize,
            System.currentTimeMillis());
      }
    }

    // Check if this is the last Job execution. If yes, then prepare for next Hourly
    // Window.
    if (!Optional.ofNullable(context.getTrigger().getNextFireTime()).isPresent()) {
      System.out.printf(
          "The last instance of the job. Job Key: %s, Job Id: %s \n", key.getName(), jobId);
      DDBUtil.updateChildJobStatus(
          dynamoDBClient,
          childJobSummaryDDBTblName,
          jobName,
          jobId,
          LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
          "Completed");
    }
  }
}
