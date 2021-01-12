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

package com.amazonaws.kda.flink.benchmarking;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.kda.flink.benchmarking.model.BenchmarkingSpecs;
import com.amazonaws.kda.flink.benchmarking.model.ChildJob;
import com.amazonaws.kda.flink.benchmarking.model.JobSchedule;
import com.amazonaws.kda.flink.benchmarking.util.DDBUtil;
import com.amazonaws.kda.flink.benchmarking.util.KDSProducerUtil;
import com.amazonaws.kda.flink.benchmarking.util.KinesisStreamUtil;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.util.IOUtils;
import com.google.gson.Gson;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Benchmarking Utility main program
 *
 * @author Ravi Itha, Amazon Web Services, Inc.
 */
public class BenchmarkScheduler {

  public static void main(String[] args) {

    Logger logger = LoggerFactory.getLogger(BenchmarkScheduler.class);

    BenchmarkingSpecs benchMarkingSpecs = parseBenchamrkingSpecs(args[0]);
    benchMarkingSpecs.setJobId(UUID.randomUUID().toString());
    benchMarkingSpecs.setNumberofChildJobs(benchMarkingSpecs.getChildJobs().size());
    benchMarkingSpecs.setJobStartTime(
        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

    String targetKinesisStreams =
        benchMarkingSpecs.getTargetKinesisStreams().stream().collect(Collectors.joining("$"));
    String startingHashKeys =
        KinesisStreamUtil.getHashKeysForOpenShards(
                AmazonKinesisClientBuilder.standard()
                    .withRegion(benchMarkingSpecs.getRegion())
                    .build(),
                benchMarkingSpecs.getTargetKinesisStreams().get(0))
            .stream()
            .collect(Collectors.joining("$"));

    /** Define JobDetail and Trigger for each Job provided in the Job Template */
    List<JobSchedule> jobSchedules = new ArrayList<JobSchedule>();
    for (ChildJob childJob : benchMarkingSpecs.getChildJobs()) {
      List<String> interactions =
          KDSProducerUtil.createInteractions(childJob.getNumberofInteractions());

      childJob.setJobId(UUID.randomUUID().toString());
      childJob.setParentJobId(benchMarkingSpecs.getJobId());

      JobDetail jobDetail =
          newJob(KinesisProducerForFlinkSessionWindow.class)
              .withIdentity(
                  childJob.getJobName().concat("-").concat(benchMarkingSpecs.getJobStartTime()),
                  childJob.getJobName())
              .usingJobData("jobId", childJob.getJobId())
              .usingJobData("jobName", childJob.getJobName())
              .usingJobData("parentJobId", childJob.getParentJobId())
              .usingJobData("isUsingDynamoDBLocal", benchMarkingSpecs.isUsingDynamoDBLocal())
              .usingJobData("dynamoDBLocalURI", benchMarkingSpecs.getDynamoDBLocalURI())
              .usingJobData(
                  "childJobSummaryDDBTblName", benchMarkingSpecs.getChildJobSummaryDDBTableName())
              .usingJobData("region", benchMarkingSpecs.getRegion())
              .usingJobData("masterJobId", benchMarkingSpecs.getJobId())
              .usingJobData("targetKinesisStreams", targetKinesisStreams)
              .usingJobData("startingHashKeys", startingHashKeys)
              .usingJobData(
                  "interactionsIds", interactions.stream().collect(Collectors.joining("$")))
              .usingJobData("stringSeparator", "$")
              .usingJobData("batchSize", childJob.getBatchSize())
              .usingJobData("startingHashKeyIndex", 0)
              .build();

      Trigger trigger =
          newTrigger()
              .withIdentity(
                  childJob.getJobName().concat("-").concat("-trigger"),
                  childJob.getJobName().concat("-").concat("min-group"))
              .startNow()
              .withSchedule(
                  simpleSchedule()
                      .withIntervalInSeconds(childJob.getBatchCadence())
                      .withRepeatCount(childJob.getNumberofBatches()))
              .build();

      JobSchedule jobSchedule = new JobSchedule();
      jobSchedule.setJobDetail(jobDetail);
      jobSchedule.setTrigger(trigger);
      jobSchedules.add(jobSchedule);
    }

    /** Schedule the Jobs via Quartz Enterprise Job Scheduler */
    try {
      Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
      scheduler.start();
      for (JobSchedule jobSchedule : jobSchedules) {
        scheduler.scheduleJob(jobSchedule.getJobDetail(), jobSchedule.getTrigger());
      }
      logger.info(
          "Put Main thread in sleeping mode for "
              + benchMarkingSpecs.getJobDurationInMinutes()
              + " minutes");

      // Update DynamoDB
      trackJobs(benchMarkingSpecs);

      Thread.sleep(benchMarkingSpecs.getJobDurationInMinutes() * 60000);
      scheduler.shutdown();
    } catch (SchedulerException se) {
      se.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * This method to parse the Job Definition
   *
   * @param filePath
   * @return
   */
  public static BenchmarkingSpecs parseBenchamrkingSpecs(String filePath) {
    BenchmarkingSpecs benchMarkingSpecs = null;
    try {
      String jsonString =
          new String(Files.readAllBytes(Paths.get(filePath)), StandardCharsets.UTF_8);
      benchMarkingSpecs = new Gson().fromJson(jsonString, BenchmarkingSpecs.class);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return benchMarkingSpecs;
  }

  /**
   * This method tracks Job status in DynamoDB
   *
   * @param benchMarkingSpecs
   */
  public static void trackJobs(BenchmarkingSpecs benchMarkingSpecs) {
    int numInteractionsProcessed = 0;
    AmazonDynamoDB dynamoDB = null;

    if (benchMarkingSpecs.isUsingDynamoDBLocal())
      dynamoDB =
          AmazonDynamoDBClientBuilder.standard()
              .withEndpointConfiguration(
                  new AwsClientBuilder.EndpointConfiguration(
                      benchMarkingSpecs.getDynamoDBLocalURI(), benchMarkingSpecs.getRegion()))
              .build();
    else
      dynamoDB =
          AmazonDynamoDBClientBuilder.standard().withRegion(benchMarkingSpecs.getRegion()).build();

    DynamoDB dynamoDBClient = new DynamoDB(dynamoDB);

    // Insert a record to kda_flink_perf_benchmarking_master_job_summary DDB table
    for (ChildJob childJob : benchMarkingSpecs.getChildJobs()) {
      numInteractionsProcessed += childJob.getNumberofInteractions();
    }
    DDBUtil.insertParentJobStatus(
        dynamoDBClient,
        benchMarkingSpecs.getParentJobSummmaryDDBTableName(),
        benchMarkingSpecs.getJobName(),
        benchMarkingSpecs.getJobId(),
        numInteractionsProcessed,
        benchMarkingSpecs.getJobStartTime(),
        "Started");

    // Insert records to kda_flink_perf_benchmarking_child_job_summary DDB Table
    for (ChildJob childJob : benchMarkingSpecs.getChildJobs()) {
      DDBUtil.insertChildJobStatus(
          dynamoDBClient,
          benchMarkingSpecs.getChildJobSummaryDDBTableName(),
          childJob.getJobName(),
          childJob.getJobId(),
          childJob.getParentJobId(),
          childJob.getNumberofInteractions(),
          benchMarkingSpecs.getJobStartTime(),
          "In Progress");
    }
  }

  public static String parse(String resource) throws IOException {
    InputStream stream = BenchmarkScheduler.class.getResourceAsStream(resource);
    try {
      String json = IOUtils.toString(stream);
      return json;
    } finally {
      stream.close();
    }
  }
}
