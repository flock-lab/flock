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

package com.amazonaws.kda.flink.benchmarking.model;

import java.util.List;

/** @author Ravi Itha, Amazon Web Services, Inc. */
public class BenchmarkingSpecs {

  private String jobName;
  private int jobDurationInMinutes;
  private String jobId;
  private String region;
  private String jobStartTime;
  private int numberofChildJobs;
  private boolean isUsingDynamoDBLocal;
  private String dynamoDBLocalURI;
  private String parentJobSummmaryDDBTableName;
  private String childJobSummaryDDBTableName;
  private List<ChildJob> childJobs;
  private List<String> targetKinesisStreams;

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public int getJobDurationInMinutes() {
    return jobDurationInMinutes;
  }

  public void setJobDurationInMinutes(int jobDurationInMinutes) {
    this.jobDurationInMinutes = jobDurationInMinutes;
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public String getRegion() {
    return region;
  }

  public void setRegion(String region) {
    this.region = region;
  }

  public String getJobStartTime() {
    return jobStartTime;
  }

  public void setJobStartTime(String jobStartTime) {
    this.jobStartTime = jobStartTime;
  }

  public int getNumberofChildJobs() {
    return numberofChildJobs;
  }

  public void setNumberofChildJobs(int numberofChildJobs) {
    this.numberofChildJobs = numberofChildJobs;
  }

  public boolean isUsingDynamoDBLocal() {
    return isUsingDynamoDBLocal;
  }

  public void setUsingDynamoDBLocal(boolean isUsingDynamoDBLocal) {
    this.isUsingDynamoDBLocal = isUsingDynamoDBLocal;
  }

  public String getDynamoDBLocalURI() {
    return dynamoDBLocalURI;
  }

  public void setDynamoDBLocalURI(String dynamoDBLocalURI) {
    this.dynamoDBLocalURI = dynamoDBLocalURI;
  }

  public String getParentJobSummmaryDDBTableName() {
    return parentJobSummmaryDDBTableName;
  }

  public void setParentJobSummmaryDDBTableName(String parentJobSummmaryDDBTableName) {
    this.parentJobSummmaryDDBTableName = parentJobSummmaryDDBTableName;
  }

  public String getChildJobSummaryDDBTableName() {
    return childJobSummaryDDBTableName;
  }

  public void setChildJobSummaryDDBTableName(String childJobSummaryDDBTableName) {
    this.childJobSummaryDDBTableName = childJobSummaryDDBTableName;
  }

  public List<ChildJob> getChildJobs() {
    return childJobs;
  }

  public void setChildJobs(List<ChildJob> childJobs) {
    this.childJobs = childJobs;
  }

  public List<String> getTargetKinesisStreams() {
    return targetKinesisStreams;
  }

  public void setTargetKinesisStreams(List<String> targetKinesisStreams) {
    this.targetKinesisStreams = targetKinesisStreams;
  }
}
