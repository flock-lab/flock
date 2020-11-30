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

/** @author Ravi Itha, Amazon Web Services, Inc. */
public class ChildJob {

  private String jobName;
  private String jobId;
  private String parentJobId;
  private int numberofInteractions;
  private int batchSize;
  private int batchCadence;
  private int numberofBatches;
  private String batchStartTime;

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public int getNumberofInteractions() {
    return numberofInteractions;
  }

  public void setNumberofInteractions(int numberofInteractions) {
    this.numberofInteractions = numberofInteractions;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public int getBatchCadence() {
    return batchCadence;
  }

  public void setBatchCadence(int batchCadence) {
    this.batchCadence = batchCadence;
  }

  public int getNumberofBatches() {
    return numberofBatches;
  }

  public void setNumberofBatches(int numberofBatches) {
    this.numberofBatches = numberofBatches;
  }

  public String getBatchStartTime() {
    return batchStartTime;
  }

  public void setBatchStartTime(String batchStartTime) {
    this.batchStartTime = batchStartTime;
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public String getParentJobId() {
    return parentJobId;
  }

  public void setParentJobId(String parentJobId) {
    this.parentJobId = parentJobId;
  }
}
