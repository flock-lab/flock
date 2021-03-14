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

package com.amazonaws.kda.flink.benchmarking.model;

import org.quartz.JobDetail;
import org.quartz.Trigger;

/** @author Ravi Itha, Amazon Web Services, Inc. */
public class JobSchedule {

  private JobDetail jobDetail;
  private Trigger trigger;

  public JobDetail getJobDetail() {
    return jobDetail;
  }

  public void setJobDetail(JobDetail jobDetail) {
    this.jobDetail = jobDetail;
  }

  public Trigger getTrigger() {
    return trigger;
  }

  public void setTrigger(Trigger trigger) {
    this.trigger = trigger;
  }
}
