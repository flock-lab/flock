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

package com.amazonaws.kda.flink.benchmarking.model;

/**
 * This is a POJO represents an Event which will be batched
 *
 * <p>Each batched event has a format per the file src/main/resources/event_sample.json
 *
 * @author Ravi Itha
 */
public class Event {

  private String attr_1;
  private String attr_2;
  private String attr_3;
  private String attr_4;
  private String attr_5;
  private String attr_6;
  private long attr_7;
  private String attr_8;
  private String session_id;
  private long timestamp;

  public String getAttr_1() {
    return attr_1;
  }

  public void setAttr_1(String attr_1) {
    this.attr_1 = attr_1;
  }

  public String getAttr_2() {
    return attr_2;
  }

  public void setAttr_2(String attr_2) {
    this.attr_2 = attr_2;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public String getAttr_3() {
    return attr_3;
  }

  public void setAttr_3(String attr_3) {
    this.attr_3 = attr_3;
  }

  public String getAttr_4() {
    return attr_4;
  }

  public void setAttr_4(String attr_4) {
    this.attr_4 = attr_4;
  }

  public String getAttr_5() {
    return attr_5;
  }

  public void setAttr_5(String attr_5) {
    this.attr_5 = attr_5;
  }

  public String getSession_id() {
    return session_id;
  }

  public void setSession_id(String session_id) {
    this.session_id = session_id;
  }

  public String getAttr_6() {
    return attr_6;
  }

  public void setAttr_6(String attr_6) {
    this.attr_6 = attr_6;
  }

  public long getAttr_7() {
    return attr_7;
  }

  public void setAttr_7(long attr_7) {
    this.attr_7 = attr_7;
  }

  public String getAttr_8() {
    return attr_8;
  }

  public void setAttr_8(String attr_8) {
    this.attr_8 = attr_8;
  }
}
