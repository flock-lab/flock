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

package com.amazonaws.kda.benchmarking.util;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import java.util.Map;

public class GetSampleData {

  public static void main(String[] args) {
    AmazonDynamoDB client =
        AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-east-1"))
            .build();

    // String tableName = "kda_flink_perf_benchmarking_with_s3";
    // String tableName = "kda_flink_perf_benchmarking_without_s3";
    // String tableName = "kda_flink_perf_benchmarking_child_job_summary";
    String tableName = "kda_flink_perf_benchmarking_parent_job_summary";

    try {
      ScanRequest scanRequest = new ScanRequest().withTableName(tableName);
      ScanResult result = client.scan(scanRequest);

      for (Map<String, AttributeValue> item : result.getItems()) {
        Map<String, AttributeValue> attributeList = item;
        for (Map.Entry<String, AttributeValue> item1 : attributeList.entrySet()) {
          String attributeName = item1.getKey();
          AttributeValue value = item1.getValue();

          // if(Optional.ofNullable(value.getN()).isPresent())

          System.out.print(
              attributeName
                  + ": "
                  + (value.getS() == null
                      ? "N=[" + value.getN() + "] "
                      : "S=[" + value.getS() + "] "));
        }
        // Move to next line
        System.out.println();
      }
    } catch (Exception e) {
      System.err.println("Unable to create table: ");
      System.err.println(e.getMessage());
    }
  }
}
