/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.test.table;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.mock.BucketConfiguration;
import com.couchbase.mock.CouchbaseMock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.descriptors.DelegatingSystemDescriptor;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.table.Table;
import org.apache.samza.table.descriptors.RemoteTableDescriptor;
import org.apache.samza.table.remote.couchbase.CouchbaseTableReadFunction;
import org.apache.samza.table.remote.couchbase.MyEnvBuilder;
import org.apache.samza.test.harness.AbstractIntegrationTestHarness;
import org.apache.samza.test.util.Base64Serializer;
import org.junit.Test;


public class TestCouchbaseRemoteTableEndToEnd extends AbstractIntegrationTestHarness {
  protected CouchbaseMock couchbaseMock;
  protected Cluster cluster;

  protected void createMockBuckets(List<String> bucketNames) throws Exception {
    ArrayList<BucketConfiguration> configList = new ArrayList<>();
    bucketNames.forEach(name -> configList.add(configBucket(name)));
    couchbaseMock = new CouchbaseMock(0, configList);
    couchbaseMock.start();
    couchbaseMock.waitForStartup();
    System.out.println("carrier port" + couchbaseMock.getCarrierPort("inputBucket"));
    System.out.println("http port" + couchbaseMock.getHttpPort());
  }

  protected BucketConfiguration configBucket(String bucketName) {
    BucketConfiguration config = new BucketConfiguration();
    config.numNodes = 1;
    config.numReplicas = 1;
    config.name = bucketName;
    return config;
  }

  protected void initClient() {
    cluster = CouchbaseCluster.create(DefaultCouchbaseEnvironment.builder()
        .bootstrapCarrierDirectPort(couchbaseMock.getCarrierPort("inputBucket"))
        .bootstrapHttpDirectPort(couchbaseMock.getHttpPort())
        .build(), "couchbase://127.0.0.1");
  }

  protected void shutdownMock() {
    cluster.disconnect();
    couchbaseMock.stop();
  }

  @Test
  public void testDummy() throws Exception {
    List<String> bucketNames = new ArrayList<>();
    String inputBucketName = "inputBucket";
    String outputBucketName = "outputBucket";
    bucketNames.add(inputBucketName);
    createMockBuckets(bucketNames);
    initClient();

    Bucket inputBucket = cluster.openBucket(inputBucketName);

    inputBucket.upsert(StringDocument.create("1", "1"));
    inputBucket.upsert(StringDocument.create("2", "b"));
    inputBucket.upsert(StringDocument.create("3", "c"));
    inputBucket.upsert(StringDocument.create("4", "d"));

//    int count = 10;
    String[] pageViews = new String[]{"1", "2", "3", "4"};

    int partitionCount = 1;
    Map<String, String> configs = TestLocalTableEndToEnd.getBaseJobConfig(bootstrapUrl(), zkConnect());

    configs.put("streams.PageView.samza.system", "test");
    configs.put("streams.PageView.source", Base64Serializer.serialize(pageViews));
    configs.put("streams.PageView.partitionCount", String.valueOf(partitionCount));
    Config config = new MapConfig(configs);

    final StreamApplication app = appDesc -> {
      DelegatingSystemDescriptor inputSystemDescriptor = new DelegatingSystemDescriptor("test");
      GenericInputDescriptor<String> inputDescriptor =
          inputSystemDescriptor.getInputDescriptor("PageView", new NoOpSerde<>());

      CouchbaseTableReadFunction<String> readFunction =
          new CouchbaseTableReadFunction<>(String.class).withEnvironmentBuilder(
              (MyEnvBuilder) new MyEnvBuilder().bootstrapCarrierDirectPort(couchbaseMock.getCarrierPort("inputBucket"))
                  .bootstrapHttpDirectPort(couchbaseMock.getHttpPort()))
              .withClusters(Collections.singletonList("couchbase://127.0.0.1"))
              .withBucketName(inputBucketName)
              .withSerde(new StringSerde());

      System.out.println(readFunction.toString());

      RemoteTableDescriptor<String, String> inputTableDesc = new RemoteTableDescriptor<>("input-table");
      inputTableDesc.withReadFunction(readFunction);
      Table<KV<String, String>> inputTable = appDesc.getTable(inputTableDesc);

      appDesc.getInputStream(inputDescriptor).map(k -> KV.of(k, k)).join(inputTable, new JoinFunction()).map(n -> {
          System.out.println(n);
          return n;
        });
    };

    final LocalApplicationRunner runner = new LocalApplicationRunner(app, config);
    executeRun(runner, config);
    runner.waitForFinish();
    shutdownMock();
  }

  static class JoinFunction
      implements StreamTableJoinFunction<String, KV<String, String>, KV<String, String>, KV<String, String>> {
//    private static final JsonParser _jsonParser = new JsonParser();

    @Override
    public KV<String, String> apply(KV<String, String> message, KV<String, String> record) {
//      return record == null ? null : message.getKey() + ", " + message.getValue() + ": " + record.getValue();
//      if (record == null) {
//        return null;
//      }
//      try {
//        JsonObject json1 = _jsonParser.parse(message.getValue()).getAsJsonObject();
//        JsonObject json2 = _jsonParser.parse(record.getValue()).getAsJsonObject();
//        for (Map.Entry<String, JsonElement> entry : json1.entrySet()) {
//          json2.add(entry.getKey(), entry.getValue());
//        }
//        return KV.of(message.getKey(), json2.toString());
//      } catch (Exception e) {
//        e.printStackTrace();
//      }
//      return null;
      return KV.of(message.getKey(), message.getValue() + record.getValue());
    }

    @Override
    public String getMessageKey(KV<String, String> message) {
      return message.getKey();
    }

    @Override
    public String getRecordKey(KV<String, String> record) {
      return record.getKey();
    }
  }
}
