package org.apache.samza.test.table;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.document.ByteArrayDocument;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.mock.CouchbaseMock;
import java.util.Collections;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.KV;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.descriptors.DelegatingSystemDescriptor;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.table.Table;
import org.apache.samza.table.descriptors.RemoteTableDescriptor;
import org.apache.samza.table.remote.couchbase.CouchbaseTableReadFunction;
import org.apache.samza.table.remote.couchbase.CouchbaseTableWriteFunction;
import org.apache.samza.test.harness.AbstractIntegrationTestHarness;
import org.apache.samza.test.util.Base64Serializer;
import org.junit.Assert;
import org.junit.Test;


public class TestCouchbasePerformance extends AbstractIntegrationTestHarness {

  protected CouchbaseEnvironment couchbaseEnvironment;
  protected CouchbaseMock couchbaseMock;
  protected Cluster cluster;
  private static String bucketName = "travel-sample-2";
  private static String username = "yixzhang";
  private static String password = "344046";

  @Test
  public void testDummy() throws Exception {

    String[] users = IntStream.range(1, 100000).mapToObj(String::valueOf).toArray(String[]::new);

    int partitionCount = 1;
    Map<String, String> configs = TestLocalTableEndToEnd.getBaseJobConfig(bootstrapUrl(), zkConnect());

    configs.put("streams.User.samza.system", "test");
    configs.put("streams.User.source", Base64Serializer.serialize(users));
    configs.put("streams.User.partitionCount", String.valueOf(partitionCount));
    Config config = new MapConfig(configs);

    final StreamApplication app = appDesc -> {
      DelegatingSystemDescriptor inputSystemDescriptor = new DelegatingSystemDescriptor("test");
      GenericInputDescriptor<String> inputDescriptor =
          inputSystemDescriptor.getInputDescriptor("User", new NoOpSerde<>());

      CouchbaseTableWriteFunction<String> writeFunction =
          new CouchbaseTableWriteFunction<>("output-table", String.class,
              Collections.singletonList("couchbase://127.0.0.1"), bucketName).withUsernameAndPassword(username,
              password).withSerde(new StringSerde());

      RemoteTableDescriptor<String, String> outputTableDesc = new RemoteTableDescriptor<>("output-table");
      outputTableDesc.withReadFunction(new TestCouchbaseRemoteTableEndToEnd.DummyReadFunction<>())
          .withWriteFunction(writeFunction);
      Table<KV<String, String>> outputTable = appDesc.getTable(outputTableDesc);

      appDesc.getInputStream(inputDescriptor).map(k -> KV.of(k, k)).sendTo(outputTable);
    };

    final LocalApplicationRunner runner = new LocalApplicationRunner(app, config);
    executeRun(runner, config);
    runner.waitForFinish();
  }
}
