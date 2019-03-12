package org.apache.samza.table.remote.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.auth.CertAuthenticator;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import java.util.HashMap;
import java.util.List;


public class CouchbaseBucketRegistry {
  private HashMap<String, Bucket> openedBuckets;
  private HashMap<String, Integer> bucketUsageCounts;
  private HashMap<String, Cluster> bucketNameToClusterMap;

  public CouchbaseBucketRegistry() {
    openedBuckets = new HashMap<>();
    bucketUsageCounts = new HashMap<>();
    bucketNameToClusterMap = new HashMap<>();
  }

  public synchronized Bucket getBucket(String bucketName, List<String> clusterNodes,
      CouchbaseEnvironmentConfigs configs) {
    if (!openedBuckets.containsKey(bucketName)) {
      openBucket(bucketName, clusterNodes, configs);
    }
    bucketUsageCounts.put(bucketName, bucketUsageCounts.getOrDefault(bucketName, 0) + 1);
    return openedBuckets.get(bucketName);
  }

  public synchronized void closeBucket(String bucketName) {
    bucketUsageCounts.put(bucketName, bucketUsageCounts.get(bucketName) - 1);
    if (bucketUsageCounts.get(bucketName) == 0) {
      openedBuckets.get(bucketName).close();
      bucketNameToClusterMap.get(bucketName).disconnect();
    }
  }

  private void openBucket(String bucketName, List<String> clusterNodes, CouchbaseEnvironmentConfigs configs) {
    DefaultCouchbaseEnvironment.Builder envBuilder = new DefaultCouchbaseEnvironment.Builder();
    envBuilder.sslEnabled(configs.sslEnabled).certAuthEnabled(configs.certAuthEnabled);
    if (configs.sslKeystoreFile != null) {
      envBuilder.sslKeystoreFile(configs.sslKeystoreFile);
    }
    if (configs.sslKeystorePassword != null) {
      envBuilder.sslKeystorePassword(configs.sslKeystorePassword);
    }
    if (configs.sslTruststoreFile != null) {
      envBuilder.sslTruststoreFile(configs.sslTruststoreFile);
    }
    if (configs.sslTruststorePassword != null) {
      envBuilder.sslTruststorePassword(configs.sslTruststorePassword);
    }
    if (configs.bootstrapCarrierDirectPort != null) {
      envBuilder.bootstrapCarrierDirectPort(configs.bootstrapCarrierDirectPort);
    }
    if (configs.bootstrapCarrierSslPort != null) {
      envBuilder.bootstrapCarrierSslPort(configs.bootstrapCarrierSslPort);
    }
    if (configs.bootstrapHttpDirectPort != null) {
      envBuilder.bootstrapHttpDirectPort(configs.bootstrapHttpDirectPort);
    }
    if (configs.bootstrapHttpSslPort != null) {
      envBuilder.bootstrapHttpSslPort(configs.bootstrapHttpSslPort);
    }
    CouchbaseEnvironment env = envBuilder.build();
    Cluster cluster = CouchbaseCluster.create(env, clusterNodes);
    if (configs.certAuthEnabled) {
      cluster.authenticate(CertAuthenticator.INSTANCE);
    } else if (configs.username != null && configs.password != null) {
      cluster.authenticate(configs.username, configs.password);
    }
    openedBuckets.put(bucketName, cluster.openBucket(bucketName));
  }
}
