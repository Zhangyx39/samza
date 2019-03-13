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
  private HashMap<String, Cluster> openedClusters;
  private HashMap<String, Integer> bucketUsageCounts;
  private HashMap<String, Integer> clusterUsageCounts;

  public CouchbaseBucketRegistry() {
    openedBuckets = new HashMap<>();
    openedClusters = new HashMap<>();
    bucketUsageCounts = new HashMap<>();
    clusterUsageCounts = new HashMap<>();
  }

  public synchronized Bucket getBucket(String bucketName, List<String> clusterNodes,
      CouchbaseEnvironmentConfigs configs) {
    String bucketId = getBucketId(bucketName, clusterNodes);
    String clusterId = getClusterId(clusterNodes);
    if (!openedClusters.containsKey(clusterId)) {
      openedClusters.put(clusterId, openCluster(clusterNodes, configs));
    }
    if (!openedBuckets.containsKey(bucketId)) {
      openedBuckets.put(bucketId, openBucket(bucketName, openedClusters.get(clusterId)));
    }
    bucketUsageCounts.put(bucketId, bucketUsageCounts.getOrDefault(bucketId, 0) + 1);
    clusterUsageCounts.put(clusterId, clusterUsageCounts.getOrDefault(clusterId, 0) + 1);
    return openedBuckets.get(bucketId);
  }

  public synchronized void closeBucket(String bucketName, List<String> clusterNodes) {
    String bucketId = getBucketId(bucketName, clusterNodes);
    String clusterId = getClusterId(clusterNodes);
    bucketUsageCounts.put(bucketId, bucketUsageCounts.get(bucketId) - 1);
    clusterUsageCounts.put(clusterId, clusterUsageCounts.get(clusterId) - 1);
    if (bucketUsageCounts.get(bucketId) == 0) {
      openedBuckets.get(bucketId).close();
      if (clusterUsageCounts.get(clusterId) == 0) {
        openedClusters.get(clusterId).disconnect();
      }
    }
  }

  private Cluster openCluster(List<String> clusterNodes, CouchbaseEnvironmentConfigs configs) {
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
    return cluster;
  }

  private Bucket openBucket(String bucketName, Cluster cluster) {
    return cluster.openBucket(bucketName);
  }

  private String getBucketId(String bucketName, List<String> clusterNodes) {
    return getClusterId(clusterNodes) + "-" + bucketName;
  }

  private String getClusterId(List<String> clusterNodes) {
    return clusterNodes.toString();
  }
}
