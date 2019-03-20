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

package org.apache.samza.table.remote.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.error.TemporaryLockFailureException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.context.Context;
import org.apache.samza.operators.functions.ClosableFunction;
import org.apache.samza.operators.functions.InitableFunction;
import org.apache.samza.serializers.Serde;


public abstract class BaseCouchbaseTableFunction<V> implements InitableFunction, ClosableFunction, Serializable {

  // Clients
  private final static CouchbaseBucketRegistry COUCHBASE_BUCKET_REGISTRY = new CouchbaseBucketRegistry();
  protected transient CouchbaseEnvironment env;
  protected transient Cluster cluster;
  protected transient Bucket bucket;

  // Function Settings
  protected Serde<V> valueSerde = null;
  protected Duration timeout = Duration.ZERO;
  protected Integer ttl = 0; // default value 0 means no ttl, data will be stored forever

  // Cluster Settings
  protected final List<String> clusterNodes;
  protected final String bucketName;

  // Environment Settings
  protected CouchbaseEnvironmentConfigs environmentConfigs;

  public BaseCouchbaseTableFunction(String bucketName, List<String> clusterNodes, Class<V> valueClass) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(bucketName), "Bucket name is not allowed to be null or empty.");
    Preconditions.checkArgument(CollectionUtils.isNotEmpty(clusterNodes),
        "Cluster nodes is not allowed to be null or empty.");
    Preconditions.checkArgument(valueClass != null, "Value class is not allowed to be null.");
    this.bucketName = bucketName;
    this.clusterNodes = ImmutableList.copyOf(clusterNodes);
    this.environmentConfigs = new CouchbaseEnvironmentConfigs();
  }

  @Override
  public void init(Context context) {
    bucket = COUCHBASE_BUCKET_REGISTRY.getBucket(bucketName, clusterNodes, environmentConfigs);
  }

  @Override
  public void close() {
    COUCHBASE_BUCKET_REGISTRY.closeBucket(bucketName, clusterNodes);
  }

  public boolean isRetriable(Throwable exception) {
    while (exception != null && !(exception instanceof TemporaryFailureException)
        && !(exception instanceof TemporaryLockFailureException)) {
      exception = exception.getCause();
    }
    return exception != null;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withTimeout(Duration timeout) {
    this.timeout = timeout;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withTtl(int ttl) {
    this.ttl = ttl;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withSerde(Serde<V> valueSerde) {
    this.valueSerde = valueSerde;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withUsernameAndPassword(String username, String password) {
    if (environmentConfigs.sslEnabled) {
      throw new IllegalArgumentException(
          "Role-Based Access Control and Certificate-Based Authentication cannot be used together.");
    }
    environmentConfigs.username = username;
    environmentConfigs.password = password;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withSslEnabled(boolean sslEnabled) {
    if (environmentConfigs.username != null && sslEnabled) {
      throw new IllegalArgumentException(
          "Role-Based Access Control and Certificate-Based Authentication cannot be used together.");
    }
    environmentConfigs.sslEnabled = sslEnabled;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withCertAuthEnabled(boolean certAuthEnabled) {
    environmentConfigs.certAuthEnabled = certAuthEnabled;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withSslKeystoreFile(String sslKeystoreFile) {
    environmentConfigs.sslKeystoreFile = sslKeystoreFile;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withSslKeystorePassword(String sslKeystorePassword) {
    environmentConfigs.sslKeystorePassword = sslKeystorePassword;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withSslTruststoreFile(String sslTruststoreFile) {
    environmentConfigs.sslTruststoreFile = sslTruststoreFile;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withSslTruststorePassword(String sslTruststorePassword) {
    environmentConfigs.sslTruststorePassword = sslTruststorePassword;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withBootstrapCarrierDirectPort(int bootstrapCarrierDirectPort) {
    environmentConfigs.bootstrapCarrierDirectPort = bootstrapCarrierDirectPort;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withBootstrapCarrierSslPort(int bootstrapCarrierSslPort) {
    environmentConfigs.bootstrapCarrierSslPort = bootstrapCarrierSslPort;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withBootstrapHttpDirectPort(int bootstrapHttpDirectPort) {
    environmentConfigs.bootstrapHttpDirectPort = bootstrapHttpDirectPort;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withBootstrapHttpSslPort(int bootstrapHttpSslPort) {
    environmentConfigs.bootstrapHttpSslPort = bootstrapHttpSslPort;
    return (T) this;
  }
}
