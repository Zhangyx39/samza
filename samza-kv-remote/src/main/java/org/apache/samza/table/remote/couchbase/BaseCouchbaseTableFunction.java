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
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.util.retry.RetryWhenFunction;
import com.google.common.collect.ImmutableList;
import java.security.KeyStore;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.samza.context.Context;
import org.apache.samza.operators.functions.ClosableFunction;
import org.apache.samza.operators.functions.InitableFunction;
import org.apache.samza.serializers.Serde;


public abstract class BaseCouchbaseTableFunction<V> implements InitableFunction, ClosableFunction {

  protected List<String> clusterNodes;
  protected String username;
  protected String password;
  protected String bucketName;
  protected transient CouchbaseEnvironment env;
  protected transient Cluster cluster;
  protected transient Bucket bucket;
  protected Class<V> valueClass;
  protected Serde<V> valueSerde;
  protected long timeout = 0L;
  protected TimeUnit timeUnit;
  protected int ttl = 0; // default value 0 means no ttl, data will be stored forever
  protected boolean enableSsl = false;
  protected String sslKeystoreFile = null;
  protected String sslKeystorePassword = null;
  protected KeyStore sslKeyStore = null;
  protected RetryWhenFunction readRetryWhenFunction = null;
  protected RetryWhenFunction writeRetryWhenFunction = null;

  //TODO maybe we can create a builder class to create both read and write functions for the same bucket so that users don't need to type in the same things twice
  public BaseCouchbaseTableFunction(Class<V> valueClass) {
    this.valueClass = valueClass;
  }

  @Override
  public void init(Context context) {
    //TODO validation
    DefaultCouchbaseEnvironment.Builder envBuilder = DefaultCouchbaseEnvironment.builder();
    if (enableSsl) {
      envBuilder.sslEnabled(true).certAuthEnabled(true);
      if (sslKeyStore != null) {
        envBuilder.sslKeystore(sslKeyStore);
      } else {
        envBuilder.sslKeystoreFile(sslKeystoreFile).sslKeystorePassword(sslKeystorePassword);
      }
    }
    env = envBuilder.build();
    cluster = CouchbaseCluster.create(env, clusterNodes);
    if (!enableSsl) {
      cluster.authenticate(username, password);
    }
    bucket = cluster.openBucket(bucketName);
    //TODO set retry policy
  }

  @Override
  public void close() {
    bucket.close();
    cluster.disconnect();
    env.shutdown();
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withClusters(Collection<String> clusters) {
    this.clusterNodes = ImmutableList.copyOf(clusters);
    //TODO try to get rid of this type casting
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withUsername(String username) {
    this.username = username;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withPassword(String password) {
    this.password = password;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withBucketName(String bucketName) {
    this.bucketName = bucketName;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withTimeout(long timeout, TimeUnit timeUnit) {
    this.timeout = timeout;
    this.timeUnit = timeUnit;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withTtl(int ttl) {
    this.ttl = ttl;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withSslKeystoreFileAndPassword(String sslKeystoreFile,
      String sslKeystorePassword) {
    this.enableSsl = true;
    this.sslKeystoreFile = sslKeystoreFile;
    this.sslKeystorePassword = sslKeystorePassword;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withSslKeystore(KeyStore sslKeyStore) {
    this.sslKeyStore = sslKeyStore;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withReadRetryStrategy(RetryWhenFunction readRetryWhenFunction) {
    this.readRetryWhenFunction = readRetryWhenFunction;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withWriteRetryStrategy(RetryWhenFunction writeRetryWhenFunction) {
    this.writeRetryWhenFunction = writeRetryWhenFunction;
    return (T) this;
  }
}
