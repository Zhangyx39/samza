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
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.context.Context;
import org.apache.samza.operators.functions.ClosableFunction;
import org.apache.samza.operators.functions.InitableFunction;
import org.apache.samza.serializers.Serde;


public abstract class BaseCouchbaseTableFunction<V> implements InitableFunction, ClosableFunction, Serializable {

  // Clients
  protected transient CouchbaseEnvironment env;
  protected transient Cluster cluster;
  protected transient Bucket bucket;

  // Function Settings
  protected final String tableId;
  protected final Class<V> valueClass;
  protected final Boolean useJsonDocumentValue;
  protected Serde<V> valueSerde = null;
  protected Long timeout = 0L;
  protected TimeUnit timeUnit = null;
  protected Integer ttl = 0; // default value 0 means no ttl, data will be stored forever
  protected SerializableRetryWhenFunction readRetryWhenFunction = null;
  protected SerializableRetryWhenFunction writeRetryWhenFunction = null;

  // Cluster Settings
  protected final List<String> clusterNodes;
  protected final String bucketName;
  protected String username = null;
  protected String password = null;

  // Environment Settings
  protected Boolean sslEnabled = false;
  protected Boolean certAuthEnabled = false;
  protected String sslKeystoreFile = null;
  protected String sslKeystorePassword = null;
  protected String sslTruststoreFile = null;
  protected String sslTruststorePassword = null;
  protected Integer bootstrapCarrierDirectPort = null;
  protected Integer bootstrapCarrierSslPort = null;
  protected Integer bootstrapHttpDirectPort = null;
  protected Integer bootstrapHttpSslPort = null;

  //TODO maybe we can create a builder class to create both read and write functions for the same bucket so that users don't need to type in the same things twice
  public BaseCouchbaseTableFunction(String tableId, Class<V> valueClass, Collection<String> clusterNodes, String bucketName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(bucketName), "Table ßid is not allowed to be null, empty or blank.");
    Preconditions.checkArgument(valueClass != null, "Value class is not allowed to be null.");
    Preconditions.checkArgument(CollectionUtils.isNotEmpty(clusterNodes),
        "Cluster nodes is not allowed to be null or empty.");
    Preconditions.checkArgument(StringUtils.isNotEmpty(bucketName), "Bucket name is not allowed to be null or empty.");
    this.tableId = tableId;
    this.valueClass = valueClass;
    this.clusterNodes = ImmutableList.copyOf(clusterNodes);
    this.bucketName = bucketName;
    this.useJsonDocumentValue = JsonDocument.class.isAssignableFrom(valueClass);
  }

  @Override
  public void init(Context context) {
    //TODO validation
    DefaultCouchbaseEnvironment.Builder envBuilder = new DefaultCouchbaseEnvironment.Builder();
    // ssl settings
    envBuilder.sslEnabled(sslEnabled).certAuthEnabled(certAuthEnabled);
    if (sslKeystoreFile != null) {
      envBuilder.sslKeystoreFile(sslKeystoreFile);
    }
    if (sslKeystorePassword != null) {
      envBuilder.sslKeystorePassword(sslKeystorePassword);
    }
    if (sslTruststoreFile != null) {
      envBuilder.sslTruststoreFile(sslTruststoreFile);
    }
    if (sslTruststorePassword != null) {
      envBuilder.sslTruststorePassword(sslTruststorePassword);
    }
    if (bootstrapCarrierDirectPort != null) {
      envBuilder.bootstrapCarrierDirectPort(bootstrapCarrierDirectPort);
    }
    if (bootstrapCarrierSslPort != null) {
      envBuilder.bootstrapCarrierSslPort(bootstrapCarrierSslPort);
    }
    if (bootstrapHttpDirectPort != null) {
      envBuilder.bootstrapHttpDirectPort(bootstrapHttpDirectPort);
    }
    if (bootstrapHttpSslPort != null) {
      envBuilder.bootstrapHttpSslPort(bootstrapHttpSslPort);
    }
    env = envBuilder.build();
    cluster = CouchbaseCluster.create(env, clusterNodes);
    if (username != null && password != null) {
      cluster.authenticate(username, password);
    }
    bucket = cluster.openBucket(bucketName);
  }

  @Override
  public void close() {
    bucket.close();
    cluster.disconnect();
    env.shutdown();
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withTimeout(long timeout, TimeUnit timeUnit) {
    this.timeout = timeout;
    this.timeUnit = timeUnit;
    //TODO try to get rid of this type casting
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

  public <T extends BaseCouchbaseTableFunction<V>> T withReadRetryWhenFunction(
      SerializableRetryWhenFunction readRetryWhenFunction) {
    this.readRetryWhenFunction = readRetryWhenFunction;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withWriteRetryWhenFunction(
      SerializableRetryWhenFunction writeRetryWhenFunction) {
    this.writeRetryWhenFunction = writeRetryWhenFunction;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withUsernameAndPassword(String username, String password) {
    this.username = username;
    this.password = password;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withSslEnabled(boolean sslEnabled) {
    this.sslEnabled = sslEnabled;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withCertAuthEnabled(boolean certAuthEnabled) {
    this.certAuthEnabled = certAuthEnabled;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withSslKeystoreFile(String sslKeystoreFile) {
    this.sslKeystoreFile = sslKeystoreFile;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withSslKeystorePassword(String sslKeystorePassword) {
    this.sslKeystorePassword = sslKeystorePassword;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withSslTruststoreFile(String sslTruststoreFile) {
    this.sslTruststoreFile = sslTruststoreFile;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withSslTruststorePassword(String sslTruststorePassword) {
    this.sslTruststorePassword = sslTruststorePassword;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withBootstrapCarrierDirectPort(int bootstrapCarrierDirectPort) {
    this.bootstrapCarrierDirectPort = bootstrapCarrierDirectPort;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withBootstrapCarrierSslPort(int bootstrapCarrierSslPort) {
    this.bootstrapCarrierSslPort = bootstrapCarrierSslPort;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withBootstrapHttpDirectPort(int bootstrapHttpDirectPort) {
    this.bootstrapHttpDirectPort = bootstrapHttpDirectPort;
    return (T) this;
  }

  public <T extends BaseCouchbaseTableFunction<V>> T withBootstrapHttpSslPort(int bootstrapHttpSslPort) {
    this.bootstrapHttpSslPort = bootstrapHttpSslPort;
    return (T) this;
  }
}
