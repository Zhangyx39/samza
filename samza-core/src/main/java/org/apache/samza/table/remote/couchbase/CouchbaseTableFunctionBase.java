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
import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.List;
import org.apache.samza.context.Context;
import org.apache.samza.operators.functions.ClosableFunction;
import org.apache.samza.operators.functions.InitableFunction;
import org.apache.samza.serializers.Serde;


public class CouchbaseTableFunctionBase<V> implements InitableFunction, ClosableFunction {

  protected List<String> clusterNodes;
  protected String username;
  protected String password;
  protected String bucketName;
  protected Cluster cluster;
  protected Bucket bucket;
  protected Class<V> valueClass;
  protected Serde<V> valueSerde;

  public CouchbaseTableFunctionBase(Class<V> valueClass) {
    this.valueClass = valueClass;
  }

  public void initial() {
    cluster = CouchbaseCluster.create(clusterNodes);
    cluster.authenticate(username, password);
    bucket = cluster.openBucket(bucketName);
  }

  @Override
  public void init(Context context) {
    cluster = CouchbaseCluster.create(clusterNodes);
    cluster.authenticate(username, password);
    bucket = cluster.openBucket(bucketName);
  }

  @Override
  public void close() {
    bucket.close();
    cluster.disconnect();
  }

  public <T extends CouchbaseTableFunctionBase<V>> T withClusters(Collection<String> clusters) {
    this.clusterNodes = ImmutableList.copyOf(clusters);
    return (T) this;
  }

  public <T extends CouchbaseTableFunctionBase<V>> T withUsername(String username) {
    this.username = username;
    return (T) this;
  }

  public <T extends CouchbaseTableFunctionBase<V>> T withPassword(String password) {
    this.password = password;
    return (T) this;
  }

  public <T extends CouchbaseTableFunctionBase<V>> T withBucketName(String bucketName) {
    this.bucketName = bucketName;
    return (T) this;
  }
}
