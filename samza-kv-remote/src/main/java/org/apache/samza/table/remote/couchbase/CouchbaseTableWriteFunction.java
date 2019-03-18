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

import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.java.document.BinaryDocument;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.samza.SamzaException;
import org.apache.samza.context.Context;
import org.apache.samza.table.remote.TableWriteFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Single;
import rx.SingleSubscriber;


public class CouchbaseTableWriteFunction<V> extends BaseCouchbaseTableFunction<V>
    implements TableWriteFunction<String, V> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseTableWriteFunction.class);

  public CouchbaseTableWriteFunction(String bucketName, List<String> clusterNodes, Class<V> valueClass) {
    super(bucketName, clusterNodes, valueClass);
  }

  @Override
  public void init(Context context) {
    super.init(context);
    LOGGER.info(String.format("Write function for bucket %s initialized successfully", bucketName));
  }

  @Override
  public CompletableFuture<Void> putAsync(String key, V record) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(record);
    Document<?> document = record instanceof JsonObject ? JsonDocument.create(key, ttl, (JsonObject) record)
        : BinaryDocument.create(key, ttl, Unpooled.copiedBuffer(valueSerde.toBytes(record)));
    return asyncWriteHelper(bucket.async().upsert(document, timeout, timeUnit).toSingle(),
        String.format("Failed to insert key %s, value %s", key, record));
  }

  @Override
  public CompletableFuture<Void> deleteAsync(String key) {
    Preconditions.checkNotNull(key);
    return asyncWriteHelper(bucket.async().remove(key, timeout, timeUnit).toSingle(),
        String.format("Failed to delete key %s", key));
  }

  private CompletableFuture<Void> asyncWriteHelper(Single<? extends Document<?>> single, String errorMessage) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    single.subscribe(new SingleSubscriber<Document>() {
      @Override
      public void onSuccess(Document v) {
        future.complete(null);
      }

      @Override
      public void onError(Throwable error) {
        future.completeExceptionally(new SamzaException(errorMessage, error));
      }
    });
    return future;
  }

  @Override
  public boolean isRetriable(Throwable throwable) {
    return super.isRetriable(throwable);
  }
}
