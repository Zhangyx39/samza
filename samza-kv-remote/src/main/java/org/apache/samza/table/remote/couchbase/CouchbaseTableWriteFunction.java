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

import com.couchbase.client.java.document.ByteArrayDocument;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import java.util.concurrent.CompletableFuture;
import org.apache.samza.SamzaException;
import org.apache.samza.table.remote.TableWriteFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Single;
import rx.SingleSubscriber;


public class CouchbaseTableWriteFunction<V> extends BaseCouchbaseTableFunction<V>
    implements TableWriteFunction<String, V> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseTableWriteFunction.class);

  public CouchbaseTableWriteFunction(Class<V> valueClass) {
    super(valueClass);
    LOGGER.info(String.format("Write function for bucket %s initialized successfully", bucketName));
  }

  @Override
  public CompletableFuture<Void> putAsync(String key, V record) {
    CompletableFuture<Void> putFuture = new CompletableFuture<>();
    Document document;
    if (JsonDocument.class.isAssignableFrom(valueClass)) {
      document = JsonDocument.create(key, ttl, ((JsonDocument) record).content());
    } else {
      document = ByteArrayDocument.create(key, ttl, valueSerde.toBytes(record));
    }
    Single<Document> singleObservable = bucket.async().upsert(document, timeout, timeUnit).toSingle();
    if (writeRetryWhenFunction != null) {
      singleObservable = singleObservable.retryWhen(writeRetryWhenFunction);
    }
    singleObservable.subscribe(new SingleSubscriber<Document>() {
      @Override
      public void onSuccess(Document v) {
        putFuture.complete(null);
      }

      @Override
      public void onError(Throwable error) {
        putFuture.completeExceptionally(
            new SamzaException(String.format("Failed to insert key %s, value %s", key, record), error));
      }
    });
    return putFuture;
  }

  //TODO deleting null key results in waiting forever
  @Override
  public CompletableFuture<Void> deleteAsync(String key) {
    CompletableFuture<Void> deleteFuture = new CompletableFuture<>();
    Document document;
    if (JsonDocument.class.isAssignableFrom(valueClass)) {
      document = JsonDocument.create(key);
    } else {
      document = ByteArrayDocument.create(key);
    }
    Single<Document> singleObservable = bucket.async().remove(document, timeout, timeUnit).toSingle();
    if (writeRetryWhenFunction != null) {
      singleObservable = singleObservable.retryWhen(writeRetryWhenFunction);
    }
    singleObservable.subscribe(new SingleSubscriber<Document>() {
      @Override
      public void onSuccess(Document v) {
        deleteFuture.complete(null);
      }

      @Override
      public void onError(Throwable error) {
        deleteFuture.completeExceptionally(new SamzaException(String.format("Failed to delete key %s", key), error));
      }
    });
    return deleteFuture;
  }

  @Override
  public boolean isRetriable(Throwable exception) {
    if (writeRetryWhenFunction != null) {
      return false;
    }
    return false;
    //TODO when do we allow retry?
  }
}
