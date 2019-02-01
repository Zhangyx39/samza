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
import com.couchbase.client.java.document.json.JsonObject;
import org.apache.samza.SamzaException;
import rx.Single;
import rx.SingleSubscriber;
import com.couchbase.client.java.document.JsonDocument;
import com.google.common.collect.ImmutableList;
import java.util.concurrent.CompletableFuture;
import org.apache.samza.table.remote.TableReadFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CouchbaseTableReadFunction<V> extends CouchbaseTableFunctionBase<V>
    implements TableReadFunction<String, V> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseTableReadFunction.class);

  public CouchbaseTableReadFunction(Class<V> valueClass) {
    super(valueClass);
  }

  @Override
  public CompletableFuture<V> getAsync(String key) {
    CompletableFuture<V> getFuture = new CompletableFuture<>();
    SingleSubscriber<Document> subscriber = new SingleSubscriber<Document>() {
      @Override
      public void onSuccess(Document v) {
        if (JsonDocument.class.isAssignableFrom(valueClass)) {
          getFuture.complete((V) v);
        } else {
          getFuture.complete(valueSerde.fromBytes((byte[]) v.content()));
        }
      }

      @Override
      public void onError(Throwable error) {
        throw new SamzaException(String.format("Failed to get key %s", key), error);
      }
    };
    Document document;
    if (JsonDocument.class.isAssignableFrom(valueClass)) {
      document = JsonDocument.create(key);
    } else {
      document = ByteArrayDocument.create(key);
    }
    Single<Document> singleObservable = bucket.async().get(document).toSingle();
    singleObservable.subscribe(subscriber);
    return getFuture;
  }

  @Override
  public boolean isRetriable(Throwable throwable) {
    return false;
  }

  public static void main(String[] args) {
    CouchbaseTableReadFunction<JsonDocument> readFunction =
        new CouchbaseTableReadFunction<>(JsonDocument.class)
            .withClusters(ImmutableList.of("localhost"))
            .withUsername("yixzhang")
            .withPassword("344046")
            .withBucketName("travel-sample");

    CouchbaseTableWriteFunction<JsonDocument> writeFunction =
        new CouchbaseTableWriteFunction<>(JsonDocument.class)
            .withClusters(ImmutableList.of("localhost"))
            .withUsername("yixzhang")
            .withPassword("344046")
            .withBucketName("travel-sample");

    readFunction.initial();
    writeFunction.initial();

    String key = "test";
    JsonDocument doc = JsonDocument.create(key, JsonObject.fromJson("{\"test\": \"an item for testing\"}"));

    writeFunction.put(key, doc);
    System.out.println(readFunction.get(key));
    writeFunction.delete(key);
    System.out.println(readFunction.get(key));

    readFunction.close();
  }
}
