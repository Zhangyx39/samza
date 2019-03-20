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

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.java.document.BinaryDocument;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.samza.SamzaException;
import org.apache.samza.context.Context;
import org.apache.samza.table.remote.TableReadFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Single;
import rx.SingleSubscriber;


public class CouchbaseTableReadFunction<V> extends BaseCouchbaseTableFunction<V>
    implements TableReadFunction<String, V> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseTableReadFunction.class);

  protected final Class<? extends Document<?>> documentType;

  public CouchbaseTableReadFunction(String bucketName, List<String> clusterNodes, Class<V> valueClass) {
    super(bucketName, clusterNodes, valueClass);
    documentType = JsonObject.class.isAssignableFrom(valueClass) ? JsonDocument.class : BinaryDocument.class;
  }

  @Override
  public void init(Context context) {
    super.init(context);
    LOGGER.info(String.format("Read function for bucket %s initialized successfully", bucketName));
  }

  @Override
  public CompletableFuture<V> getAsync(String key) {
    Preconditions.checkNotNull(key);
    CompletableFuture<V> future = new CompletableFuture<>();
    Single<? extends Document<?>> singleObservable =
        bucket.async().get(key, documentType, timeout.toMillis(), TimeUnit.MILLISECONDS).toSingle();
    singleObservable.subscribe(new SingleSubscriber<Document<?>>() {
      @Override
      public void onSuccess(Document<?> document) {
        if (document != null) {
          if (document instanceof BinaryDocument) {
            handleGetAsyncBinaryDocument((BinaryDocument) document, future, key);
          } else {
            // V is of type JsonObject
            future.complete((V) document.content());
          }
        } else {
          future.complete(null);
        }
      }

      @Override
      public void onError(Throwable throwable) {
        if (throwable instanceof NoSuchElementException) {
          // There is no element returned by the observable, meaning the key doesn't exist.
          future.complete(null);
        } else {
          future.completeExceptionally(new SamzaException(String.format("Failed to get key %s", key), throwable));
        }
      }
    });
    return future;
  }

  private void handleGetAsyncBinaryDocument(BinaryDocument binaryDocument, CompletableFuture<V> future, String key) {
    ByteBuf buffer = binaryDocument.content();
    try {
      byte[] bytes;
      if (buffer.hasArray() && buffer.arrayOffset() == 0 && buffer.readableBytes() == buffer.array().length) {
        bytes = buffer.array();
      } else {
        bytes = new byte[buffer.readableBytes()];
        buffer.readBytes(bytes);
      }
      future.complete(valueSerde.fromBytes(bytes));
    } catch (Exception e) {
      future.completeExceptionally(
          new SamzaException(String.format("Failed to deserialize value of key %s with given serde", key), e));
    } finally {
      ReferenceCountUtil.release(buffer);
    }
  }
}
