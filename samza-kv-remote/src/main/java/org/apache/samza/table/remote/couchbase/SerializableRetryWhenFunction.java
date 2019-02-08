package org.apache.samza.table.remote.couchbase;

import java.io.Serializable;
import rx.Observable;
import rx.functions.Func1;


public interface SerializableRetryWhenFunction
    extends Func1<Observable<? extends Throwable>, Observable<?>>, Serializable {
}
