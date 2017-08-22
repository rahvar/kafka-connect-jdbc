/**
 * Copyright 2015 RedLock Inc.
 *
 **/

package io.confluent.connect.jdbc.source;

public interface Transformer<T> {

  T transform(int type, T value, String[] fieldArgs);

}
