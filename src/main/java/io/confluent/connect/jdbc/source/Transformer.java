package io.confluent.connect.jdbc.source;

/**
 * Created by shawnvarghese on 6/9/17.
 */

public interface Transformer<T> {

    public T transform(int type, T value, String[] fieldArgs);

}
