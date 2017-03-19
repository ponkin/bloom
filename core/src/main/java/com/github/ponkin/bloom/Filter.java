package com.github.ponkin.bloom;

import java.io.Closeable;
import java.util.Set;

/**
 * Common filter interface
 *
 * @author Alexey Ponkin
 */
public interface Filter extends Closeable {

  default void put(Set<String> items) {
    for (String item : items) {
      put(item);
    }
  }

  default boolean put(String item) {
    boolean itemAdded = false;
    if(item != null && !item.isEmpty()) {
      itemAdded = put(Utils.getBytesFromUTF8String(item));
    }
    return itemAdded;
  }

  default boolean mightContain(String item) {
    boolean mightContain = false;
    if(item != null && !item.isEmpty()) {
      mightContain = mightContain(Utils.getBytesFromUTF8String(item));
    }
    return mightContain;
  }

  default void remove(Set<String> items) {
    for (String item : items) {
      remove(item);
    }
  }

  default boolean remove(String item) {
    boolean removed = true;
    if(item != null && !item.isEmpty()) {
      removed = remove(Utils.getBytesFromUTF8String(item));
    }
    return removed;
  }

  boolean remove(byte[] bytes);

  boolean mightContain(byte[] bytes);

  boolean put(byte[] bytes);

  double expectedFpp();

  void clear();

  Filter mergeInPlace(Filter other) throws Exception;

}
