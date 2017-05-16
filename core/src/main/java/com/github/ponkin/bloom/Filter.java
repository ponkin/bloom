package com.github.ponkin.bloom;

import java.io.Closeable;
import java.util.Set;

/**
 * Common filter interface.
 * Note: method {@link #remove(String)} and {@link #remove(Set)}
 * may be <strong>not</strong> implemented in particular implementations.
 *
 * @author Alexey Ponkin
 */
public interface Filter extends Closeable {

  /**
   * Put all items from set into the filter.
   *
   * @param items to put
   */
  default void put(Set<String> items) {
    for (String item : items) {
      put(item);
    }
  }

  /**
   * Put <code>item</code> inside filter
   * If <code>item</code> is empty string,
   * method will return false.
   *
   * @param item string representationof item
   * @return true if item was successfully put in filter,
   * false otherwise.
   */
  default boolean put(String item) {
    boolean itemAdded = false;
    if(item != null && !item.isEmpty()) {
      itemAdded = put(Utils.getBytesFromUTF8String(item));
    }
    return itemAdded;
  }

  /**
   * Check item against filter.
   *
   * @param item byte array representation
   * @return false if item is not inside filter(100% sure),
   * true - <code>item</code> might be inside filter with
   * some probability
   */
  default boolean mightContain(String item) {
    boolean mightContain = false;
    if(item != null && !item.isEmpty()) {
      mightContain = mightContain(Utils.getBytesFromUTF8String(item));
    }
    return mightContain;
  }

  /**
   * Remove set of items from filter
   * <p>
   * Note: can throw  {@link java.lang.UnsupportedOperationException}
   *
   * @param items set of items
   */
  default void remove(Set<String> items) {
    for (String item : items) {
      remove(item);
    }
  }

  /**
   * Remove item from filter
   * <p>
   * Note: can throw {@link java.lang.UnsupportedOperationException}
   *
   * @param item string representation
   * @return true if item was removed, false otherwise
   */
  default boolean remove(String item) {
    boolean removed = true;
    if(item != null && !item.isEmpty()) {
      removed = remove(Utils.getBytesFromUTF8String(item));
    }
    return removed;
  }

  /**
   * Remove item from filter
   * <p>
   * Note: can throw {@link java.lang.UnsupportedOperationException}
   *
   * @param bytes byte array representation
   * @return true if item was removed, false otherwise
   */
  boolean remove(byte[] bytes);

  /**
   * Check item against filter.
   *
   * @param bytes byte array representation
   * @return false if item is not inside filter(100% sure),
   * true - <code>item</code> might be inside filter with
   * some probability
   */
  boolean mightContain(byte[] bytes);

  /**
   * Put <code>item</code> inside filter
   *
   * @param bytes byte array representation
   * @return true if item was successfully put in filter,
   * false otherwise.
   */
  boolean put(byte[] bytes);

  /**
   * Get expected false positive rate
   * for this filter
   *
   * @return number between 0 and 1
   */
  double expectedFpp();

  /**
   * Remove all items from filter
   */
  void clear();

  /**
   * Merge in place two filters with same type
   *
   * @param other must be compatible or IncompatibleMergeException can be thrown
   * @return reference to this filter
   * @throws Exception {@link IncompatibleMergeException}
   */
  Filter mergeInPlace(Filter other) throws Exception;

}
