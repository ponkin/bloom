package com.github.ponkin.bloom;

import java.io.Closeable;

/**
 * Common interface for all bit vector
 * implementations
 *
 * @author Alexey Ponkin
 */
interface BitSet extends Closeable {
  boolean get(long index);
  boolean set(long index);
  boolean unset(long index);
  long cardinality();
  long bitSize();
  void clear();

  /**
   * Put all elements of <code>array</code>
   * inside this bitset if applicable
   *
   * @param array - BitSet to merge with
   * @throws IllegalArgumentException if <code>array</code> is not the same type
   */
  void putAll(BitSet array) throws Exception;
}
