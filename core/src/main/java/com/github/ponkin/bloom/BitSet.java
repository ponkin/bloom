package com.github.ponkin.bloom;

import java.io.Closeable;

/**
 * Common interface for all bit vector
 * implementations. Must be Closeable
 * because some filters can be mapped on
 * files.
 *
 * @author Alexey Ponkin
 * @see <a href="https://en.wikipedia.org/wiki/Bit_array">Bit array</a>
 */
interface BitSet extends Closeable {

  /**
   * Get bit with index <code>index</code>
   *
   * @param index index in underlying bit array
   * @return true if bit in underlying bit array is set tuo <code>1</code>,
   * false otherwise
   */
  boolean get(long index);

  /**
   * Set bit with index <code>index</code>
   *
   * @param index index in underlying bit array
   * @param true if bit in underlying bit array was changed from 0 to 1
   * false - otherwise
   */
  boolean set(long index);
  
  /**
   * Unset bit with index <code>index</code>
   *
   * @param index index in underlying bit array
   * @param true if bit in underlying bit array was changed from 1 to 0
   * false - otherwise
   */
  boolean unset(long index);

  /**
   * Return number of bits in underlying array
   * that are set to <code>1</code>
   *
   * @return number of <code>1`s</code> in bit array
   */
  long cardinality();

  /**
   * Return number of available bits 
   * in underlying array. Length.
   *
   * @return bit array length
   */
  long bitSize();

  /**
   * Set all bits in bit array
   * to <code>0</code>
   */
  void clear();

  /**
   * Put all elements of <code>array</code>
   * inside this bitset if applicable in other words
   * Combines the two BitSets using bitwise OR.
   *
   * @param array - BitSet to merge with
   * @throws IllegalArgumentException if <code>array</code> is not the same type
   */
  void putAll(BitSet array) throws Exception;
}
