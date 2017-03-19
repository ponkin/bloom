package com.github.ponkin.bloom;

import java.util.Arrays;

/**
 * On heap Bit array implementation
 * 
 * @author Alexey Ponkin
 */
final class BitArray implements BitSet {

  private final long[] data;
  private long bitCount;
  private static final long ZERO = 0L;

  static int numWords(long numBits) {
    if (numBits <= 0) {
      throw new IllegalArgumentException("numBits must be positive, but got " + numBits);
    }
    long numWords = (long) Math.ceil(numBits / 64.0);
    if (numWords > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Can't allocate enough space for " + numBits + " bits");
    }
    return (int) numWords;
  }

  BitArray(long numBits) {
    this(new long[numWords(numBits)]);
  }
    

  private BitArray(long[] data) {
    this.data = data;
    long bitCount = 0;
    for (long word : data) {
      bitCount += Long.bitCount(word);
    }
    this.bitCount = bitCount;
  }

  /** Returns true if the bit changed value. */
  @Override
  public boolean set(long index) {
    if (!get(index)) {
      data[(int) (index >>> 6)] |= (1L << index);
      bitCount++;
      return true;
    }
    return false;
  }

  /** Returns true if the bit changed value. */
  @Override
  public boolean unset(long index) {
    if (get(index)) {
      data[(int) (index >>> 6)] &= ~(1L << index);
      bitCount--;
      return true;
    }
    return false;
  }

  @Override
  public boolean get(long index) {
    return (data[(int) (index >>> 6)] & (1L << index)) != 0;
  }

  /** Number of bits */
  @Override
  public long bitSize() {
    return (long) data.length * Long.SIZE;
  }

  /** Number of set bits (1s) */
  @Override
  public long cardinality() {
    return bitCount;
  }

  @Override
  public void clear() {
    Arrays.fill(data, ZERO);
  }

  /** Combines the two BitArrays using bitwise OR. */
  @Override
  public void putAll(BitSet array) throws IncompatibleMergeException {
    if (array == null || !(array instanceof BitArray))  {
        throw new IncompatibleMergeException("Can't merge different blomfilters");
    }
    BitArray other = (BitArray) array;
    if (data.length != other.data.length) {
      throw new IncompatibleMergeException("BitArrays must be of equal length when merging");
    }
    long bitCount = 0;
    for (int i = 0; i < data.length; i++) {
      data[i] |= other.data[i];
      bitCount += Long.bitCount(data[i]);
    }
    this.bitCount = bitCount;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || !(other instanceof BitArray)) return false;
    BitArray that = (BitArray) other;
    return Arrays.equals(data, that.data);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(data);
  }
  
  @Override
  public void close() {
    /* DONOTHING */;
  }
}
