package com.github.ponkin.bloom;

/**
 * HashFunctions implementations
 *
 * @author Alexey Ponkin
 */
public class Hashers {

  /**
   * 32 bit Murmur3 hasher
   */
  public static final HashFunction MURMUR3_32 = (item, hashes) -> {
    int h1 = Murmur3_x86_32.hashUnsafeBytes(item, Platform.BYTE_ARRAY_OFFSET, item.length, 0);
    int h2 = Murmur3_x86_32.hashUnsafeBytes(item, Platform.BYTE_ARRAY_OFFSET, item.length, h1);

    for (int i = 1; i <= hashes.length; i++) {
      int combinedHash = h1 + (i * h2);
      // Flip all the bits if it's negative (guaranteed positive number)
      if (combinedHash < 0) {
        combinedHash = ~combinedHash;
      }
      hashes[i-1] = combinedHash;
    }
  };

  /**
   * 128 bit Murmur3 hasher
   */
  public static final HashFunction MURMUR3_128 = (item, hashes) -> {
    long[] hash = new long[2];
    Murmur3_128.hashBytes(item, 0, hash);
    long h1 = hash[0];
    long h2 = hash[1];

    long combinedHash = h1;
    for (int i = 1; i <= hashes.length; i++) {
      // Make combinedHash positive and indexable
      hashes[i-1] = combinedHash & Long.MAX_VALUE;
      combinedHash += h2;
    }
  };
}
