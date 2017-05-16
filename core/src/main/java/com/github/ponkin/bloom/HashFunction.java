package com.github.ponkin.bloom;

/**
 * Common functional interface for
 * all hashing strategies.
 * All available embeded hash functions can be found in {@link com.github.ponkin.bloom.Hashers}
 *
 * @author Alexey Ponkin
 */
@FunctionalInterface
public interface HashFunction {

  /**
   * For speed and memory eficiency
   * we do not return anything but
   * reuse <code>hashes</code> array instead.
   * so after method execution all data 
   * inside hashes will be overwriten.
   * Method will generate as many hashes as
   * array size.
   *
   * @param item to hash
   * @param hashes array where all independent hashes will be stored
   */
  void hashes(byte[] item, long[] hashes);

}
