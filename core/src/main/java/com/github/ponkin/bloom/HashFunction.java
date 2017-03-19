package com.github.ponkin.bloom;

/**
 * Common functional interface for
 * all hashing strategies
 *
 * @author Alexey Ponkin
 */
@FunctionalInterface
public interface HashFunction {

  void hashes(byte[] item, long[] hashes);

}
