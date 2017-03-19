package com.github.ponkin.bloom;

import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Lock;

import java.util.logging.Logger;
import java.util.logging.Level;

import java.io.File;
import java.io.IOException;

public class BloomFilter implements Filter {

  private static final Logger log = Logger.getLogger(BloomFilter.class.getName());

  private final int numHashFunctions;

  private final BitSet bits;

  /**
   * Mask for fast division by 32
   * since we have default parallelism = 32
   * https://graphics.stanford.edu/~seander/bithacks.html#ModulusDivision
   */
  private static final long FAST_MOD_32 = 0x1FL;

  private final HashFunction strategy;

  /**
   * Number of memory segments
   * Clients can put items in each segment concurrently
   */
  private static final int DEFAULT_CONCURRENCY_LEVEL = 32; // parallelism

  private final Lock[] segments = new Lock[DEFAULT_CONCURRENCY_LEVEL];

  BloomFilter(BitSet bits, int numHashFunctions, HashFunction strategy) {
    log.log(Level.INFO,
      String.format(
        "Bloom filter: %1$d hash functions, %2$d bits",
          numHashFunctions, bits.bitSize()));
    this.bits = bits;
    this.numHashFunctions = numHashFunctions;
    this.strategy = strategy;
    for(int i=0;i<DEFAULT_CONCURRENCY_LEVEL;i++) {
      segments[i] = new ReentrantLock();
    }
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (other == null || !(other instanceof BloomFilter)) {
      return false;
    }
    BloomFilter that = (BloomFilter) other;
    return this.numHashFunctions == that.numHashFunctions && this.bits.equals(that.bits);
  }

  @Override
  public int hashCode() {
    return bits.hashCode() * 31 + numHashFunctions;
  }

  @Override
  public double expectedFpp() {
    return Math.pow((double) bits.cardinality() / bits.bitSize(), numHashFunctions);
  }

  public int getNumOfHashFunctions(){
    return this.numHashFunctions;
  }

  public long bitSize() {
    return bits.bitSize();
  }

  @Override
  public boolean put(byte[] item) {
    long bitSize = bits.bitSize();
    long[] hashes = new long[numHashFunctions];
    strategy.hashes(item, hashes);

    boolean bitsChanged = false;
    for (int i = 0; i < hashes.length; i++) {
      // x mod n = x & (n-1) if n equals power of 2
      // hashes[i] is always positive
      Lock currentLock = segments[(int)(hashes[i] & FAST_MOD_32)];
      currentLock.lock();
      try { // just in case something goes wrong
        bitsChanged |= bits.set(hashes[i] % bitSize);
      } finally {
        currentLock.unlock();
      }
    }
    return bitsChanged;
  }

  @Override
  public boolean remove(byte[] item) {
    throw new UnsupportedOperationException("Bloom filter does not support removal");
  }

  @Override
  public boolean mightContain(byte[] item) {
    long bitSize = bits.bitSize();
    long[] hashes = new long[numHashFunctions];
    strategy.hashes(item, hashes);

    boolean mightContain = true;
    for(int i = 0; i < hashes.length && mightContain; i++) {
      if (!bits.get(hashes[i] % bitSize)) {
        mightContain = false;
      }
    }
    return mightContain;
  }

  /**
   * Fill underlying bit vector with all 0
   */
  @Override
  public void clear() {
    for (Lock seg : segments) {
      seg.lock();
    }
    try { // just in case something goes wrong
      bits.clear();
    } finally {
      for (Lock seg : segments) {
        seg.unlock();
      }
    }
  }
  
  @Override
  public void close(){
    try{
      bits.close();
    } catch (Exception err) {
      log.log(Level.SEVERE, "Can not close BloomFilter", err);
    }
  }

  @Override
  public Filter mergeInPlace(Filter other) throws Exception {
    if (other == null) {
      throw new IncompatibleMergeException("Cannot merge null bloom filter");
    }

    if (!(other instanceof BloomFilter)) {
      throw new IncompatibleMergeException(
          "Cannot merge bloom filter of class " + other.getClass().getName()
          );
    }

    BloomFilter that = (BloomFilter) other;

    if (this.bitSize() != that.bitSize()) {
      throw new IncompatibleMergeException("Cannot merge bloom filters with different bit size");
    }

    if (this.numHashFunctions != that.numHashFunctions) {
      throw new IncompatibleMergeException(
          "Cannot merge bloom filters with different number of hash functions"
          );
    }

    for (Lock seg : segments) {
      seg.lock();
    }
    try { // just in case something goes wrong
      this.bits.putAll(that.bits);
    } finally {
      for (Lock seg : segments) {
        seg.unlock();
      }
    }
    return this;
  }

  public static FilterBuilder<BloomFilter> builder() {
    return new Builder();
  }

  /**
   * Builder for BloomFilter
   */
  static class Builder implements FilterBuilder<BloomFilter> {
    private double fpp = Utils.DEFAULT_FPP;
    private long capacity = 0L;
    private File file = null;
    private boolean useOffHeapMemory = false;
    private HashFunction hasher = Hashers.MURMUR3_128;

    private Builder() {
      super();
    }

    public Builder withFalsePositiveRate(double fpp) {
      Utils.checkArgument(fpp > 0.0 && fpp < 1.0,
         String.format("False positive rate(%s) must be in range (0, 1)", fpp));
      this.fpp = fpp;
      return this;
    }

    public Builder withExpectedNumberOfItems(long expected) {
      Utils.checkArgument(expected > 0,
         String.format("Expected number of insertions (%s) must be > 0", expected ));
      this.capacity = expected;
      return this;
    }

    public Builder useOffHeapMemory(boolean useOffHeapMemory) {
      this.useOffHeapMemory = useOffHeapMemory;
      return this;
    }

    public Builder withFileMapped(File file) {
      this.file = file;
      return this;
    }

    @Override
    public FilterBuilder withHasher(HashFunction hasher) {
      this.hasher = hasher;
      return this;
    }

    public BloomFilter build() throws IOException {
      if(!useOffHeapMemory) {
        Utils.checkArgument(file == null,
           String.format("Can not map file(%s) to on-heap bit vector", file));
      }

      long numBits = Utils.optimalNumOfBits(capacity, fpp);
      log.log(Level.INFO, "Optimal num bits are"+String.valueOf(numBits));
      int numHashFunctions = Utils.optimalNumOfHashFunctions(capacity, numBits);

      BitSet bitset = null;
      if(file != null) {
        bitset = new OffHeapBitArray(file, numBits);
      } else {
        if(useOffHeapMemory) {
          bitset = new OffHeapBitArray(numBits);
        } else {
          bitset = new BitArray(numBits);
        }
      }
      return new BloomFilter(bitset, numHashFunctions, hasher);
    }
  }
}
