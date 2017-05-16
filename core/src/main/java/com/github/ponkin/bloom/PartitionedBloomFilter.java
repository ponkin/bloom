package com.github.ponkin.bloom;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.atomic.AtomicLong;
import java.io.File;
import java.io.IOException;

/**
 * PartitionedBloomFilter implements a variation of a classic Bloom filter as
 * described by Almeida, Baquero, Preguica, and Hutchison in Scalable Bloom
 * Filters:
 *
 * http://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf
 *
 * This filter works by partitioning the M-sized bit array into k slices of
 * size m = M/k bits. Each hash function produces an index over m for its
 * respective slice. Thus, each element is described by exactly k bits, meaning
 * the distribution of false positives is uniform across all elements.
 * This filter has slightly more false positive rate, because
 * filter will contain more 1`s in bit vector comparing with classic
 * Bloom filter.
 * There is no reason to use it instead of classic bloom filter.
 * We use it only inside {@link ScalableBloomFilter}
 * @see ScalableBloomFilter
 *
 * @author Alexey Ponkin
 */
class PartitionedBloomFilter implements Filter {

  private static final Logger log = Logger.getLogger(PartitionedBloomFilter.class.getName());

  /*
   * Using one contigous bit array
   * slices are just parts of
   * underlying bit vector
   */
  private final BitSet bits;

  private final int numHashFunctions;

  private final HashFunction strategy;

  /*
   * Size of slice  (bits.bitSize() / k)
   */
  private final long sliceSize;

  /**
   * Mask for fast division by 32
   * since we have default parallelism = 32
   * https://graphics.stanford.edu/~seander/bithacks.html#ModulusDivision
   */
  private static final long FAST_MOD_32 = 0x1FL;

  /*
   * Number of memory segments
   * Clients can put items in each segment concurrently
   */
  private static final int DEFAULT_CONCURRENCY_LEVEL = 32; // parallelism

  private final ReentrantReadWriteLock[] segments = new ReentrantReadWriteLock[DEFAULT_CONCURRENCY_LEVEL];

  /*
   * Number of items added
   */
  private final AtomicLong numItems;

  PartitionedBloomFilter(BitSet bits, int numHashFunctions, HashFunction strategy, long sliceSize) {
    log.log(Level.FINE,
      String.format(
        "PartitionedBloomFilter: %1$d hash functions, %2$d bits, %3$d slice length",
          numHashFunctions, bits.bitSize(), sliceSize));
    this.bits = bits;
    this.numHashFunctions = numHashFunctions;
    this.strategy = strategy;
    this.sliceSize = sliceSize; // sliceSize must be equals sliceSize*numHashFunctions
    this.numItems = new AtomicLong(0);
    for(int i=0;i<DEFAULT_CONCURRENCY_LEVEL;i++) {
      segments[i] = new ReentrantReadWriteLock();
    }
  }

  @Override
  public boolean remove(byte[] bytes) {
    throw new UnsupportedOperationException("remove() method is not supported in PartitionedBloomFilter");
  }

  @Override
  public boolean mightContain(byte[] bytes) {
    long[] hashes = new long[numHashFunctions];
    strategy.hashes(bytes, hashes);

    boolean mightContain = true;
    for(int i = 0; i < hashes.length && mightContain; i++) {
      long idx = i * sliceSize + (hashes[i] % sliceSize);
      // x mod n = x & (n-1) if n equals power of 2
      // idx is always positive
      ReentrantReadWriteLock.ReadLock currentLock = segments[(int)(idx & FAST_MOD_32)].readLock();
      currentLock.lock();
      try { // just in case something goes wrong
        if (!bits.get(idx)) {
          mightContain = false;
        }
      } finally {
        currentLock.unlock();
      }
    }
    return mightContain;
  }

  @Override
  public boolean put(byte[] bytes) {
    // each of k hashes has it`s own bit vector slice
    long[] hashes = new long[numHashFunctions];
    strategy.hashes(bytes, hashes);

    boolean bitsChanged = false;
    for (int i = 0; i < hashes.length; i++) {
      long idx = i * sliceSize + (hashes[i] % sliceSize);
      // x mod n = x & (n-1) if n equals power of 2
      // idx is always positive
      ReentrantReadWriteLock.WriteLock currentLock = segments[(int)(idx & FAST_MOD_32)].writeLock();
      currentLock.lock();
      try { // just in case something goes wrong
        bitsChanged |= bits.set(idx);
      } finally {
        currentLock.unlock();
      }
    }
    if(bitsChanged) {
      numItems.incrementAndGet();
    }
    return bitsChanged;
  }

  public double estimatedFillRatio() {
    return 1D - Math.exp((double)numItems.get()/(double)sliceSize);
  }

  @Override
  public void clear() {
    ReentrantReadWriteLock.WriteLock[] locks = 
      new ReentrantReadWriteLock.WriteLock[segments.length];
    for(int i=0; i<segments.length; i++) {
      locks[i] = segments[i].writeLock();
      locks[i].lock();
    }
    try { // just in case something goes wrong
      bits.clear();
    } finally {
      for (ReentrantReadWriteLock.WriteLock seg : locks) {
        seg.unlock();
      }
    }
  }

  @Override
  public Filter mergeInPlace(Filter other) throws Exception {
    if (other == null) {
      throw new IncompatibleMergeException("Cannot merge null bloom filter");
    }

    if (!(other instanceof PartitionedBloomFilter)) {
      throw new IncompatibleMergeException(
          String.format("Cannot merge bloom filter of class %s", other.getClass().getName()));
    }

    PartitionedBloomFilter that = (PartitionedBloomFilter) other;

    if (this.bitSize() != that.bitSize()) {
      throw new IncompatibleMergeException("Cannot merge bloom filters with different bit size");
    }

    if (this.numHashFunctions != that.numHashFunctions) {
      throw new IncompatibleMergeException(
          "Cannot merge bloom filters with different number of hash functions");
    }

    // lock all segments
    ReentrantReadWriteLock.WriteLock[] locks = 
      new ReentrantReadWriteLock.WriteLock[segments.length];
    for(int i=0; i<segments.length; i++) {
      locks[i] = segments[i].writeLock();
      locks[i].lock();
    }

    try { // just in case something goes wrong
      this.bits.putAll(that.bits);
    } finally {
      for (ReentrantReadWriteLock.WriteLock seg : locks) {
        seg.unlock();
      }
    }
    return this;
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
  public void close() {
    try{
      bits.close();
    } catch (Exception err) {
      log.log(Level.SEVERE, "Can not close PartitionedBloomFilter", err);
    }
  }
  
  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (other == null || !(other instanceof PartitionedBloomFilter)) {
      return false;
    }
    PartitionedBloomFilter that = (PartitionedBloomFilter) other;
    return this.numHashFunctions == that.numHashFunctions && this.bits.equals(that.bits);
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for PartitionedBloomFilter
   */
  public static class Builder implements FilterBuilder<PartitionedBloomFilter> {
    private double fpp = Utils.DEFAULT_FPP;
    private long capacity;
    private File file = null;
    private boolean useOffHeapMemory = false;
    private HashFunction hasher = Hashers.MURMUR3_128;

    private Builder() {
      super();
    }

    public Builder withFalsePositiveRate(double rate) {
      Utils.checkArgument(rate > 0.0 && rate < 1.0,
         String.format("False positive rate(%s) must be in range (0, 1)", rate));
      this.fpp = rate;
      return this;
    }

    public Builder withExpectedNumberOfItems(long expected) {
      Utils.checkArgument(expected> 0,
         String.format("Expected number of insertions (%s) must be > 0", expected));
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

    public PartitionedBloomFilter build() throws IOException {
      if(!useOffHeapMemory) {
        Utils.checkArgument(file == null,
           String.format("Can not map file(%s) to onheap bit vector", file));
      }

      BitSet bitset = null;
      long numBits = Utils.optimalNumOfBits(capacity, fpp);
      log.log(Level.FINE, "Optimal num bits are"+String.valueOf(numBits));
      int numHashFunctions = Utils.optimalNumOfHashFunctions(capacity, numBits);
      // align numBits with modulo k - to have equal size slices
      numBits = ((numBits+numHashFunctions-1) / numHashFunctions) * numHashFunctions;
      long sliceSize = numBits / numHashFunctions;
      if(file != null) {
        bitset = new OffHeapBitArray(file, numBits);
      } else {
        if(useOffHeapMemory) {
          bitset = new OffHeapBitArray(numBits);
        } else {
          bitset = new BitArray(numBits);
        }
      }
      return new PartitionedBloomFilter(bitset, numHashFunctions, hasher, sliceSize);
    }
  }
}
