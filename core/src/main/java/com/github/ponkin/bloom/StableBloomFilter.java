package com.github.ponkin.bloom;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.io.File;
import java.io.IOException;

import static java.lang.Math.pow;

/**
 *
 * StableBloomFilter implements a Stable Bloom Filter as described by Deng and
 * Rafiei in Approximately Detecting Duplicates for Streaming Data using Stable
 * Bloom Filters:
 *   
 *    http://webdocs.cs.ualberta.ca/~drafiei/papers/DupDet06Sigmod.pdf
 *   
 * A Stable Bloom Filter (SBF) continuously evicts stale information so that it
 * has room for more recent elements. Like traditional Bloom filters, an SBF
 * has a non-zero probability of false positives, which is controlled by
 * several parameters. Unlike the classic Bloom filter, an SBF has a tight
 * upper bound on the rate of false positives while introducing a non-zero rate
 * of false negatives. The false-positive rate of a classic Bloom filter
 * eventually reaches 1, after which all queries result in a false positive.
 * The stable-point property of an SBF means the false-positive rate
 * asymptotically approaches a configurable fixed constant. A classic Bloom
 * filter is actually a special case of SBF where the eviction rate is zero, so
 * this package provides support for them as well.
 *   
 * Stable Bloom Filters are useful for cases where the size of the data set
 * isn't known a priori, which is a requirement for traditional Bloom filters,
 * and memory is bounded.  For example, an SBF can be used to deduplicate
 * events from an unbounded event stream with a specified upper bound on false
 *
 * @author Alexey Ponkin
 *
 */
public class StableBloomFilter implements Filter {

  private static final Logger log = Logger.getLogger(StableBloomFilter.class.getName());

  private final BucketSet bucketSet;

  private final int numHashFunctions;

  private final long numOfBuckets;

  /*
   * Larger values are for
   * larger gaps between duplicate items
   */
  private final int bitsPerBucket;

  private final HashFunction strategy;

  private final long bucketsToDecrement;

  /**
   * Mask for fast division by 32
   * since we have default parallelism = 32
   * Mask equals 31
   * https://graphics.stanford.edu/~seander/bithacks.html#ModulusDivision
   */
  private static final long FAST_MOD_32 = 0x1FL;

  /*
   * Number of memory segments
   * Clients can put items in each segment concurrently
   */
  private static final int DEFAULT_CONCURRENCY_LEVEL = 32; // parallelism

  private final ReentrantReadWriteLock[] segments = new ReentrantReadWriteLock[DEFAULT_CONCURRENCY_LEVEL];

  StableBloomFilter(BitSet bitset, long numOfBuckets, int bitsPerBucket, long bucketsToDecrement, int numHashFunctions, HashFunction strategy) {
    // allow 1 item per bucket
    this.bucketSet = new BucketSet(bitsPerBucket, 1, numOfBuckets, bitset);
    this.numHashFunctions = numHashFunctions;
    this.numOfBuckets = numOfBuckets;
    this.bitsPerBucket = bitsPerBucket;
    this.strategy = strategy;
    this.bucketsToDecrement = bucketsToDecrement;
    for(int i=0;i<DEFAULT_CONCURRENCY_LEVEL;i++) {
      segments[i] = new ReentrantReadWriteLock();
    }
    log.log(
        Level.FINE,
        String.format(
          "Stable Bloom filter: %1$d hash functions, %2$d bits, %3$d bits per elemnent",
          numHashFunctions,
          bitset.bitSize(),
          bitsPerBucket)
        );
  }

  @Override
  public boolean remove(byte[] bytes) {
    throw new UnsupportedOperationException("remove() method is not supported in StableBloomFilter");
  }

  @Override
  public boolean mightContain(byte[] bytes) {
    long[] hashes = new long[numHashFunctions];
    strategy.hashes(bytes, hashes);

    // if one of the buckets == 0 than return false
    boolean mightContain = true;
    for(int i = 0; i < hashes.length && mightContain; i++) {
      long idx = hashes[i] % numOfBuckets;
      ReentrantReadWriteLock.ReadLock currentLock = segments[(int)(idx & FAST_MOD_32)].readLock();
      currentLock.lock();
      try { // just in case something goes wrong
        long bucketVal = bucketSet.readTag(idx, 0);
        if (bucketVal == 0L) {
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
    long[] hashes = new long[numHashFunctions];
    strategy.hashes(bytes, hashes);
    // make room for new values
    decrement();
    for (int i = 0; i < hashes.length; i++) {
      long idx = hashes[i] % numOfBuckets;
      ReentrantReadWriteLock.WriteLock currentLock = segments[(int)(idx & FAST_MOD_32)].writeLock();
      currentLock.lock();
      try { // just in case something goes wrong
        bucketSet.writeTag(idx, 0, Utils.MASKS[bitsPerBucket]); // write max val for bucket
      } finally {
        currentLock.unlock();
      }
    }
    // forever true since we always overwrite bucket content
    return true;
  }

  @Override
  public double expectedFpp() {
    return pow(1D - stablePoint(), (double)numHashFunctions);
  }

  @Override
  public void clear() {
    bucketSet.clear();
  }

  @Override
  public void close() {
    log.log(Level.INFO, "Closing StableBloomFilter");
    bucketSet.close();
  }

  @Override
  public Filter mergeInPlace(Filter other) throws Exception {
    if (other == null) {
      throw new IncompatibleMergeException("Cannot merge null stable bloom filter");
    }

    if (!(other instanceof StableBloomFilter)) {
      throw new IncompatibleMergeException(
          "Cannot merge bloom filter of class " + other.getClass().getName()
          );
    }

    StableBloomFilter that = (StableBloomFilter) other;

    if (this.bucketSet.sizeInBits() != that.bucketSet.sizeInBits()) {
      throw new IncompatibleMergeException("Cannot merge bloom filters with different bit size");
    }

    if (this.numHashFunctions != that.numHashFunctions) {
      throw new IncompatibleMergeException(
          "Cannot merge bloom filters with different number of hash functions"
          );
    }

    // lock all segments
    ReentrantReadWriteLock.WriteLock[] locks = 
      new ReentrantReadWriteLock.WriteLock[segments.length];
    for(int i=0; i<segments.length; i++) {
      locks[i] = segments[i].writeLock();
      locks[i].lock();
    }
    try {
      this.bucketSet.putAll(that.bucketSet);
    } finally {
      for (ReentrantReadWriteLock.WriteLock seg : locks) {
        seg.unlock();
      }
    }
    return this;
  }

  /*
   * decrement will decrement a random cell and (p-1) adjacent cells by 1. This
   * is faster than generating p random numbers. Although the processes of
   * picking the p cells are not independent, each cell has a probability of p/m
   * for being picked at each iteration, which means the properties still hold.
   */
  private void decrement() {
    long pivot = ThreadLocalRandom.current().nextLong(numOfBuckets);
    for(int i=0; i<bucketsToDecrement; i++) {
      long idx = (pivot + i) % numOfBuckets;
      ReentrantReadWriteLock.WriteLock currentLock = segments[(int)(idx & FAST_MOD_32)].writeLock();
      currentLock.lock();
      try { // just in case something goes wrong
        long bucketVal = bucketSet.readTag(idx, 0);
        if(bucketVal != 0L) {
          bucketSet.writeTag(idx, 0, bucketVal-1);
        }
      } finally {
        currentLock.unlock();
      }
    }
  }
 
  /* 
   * stablePoint returns the limit of the expected fraction of zeros in the
   * Stable Bloom Filter when the number of iterations goes to infinity. When
   * this limit is reached, the Stable Bloom Filter is considered stable
   *
   * @return - limit of the expected fraction of 0`s
   */
  private double stablePoint() {
    double subDenom = ((double)bucketsToDecrement) * (1D / numHashFunctions - 1D / numOfBuckets);
    double denom    = 1D + 1D / subDenom;
    double base     = 1 / denom;
    return pow(base, Utils.MASKS[bitsPerBucket]);
  }

  /*
   * Optimal number of buckets to decrement
   * with given false positive rate, number of buckets,
   * bits per bucket and num of hash functions
   */
  static int optimalP(long numOfBuckets, int numOfHashFunctions, int bitsPerBucket, double fpRate) {
    double subDenom = pow(1D - pow(fpRate, (1D/(double)numOfHashFunctions)), 1D/(double)Utils.MASKS[bitsPerBucket]);
    double denom = (1D/subDenom - 1D) * (1D/(double)numOfHashFunctions - 1D/(double)numOfBuckets);
    int p = (int)(1/denom);
    if(p <= 0){
      p = 1;
    }
    return p;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for  StableBloomFilter
   */
  public static class Builder implements FilterBuilder<StableBloomFilter>{
    private double fpp = Utils.DEFAULT_FPP;
    private long capacity = 0L;
    private File file = null;
    private boolean useOffHeapMemory = false;
    private int bitsPerBucket = 1;
    private HashFunction hasher = Hashers.MURMUR3_128;

    private Builder() {
      super();
    }
    
    @Override
    public Builder withFalsePositiveRate(double fpp) {
      Utils.checkArgument(fpp > 0.0 && fpp < 1.0, 
          String.format("False positive rate(%f) must be in range (0, 1)", fpp));
      this.fpp = fpp;
      return this;
    }

    @Override
    public Builder withExpectedNumberOfItems(long expected) {
      Utils.checkArgument(expected > 0, 
          String.format("Expected number of insertions (%d) must be > 0", expected));
      this.capacity = expected;
      return this;
    }

    @Override
    public Builder useOffHeapMemory(boolean useOffHeapMemory) {
      this.useOffHeapMemory = useOffHeapMemory;
      return this;
    }

    @Override
    public Builder withFileMapped(File file) {
      this.file = file;
      return this;
    }

    @Override
    public FilterBuilder withHasher(HashFunction hasher) {
      this.hasher = hasher;
      return this;
    }

    public Builder withBitsPerBucket(int bitsPerBucket) {
      Utils.checkArgument(bitsPerBucket > 0 && bitsPerBucket < 64, 
          String.format("number of bits(%d) for each bucket must in range (0, 64)", bitsPerBucket));
      this.bitsPerBucket = bitsPerBucket;
      return this;
    }

    @Override
    public StableBloomFilter build() throws IOException {
      if(!useOffHeapMemory) {
        Utils.checkArgument(file == null,
           String.format("Can not map file(%s) to onheap bit vector", file));
      }

      long numBuckets = Utils.optimalNumOfBits(capacity, fpp);
      log.log(Level.FINE, "Optimal num of buckets are "+String.valueOf(numBuckets));
      int numHashFunctions = Utils.optimalNumOfHashFunctions(capacity, numBuckets);
      log.log(Level.FINE, "Optimal num of hash functions "+String.valueOf(numHashFunctions));
      // p - number of buckets to decrement
      long p = optimalP(numBuckets, numHashFunctions, bitsPerBucket, fpp);
      log.log(Level.FINE, "Number of buckets to decrment "+String.valueOf(p));

      BitSet bitset = null;
      if(file != null) {
        bitset = new OffHeapBitArray(file, numBuckets*bitsPerBucket);
      } else {
        if(useOffHeapMemory) {
          bitset = new OffHeapBitArray(numBuckets*bitsPerBucket);
        } else {
          bitset = new BitArray(numBuckets*bitsPerBucket);
        }
      }
      return new StableBloomFilter(bitset, numBuckets, bitsPerBucket, p, numHashFunctions, hasher);
    }
  }
}
