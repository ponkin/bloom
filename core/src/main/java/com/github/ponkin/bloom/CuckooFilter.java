package com.github.ponkin.bloom;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.atomic.AtomicLong;
import java.io.File;
import java.io.IOException;

import static java.lang.Math.ceil;
import static java.math.RoundingMode.CEILING;
import static java.math.RoundingMode.UP;
import static java.lang.Math.pow;

/**
 * Cuckoo filter implementation.
 *
 * https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf
 *
 * Some parts were taken from 
 * https://github.com/bdupras/guava-probably
 *
 * @author Alexey Ponkin
 */
public class CuckooFilter implements Filter {

  /**
   * Maximum number of tries
   * to find alternative position for tag
   */
  private static final int MAX_KICK_NUM = 500;

  private static final int MAX_ENTRIES_PER_BUCKET = 8;
  private static final int MIN_ENTRIES_PER_BUCKET = 2;

  private static double MIN_FPP = 1.0D / pow(2, 60);
  private static double MAX_FPP = 0.99D;

  /**
   * Mask for fast division by 32
   * since we have default parallelism = 32
   * https://graphics.stanford.edu/~seander/bithacks.html#ModulusDivision
   */
  private static final long FAST_MOD_32 = 0x1FL;

  /**
   * Number of memory segments that can be
   * concurrently proccessed
   */
  private static final int DEFAULT_CONCURRENCY_LEVEL = 32; // parallelism

  private static final Logger log = Logger.getLogger(CuckooFilter.class.getName());

  private final BucketSet table;

  private final HashFunction strategy;

  private final ReentrantReadWriteLock[] segments = new ReentrantReadWriteLock[DEFAULT_CONCURRENCY_LEVEL];

  private final int bitsPerTag;
  private final long numBuckets;
  private final int tagsPerBucket;
  private final AtomicLong count;

  /**
   * Optimal number of
   * tags per one bucket
   * with given false positive rate
   *
   * @param e false positive rate
   */
  private static int optimalEntriesPerBucket(double e) {
    Utils.checkArgument(e > 0.0D,
       String.format("False positive rate must be > 0.0"));
    if (e <= 0.00001) {
      return MAX_ENTRIES_PER_BUCKET;
    } else if (e <= 0.002) {
      return MAX_ENTRIES_PER_BUCKET / 2;
    } else {
      return MIN_ENTRIES_PER_BUCKET;
    }
  }

  /**
   * Optimal load factor for cuckoo filter
   * with given number of tags inside one
   * bucket
   * 1.0 - filter is full
   * 0.0 - filter is empty
   *
   * @param b tags per bucket
   */
  private static double optimalLoadFactor(int b) {
    Utils.checkArgument(b == 2 || b == 4 || b == 8,
       String.format("Number of tags per bucket must be 2, 4, or 8"));
    if (b == 2) {
      return 0.84D;
    } else if (b == 4) {
      return 0.955D;
    } else {
      return 0.98D;
    }
  }

  /**
   * Optimal bits per entry
   * with given false positive rate
   * and number of tags per bucket
   *
   * @param e false positive rate
   * @param b tags per bucket
   */
  private static int optimalBitsPerEntry(double e, int b) {
    Utils.checkArgument(e >= MIN_FPP, 
        String.format("Cannot create CuckooFilter with FPP[%1$,.2f] < CuckooFilter.MIN_FPP[%2$,.2f]", e, MIN_FPP));
    return (int) ceil(Utils.log2((1 / e) + 3) / optimalLoadFactor(b));
  }

  /**
   * Optilmal number of buckets
   *
   * @param n expected number of elements
   * @param b number of tags inside one bucket
   */
  private static long optimalNumberOfBuckets(long n, int b) {
    Utils.checkArgument(n > 0,
       String.format("Expected number of elements must be > 0"));
    return evenCeil(divide((long) ceil(n / optimalLoadFactor(b)), b));
  }

  /**
   * Divide long with ceiling up to
   * positive infinity
   */
  private static long divide(long p, long q) {
    long div = p / q;
    return div + 1;
  }


  /**
   * Ceil number to closest
   * even predecessor
   */
  private static long evenCeil(long n) {
    return (n + 1) / 2 * 2;
  }

  CuckooFilter(int bitsPerTag, int tagsPerBucket, long numBuckets, BitSet bitset, HashFunction strategy) {
    this.strategy = strategy;
    this.table = new BucketSet(bitsPerTag, tagsPerBucket, numBuckets, bitset);
    this.bitsPerTag = bitsPerTag;
    this.numBuckets = numBuckets;
    this.tagsPerBucket = tagsPerBucket;
    this.count = new AtomicLong(0);
    for(int i=0; i<segments.length; i++) {
      segments[i] = new ReentrantReadWriteLock();
    }
  }

  public long count() {
    return count.get();
  }

  @Override
  public boolean put(byte[] item) {
    long[] hashes = new long[2];
    strategy.hashes(item, hashes);
    long bucketIdx = hashes[0] % numBuckets;
    long tag = fingerprint(hashes[1]);
    boolean itemAdded = false;
    ReentrantReadWriteLock.WriteLock lock = segments[(int)(bucketIdx & FAST_MOD_32)].writeLock();
    lock.lock();
    try {
      itemAdded = table.append(bucketIdx, tag);
    } finally {
      lock.unlock();
    }
    if(!itemAdded) {
      itemAdded = putInAlt(bucketIdx, tag);
    }
    if(itemAdded) {
      count.incrementAndGet();
    } else {
      log.log(Level.WARNING, String.format("Cucko table exceed capacity: %1$d elements", count.get()));
    }
    return itemAdded;
  }

  /**
   * Try to put tag in alternative bucket
   */
  private boolean putInAlt(long bucketIdx, long tag) {
    int kickNum = 0;
    long altIdx = altIndex(bucketIdx, tag);
    boolean itemAdded = false;
    while(!itemAdded && kickNum < MAX_KICK_NUM ) {
      ReentrantReadWriteLock.WriteLock lock = segments[(int)(altIdx & FAST_MOD_32)].writeLock();
      lock.lock();
      try {
        itemAdded = table.append(altIdx, tag);
        if(!itemAdded) {
          int posToKick = ThreadLocalRandom.current().nextInt(tagsPerBucket);
          long oldTag = table.readTag(altIdx, posToKick);
          table.writeTag(altIdx, posToKick, tag);
          tag = oldTag;
          altIdx = altIndex(altIdx, oldTag);
        }
      } finally {
        lock.unlock();
      }
      kickNum++;
    }
    return itemAdded;
  }

  @Override
  public boolean remove(byte[] item) {
    long[] hashes = new long[2];
    strategy.hashes(item, hashes);
    long bucketIdx = hashes[0] % numBuckets;
    long tag = fingerprint(hashes[1]);
    boolean itemDeleted = false;
    ReentrantReadWriteLock.WriteLock lock = segments[(int)(bucketIdx & FAST_MOD_32)].writeLock();
    lock.lock();
    try {
      int tagPos = table.checkTag(bucketIdx, tag);
      if(tagPos > -1) {
        table.deleteTag(bucketIdx,tagPos);
        itemDeleted = true;
      }
    } finally {
      lock.unlock();
    }
    if(!itemDeleted) {// check tag in alternative bucket
      long altIdx = altIndex(bucketIdx, tag);
      ReentrantReadWriteLock.WriteLock altLock = segments[(int)(altIdx & FAST_MOD_32)].writeLock();
      altLock.lock();
      try {
        int tagPos = table.checkTag(altIdx, tag);
        if(tagPos > -1) {
          table.deleteTag(altIdx, tagPos);
          itemDeleted = true;
        }
      } finally {
        altLock.unlock();
      }
    }
    if(itemDeleted) {
      count.decrementAndGet();
    }
    return itemDeleted;
  }

  @Override
  public boolean mightContain(byte[] item) {
    long[] hashes = new long[2];
    strategy.hashes(item, hashes);
    long bucketIdx = hashes[0] % numBuckets;
    long tag = fingerprint(hashes[1]);
    boolean mightContain = false;
    ReentrantReadWriteLock.ReadLock mainLock = segments[(int)(bucketIdx & FAST_MOD_32)].readLock();
    mainLock.lock();
    try {
      mightContain = table.checkTag(bucketIdx, tag) != -1L;
    } finally {
      mainLock.unlock();
    }
    if(!mightContain) {
      long altIdx = altIndex(bucketIdx, tag);
      ReentrantReadWriteLock.ReadLock altLock = segments[(int)(altIdx & FAST_MOD_32)].readLock();
      altLock.lock();
      try {
        mightContain = table.checkTag(altIdx, tag) != -1L;
      } finally {
        altLock.unlock();
      }
    }
    return mightContain;
  }

  @Override
  public void clear() {
    table.clear();
  }

  @Override
  public Filter mergeInPlace(Filter other) throws Exception {
    throw new UnsupportedOperationException("mergeInPlace method is not supported in CuckooFilter");
  }

  @Override
  public void close() {
    try{
      table.close();
    } catch (Exception err) {
      log.log(Level.SEVERE, "Can not close CuckooFilter", err);
    }
  }

  @Override
  public double expectedFpp() {
    double load = (double) count.get() / ((double) numBuckets*tagsPerBucket);
    return 1.0 - Math.pow((Math.pow(2, bitsPerTag) - 2.0 ) / (Math.pow(2, bitsPerTag) - 1.0 ) , 2.0 * tagsPerBucket * load);
  }  

  private final long fingerprint(long hash) {
    long tag = hash & Utils.MASKS[bitsPerTag];// remove unused bits
    while(tag == 0L) {
      hash = Murmur3_x86_32.hashLong(hash, 17);
      tag = hash & Utils.MASKS[bitsPerTag];
    }
    return tag;
  }

  /**
   * Calculate reversible hash code.
   * Simple linear probing.
   * Based on <code>tag</code> and current <code>bucketIdx</code>.
   * instead of 0x5bd1e995L  we can use anything else(big enough).
   * 0x5bd1e995L is just constant from Murmur Hashing algo.
   *
   * @param bucketIdx - index of bucket where tag is located
   * @param tag - item fingerprint
   * @return alternative bucket index where tag can be replaced
   */
  private final long altIndex(long bucketIdx, long tag) {
    long hash2 = (tag * 0x5bd1e995L) & Long.MAX_VALUE; 
    return Utils.mod(protectedSum(bucketIdx, parsign(bucketIdx) * odd(hash2), numBuckets), numBuckets);
  }

  /**
   * Get the parity sign
   */
  static long parsign(long i) {
    return (i & 0x01L) == 0L ? 1L : -1L;
  }

  static long odd(long i) {
    return i | 0x01L;
  }

  static long protectedSum(long index, long offset, long mod) {
    return canSum(index, offset) ? index + offset : protectedSum(index - mod, offset, mod);
  }

  static boolean canSum(long a, long b) {
    return (a ^ b) < 0 | (a ^ (a + b)) >= 0;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for CuckooFilter
   */
  static class Builder implements FilterBuilder<CuckooFilter>{
    private double fpp = 0.03;
    private long capacity = 0L;
    private File file = null;
    private boolean useOffHeapMemory = false;
    private HashFunction hasher = Hashers.MURMUR3_128;

    private Builder() {
      super();
    }

    public Builder useOffHeapMemory(boolean off) {
      this.useOffHeapMemory = off;
      return this;
    }
    
    public Builder withFalsePositiveRate(double fprate) {
      Utils.checkArgument(fprate > 0.0 && fprate < 1.0,
         String.format("False positive rate(%s) must be in range (0, 1)", fprate));
      this.fpp = fprate;
      return this;
    }

    public Builder withExpectedNumberOfItems(long expected) {
      Utils.checkArgument(expected > 0,
         String.format("Expected number of insertions (%s) must be > 0", expected));
      this.capacity = expected;
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

    public CuckooFilter build() throws IOException {
      if(!useOffHeapMemory) {
        Utils.checkArgument(file == null,
           String.format("Can not map file(%s) to onheap bit vector", file));
      }

      int tagsPerBucket = optimalEntriesPerBucket(fpp);
      long numBuckets = optimalNumberOfBuckets(capacity, tagsPerBucket);
      int bitsPerTag = optimalBitsPerEntry(fpp, tagsPerBucket);

      BitSet bitset = null;
      if(file != null) {
        bitset = new OffHeapBitArray(file, bitsPerTag * tagsPerBucket * numBuckets);
      } else {
        if (useOffHeapMemory) {
          bitset = new OffHeapBitArray(bitsPerTag * tagsPerBucket * numBuckets);
        } else {
          bitset = new BitArray(bitsPerTag * tagsPerBucket * numBuckets);
        }
      }
      return new CuckooFilter(bitsPerTag, tagsPerBucket, numBuckets, bitset, hasher);
    }
  }
}

