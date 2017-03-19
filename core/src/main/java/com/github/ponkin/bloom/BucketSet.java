package com.github.ponkin.bloom;

import java.io.Closeable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * bit array implementation with
 * pre-defined bucket size.
 *
 * @author Alexey Ponkin
 *
 */
public class BucketSet implements Closeable {

  private static final Logger log = Logger.getLogger(BucketSet.class.getName());

  /**
   * masks for tags - masks[i]
   * where MASKS[i] - long with all zeros except 1 in i position
   */
  private static final long[] MASKS = {
    0L, 1L, 1L << 1, 1L << 2, 1L << 3, 1L << 4, 1L << 5, 1L << 6, 1L << 7, 1L << 8,
    1L << 9,  1L << 10, 1L << 11, 1L << 12, 1L << 13, 1L << 14, 1L << 15, 1L << 16,
    1L << 17, 1L << 18, 1L << 19, 1L << 20, 1L << 21, 1L << 22, 1L << 23, 1L << 24,
    1L << 25, 1L << 26, 1L << 27, 1L << 28, 1L << 29, 1L << 30, 1L << 31, 1L << 32,
    1L << 33, 1L << 34, 1L << 35, 1L << 36, 1L << 37, 1L << 38, 1L << 39, 1L << 40,
    1L << 41, 1L << 42, 1L << 43, 1L << 44, 1L << 45, 1L << 46, 1L << 47, 1L << 48,
    1L << 49, 1L << 50, 1L << 51, 1L << 52, 1L << 53, 1L << 54, 1L << 55, 1L << 56,
    1L << 57, 1L << 58, 1L << 59, 1L << 60, 1L << 61, 1L << 62, 1L << 63, 1L << 64,
  };

  private final int tagsPerBucket;
  private final int bytesPerBucket;
  private final long numBuckets;
  private final int bitsPerTag;
  private final BitSet bitset;

  BucketSet(int bitsPerTag, int tagsPerBucket, long numBuckets, BitSet bitset) {
    this.bitset = bitset;
    this.bitsPerTag = bitsPerTag;
    this.tagsPerBucket = tagsPerBucket;
    bytesPerBucket = (bitsPerTag * tagsPerBucket + 7) >> 3;
    this.numBuckets = numBuckets;
    log.log(Level.INFO,
      String.format(
        "Bucket set: %1$d buckets, %2$d tags per bucket, %3$d bits per tag, %4$d total bits",
          numBuckets, tagsPerBucket, bitsPerTag, bitset.bitSize()));
  }


  /**
   * Append tag to bucket if there is free space
   * If tag is already in the bucket just return true
   *
   * @param bucketIdx target bucket index
   * @param tag tag to append
   * @return true if there is free space in bucket and
   * tag was successfully appended, or tag is already inside bucket, false otherwise
   */
  boolean append(long bucketIdx, long tag) {
    boolean result = true;
    if(checkTag(bucketIdx, tag) == -1) { // no tag exists in the bucket
      int pos = getFreePosInBucket(bucketIdx);
      if(pos > -1) {
        writeTag(bucketIdx, pos, tag);
        result = true;
      } else {
        result = false; // no free space avaialable
      }
    }
    return result;
  }

  /**
   * Calculate position of tag inside underlying
   * bit vector
   *
   * @param bucketIdx target bucket index
   * @param posInBucket position inside target bucket
   * @return index of first bit inside underlying bit vector
   */
  private final long startPos(long bucketIdx, int posInBucket) {
    return bucketIdx*tagsPerBucket*bitsPerTag + posInBucket*bitsPerTag;
  }

  /**
   * Overwrite tag with zeros inside underlying bit vector
   *
   * @param bucketIdx target bucket index
   * @param posInBucket target position in the bucket
   */
  void deleteTag(long bucketIdx, int posInBucket) {
    writeTag(bucketIdx, posInBucket, 0L);
  }

  /**
   * Put tag in concrete bucket and position
   * overwriting previous value
   *
   * @param bucketIdx target bucket index
   * @param posInBucket position inside bucket
   * @param tag value
   */
  void writeTag(long bucketIdx, int posInBucket, long tag) {
    long tagIdx = startPos(bucketIdx, posInBucket);
    long mask = MASKS[bitsPerTag];
    for(long i=tagIdx; i<tagIdx+bitsPerTag; i++) {
      if((mask & tag) == 0L) {
        bitset.unset(i);
      } else {
        bitset.set(i);
      }
      mask = mask >> 1;
    }
  }

  /**
   * Read tag in bucket with bucketIdx from pos
   *
   * @param bucketIdx target bucket index
   * @param posInBucket concrete tag position in bucket
   * @return tag in given position inside bucket
   */
  final long readTag(long bucketIdx, int posInBucket) {
    long tagIdx = startPos(bucketIdx, posInBucket);
    long tag = 0L;
    long mask = MASKS[bitsPerTag];
    for(long i=tagIdx; i<tagIdx+bitsPerTag; i++) {
      if(bitset.get(i)) {
        tag |= mask;
      }
      mask = mask >> 1;
    }
    return tag;
  }

  /**
   * Return first free position
   * in bucket if exists
   *
   * @param bucketIdx - target bucket index
   * @return free position in given bucket or -1 if none
   */
  int getFreePosInBucket(long bucketIdx) {
    return checkTag(bucketIdx, 0L);
  }

  /**
   * Check whether given tag exists
   * in the bucket
   *
   * @param tag - tag to check
   * @return index of given tag or -1 if none
   */
  int checkTag(long bucketIdx, long tag) {
    for(int pos=0; pos<tagsPerBucket; pos++) {
      if(tag == readTag(bucketIdx, pos)) {
        return pos;
      }
    }
    return -1;
  }

  long sizeInBits() {
    return bitset.bitSize();
  }

  void putAll(BucketSet other) throws Exception {
    this.bitset.putAll(other.bitset);
  }
  
  @Override
  public void close() {
    log.log(Level.INFO, "Closing Bucket Set");
    try{
      bitset.close();
    } catch (Exception err) {
      log.log(Level.SEVERE, "Can not close BucketSet", err);
    }
  }

  public void clear() {
    bitset.clear();
  }
}

