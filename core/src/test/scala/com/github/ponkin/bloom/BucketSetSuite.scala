package com.github.ponkin.bloom

import org.scalatest.FunSuite // scalastyle:ignore funsuite

class BucketSetSuite extends FunSuite {

  val bitsPerTag = 31
  val tagsPerBucket = 7
  val numBuckets = 13

  val tag = Utils.MASKS(31)

  test("Append/Delete tag") {

    val bitset = new BitArray(bitsPerTag * tagsPerBucket * numBuckets)
    val bucketSet = new BucketSet(bitsPerTag, tagsPerBucket, numBuckets, bitset)

    assert(bucketSet.append(10, tag))
    val pos = bucketSet.checkTag(10, tag)
    assert(pos == 0) // first position in bucket is 0
    assert(bucketSet.readTag(10, pos) == tag)
    bucketSet.deleteTag(10, pos)
    assert(bucketSet.checkTag(10, tag) == -1)
  }

}
