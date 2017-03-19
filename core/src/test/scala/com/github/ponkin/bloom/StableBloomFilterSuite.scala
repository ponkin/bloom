package com.github.ponkin.bloom

import org.apache.commons.lang3.StringUtils;

import scala.util.Random
import org.scalatest.FunSuite // scalastyle:ignore funsuite

class StableBloomFilterSuite extends FunSuite { // scalastyle:ignore funsuite
  private final val EPSILON = 0.01
  private final val numItems = 100000
  private val itemGen: Random => String = { r =>
    r.nextString(r.nextInt(512))
  }

  test(s"accuracy - String") {
    // use a fixed seed to make the test predictable.
    val r = new Random(37)
    val fpp = 0.01
    val numInsertion = numItems / 10

    val allItems = Array.fill(numItems)(itemGen(r))

    val filter = StableBloomFilter.builder
      .withBitsPerBucket(8)
      .withExpectedNumberOfItems(numInsertion)
      .withFalsePositiveRate(fpp)
      .build()

    // insert first `numInsertion` items.
    var inserted = allItems.take(numInsertion).filter(StringUtils.isNotEmpty)
    inserted.foreach(filter.put)

    // The number of inserted items doesn't exceed `expectedNumItems`, so the `expectedFpp`
    // should not be significantly higher 
    // than the one we passed in to create this bloom filter.
    assert(filter.expectedFpp() - fpp < EPSILON)

    val errorCount = allItems.drop(numInsertion).count(filter.mightContain)

    // Also check the actual fpp is not significantly higher than we expected.
    val actualFpp = errorCount.toDouble / (numItems - numInsertion)
    assert(actualFpp - fpp < EPSILON)
  }

}
