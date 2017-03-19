package com.github.ponkin.bloom

import org.scalatest.FunSuite // scalastyle:ignore funsuite

import org.apache.commons.lang3.StringUtils;

import scala.util.Random

class CuckooFilterSuite extends FunSuite { // scalastyle:ignore funsuite
  private final val EPSILON = 0.01
  private final val numItems = 100000
  private val itemGen: Random => String = { r =>
    r.nextString(r.nextInt(512))
  }

  test(s"delete - String") {
    val r = new Random(37)
    val fpp = 0.01
    val numInsertion = numItems / 10

    val allItems = Array.fill(numItems)(itemGen(r))

    val filter = CuckooFilter.builder
      .withFalsePositiveRate(fpp)
      .withExpectedNumberOfItems(numInsertion)
      .build()

    // insert first `numInsertion` items.
    val inserted = allItems.take(numInsertion).filter(StringUtils.isNotEmpty)
    inserted.foreach(filter.put)

    assert(inserted.forall(filter.mightContain))

    inserted.foreach(filter.remove)

    assert(inserted.forall(!filter.mightContain(_)))
  }

  test(s"accuracy - String") {
    // use a fixed seed to make the test predictable.
    val r = new Random(37)
    val fpp = 0.01
    val numInsertion = numItems / 10

    val allItems = Array.fill(numItems)(itemGen(r))

    val filter = CuckooFilter.builder
      .withFalsePositiveRate(fpp)
      .withExpectedNumberOfItems(numInsertion)
      .build()

    // insert first `numInsertion` items.
    val inserted = allItems.take(numInsertion).filter(StringUtils.isNotEmpty)
    inserted.foreach(filter.put)

    // false negative is not allowed.
    assert(inserted.forall(filter.mightContain))

    // The number of inserted items doesn't exceed `expectedNumItems`, so the `expectedFpp`
    // should not be significantly higher than the one we passed in to create this bloom filter.
    assert(filter.expectedFpp() - fpp < EPSILON)

    val errorCount = allItems.drop(numInsertion).count(filter.mightContain)

    // Also check the actual fpp is not significantly higher than we expected.
    val actualFpp = errorCount.toDouble / (numItems - numInsertion)
    assert(actualFpp - fpp < EPSILON)
  }
}
