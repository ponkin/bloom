package com.github.ponkin.bloom

import org.apache.commons.lang3.StringUtils

import scala.util.Random
import org.scalatest.FunSuite // scalastyle:ignore funsuite

class BloomFilterSuite extends FunSuite { // scalastyle:ignore funsuite
  private final val EPSILON = 0.01
  private final val numItems = 100000
  private val itemGen: Random => String = { r =>
    r.nextString(r.nextInt(512))
  }

  test(s"accuracy - String") {
    // use a fixed seed to make the test predictable.
    val r = new Random(37)
    val fpp = 0.02
    val numInsertion = numItems / 10

    val allItems = Array.fill(numItems)(itemGen(r))

    val filter = BloomFilter.builder
      .withExpectedNumberOfItems(numInsertion)
      .withFalsePositiveRate(fpp)
      .build()

    // insert first `numInsertion` items.
    var inserted = allItems.take(numInsertion).filter(StringUtils.isNotEmpty)
    inserted.foreach(filter.put)

    // false negative is not allowed.
    assert(inserted.forall(filter.mightContain))

    // The number of inserted items doesn't exceed `expectedNumItems`, so the `expectedFpp`
    // should not be significantly higher 
    // than the one we passed in to create this bloom filter.
    assert(filter.expectedFpp() - fpp < EPSILON)

    val errorCount = allItems.drop(numInsertion).count(filter.mightContain)

    // Also check the actual fpp is not significantly higher than we expected.
    val actualFpp = errorCount.toDouble / (numItems - numInsertion)
    assert(actualFpp - fpp < EPSILON)
  }

  test(s"mergeInPlace - String") {
    // use a fixed seed to make the test predictable.
    val r = new Random(37)

    val items1 = Array.fill(numItems / 2)(itemGen(r)).filter(StringUtils.isNotEmpty)
    val items2 = Array.fill(numItems / 2)(itemGen(r)).filter(StringUtils.isNotEmpty)

    val filter1 = BloomFilter.builder
      .withFalsePositiveRate(Utils.DEFAULT_FPP)
      .withExpectedNumberOfItems(numItems)
      .build()
    items1.foreach(filter1.put)

    val filter2 = BloomFilter.builder
      .withFalsePositiveRate(Utils.DEFAULT_FPP)
      .withExpectedNumberOfItems(numItems)
      .build()
    items2.foreach(filter2.put)

    filter1.mergeInPlace(filter2)

    // After merge, `filter1` has `numItems` items which doesn't exceed `expectedNumItems`,
    // so the `expectedFpp` should not be significantly higher than the default one.
    assert(filter1.expectedFpp() - Utils.DEFAULT_FPP < EPSILON)
    items1.foreach(i => assert(filter1.mightContain(i)))
    items2.foreach(i => assert(filter1.mightContain(i)))
  }

  test("incompatible merge") {
    intercept[IncompatibleMergeException] {
      BloomFilter.builder
        .withExpectedNumberOfItems(1000)
        .build()
        .mergeInPlace(null)
    }

    intercept[IncompatibleMergeException] {
      val filter1 = BloomFilter.builder
        .withFalsePositiveRate(0.001)
        .withExpectedNumberOfItems(1000)
        .build()
      val filter2 = BloomFilter.builder
        .withFalsePositiveRate(0.002)
        .withExpectedNumberOfItems(1000)
        .build()
      filter1.mergeInPlace(filter2)
    }

    intercept[IncompatibleMergeException] {
      val filter1 = BloomFilter.builder
        .withFalsePositiveRate(0.001)
        .withExpectedNumberOfItems(1000)
        .build()
      val filter2 = BloomFilter.builder
        .withFalsePositiveRate(0.001)
        .withExpectedNumberOfItems(2000)
        .build()
      filter1.mergeInPlace(filter2)
    }
  }
}
