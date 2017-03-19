package com.github.ponkin.bloom

import scala.util.Random
import java.io.File

import org.scalatest.FunSuite // scalastyle:ignore funsuite

class OffHeapBitArraySuite extends FunSuite { // scalastyle:ignore funsuite

  test("set") {
    val file = File.createTempFile("test_set_bloom_filter", ".data")
    val bitArray = new OffHeapBitArray(file, 64)
    assert(bitArray.set(1))
    assert(bitArray.get(1))
    // Only returns true if the bit changed.
    assert(!bitArray.set(1))
    assert(bitArray.set(2))
    file.delete()
  }

  test("unset") {
    val file = File.createTempFile("test_unset_bloom_filter", ".data")
    val bitArray = new OffHeapBitArray(file, 64)
    assert(bitArray.set(1))
    assert(bitArray.unset(1))
    // Only returns true if the bit changed.
    assert(!bitArray.get(1))
    file.delete()
  }

  test("normal operation") {
    // use a fixed seed to make the test predictable.

    val r = new Random(37)
    val file = File.createTempFile("test_setget_bloom_filter", ".data")
    file.createNewFile()

    val bitArray = new OffHeapBitArray(file, 320)
    val indexes = (1 to 100).map(_ => r.nextInt(320).toLong).distinct

    indexes.foreach(bitArray.set)
    indexes.foreach(i => assert(bitArray.get(i)))
    assert(bitArray.cardinality() == indexes.length)
    file.delete()
  }

  test("restore filter state") {
    // use a fixed seed to make the test predictable.
    val r = new Random(37)
    val file = File.createTempFile("test_restore_bloom_filter", ".data")
    // file.createNewFile()
    val isEven: Long => Boolean = _ % 2 == 0
    val isOdd: Long => Boolean = _ % 2 != 0

    val bitArray = new OffHeapBitArray(file, 100)

    val indexes = (1L to 100L).toList

    indexes.filter(isEven).foreach(bitArray.set)
    assert(indexes.filter(isEven).forall(bitArray.get))
    bitArray.close()

    val bitArrayRestored = new OffHeapBitArray(file, 100)

    assert(indexes.filter(isEven).forall(bitArrayRestored.get))
    assert(indexes.filter(isOdd).forall(!bitArrayRestored.get(_)))
    file.delete()
  }
}
