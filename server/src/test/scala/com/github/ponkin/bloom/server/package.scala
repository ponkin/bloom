package com.github.ponkin.bloom

import org.scalacheck.{ Gen, Arbitrary }

package object server {

  val mapGen: Gen[Map[String, String]] = for {
    keyValue <- Gen.listOf(Gen.alphaStr)
  } yield (keyValue zip keyValue).toMap

  val strOfLen: Int => Gen[String] = Gen.listOfN(_, Gen.alphaChar).map(_.mkString)

  val filterDescGen: Gen[FilterDescriptor] = for {
    name <- strOfLen(10)
    filterType <- Gen.oneOf(BloomType.Standart, BloomType.Stable, BloomType.Cuckoo)
    maxElements <- Gen.posNum[Long]
    fpp <- Gen.posNum[Double]
    dataPath <- Gen.option(Gen.alphaStr)
    options <- mapGen
  } yield FilterDescriptor(name, filterType, maxElements, fpp, dataPath, options)

  val listOfDescriptors: Gen[List[FilterDescriptor]] = Gen.listOfN(10, filterDescGen)
}

