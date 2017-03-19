package com.github.ponkin.bloom.server

import org.scalatest.prop.Checkers
import org.scalatest.FunSuite
import org.scalacheck.Arbitrary._
import org.scalacheck.Prop._

import org.scalacheck.Prop.forAll

import java.io.{ ByteArrayOutputStream, ByteArrayInputStream }

class FilterMetaSerializerSuite extends FunSuite with Checkers {

  test("Read/Write FilterDescriptor") {
    check {
      forAll(filterDescGen) { descriptor =>
        val bos = new ByteArrayOutputStream
        KryoSerializer.descriptorSerializer.writeTo(descriptor, bos).isSuccess &&
          (KryoSerializer.descriptorSerializer.readFrom(new ByteArrayInputStream(bos.toByteArray())) match {
            case Some(d) => d == descriptor
            case None => false
          })
      }
    }
  }
}
