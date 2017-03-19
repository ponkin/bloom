package com.github.ponkin.bloom.server

import java.nio.file.Files
import org.scalatest.prop.Checkers
import org.scalatest.FunSuite
import org.scalacheck.Arbitrary._
import org.scalacheck.Prop._

import org.scalacheck.Prop.forAll

import org.scalatest.FunSuite // scalastyle:ignore funsuite

class DiskStoreManagerSuite extends FunSuite with Checkers {

  import KryoSerializer._

  test("Save/Read descriptors in directories") {
    check {
      forAll(listOfDescriptors) { descs =>
        val tmpDir = Files.createTempDirectory("disk_store_manager_tests")
        val mgr = new DiskStoreManager(tmpDir.toFile)
        // descriptor is saved in file with name FilterDescriptor.name
        // remove descriptors with the same name field
        val unique = descs.groupBy(_.name).mapValues(_.head).map(_._2)
        val saveResults = unique.map(mgr.save _)
        val mgr2 = new DiskStoreManager(tmpDir.toFile) // now read from the same directory
        unique.forall(mgr2.listAll.contains _)
      }
    }
  }
}
