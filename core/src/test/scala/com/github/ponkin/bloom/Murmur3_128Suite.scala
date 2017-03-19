package com.github.ponkin.bloom

import org.scalatest.FunSuite // scalastyle:ignore funsuite

class Murmur3_128Suite extends FunSuite { // scalastyle:ignore funsuite

  test("Proper string hashing") {
    assertHash(0, 0x629942693e10f867L, 0x92db0b82baeb5347L, "hell")
    assertHash(1, 0xa78ddff5adae8d10L, 0x128900ef20900135L, "hello")
    assertHash(2, 0x8a486b23f422e826L, 0xf962a2c58947765fL, "hello ")
    assertHash(3, 0x2ea59f466f6bed8cL, 0xc610990acc428a17L, "hello w")
    assertHash(4, 0x79f6305a386c572cL, 0x46305aed3483b94eL, "hello wo")
    assertHash(5, 0xc2219d213ec1f1b5L, 0xa1d8e2e0a52785bdL, "hello wor")
    assertHash(0, 0xe34bbc7bbc071b6cL, 0x7a433ca9c49a9347L,
      "The quick brown fox jumps over the lazy dog")
    assertHash(0, 0x658ca970ff85269aL, 0x43fee3eaa68e5c3eL,
      "The quick brown fox jumps over the lazy cog")
  }

  def assertHash(seed: Int, expected1: Long, expected2: Long, stringInput: String) {
    val hash128bit = Array(0L, 0L)
    val data = stringInput.getBytes("UTF-8")
    Murmur3_128.hashBytes(data, seed, hash128bit)
    assert(expected1 == hash128bit(0))
    assert(expected2 == hash128bit(1))
  }
}
