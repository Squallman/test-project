package com.shepel.test

import org.scalatest.flatspec.AnyFlatSpec

class CoordinatesUtilsTest extends AnyFlatSpec {
  "CoordinatesUtils" should "calculate distance correctly" in {
    val distance = CoordinatesUtils.calculateDistance(49.86361217816782, 24.01297751250349, 49.86258514162479, 24.013503261689955)
    assert(distance == 120.25829334624815)
  }
}
