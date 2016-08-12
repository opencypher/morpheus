package org.opencypher.spark.impl.util

import org.opencypher.spark.StdTestSuite

class ProductConversionTest extends StdTestSuite {

  test("Converts vector to products") {
    (1 to 9).foreach { size =>
      val builder = Vector.newBuilder[Int]
      (1 to size).foreach { i => builder += i }
      val vector = builder.result()

      vector.toProduct.toVector should equal(vector)
    }
  }

  test("Get single value from product") {
    (1 to 9).foreach { size =>

      val builder = Vector.newBuilder[Int]
      (1 to size).foreach { i => builder += i * i }
      val product = builder.result().toProduct

      (0 until size).foreach { i =>
        product.getAs[Int](i) should equal((i+1) * (i+1))
      }
    }
  }

  test("Append one element to a product") {
    (1 until 9).foreach { size =>

      val builder = Vector.newBuilder[Int]
      (1 to size).foreach { i => builder += i * i }
      val product = builder.result().toProduct

      val result = product :+ (size * 10)

      result.get(size) should equal (size * 10)
    }
  }
}
