/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.spark_legacy.impl.util

import org.opencypher.spark.BaseTestSuite

class ProductConversionTest extends BaseTestSuite {

  test("Converts vector to products") {
    (1 to 9).foreach { size =>
      val builder = Vector.newBuilder[Int]
      (1 to size).foreach { i => builder += i }
      val vector = builder.result()

      vector.asProduct.asVector should equal(vector)
    }
  }

  test("Get single value from product") {
    (1 to 9).foreach { size =>

      val builder = Vector.newBuilder[Int]
      (1 to size).foreach { i => builder += i * i }
      val product = builder.result().asProduct

      (0 until size).foreach { i =>
        product.getAs[Int](i) should equal((i+1) * (i+1))
      }
    }
  }

  test("Append one element to a product") {
    (1 until 9).foreach { size =>

      val builder = Vector.newBuilder[Int]
      (1 to size).foreach { i => builder += i * i }
      val product = builder.result().asProduct

      val result = product :+ (size * 10)

      result.get(size) should equal (size * 10)
    }
  }
}
