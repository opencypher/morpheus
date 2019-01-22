/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.spark.impl

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, functions}
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._
import org.opencypher.spark.impl.table.SparkTable.{DataFrameTable, _}
import org.opencypher.spark.testing.CAPSTestSuite
import org.scalatest.Matchers
import org.scalatest.prop.GeneratorDrivenPropertyChecks

import scala.collection.JavaConverters._
import scala.collection.mutable.WrappedArray.ofLong

class SparkTableTest extends CAPSTestSuite with Matchers with GeneratorDrivenPropertyChecks {
  import caps.sparkSession.sqlContext.implicits._

  it("it should cast integer columns to long") {

    val df = sparkSession.createDataFrame(List(
      Row(1, 2L, Array(42), Array(42), Array(42L), Row(42, 42L))
    ).asJava, StructType(Seq(
      StructField("a", IntegerType, nullable = true),
      StructField("b", LongType, nullable = false),
      StructField("c", ArrayType(IntegerType, containsNull = true), nullable = true),
      StructField("d", ArrayType(IntegerType, containsNull = false), nullable = false),
      StructField("e", ArrayType(LongType, containsNull = false), nullable = false),
      StructField("f", StructType(Seq(
        StructField("foo", IntegerType, true),
        StructField("bar", LongType, false)
      )), nullable = true)
    )))

    val updatedDf = df.castToLong

    updatedDf.schema should equal(StructType(Seq(
      StructField("a", LongType, nullable = true),
      StructField("b", LongType, nullable = false),
      StructField("c", ArrayType(LongType, containsNull = true), nullable = true),
      StructField("d", ArrayType(LongType, containsNull = false), nullable = false),
      StructField("e", ArrayType(LongType, containsNull = false), nullable = false),
      StructField("f", StructType(Seq(
        StructField("foo", LongType, true),
        StructField("bar", LongType, true)
      )), nullable = false)
    )))

    updatedDf.collect().toBag should equal(Bag(
      Row(1L, 2L, new ofLong(Array(42L)), new ofLong(Array(42L)), new ofLong(Array(42L)), Row(42L, 42L))
    ))
  }

  // These tests verify the workaround for https://issues.apache.org/jira/browse/SPARK-26572
  // in `DataFrameTable#distinct` is correct and actually works around the bug.
  describe("distinct workaround") {

    it("correct on single column") {
      val df: DataFrameTable = Seq(1, 1).toDF("idx")
      df.distinct.df.count() shouldBe 1
    }

    it("correct on multiple columns") {
      val df: DataFrameTable = Seq((1, 2, 3), (1, 2, 3)).toDF("one", "two", "three")
      df.distinct.df.count() shouldBe 1
    }

    it("correct on subset of columns") {
      val table: DataFrameTable = Seq((1, 2), (1, 3)).toDF("first", "second")
      val result = table.distinct("first").df
      result.count() shouldBe 1
      result.columns.toSeq should equal(table.df.columns.toSeq)
    }

    // This test ensures that our `DataFrameTable#distinct` workaround does not suffer from the same bug as the Spark
    // implementation.
    it("actually works around the Spark bug") {
      val baseTable: DataFrameTable = Seq(1, 1).toDF("idx")

      // Uses `DataFrameTable#distinct` workaround
      val distinctWithId = baseTable.distinct.df.withColumn("id", functions.monotonically_increasing_id())

      val monotonicallyOnRight = baseTable.df.join(distinctWithId, "idx")
      val monotonicallyOnLeft = distinctWithId.join(baseTable.df, "idx")

      // verify with scala distinct
      monotonicallyOnLeft.select("id").collect().map(_.get(0)).distinct.length shouldBe 1
      monotonicallyOnRight.select("id").collect().map(_.get(0)).distinct.length shouldBe 1
    }

    // This test reproduces the Spark `distinct` bug and asserts that the bug is present.
    // When this test fails, please remove the `DataFrameTable#distinct` workarounds,
    // and all tests in `distinct workaround`.
    it("detects if the Spark bug was fixed") {
      val baseTable = Seq(1, 1).toDF("idx")

      // Uses Spark distinct
      val distinctWithId = baseTable.distinct.withColumn("id", functions.monotonically_increasing_id())

      val monotonicallyOnLeft = distinctWithId.join(baseTable, "idx")

      // Bug in Spark: "monotonically_increasing_id" is pushed down when it shouldn't be. Push down only happens when the
      // DF containing the "monotonically_increasing_id" expression is on the left side of the join.
      monotonicallyOnLeft.select("id").collect().map(_.get(0)).distinct.length shouldBe 2
    }

  }

}
