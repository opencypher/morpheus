/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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

import org.opencypher.spark.api.SparkConfiguration._
import org.opencypher.spark.api.Tags
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.testing.CAPSTestSuite
import org.scalatest.Matchers
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class DataFrameOpsTest extends CAPSTestSuite with Matchers with GeneratorDrivenPropertyChecks {

  it("sets tags - DF") {
    val tag = 1

    val df = session.range(10)
    val ids = df.col("id")
    val tagged = ids.setTag(tag)
    val taggedDf = df.withColumn("tagged", tagged)

    val extracted = tagged.getTag
    val extractedTagDf = taggedDf.withColumn("extracted", extracted)
    extractedTagDf.select("extracted").collect().forall(row => row.get(0) == tag) shouldBe true
  }

  it("overwrites tags - DF") {
    val tag1 = 1
    val tag2 = 2

    val df = session.range(10)
    val ids = df.col("id")
    val tagged1 = ids.setTag(tag1)
    val tagged1Df = df.withColumn("tagged1", tagged1)
    val taggedIds = tagged1Df.col("tagged1")

    val tagged2 = taggedIds.setTag(tag2)
    val tagged2Df = tagged1Df.withColumn("tagged2", tagged2)

    val extracted = tagged2.getTag
    val extractedTagDf = tagged2Df.withColumn("extracted", extracted)

    extractedTagDf.select("extracted").collect().forall(row => row.get(0) == tag2) shouldBe true
  }

  it("replaces a tag with other tag - DF") {
    val tag1 = 1
    val tag2 = 2
    val tag3 = 3

    val df1 = session.range(2)
    val taggedDf1 = df1.withColumn("id", df1.col("id").setTag(tag1))

    val df2 = session.range(2)
    val taggedDf2 = df2.withColumn("id", df2.col("id").setTag(tag2))

    val unionDf = taggedDf1.union(taggedDf2)

    val replacedDf = unionDf.withColumn("id", unionDf.col("id").replaceTag(tag1, tag3))

    val idRows = replacedDf.select(replacedDf.col("id").getTag).collect()

    idRows.toList.map(_.get(0).asInstanceOf[Long]).sorted shouldBe List(2, 2, 3, 3)
  }

  def toTag(n: Int) = (n & Int.MaxValue) % 1000
  def toId(n: Int) = (n & Int.MaxValue) >>> Tags.tagBits

  it("sets tags - Scala") {
    forAll { (n1: Int, n2: Int) =>
      val tag = toTag(n1)
      val id = toId(n2)

      id.setTag(tag).getTag should equal(tag)
    }
  }

  it("overwrites tags - Scala") {
    forAll { (n1: Int, n2: Int, n3: Int) =>
      val tag1 = toTag(n1)
      val tag2 = toTag(n2)
      val id = toId(n3)

      id.setTag(tag1).setTag(tag2).getTag should equal(tag2)
    }
  }

  it("replaces a tag with another tag - Scala") {
    forAll { (n1: Int, n2: Int, n3: Int, n4: Int) =>
      val tag1 = toTag(n1)
      val tag2 = toTag(n2)
      val tag3 = toTag(n3)
      val id = toId(n4)

      val afterOpTag = id.setTag(tag1).replaceTag(tag2, tag3).getTag

      if (tag1 == tag2) {
        afterOpTag should equal(tag3)
      } else {
        afterOpTag should equal(tag1)
      }
    }
  }

}
