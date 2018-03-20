/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.spark.schema

import java.lang

import org.apache.spark.sql.{Column, Dataset, functions}
import org.opencypher.spark.test.CAPSTestSuite
import org.scalatest.Matchers

class GraphTagsTest extends CAPSTestSuite with Matchers {

  val nodeId1 = 1L
  val nodeId2 = 2L

  val totalBits = 64
  val idBits = 50

  val tagMask: Long = -1L << idBits
  val invertedTagMask: Long = ~tagMask
  val invertedTagMaskLit: Column = functions.lit(invertedTagMask)

  def setTag(nodeId: Long, tag: Long): Long = {
    require((nodeId & tagMask) == 0L)
    val r = (nodeId & invertedTagMask) | (tag << idBits)
    println(s"setTag(nodeId=$nodeId, tag=$tag)=$r")
    r
  }

  def getTag(nodeId: Long): Long = {
    val r = (nodeId & tagMask) >> idBits
    println(s"getTag(nodeId=$nodeId)=$r")
    r
  }

  implicit class Tagging(val nodeId: Column) {

    def replaceTag(from: Long, to: Long): Column = functions
      .when(getTag === functions.lit(from), setTag(to))
      .otherwise(nodeId)

    def setTag(tag: Long): Column = {
      val tagLit = functions.lit(tag << idBits)
      val newId = nodeId
        .bitwiseAND(invertedTagMaskLit)
        .bitwiseOR(tagLit)
      newId
    }

    def getTag: Column = functions.shiftRight(nodeId, idBits)

  }

  it("successfully sets tags") {
    val tag = 1L

    val df: Dataset[lang.Long] = sparkSession.range(10)
    val ids = df.col("id")
    val tagged = ids.setTag(tag)
    val taggedDf = df.withColumn("tagged", tagged)

    val extracted = tagged.getTag
    val extractedTagDf = taggedDf.withColumn("extracted", extracted)
    extractedTagDf.select("extracted").collect().forall(row => row.get(0) == tag) shouldBe true
  }

  it("should overwrite tags") {
    val tag1 = 1L
    val tag2 = 2L

    val df = sparkSession.range(10)
    val ids = df.col("id")
    val tagged1 = ids.setTag(tag1)
    val tagged1Df = df.withColumn("tagged1", tagged1)
    val taggedIds = tagged1Df.col("tagged1")

    val tagged2 = taggedIds.setTag(tag2)
    val tagged2Df = tagged1Df.withColumn("tagged2", tagged2)

    val extracted = tagged2.getTag
    val extractedTagDf = tagged2Df.withColumn("extracted", extracted)

    extractedTagDf.show()
    extractedTagDf.select("extracted").collect().forall(row => row.get(0) == tag2) shouldBe true
  }

  it("should replace tag with other tag") {
    val tag1 = 1L
    val tag2 = 2L
    val tag3 = 3L

    val df1 = sparkSession.range(2)
    val taggedDf1 = df1.withColumn("id", df1.col("id").setTag(tag1))

    val df2 = sparkSession.range(2)
    val taggedDf2 = df2.withColumn("id", df2.col("id").setTag(tag2))

    val unionDf = taggedDf1.union(taggedDf2)

    val replacedDf = unionDf.withColumn("id", unionDf.col("id").replaceTag(tag1, tag3))

    val idRows = replacedDf.select(replacedDf.col("id").getTag).collect()

    idRows.toList.map(_.get(0).asInstanceOf[Long]).sorted shouldBe List(2, 2, 3, 3)
  }

}
