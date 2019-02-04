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
package org.opencypher.spark.jmh

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.openjdk.jmh.annotations._

import scala.reflect.runtime.universe.TypeTag
import scala.util.Random

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
class JoinBenchmark {

  implicit var sparkSession: SparkSession = _

  val idColumn = "id"

  var leftData: List[Long] = _
  var rightData: List[Long] = _

  var leftLong: DataFrame = _
  var rightLong: DataFrame = _

  var leftArrayLong: DataFrame = _
  var rightArrayLong: DataFrame = _

  var leftNaiveString: DataFrame = _
  var rightNaiveString: DataFrame = _

  var leftByteArray: DataFrame = _
  var rightByteArray: DataFrame = _

  var leftEfficientString: DataFrame = _
  var rightEfficientString: DataFrame = _

  @Setup
  def setUp(): Unit = {
    sparkSession = SparkSession.builder().master("local[*]").getOrCreate()

    val rangeStart = 1000000000L
    val joinCount = 10000
    val leftRandomCount = 100000
    val rightRandomCount = 10000
    val joinRange = Range(0, joinCount).map(_.toLong + rangeStart)

    leftData = List.fill(leftRandomCount)(Random.nextLong()) ++ joinRange
    rightData = List.fill(rightRandomCount)(Random.nextLong()) ++ joinRange

    def prepareDf[A : TypeTag](data: List[A]): DataFrame = {
      sparkSession.createDataFrame(data.map(Tuple1(_))).toDF(idColumn)
    }

    leftLong = prepareDf(leftData).partitionAndCache
    rightLong = prepareDf(rightData).partitionAndCache

    leftArrayLong = prepareDf(leftData.map(Array(_))).partitionAndCache
    rightArrayLong = prepareDf(rightData.map(Array(_))).partitionAndCache

    leftNaiveString = prepareDf(leftData.map(_.toString)).partitionAndCache
    rightNaiveString = prepareDf(rightData.map(_.toString)).partitionAndCache

    leftByteArray = prepareDf(leftData.map(longToByteArray)).partitionAndCache
    rightByteArray = prepareDf(rightData.map(longToByteArray)).partitionAndCache

    leftEfficientString = prepareDf(leftData.map(longToByteArray)).castIdToString.partitionAndCache
    rightEfficientString = prepareDf(rightData.map(longToByteArray)).castIdToString.partitionAndCache
  }

  @Benchmark
  def joinLongIds(): Long = leftLong.join(rightLong, idColumn).count()

  @Benchmark
  def joinArrayLongIds(): Long = leftArrayLong.join(rightArrayLong, idColumn).count()

  @Benchmark
  def joinNaiveStringIds(): Long = leftNaiveString.join(rightNaiveString, idColumn).count()

  @Benchmark
  def joinByteArrayIds(): Long = leftByteArray.join(rightByteArray, idColumn).count()

  @Benchmark
  def joinEfficientStringIds(): Long = leftEfficientString.join(rightEfficientString, idColumn).count()


  implicit class DataFrameSetup(df: DataFrame) {

    def partitionAndCache : DataFrame = {
      val cached = df.repartition(10).persist(StorageLevel.MEMORY_ONLY)
      cached.count()
      cached
    }

    def castIdToString: DataFrame = df.select(df.col(idColumn).cast(StringType).as(idColumn))
  }

  private def longToByteArray(l: Long): Array[Byte] = {
    val a = new Array[Byte](8)
    a(0) = (l & 0xFF).asInstanceOf[Byte]
    a(1) = ((l >> 8) & 0xFF).asInstanceOf[Byte]
    a(2) = ((l >> 16) & 0xFF).asInstanceOf[Byte]
    a(3) = ((l >> 24) & 0xFF).asInstanceOf[Byte]
    a(4) = ((l >> 32) & 0xFF).asInstanceOf[Byte]
    a(5) = ((l >> 40) & 0xFF).asInstanceOf[Byte]
    a(6) = ((l >> 48) & 0xFF).asInstanceOf[Byte]
    a(7) = ((l >> 56) & 0xFF).asInstanceOf[Byte]
    a
  }
}
