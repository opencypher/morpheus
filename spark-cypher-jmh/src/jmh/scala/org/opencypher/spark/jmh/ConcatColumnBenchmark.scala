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
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.storage.StorageLevel
import org.opencypher.spark.impl.MorpheusFunctions
import org.opencypher.spark.impl.expressions.EncodeLong._
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
class ConcatColumnBenchmark {

  implicit var sparkSession: SparkSession = _

  var df: DataFrame = _

  @Setup
  def setUp(): Unit = {
    sparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val fromRow = 100000000L
    val numRows = 1000000
    val rangeDf = sparkSession.range(fromRow, fromRow + numRows).toDF("i")
    val indexCol = rangeDf.col("i")
    df = rangeDf
      .withColumn("s", indexCol.cast(StringType))
      .withColumn("b", indexCol.encodeLongAsMorpheusId)
      .partitionAndCache
  }

  @Benchmark
  def concatWs(): Int = {
    val result = df.withColumn("c", functions.concat_ws("|", df.col("i"), df.col("s"), df.col("b")))
    result.select("c").collect().length
  }

  @Benchmark
  def serialize(): Int = {
    val result = df.withColumn("c", MorpheusFunctions.serialize(df.col("i"), df.col("s"), df.col("b")))
    result.select("c").collect().length
  }

  implicit class DataFrameSetup(df: DataFrame) {

    def partitionAndCache: DataFrame = {
      val cached = df.repartition(10).persist(StorageLevel.MEMORY_ONLY)
      cached.count()
      cached
    }
  }

}
