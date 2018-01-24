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
 */
package org.opencypher.caps.demo

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.opencypher.caps.api.{CAPSNodeSchema, CAPSSession}
import org.opencypher.caps.impl.record.NodeScan

import scala.collection.JavaConverters._

object DataFrameExample extends App {
  // 1) Create CAPS session and retrieve Spark session
  implicit val session = CAPSSession.local()
  val spark = session.sparkSession

  // 2) Generate some DataFrames that we'd like to interpret as a property graph.
  val nodes = SocialNetworkDataFrames.nodes(spark)
  val rels = SocialNetworkDataFrames.rels(spark)

  val nodeSchema = CAPSNodeSchema(idColumn = "id", labelColumns = Set("Person"))

  session.readFromRecords()

  //(dataframe + case class schema) -> graph scan

  nodes.schema


  // 3) Construct a CypherRecord

  // ROW => CypherNode für node scan
  // ROW => CypherRelationship für Rel Scan 

  //session.readFromDataFrames()


}

object SocialNetworkDataFrames {
  def nodes(session: SparkSession): DataFrame = {
    val nodes = List(
      Row(0L, true, "Alice"),
      Row(1L, true, "Bob"),
      Row(2L, true, "Eve")
    ).asJava
    val nodeSchema = StructType(List(
      StructField("id", LongType, false),
      StructField("Person", BooleanType, false),
      StructField("name", StringType, false))
    )
    session.createDataFrame(nodes, nodeSchema)
  }

  def rels(session: SparkSession): DataFrame = {
    val rels = List(
      Row(0L, 0L, 1L, "FRIEND_OF", "23/01/1987"),
      Row(1L, 1L, 2L, "FRIEND_OF", "12/12/2009")
    ).asJava
    val relSchema = StructType(List(
      StructField("id", LongType, false),
      StructField("source", LongType, false),
      StructField("target", LongType, false),
      StructField("type", StringType, false),
      StructField("since", StringType, false))
    )
    session.createDataFrame(rels, relSchema)
  }
}
