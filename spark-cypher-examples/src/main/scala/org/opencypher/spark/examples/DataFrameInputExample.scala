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
package org.opencypher.spark.examples

// tag::full-example[]
import org.apache.spark.sql.DataFrame
import org.opencypher.spark.api.MorpheusSession
import org.opencypher.spark.api.io.{MorpheusNodeTable, MorpheusRelationshipTable}
import org.opencypher.spark.util.App

/**
  * Demonstrates basic usage of the Morpheus API by loading an example graph from [[DataFrame]]s.
  */
object DataFrameInputExample extends App {
  // 1) Create Morpheus session and retrieve Spark session
  implicit val morpheus: MorpheusSession = MorpheusSession.local()
  val spark = morpheus.sparkSession

  import spark.sqlContext.implicits._

  // 2) Generate some DataFrames that we'd like to interpret as a property graph.
  val nodesDF = spark.createDataset(Seq(
    (0L, "Alice", 42L),
    (1L, "Bob", 23L),
    (2L, "Eve", 84L)
  )).toDF("id", "name", "age")
  val relsDF = spark.createDataset(Seq(
    (0L, 0L, 1L, "23/01/1987"),
    (1L, 1L, 2L, "12/12/2009")
  )).toDF("id", "source", "target", "since")

  // 3) Generate node- and relationship tables that wrap the DataFrames. The mapping between graph elements and columns
  //    is derived using naming conventions for identifier columns.
  val personTable = MorpheusNodeTable(Set("Person"), nodesDF)
  val friendsTable = MorpheusRelationshipTable("KNOWS", relsDF)

  // 4) Create property graph from graph scans
  val graph = morpheus.readFrom(personTable, friendsTable)

  // 5) Execute Cypher query and print results
  val result = graph.cypher("MATCH (n:Person) RETURN n.name")

  // 6) Collect results into string by selecting a specific column.
  //    This operation may be very expensive as it materializes results locally.
  val names: Set[String] = result.records.table.df.collect().map(_.getAs[String]("n_name")).toSet

  println(names)
}

// end::full-example[]
