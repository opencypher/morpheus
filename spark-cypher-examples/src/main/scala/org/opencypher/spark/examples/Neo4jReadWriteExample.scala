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
// tag::full-example[]
package org.opencypher.spark.examples

import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.okapi.neo4j.io.testing.Neo4jTestUtils._
import org.opencypher.spark.api.io.fs.FSGraphSource
import org.opencypher.spark.api.io.neo4j.Neo4jPropertyGraphDataSource
import org.opencypher.spark.api.{MorpheusSession, GraphSources}
import org.opencypher.spark.util.App

object Neo4jReadWriteExample extends App {
  // Create Morpheus session
  implicit val morpheus: MorpheusSession = MorpheusSession.local()

  // Connect to a Neo4j instance and populate it with social network data
  // To run a test instance you may use
  //  ./gradlew :okapi-neo4j-io-testing:neo4jStart
  //  ./gradlew :okapi-neo4j-io-testing:neo4jStop
  val neo4j = connectNeo4j()

  // Register Property Graph Data Sources (PGDS)
  private val neo4jPgds: Neo4jPropertyGraphDataSource = GraphSources.cypher.neo4j(neo4j.config)
  private val filePgds: FSGraphSource = GraphSources.fs(rootPath = getClass.getResource("/fs-graphsource/csv").getFile).csv

  morpheus.registerSource(Namespace("Neo4j"), neo4jPgds)
  morpheus.registerSource(Namespace("CSV"), filePgds)

  // Copy products graph from File-based PGDS to Neo4j PGDS
  morpheus.cypher(
    s"""
       |CATALOG CREATE GRAPH Neo4j.products {
       |  FROM GRAPH CSV.products RETURN GRAPH
       |}
     """.stripMargin)

  // Read graph from Neo4j and run a Cypher query
  morpheus.cypher(
    s"""
       |FROM Neo4j.products
       |MATCH (n:Customer)-[r:BOUGHT]->(m:Product)
       |RETURN m.title AS product, avg(r.rating) AS avg_rating, count(n) AS purchases
       |ORDER BY avg_rating DESC, purchases DESC, product ASC
     """.stripMargin).show

  // Clear Neo4j test instance and close session / driver
  neo4j.close()
}

// end::full-example[]
