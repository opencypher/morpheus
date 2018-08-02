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
// tag::full-example[]
package org.opencypher.spark.examples

import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.testing.api.neo4j.Neo4jHarnessUtils._
import org.opencypher.spark.util.ConsoleApp

/**
  * Demonstrates creating a view on top of an existing Neo4j database.
  */
object ViewExample extends ConsoleApp {

  // Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // Start a Neo4j instance and populate it with social network data
  val neo4j = startNeo4j()

  // Register Property Graph Data Sources (PGDS)
  session.registerSource(Namespace("neo"), GraphSources.cypher.neo4j(neo4j.dataSourceConfig))
  session.registerSource(Namespace("fs"), GraphSources.fs(rootPath = getClass.getResource("/csv").getFile).csv)

  // Copy the products from filesystem (csv) to Neo4j
  session.cypher(
    """
      |CREATE GRAPH neo.products {
      | FROM GRAPH fs.products
      | RETURN GRAPH
      |}
    """.stripMargin)

  // Create a view that contains only DVDs and customers who bought them
  session.registerView(Namespace("DVD_view"),
    """
      |MATCH (p:Product)
      |WHERE p.category = 'DVD'
      |OPTIONAL MATCH (p)<-[b:BOUGHT]-(c:Customer)
      |CONSTRUCT
      |  NEW (p)<-[b]-(c)
      |RETURN GRAPH
    """.stripMargin)

  // Return all nodes in the `neo.products` graph when viewed via the DVD view
  session.cypher(
    """
      |FROM GRAPH DVD_view.neo.products
      |MATCH (n) RETURN n
      |ORDER BY labels(n), n.name, n.title
    """.stripMargin).show

  neo4j.stop()

}
// end::full-example[]
