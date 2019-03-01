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
import org.opencypher.okapi.neo4j.io.MetaLabelSupport._
import org.opencypher.okapi.neo4j.io.testing.Neo4jUtils._
import org.opencypher.spark.api.io.neo4j.sync.Neo4jGraphMerge
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.util.ConsoleApp

/**
  * Demonstrates merging a graph into an existing Neo4j database.
  *
  * This merge requires node and relationship keys to identify same entities in the merge graph and the Neo4j database.
  */
object Neo4jMergeExample extends ConsoleApp {
  // Create CAPS session
  implicit val session: CAPSSession = CAPSSession.local()

  // Start a Neo4j instance and populate it with social network data
  val neo4j = startNeo4j(
    """
       |CREATE (a:Person { name: 'Alice', age: 10 })
       |CREATE (b:Person { name: 'Bob', age: 20})
       |CREATE (c:Person { name: 'Carol', age: 15})
       |CREATE (a)-[:FRIEND_OF { id: 0, since: '23/01/1987' }]->(b)
       |CREATE (b)-[:FRIEND_OF { id: 1, since: '12/12/2009' }]->(c)
    """.stripMargin
  )

  // Define the node and relationship keys
  val nodeKeys = Map("Person" -> Set("name"))
  val relKeys = Map("FRIEND_OF" -> Set("id"), "MARRIED_TO" -> Set("id"))

  // Create a merge graph with updated data
  val mergeGraph = session.cypher(
    """
      |CONSTRUCT
      | CREATE (a:Person { name: 'Alice', age: 11 })
      | CREATE (b:Person { name: 'Bob', age: 21})
      | CREATE (c:Person { name: 'Carol', age: 16})
      | CREATE (t:Person { name: 'Tim', age: 25})
      |
      | CREATE (b)-[:MARRIED_TO { id: 1 }]->(t)
      | CREATE (c)-[:FRIEND_OF { id: 2, since: '23/01/2019' }]->(t)
      |RETURN GRAPH
    """.stripMargin).graph

  // Speed up merge operation. Requires Neo4j Enterprise Edition
  // Neo4jGraphMerge.createIndexes(entireGraphName, neo4j.dataSourceConfig, nodeKeys)

  // Merge graph into existing Neo4j database
  Neo4jGraphMerge.merge(entireGraphName, mergeGraph, neo4j.dataSourceConfig, Some(nodeKeys), Some(relKeys))

  // Register Property Graph Data Source (PGDS) to read the updated graph from Neo4j
  session.registerSource(Namespace("updatedSocialNetwork"), GraphSources.cypher.neo4j(neo4j.dataSourceConfig))

  // Access the graphs via their qualified graph names
  val updatedSocialNetwork = session.catalog.graph("updatedSocialNetwork.graph")

  updatedSocialNetwork.cypher(
    """
      |MATCH (p:Person)
      |RETURN p
      |ORDER BY p.name
    """.stripMargin).records.show

  updatedSocialNetwork.cypher(
    """
      |MATCH (p:Person)
      |OPTIONAL MATCH (p)-[r:FRIEND_OF|MARRIED_TO]->(o:Person)
      |RETURN p, r, o
      |ORDER BY p.name, r.since
    """.stripMargin).records.show

  // Shutdown Neo4j test instance
  neo4j.stop()

  }
// end::full-example[]
