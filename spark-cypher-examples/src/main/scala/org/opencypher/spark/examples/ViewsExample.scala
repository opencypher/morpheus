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

import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.util.App

/**
  * Demonstrates how to use views to filter graphs.
  */
object ViewsExample extends App {
  // 1) Create CAPS session and retrieve Spark session
  implicit val session: CAPSSession = CAPSSession.local()

  // 2) Create a graph
  session.cypher(
    """|CATALOG CREATE GRAPH sn {
       |  CONSTRUCT
       |    CREATE (a: Person {age: 18})
       |    CREATE (b: Person {age: 20})
       |    CREATE (c: Person {age: 22})
       |    CREATE (d: Person {age: 25})
       |    CREATE (e: Person {age: 30})
       |    CREATE (a)-[:KNOWS]->(b)
       |    CREATE (a)-[:KNOWS]->(d)
       |    CREATE (b)-[:KNOWS]->(d)
       |    CREATE (b)-[:KNOWS]->(e)
       |    CREATE (c)-[:KNOWS]->(b)
       |    CREATE (c)-[:KNOWS]->(e)
       |    CREATE (d)-[:KNOWS]->(c)
       |    CREATE (e)-[:KNOWS]->(a)
       |    CREATE (e)-[:KNOWS]->(d)
       |  RETURN GRAPH
       |}
    """.stripMargin)

  // 3) Define a view that filters young friends
  session.cypher(
    """
      |CATALOG CREATE VIEW youngFriends($people) {
      | FROM $people
      | MATCH (p1: Person)-[r]->(p2: Person)
      | WHERE p1.age < 25 AND p2.age < 25
      | CONSTRUCT
      |   CREATE (p1)-[r]->(p2)
      | RETURN GRAPH
      |}
    """.stripMargin)

  // 3) Apply `youngFriends` view to graph `sn`
  val results = session.cypher(
    """|FROM youngFriends(sn)
       |MATCH (p: Person)-[r]->(e)
       |RETURN p, r, e
       |ORDER BY p.age""".stripMargin)

  results.show
}
// end::full-example[]
