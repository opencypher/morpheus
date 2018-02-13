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
package org.opencypher.caps.cosc.impl

import org.opencypher.caps.api.configuration.Configuration.PrintTimings
import org.opencypher.caps.api.value.CypherValue.{CypherInteger, CypherMap, CypherString}
import org.opencypher.caps.cosc.impl.value.{COSCNode, COSCRelationship}

object Demo extends App {

  PrintTimings.set

  val query = "MATCH (n) WHERE n.age > 40 RETURN n"

  println(
    s"""Executing query:
       |$query
       """.stripMargin)

  implicit val coscSession: COSCSession = COSCSession.create

  val graph = COSCGraph.create(DemoData.nodes, DemoData.rels)

  graph.cypher(query).print
}

object DemoData {

  def nodes = Seq(alice, bob)

  def rels = Seq(aliceKnowsBob)

  val aliceId = 0L
  val alice = COSCNode(
    aliceId,
    Set("Person"),
    CypherMap(
      "name" -> CypherString("Alice"),
      "age" -> CypherInteger(42)
    )
  )

  val bobId = 1L
  val bob = COSCNode(
    bobId,
    Set("Person"),
    CypherMap(
      "name" -> CypherString("Bob"),
      "age" -> CypherInteger(23)
    )
  )

  val aliceKnowsBob = COSCRelationship(
    0L,
    aliceId,
    bobId,
    "KNOWS",
    CypherMap("since" -> CypherInteger(2018))
  )
}
