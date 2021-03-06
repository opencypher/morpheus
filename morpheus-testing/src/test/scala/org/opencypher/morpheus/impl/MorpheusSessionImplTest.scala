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
package org.opencypher.morpheus.impl

import org.opencypher.morpheus.api.value.MorpheusNode
import org.opencypher.morpheus.testing.MorpheusTestSuite
import org.opencypher.morpheus.testing.fixture.{GraphConstructionFixture, TeamDataFixture}
import org.opencypher.okapi.api.graph.{Namespace, QualifiedGraphName}
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.io.SessionGraphDataSource
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._

class MorpheusSessionImplTest extends MorpheusTestSuite with TeamDataFixture with GraphConstructionFixture {

  it("can use multiple session graph data sources") {
    morpheus.registerSource(Namespace("working"), new SessionGraphDataSource())
    morpheus.registerSource(Namespace("foo"), new SessionGraphDataSource())

    val g1 = initGraph("CREATE (:A)")
    val g2 = initGraph("CREATE (:B)")
    val g3 = initGraph("CREATE (:C)")

    morpheus.catalog.store(QualifiedGraphName("session.a"), g1)
    morpheus.catalog.store(QualifiedGraphName("working.a"), g2)
    morpheus.cypher("CATALOG CREATE GRAPH working.b { FROM GRAPH working.a RETURN GRAPH }")
    morpheus.catalog.store(QualifiedGraphName("foo.bar.baz.a"), g3)

    val r1 = morpheus.cypher("FROM GRAPH a MATCH (n) RETURN n")
    val r2 = morpheus.cypher("FROM GRAPH working.a MATCH (n) RETURN n")
    val r3 = morpheus.cypher("FROM GRAPH working.b MATCH (n) RETURN n")
    val r4 = morpheus.cypher("FROM GRAPH foo.bar.baz.a MATCH (n) RETURN n")

    r1.records.collect.toBag should equal(Bag(
      CypherMap("n" -> MorpheusNode(0L, Set("A")))
    ))
    r2.records.collect.toBag should equal(Bag(
      CypherMap("n" -> MorpheusNode(0L, Set("B")))
    ))
    r3.records.collect.toBag should equal(Bag(
      CypherMap("n" -> MorpheusNode(0L, Set("B")))
    ))
    r4.records.collect.toBag should equal(Bag(
      CypherMap("n" -> MorpheusNode(0L, Set("C")))
    ))
  }

  it("can execute sql on registered tables") {
    morpheus.records.wrap(personDF).df.createOrReplaceTempView("people")
    morpheus.records.wrap(knowsDF).df.createOrReplaceTempView("knows")

    val sqlResult = morpheus.sql(
      """
        |SELECT people.name AS me, knows.since AS since, p2.name AS you
        |FROM people
        |INNER JOIN knows ON knows.src = people.id
        |INNER JOIN people p2 ON knows.dst = p2.id
      """.stripMargin)

    sqlResult.collect.toBag should equal(Bag(
      CypherMap("me" -> "Mats", "since" -> 2017, "you" -> "Martin"),
      CypherMap("me" -> "Mats", "since" -> 2016, "you" -> "Max"),
      CypherMap("me" -> "Mats", "since" -> 2015, "you" -> "Stefan"),
      CypherMap("me" -> "Martin", "since" -> 2016, "you" -> "Max"),
      CypherMap("me" -> "Martin", "since" -> 2013, "you" -> "Stefan"),
      CypherMap("me" -> "Max", "since" -> 2016, "you" -> "Stefan")
    ))
  }

}
