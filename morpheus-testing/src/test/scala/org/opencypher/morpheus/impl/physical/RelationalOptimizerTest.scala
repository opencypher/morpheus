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
package org.opencypher.morpheus.impl.physical

import org.opencypher.morpheus.impl.MorpheusConverters._
import org.opencypher.morpheus.impl.table.SparkTable.DataFrameTable
import org.opencypher.morpheus.testing.MorpheusTestSuite
import org.opencypher.morpheus.testing.fixture.GraphConstructionFixture
import org.opencypher.okapi.api.graph.{NodePattern, QualifiedGraphName}
import org.opencypher.okapi.api.types.CTNode
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.logical.impl.LogicalCatalogGraph
import org.opencypher.okapi.relational.api.planning.RelationalRuntimeContext
import org.opencypher.okapi.relational.api.table.Table
import org.opencypher.okapi.relational.impl.operators.{Cache, Join, RelationalOperator, SwitchContext}
import org.opencypher.okapi.relational.impl.planning.RelationalPlanner._
import org.opencypher.okapi.relational.impl.planning.{CrossJoin, RelationalOptimizer}

class RelationalOptimizerTest extends MorpheusTestSuite with GraphConstructionFixture {

  implicit class OpContainsCache[T <: Table[T]](op: RelationalOperator[T]) {
    def containsCache: Boolean = op.exists {
      case _: Cache[T] => true
      case _ => false
    }
  }

  test("Test insert Cache operators") {
    implicit val context: RelationalRuntimeContext[DataFrameTable] = morpheus.basicRuntimeContext()

    val g = initGraph(
      """
        |CREATE ()
      """.stripMargin)

    val qgn = QualifiedGraphName("session.test")
    val logicalGraph = LogicalCatalogGraph(qgn, g.schema)
    morpheus.catalog.store(qgn, g)

    val aVar = Var("A")(CTNode)
    val bVar = Var("B")(CTNode)
    val cVar = Var("C")(CTNode)
    val dVar = Var("D")(CTNode)

    val pattern = NodePattern(CTNode)

    val aPlan = planScan(None, logicalGraph, pattern, Map(aVar -> pattern.nodeElement))
    val bPlan = planScan(None, logicalGraph, pattern, Map(bVar -> pattern.nodeElement))

    val cPlan = planScan(None, logicalGraph, pattern, Map(cVar -> pattern.nodeElement))
    val dPlan = planScan(None, logicalGraph, pattern, Map(dVar -> pattern.nodeElement))

    val join1 = aPlan.join(bPlan, Seq.empty, CrossJoin)
    val join2 = cPlan.join(dPlan, Seq.empty, CrossJoin)

    val plan = join1.join(join2, Seq.empty, CrossJoin)


    val rewrittenPlan = RelationalOptimizer.process(plan)

    val eachSideOfAllJoinsContainsCache = rewrittenPlan.transform[Boolean] {
      case (Join(l, r, _, _), _) => l.containsCache && r.containsCache
      case (_, childValues) => childValues.contains(true)
    }

    withClue(s"Each side of each join should contain a Cache operator. Actual tree was:\n${rewrittenPlan.pretty}") {
      eachSideOfAllJoinsContainsCache should equal(true)
    }
  }

  it("caches expand into for triangle") {
    // Given
    val given = initGraph(
      """
        |CREATE (p1:Person {name: "Alice"})
        |CREATE (p2:Person {name: "Bob"})
        |CREATE (p3:Person {name: "Eve"})
        |CREATE (p1)-[:KNOWS]->(p2)
        |CREATE (p2)-[:KNOWS]->(p3)
        |CREATE (p1)-[:KNOWS]->(p3)
      """.stripMargin)

    // When
    val result = given.cypher(
      """
        |MATCH (p1:Person)-[e1:KNOWS]->(p2:Person),
        |(p2)-[e2:KNOWS]->(p3:Person),
        |(p1)-[e3:KNOWS]->(p3)
        |RETURN p1.name, p2.name, p3.name
      """.stripMargin)

    val optimizedRelationalPlan = result.asMorpheus.plans.relationalPlan.get

    val cachedScans = optimizedRelationalPlan.transform[Int] {
      case (s: SwitchContext[DataFrameTable], _) => if (s.containsCache) 1 else 0
      case (_, childValues) => childValues.sum
    }

    // Then
    withClue(
      s"""Each scan should contain a Cache operator. Actual tree was:
             ${optimizedRelationalPlan.pretty}""") {
      cachedScans shouldBe 6
    }
  }

  // Takes a long time to run with little extra info
  test("test caching expand into after var expand") {
    // Given
    val given = initGraph(
      """
        |CREATE (p1:Person {name: "Alice"})
        |CREATE (p2:Person {name: "Bob"})
        |CREATE (comment:Comment)
        |CREATE (post1:Post {content: "asdf"})
        |CREATE (post2:Post {content: "foobar"})
        |CREATE (p1)-[:KNOWS]->(p2)
        |CREATE (p2)<-[:HASCREATOR]-(comment)
        |CREATE (comment)-[:REPLYOF]->(post1)-[:REPLYOF]->(post2)
        |CREATE (post2)-[:HASCREATOR]->(p1)
      """.stripMargin)

    // When
    val result = given.cypher(
      """
        |MATCH (p1:Person)-[e1:KNOWS]->(p2:Person),
        |      (p2)<-[e2:HASCREATOR]-(comment:Comment),
        |      (comment)-[e3:REPLYOF*1..10]->(post:Post),
        |      (p1)<-[:HASCREATOR]-(post)
        |WHERE p1.name = "Alice"
        |RETURN p1.name, p2.name, post.content
      """.stripMargin
    )

    // Then
    val cacheOps = result.asMorpheus.plans.relationalPlan.get.collect { case c: Cache[DataFrameTable] => c }
    cacheOps.size shouldBe 115
  }

  // Adds little extra info
  test("test caching optional match with duplicates") {
    // Given
    val given = initGraph(
      """
        |CREATE (p1:Person {name: "Alice"})
        |CREATE (p2:Person {name: "Bob"})
        |CREATE (p3:Person {name: "Eve"})
        |CREATE (p4:Person {name: "Paul"})
        |CREATE (p1)-[:KNOWS]->(p3)
        |CREATE (p2)-[:KNOWS]->(p3)
        |CREATE (p3)-[:KNOWS]->(p4)
      """.stripMargin)

    // When
    val result = given.cypher(
      """
        |MATCH (a:Person)-[e1:KNOWS]->(b:Person)
        |OPTIONAL MATCH (b)-[e2:KNOWS]->(c:Person)
        |RETURN b.name, c.name
      """.stripMargin)

    // Then
    val cacheOps = result.asMorpheus.plans.relationalPlan.get.collect { case c: Cache[DataFrameTable] => c }
    cacheOps.size shouldBe 7
  }
}
