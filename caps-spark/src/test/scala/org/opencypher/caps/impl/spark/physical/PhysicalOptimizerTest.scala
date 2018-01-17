/*
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.impl.spark.physical

import java.net.URI

import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.CAPSRecords
import org.opencypher.caps.api.types.CTNode
import org.opencypher.caps.impl.record.RecordHeader
import org.opencypher.caps.impl.spark.physical.operators._
import org.opencypher.caps.ir.api.expr.Var
import org.opencypher.caps.logical.impl.LogicalExternalGraph
import org.opencypher.caps.test.CAPSTestSuite
import org.opencypher.caps.test.fixture.GraphCreationFixture

class PhysicalOptimizerTest extends CAPSTestSuite with GraphCreationFixture {
  val emptyRecords = CAPSRecords.empty(RecordHeader.empty)
  val emptyGraph = LogicalExternalGraph("foo", URI.create("example.com"), Schema.empty)

  test("Test insert Cache operators") {
    val plan = CartesianProduct(
      CartesianProduct(
        Scan(
          Start(emptyRecords, emptyGraph),
          emptyGraph,
          Var("C")(CTNode),
          RecordHeader.empty
        ),
        Scan(
          Start(emptyRecords, emptyGraph),
          emptyGraph,
          Var("B")(CTNode),
          RecordHeader.empty
        ),
        RecordHeader.empty
      ),
      CartesianProduct(
        Scan(
          Start(emptyRecords, emptyGraph),
          emptyGraph,
          Var("C")(CTNode),
          RecordHeader.empty
        ),
        Scan(
          Start(emptyRecords, emptyGraph),
          emptyGraph,
          Var("B")(CTNode),
          RecordHeader.empty
        ),
        RecordHeader.empty
      ),
      RecordHeader.empty
    )

    implicit val context = PhysicalOptimizerContext()
    val rewrittenPlan = new PhysicalOptimizer().process(plan)

    rewrittenPlan should equal(
      CartesianProduct(
        Cache(
          CartesianProduct(
            Scan(
              Start(emptyRecords, emptyGraph),
              emptyGraph,
              Var("C")(CTNode),
              RecordHeader.empty
            ),
            Scan(
              Start(emptyRecords, emptyGraph),
              emptyGraph,
              Var("B")(CTNode),
              RecordHeader.empty
            ),
            RecordHeader.empty
          )
        ),
        Cache(
          CartesianProduct(
            Scan(
              Start(emptyRecords, emptyGraph),
              emptyGraph,
              Var("C")(CTNode),
              RecordHeader.empty
            ),
            Scan(
              Start(emptyRecords, emptyGraph),
              emptyGraph,
              Var("B")(CTNode),
              RecordHeader.empty
            ),
            RecordHeader.empty
          )
        ),
        RecordHeader.empty
      )
    )
  }

  test("test caches expand into for triangle") {
    // Given
    val given = initGraph("""
        |CREATE (p1:Person {name: "Alice"})
        |CREATE (p2:Person {name: "Bob"})
        |CREATE (p3:Person {name: "Eve"})
        |CREATE (p1)-[:KNOWS]->(p2)
        |CREATE (p2)-[:KNOWS]->(p3)
        |CREATE (p1)-[:KNOWS]->(p3)
      """.stripMargin)

    // When
    val result = given.cypher("""
        |MATCH (p1:Person)-[e1:KNOWS]->(p2:Person),
        |(p2)-[e2:KNOWS]->(p3:Person),
        |(p1)-[e3:KNOWS]->(p3)
        |RETURN p1.name, p2.name, p3.name
      """.stripMargin)

    // Then
    val cacheOps = result.explain.physical.plan.collect { case c: Cache => c }
    cacheOps.size shouldBe 2
  }

  test("test caching expand into after var expand") {
    // Given
    val given = initGraph("""
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
    val cacheOps = result.explain.physical.plan.collect { case c: Cache => c }
    cacheOps.size shouldBe 2
  }

  test("test caching optional match with duplicates") {
    // Given
    val given = initGraph("""
        |CREATE (p1:Person {name: "Alice"})
        |CREATE (p2:Person {name: "Bob"})
        |CREATE (p3:Person {name: "Eve"})
        |CREATE (p4:Person {name: "Paul"})
        |CREATE (p1)-[:KNOWS]->(p3)
        |CREATE (p2)-[:KNOWS]->(p3)
        |CREATE (p3)-[:KNOWS]->(p4)
      """.stripMargin)

    // When
    val result = given.cypher("""
        |MATCH (a:Person)-[e1:KNOWS]->(b:Person)
        |OPTIONAL MATCH (b)-[e2:KNOWS]->(c:Person)
        |RETURN b.name, c.name
      """.stripMargin)

    // Then
    val cacheOps = result.explain.physical.plan.collect { case c: Cache => c }
    cacheOps.size shouldBe 2
  }
}
