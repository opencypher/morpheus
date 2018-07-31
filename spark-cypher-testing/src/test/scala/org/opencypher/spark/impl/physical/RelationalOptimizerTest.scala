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
package org.opencypher.spark.impl.physical

import org.opencypher.okapi.api.types.CTNode
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.impl.operators._
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.CAPSRecords
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.fixture.GraphConstructionFixture

class RelationalOptimizerTest extends CAPSTestSuite with GraphConstructionFixture {
  val emptyRecords = caps.records.empty()


  //TODO: Re-enable once caching optimizer is back
//  def start(qgn: QualifiedGraphName, records: CAPSRecords)(implicit caps: CAPSSession): Start = {
//    Start(qgn, Some(records))
//  }

//  test("Test insert Cache operators") {
//    val plan = Join(
//      Join(
//        NodeScan(
//          Start(testQualifiedGraphName, emptyRecords),
//          Var("C")(CTNode)
//        ),
//        NodeScan(
//          start(testQualifiedGraphName, emptyRecords),
//          Var("B")(CTNode)
//        )
//      ),
//      Join(
//        NodeScan(
//          start(testQualifiedGraphName, emptyRecords),
//          Var("C")(CTNode)
//        ),
//        NodeScan(
//          start(testQualifiedGraphName, emptyRecords),
//          Var("B")(CTNode)
//        )
//      )
//    )
//
//    implicit val context = RelationalOptimizerContext()
//    val rewrittenPlan = new RelationalOptimizer().process(plan)
//
//    rewrittenPlan should equal(
//      Join(
//        Cache(
//          Join(
//            NodeScan(
//              start(testQualifiedGraphName, emptyRecords),
//              Var("C")(CTNode)
//            ),
//            NodeScan(
//              start(testQualifiedGraphName, emptyRecords),
//              Var("B")(CTNode)
//            )
//          )
//        ),
//        Cache(
//          Join(
//            NodeScan(
//              start(testQualifiedGraphName, emptyRecords),
//              Var("C")(CTNode)
//            ),
//            NodeScan(
//              start(testQualifiedGraphName, emptyRecords),
//              Var("B")(CTNode)
//            )
//          )
//        )
//      )
//    )
//  }
//
//  it("test caches expand into for triangle") {
//    // Given
//    val given = initGraph(
//      """
//        |CREATE (p1:Person {name: "Alice"})
//        |CREATE (p2:Person {name: "Bob"})
//        |CREATE (p3:Person {name: "Eve"})
//        |CREATE (p1)-[:KNOWS]->(p2)
//        |CREATE (p2)-[:KNOWS]->(p3)
//        |CREATE (p1)-[:KNOWS]->(p3)
//      """.stripMargin)
//
//    // When
//    val result = given.cypher(
//      """
//        |MATCH (p1:Person)-[e1:KNOWS]->(p2:Person),
//        |(p2)-[e2:KNOWS]->(p3:Person),
//        |(p1)-[e3:KNOWS]->(p3)
//        |RETURN p1.name, p2.name, p3.name
//      """.stripMargin)
//
//    // Then
//    val cacheOps = result.asCaps.plans.relationalPlan.get.collect { case c: Cache => c }
//    cacheOps.size shouldBe 2
//  }
//
//  it("test caching expand into after var expand") {
//    // Given
//    val given = initGraph(
//      """
//        |CREATE (p1:Person {name: "Alice"})
//        |CREATE (p2:Person {name: "Bob"})
//        |CREATE (comment:Comment)
//        |CREATE (post1:Post {content: "asdf"})
//        |CREATE (post2:Post {content: "foobar"})
//        |CREATE (p1)-[:KNOWS]->(p2)
//        |CREATE (p2)<-[:HASCREATOR]-(comment)
//        |CREATE (comment)-[:REPLYOF]->(post1)-[:REPLYOF]->(post2)
//        |CREATE (post2)-[:HASCREATOR]->(p1)
//      """.stripMargin)
//
//    // When
//    val result = given.cypher(
//      """
//        |MATCH (p1:Person)-[e1:KNOWS]->(p2:Person),
//        |      (p2)<-[e2:HASCREATOR]-(comment:Comment),
//        |      (comment)-[e3:REPLYOF*1..10]->(post:Post),
//        |      (p1)<-[:HASCREATOR]-(post)
//        |WHERE p1.name = "Alice"
//        |RETURN p1.name, p2.name, post.content
//      """.stripMargin
//    )
//
//    // Then
//    val cacheOps = result.asCaps.plans.relationalPlan.get.collect { case c: Cache => c }
//    cacheOps.size shouldBe 348
//  }
//
//  test("test caching optional match with duplicates") {
//    // Given
//    val given = initGraph(
//      """
//        |CREATE (p1:Person {name: "Alice"})
//        |CREATE (p2:Person {name: "Bob"})
//        |CREATE (p3:Person {name: "Eve"})
//        |CREATE (p4:Person {name: "Paul"})
//        |CREATE (p1)-[:KNOWS]->(p3)
//        |CREATE (p2)-[:KNOWS]->(p3)
//        |CREATE (p3)-[:KNOWS]->(p4)
//      """.stripMargin)
//
//    // When
//    val result = given.cypher(
//      """
//        |MATCH (a:Person)-[e1:KNOWS]->(b:Person)
//        |OPTIONAL MATCH (b)-[e2:KNOWS]->(c:Person)
//        |RETURN b.name, c.name
//      """.stripMargin)
//
//    // Then
//    val cacheOps = result.asCaps.plans.relationalPlan.get.collect { case c: Cache => c }
//    cacheOps.size shouldBe 2
//  }
}
