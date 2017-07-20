/**
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
package org.opencypher.spark_legacy

import org.opencypher.spark_legacy.api.CypherRecord
import org.opencypher.spark_legacy.impl._
import org.opencypher.spark.api.value.CypherList
import org.opencypher.spark.{BaseTestSuite, SparkTestSession}

class GraftingCypherOnSparkFunctionalityTest extends BaseTestSuite with SparkTestSession.Fixture {

  implicit val factory = PropertyGraphFactory.create

  override protected def beforeEach(): Unit = {
    factory.clear()
  }

  import factory._

  test("all node scan") {
    val a = add(newNode)
    val b = add(newLabeledNode("Foo", "Bar"))
    val c = add(newLabeledNode("Foo"))
    add(newRelationship(a -> "KNOWS" -> b))
    add(newRelationship(a -> "KNOWS" -> a))

    val result = graph.cypher(NodeScan())

    result.records.collectAsScalaSet should equal(Set(
      CypherRecord("n" -> a),
      CypherRecord("n" -> b),
      CypherRecord("n" -> c)
    ))
  }

  test("get all nodes and project two properties into multiple columns") {
    add(newNode.withProperties("name" -> "Mats"))
    add(newNode.withProperties("name" -> "Stefan", "age" -> 37))
    add(newNode.withProperties("age" -> 7))
    add(newNode)

    // MATCH (n) RETURN n.name, n.age
    val result = graph.cypher(NodeScanWithProjection())

    result.records.collectAsScalaSet should equal(Set(
      CypherRecord("n.name" -> "Mats", "n.age" -> null),
      CypherRecord("n.name" -> "Stefan", "n.age" -> 37),
      CypherRecord("n.name" -> null, "n.age" -> 7),
      CypherRecord("n.name" -> null, "n.age" -> null)
    ))
  }

  test("simple pattern matching") {
    val a1 = add(newLabeledNode("A"))
    val a2 = add(newLabeledNode("A"))
    val b1 = add(newLabeledNode("B"))
    val b2 = add(newLabeledNode("B"))
    val b3 = add(newLabeledNode("B"))
    add(newRelationship(a1 -> "A_TO_A" -> a1))
    val r1 = add(newRelationship(a1 -> "A_TO_B" -> b1))
    val r2 = add(newRelationship(a2 -> "A_TO_B" -> b1))
    val r3 = add(newRelationship(a2 -> "X" -> b2))
    add(newRelationship(b2 -> "B_TO_B" -> b3))

    // MATCH (:A)-[r]->(:B) RETURN r
    val result = graph.cypher(SimplePattern(startLabels = IndexedSeq("A"), endLabels = IndexedSeq("B")))

    result.records.collectAsScalaSet should equal(Set(
      CypherRecord("r" -> r1),
      CypherRecord("r" -> r2),
      CypherRecord("r" -> r3)
    ))
  }

  test("simple union all") {
    val a = add(newLabeledNode("A").withProperties("name" -> "Mats"))
    add(newLabeledNode("B").withProperties("name" -> 123))
    val ab = add(newLabeledNode("A", "B").withProperties("name" -> "Foo"))
    val c = add(newLabeledNode("C").withProperties("name" -> "Bar"))
    add(newNode)
    add(newUntypedRelationship(a -> ab))
    add(newUntypedRelationship(ab -> c))

    // MATCH (a:A) RETURN a.name AS name UNION ALL MATCH (b:B) RETURN b.name AS name
    val result = graph.cypher(SimpleUnionAll(IndexedSeq("A"), 'name, IndexedSeq("B"), 'name))

    result.records.collectAsScalaSet should equal(Set(
      CypherRecord("name" -> "Mats"),
      CypherRecord("name" -> "Foo"),
      CypherRecord("name" -> "Foo"),
      CypherRecord("name" -> 123)
    ))
  }

  test("get all node ids sorted desc") {
    add(newNode)
    add(newNode.withLabels("Foo"))
    add(newNode.withProperties("a" -> 10))
    add(newNode)

      // MATCH (n) RETURN id(n) AS id ORDER BY id DESC
    val result = graph.cypher(NodeScanIdsSorted())

    result.records.collectAsScalaList should equal(List(
      CypherRecord("id" -> 4),
      CypherRecord("id" -> 3),
      CypherRecord("id" -> 2),
      CypherRecord("id" -> 1)
    ))
  }

  test("match aggregate") {
    add(newLabeledNode("A").withProperties("name" -> "Mats"))
    add(newLabeledNode("A").withProperties("name" -> "Mats"))
    add(newLabeledNode("A").withProperties("name" -> "Stefan"))
    add(newLabeledNode("A", "B").withProperties("notName" -> "foo"))

    // MATCH (a:A) RETURN collect(a.name) AS names
    val result = graph.cypher(CollectNodeProperties(IndexedSeq("A"), 'name))

    result.records.collectAsScalaSet should equal(Set(
      CypherRecord("names" -> CypherList(Seq("Mats", "Mats", "Stefan")))
    ))
  }

  test("match aggregate unwind") {
    add(newLabeledNode("A").withProperties("name" -> "Mats"))
    add(newLabeledNode("A").withProperties("name" -> "Mats"))
    add(newLabeledNode("A").withProperties("name" -> "Stefan"))
    add(newLabeledNode("A", "B").withProperties("notName" -> "foo"))

    // MATCH (a:A) WITH collect(a.name) AS names UNWIND names AS name RETURN name
    val result = graph.cypher(CollectAndUnwindNodeProperties(IndexedSeq("A"), 'name, 'name))

    result.records.collectAsScalaSet should equal(Set(
      CypherRecord("name" -> "Mats"),
      CypherRecord("name" -> "Mats"),
      CypherRecord("name" -> "Stefan")
    ))
  }

  test("get all relationships of type T") {
    val a = add(newNode)
    val b = add(newNode)
    val c = add(newNode)
    val r1 = add(newRelationship(a, "T", b))
    add(newRelationship(a, "NOT_T", b))
    val r2 = add(newRelationship(a, "T", c))
    val r3 = add(newRelationship(b, "T", b))
    add(newRelationship(b, "FOO", b))

    // MATCH ()-[r:T]->() RETURN r
    val result = graph.cypher(SimplePattern(types = IndexedSeq("T")))

    result.records.collectAsScalaSet should equal(Set(
      CypherRecord("r" -> r1),
      CypherRecord("r" -> r2),
      CypherRecord("r" -> r3)
    ))
  }

  test("optional match") {
    val a = add(newNode.withLabels("A"))
    val b = add(newNode.withLabels("A"))
    val c = add(newNode.withLabels("B"))
    add(newNode)
    val r1 = add(newUntypedRelationship(a, b))
    val r2 = add(newUntypedRelationship(a, b))
    add(newUntypedRelationship(c, a))
    add(newUntypedRelationship(c, b))

    // MATCH (a:A) OPTIONAL MATCH (a)-[r]->(b) RETURN r
    val result = graph.cypher(MatchOptionalExpand(startLabels = IndexedSeq("A")))

    result.records.collectAsScalaSet should equal(Set(
      CypherRecord("r" -> r1),
      CypherRecord("r" -> r2),
      CypherRecord("r" -> null)
    ))
  }

  test("bounded variable length") {
    val a1 = add(newNode.withLabels("A"))
    val a2 = add(newNode.withLabels("A"))
    val b = add(newNode)
    val c = add(newNode)
    val d = add(newNode)
    val r1 = add(newUntypedRelationship(a1, a1))
    val r2 = add(newUntypedRelationship(a1, b))
    val r3 = add(newUntypedRelationship(b, c))
    add(newUntypedRelationship(c, d))

    val r5 = add(newUntypedRelationship(a2, d))
    val r6 = add(newUntypedRelationship(d, a1))

    // MATCH (a:A)-[r*1..2]->() RETURN r
    val result = graph.cypher(BoundVariableLength(1, 2, "A"))

    result.records.collectAsScalaSet should equal(Set(
      CypherRecord("r" -> CypherList(Seq(r1))),
      CypherRecord("r" -> CypherList(Seq(r1, r2))),
      CypherRecord("r" -> CypherList(Seq(r2, r3))),
      CypherRecord("r" -> CypherList(Seq(r5))),
      CypherRecord("r" -> CypherList(Seq(r5, r6))),
      CypherRecord("r" -> CypherList(Seq(r2)))
    ))
  }

  //  test("all node scan on node-only graph that uses all kinds of properties") {
//    val pg = factory(createGraph2(_)).graph
//
//    val cypher: CypherResult = pg.cypher(SupportedQueries.allNodesScan)
//    val result = cypher.map { map =>
//      map.toString()
//    }.show()
//  }
//
//  test("get all node ids") {
//    val pg = factory(createGraph1(_)).graph
//
//    val cypher: CypherResult = pg.cypher(SupportedQueries.allNodeIds)
//    val result: Dataset[String] = cypher.map { map =>
//      map.toString()
//    }
//
//    result.show(false)
//  }
//
//  test("get all nodes and project two properties into multiple columns using toDS()") {
//    val pg = factory(createGraph3(_)).graph
//
//    val cypher: CypherResult = pg.cypher(SupportedQueries.allNodesScanProjectAgeName)
//    val result = cypher.map { record =>
//      (record("name"), record("age"))
//    }
//
//    result.show(false)
//  }
//
//  test("simple union distinct") {
//    val pg = factory(createGraph3(_)).graph
//
//    val result: CypherResult = pg.cypher(SupportedQueries.simpleUnionDistinct)
//
//    result.show()
//  }
//
//  test("unwind") {
//    val pg = factory(createGraph1(_)).graph
//
//    val result: CypherResult = pg.cypher(SupportedQueries.unwind)
//
//    result.show()
//  }
//
//  test("bound variable length") {
//    val pg = factory(createGraph4(_)).graph
//
//    val result: CypherResult = pg.cypher(SupportedQueries.boundVarLength)
//
//    result.show()
//  }
}
