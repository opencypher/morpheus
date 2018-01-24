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
package org.opencypher.caps.impl.spark

import org.apache.spark.sql.Row
import org.opencypher.caps.api.exception.CypherException
import org.opencypher.caps.api.types.{CTBoolean, CTNode, CTRelationship, CTString, _}
import org.opencypher.caps.api.value.{CypherMap, CypherNode, Properties}
import org.opencypher.caps.impl.record._
import org.opencypher.caps.ir.api.expr._
import org.opencypher.caps.ir.api.{Label, PropertyKey}
import org.opencypher.caps.test.CAPSTestSuite
import org.opencypher.caps.test.fixture.GraphCreationFixture

import scala.collection.Bag

class CAPSRecordsTest extends CAPSTestSuite with GraphCreationFixture {

  test("contract and scan nodes") {
    val given = CAPSRecords.create(
      session
        .createDataFrame(
          Seq(
            (1L, true, "Mats"),
            (2L, false, "Martin"),
            (3L, false, "Max"),
            (4L, false, "Stefan")
          ))
        .toDF("ID", "IS_SWEDE", "NAME"))

    val embeddedNode = EmbeddedNode("n" -> "ID").build
      .withImpliedLabel("Person")
      .withOptionalLabel("Swedish" -> "IS_SWEDE")
      .withPropertyKey("name" -> "NAME")
      .verify

    val result = given.contract(embeddedNode)
    val entityVar = Var("n")(CTNode("Person"))

    result.header.slots.map(_.content).toVector should equal(
      Vector(
        OpaqueField(entityVar),
        ProjectedExpr(HasLabel(entityVar, Label("Swedish"))(CTBoolean)),
        ProjectedExpr(Property(entityVar, PropertyKey("name"))(CTString.nullable))
      ))

    val scan = GraphScan(embeddedNode).from(given)
    scan.entity should equal(entityVar)
    scan.records.header should equal(result.header)
  }

  test("contract relationships with a fixed type") {

    val given = CAPSRecords.create(
      session
        .createDataFrame(
          Seq(
            (10L, 1L, 2L, "red"),
            (11L, 2L, 3L, "blue"),
            (12L, 3L, 4L, "green"),
            (13L, 4L, 1L, "yellow")
          ))
        .toDF("ID", "FROM", "TO", "COLOR"))

    val embeddedRel = EmbeddedRelationship("r" -> "ID")
      .from("FROM")
      .to("TO")
      .relType("NEXT")
      .build
      .withPropertyKey("color" -> "COLOR")
      .verify
    val result = given.contract(embeddedRel)

    val entityVar = Var("r")(CTRelationship("NEXT"))

    result.header.slots.map(_.content).toVector should equal(
      Vector(
        OpaqueField(entityVar),
        ProjectedExpr(StartNode(entityVar)(CTNode)),
        ProjectedExpr(EndNode(entityVar)(CTNode)),
        ProjectedExpr(Property(entityVar, PropertyKey("color"))(CTString.nullable))
      ))

    val scan = GraphScan(embeddedRel).from(given)
    scan.entity should equal(entityVar)
    scan.records.header should equal(result.header)
  }

  test("contract relationships with a dynamic type") {
    val given = CAPSRecords.create(
      session
        .createDataFrame(
          Seq(
            (10L, 1L, 2L, "RED"),
            (11L, 2L, 3L, "BLUE"),
            (12L, 3L, 4L, "GREEN"),
            (13L, 4L, 1L, "YELLOW")
          ))
        .toDF("ID", "FROM", "TO", "COLOR"))

    val embeddedRel =
      EmbeddedRelationship("r" -> "ID").from("FROM").to("TO").relTypes("COLOR", "RED", "BLUE", "GREEN", "YELLOW").build

    val result = given.contract(embeddedRel)

    val entityVar = Var("r")(CTRelationship("RED", "BLUE", "GREEN", "YELLOW"))

    result.header.slots.map(_.content).toVector should equal(
      Vector(
        OpaqueField(entityVar),
        ProjectedExpr(StartNode(entityVar)(CTNode)),
        ProjectedExpr(EndNode(entityVar)(CTNode)),
        ProjectedExpr(Type(entityVar)(CTRelationship("RED", "BLUE", "GREEN", "YELLOW")))
      ))

    val scan = GraphScan(embeddedRel).from(given)
    scan.entity should equal(entityVar)
    scan.records.header should equal(result.header)
  }

  test("can not construct records with data/header column name conflict") {
    val data = session.createDataFrame(Seq((1, "foo"), (2, "bar"))).toDF("int", "string")
    val header = RecordHeader.from(OpaqueField(Var("int")()), OpaqueField(Var("notString")()))

    a[CypherException] shouldBe thrownBy {
      CAPSRecords.create(header, data)
    }
  }

  test("can construct records with matching data/header") {
    val data = session.createDataFrame(Seq((1L, "foo"), (2L, "bar"))).toDF("int", "string")
    val header = RecordHeader.from(OpaqueField(Var("int")(CTInteger)), OpaqueField(Var("string")(CTString)))

    val records = CAPSRecords.create(header, data) // no exception is thrown
    records.data.select("int").collect() should equal(Array(Row(1), Row(2)))
  }

  test("toCypherMaps delegates to details") {
    val g = initGraph("CREATE (:Foo {p: 1})")

    val result = g.cypher("MATCH (n) WITH 5 - n.p + 1 AS b, n RETURN n, b")

    result.records.iterator.toBag should equal(
      Bag(
        CypherMap(
          "n" -> CypherNode(0L, Seq("Foo"), Properties("p" -> 1)),
          "b" -> 5
        )
      ))
  }
}
