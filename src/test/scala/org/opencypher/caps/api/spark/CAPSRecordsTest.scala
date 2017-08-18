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
package org.opencypher.caps.api.spark

import org.apache.spark.sql.Row
import org.opencypher.caps.api.exception.CAPSException
import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.ir.global.{Label, PropertyKey, TokenRegistry}
import org.opencypher.caps.api.record._
import org.opencypher.caps.api.types.{CTBoolean, CTNode, CTRelationship, CTString, _}
import org.opencypher.caps.{BaseTestSuite, SparkTestSession}

class CAPSRecordsTest extends BaseTestSuite with SparkTestSession.Fixture {

  implicit val space = SparkGraphSpace.empty(session, TokenRegistry.empty)

  test("contract and scan nodes") {
    val given = CAPSRecords.create(session.createDataFrame(Seq(
      (1, true, "Mats"),
      (2, false, "Martin"),
      (3, false, "Max"),
      (4, false, "Stefan")
    )).toDF("ID", "IS_SWEDE", "NAME"))

    val embeddedNode = EmbeddedNode("n" -> "ID").build
      .withImpliedLabel("Person")
      .withOptionalLabel("Swedish" -> "IS_SWEDE")
      .withPropertyKey("name" -> "NAME")
      .verify


    val result = given.contract(embeddedNode)
    val entityVar = Var("n")(CTNode(Map("Person" -> true)))

    result.details.header.slots.map(_.content).toVector should equal(Vector(
      OpaqueField(entityVar),
      ProjectedExpr(HasLabel(entityVar, Label("Swedish"))(CTBoolean)),
      ProjectedExpr(Property(entityVar, PropertyKey("name"))(CTString.nullable))
    ))

    val scan = GraphScan(embeddedNode).from(given)
    scan.entity should equal(entityVar)
    scan.records.header should equal(result.header)
  }

  test("contract relationships with a fixed type") {

    val given = CAPSRecords.create(session.createDataFrame(Seq(
      (10, 1, 2, "red"),
      (11, 2, 3, "blue"),
      (12, 3, 4, "green"),
      (13, 4, 1, "yellow")
    )).toDF("ID", "FROM", "TO", "COLOR"))

    val embeddedRel = EmbeddedRelationship("r" -> "ID").from("FROM").to("TO").relType("NEXT").build
      .withPropertyKey("color" -> "COLOR")
      .verify
    val result = given.contract(embeddedRel)

    val entityVar = Var("r")(CTRelationship("NEXT"))

    result.details.header.slots.map(_.content).toVector should equal(Vector(
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
    // TODO: Reject records using unknown tokes
    val given = CAPSRecords.create(session.createDataFrame(Seq(
      (10, 1, 2, 50),
      (11, 2, 3, 51),
      (12, 3, 4, 52),
      (13, 4, 1, 53)
    )).toDF("ID", "FROM", "TO", "COLOR"))

    val embeddedRel =
      EmbeddedRelationship("r" -> "ID").from("FROM").to("TO").relTypes("COLOR", "RED", "BLUE", "GREEN", "YELLOW").build

    val result = given.contract(embeddedRel)

    val entityVar = Var("r")(CTRelationship("RED", "BLUE", "GREEN", "YELLOW"))

    // TODO: Use schema for determining more precise node types
    result.details.header.slots.map(_.content).toVector should equal(Vector(
      OpaqueField(entityVar),
      ProjectedExpr(StartNode(entityVar)(CTNode)),
      ProjectedExpr(EndNode(entityVar)(CTNode)),
      ProjectedExpr(TypeId(entityVar)(CTRelationship("RED", "BLUE", "GREEN", "YELLOW")))
    ))

    val scan = GraphScan(embeddedRel).from(given)
    scan.entity should equal(entityVar)
    scan.records.header should equal(result.header)
  }

  test("can not construct records with data/header column name conflict") {
    val data = session.createDataFrame(Seq((1, "foo"), (2, "bar"))).toDF("int", "string")
    val header = RecordHeader.from(OpaqueField(Var("int")()), OpaqueField(Var("notString")()))

    a [CAPSException] shouldBe thrownBy {
      CAPSRecords.create(header, data)
    }
  }

  test("can construct records with matching data/header") {
    val data = session.createDataFrame(Seq((1, "foo"), (2, "bar"))).toDF("int", "string")
    val header = RecordHeader.from(OpaqueField(Var("int")(CTInteger)), OpaqueField(Var("string")(CTString)))

    val records = CAPSRecords.create(header, data) // no exception is thrown
    records.data.select("int").collect() should equal(Array(Row(1), Row(2)))
  }
}
