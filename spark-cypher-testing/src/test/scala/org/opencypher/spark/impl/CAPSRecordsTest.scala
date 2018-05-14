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
package org.opencypher.spark.impl

import org.apache.spark.sql.{DataFrame, Row}
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CAPSNode
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.impl.exception.InternalException
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, PropertyKey}
import org.opencypher.okapi.relational.impl.table.{ColumnName, OpaqueField, ProjectedExpr, RecordHeader}
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._
import org.opencypher.spark.api.io.{CAPSNodeTable, CAPSRelationshipTable}
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.fixture.{GraphConstructionFixture, TeamDataFixture}

class CAPSRecordsTest extends CAPSTestSuite with GraphConstructionFixture with TeamDataFixture {

  test("retags a node variable") {
    val givenDF = session.createDataFrame(
      Seq(
        (1L, true, "Mats"),
        (2L, false, "Martin"),
        (3L, false, "Max"),
        (4L, false, "Stefan")
      )).toDF("ID", "IS_SWEDE", "NAME")

    val givenMapping = NodeMapping.on("ID")
      .withImpliedLabel("Person")
      .withOptionalLabel("Swedish" -> "IS_SWEDE")
      .withPropertyKey("name" -> "NAME")

    val nodeTable = CAPSNodeTable.fromMapping(givenMapping, givenDF)

    val records = CAPSRecords.create(nodeTable)

    val entityVar = Var(CAPSRecords.placeHolderVarName)(CTNode("Person"))

    val fromTag = 0
    val toTag = 1

    val retagged = records.retagVariable(entityVar, Map(fromTag -> toTag))

    val nodeIdCol = ColumnName.of(records.header.slotFor(entityVar))

    validateTag(records.data, nodeIdCol, fromTag)
    validateTag(retagged.data, nodeIdCol, toTag)
  }

  it("retags a relationship variable") {
    val givenDF = session.createDataFrame(
      Seq(
        (10L, 1L, 2L, "RED"),
        (11L, 2L, 3L, "BLUE"),
        (12L, 3L, 4L, "GREEN"),
        (13L, 4L, 1L, "YELLOW")
      )).toDF("ID", "FROM", "TO", "COLOR").setNonNullable("COLOR")

    val givenMapping = RelationshipMapping.on("ID")
      .from("FROM")
      .to("TO")
      .withSourceRelTypeKey("COLOR", Set("RED", "BLUE", "GREEN", "YELLOW"))

    val relTable = CAPSRelationshipTable.fromMapping(givenMapping, givenDF)

    val records = CAPSRecords.create(relTable)

    val entityVar = Var(CAPSRecords.placeHolderVarName)(CTRelationship("RED", "BLUE", "GREEN", "YELLOW"))

    val fromTag = 0
    val toTag = 1

    val retagged = records.retagVariable(entityVar, Map(fromTag -> toTag))

    val relIdCol = ColumnName.of(records.header.slotFor(entityVar))
    val sourceIdCol = ColumnName.of(records.header.sourceNodeSlot(entityVar))
    val targetIdCol = ColumnName.of(records.header.targetNodeSlot(entityVar))

    validateTag(records.data, relIdCol, fromTag)
    validateTag(retagged.data, relIdCol, toTag)

    validateTag(records.data, sourceIdCol, fromTag)
    validateTag(retagged.data, sourceIdCol, toTag)

    validateTag(records.data, targetIdCol, fromTag)
    validateTag(retagged.data, targetIdCol, toTag)
  }

  private def validateTag(df: DataFrame, col: String, tag: Int): Unit = {
    df.select(col).collect().forall(_.getLong(0).getTag == tag) shouldBe true
  }

  it("can wrap a dataframe") {
    // Given (generally produced by a SQL query)
    val records = CAPSRecords.wrap(personDF)

    records.header.slots.map(s => s.content -> s.content.cypherType).toSet should equal(Set(
      OpaqueField(Var("ID")()) -> CTInteger,
      OpaqueField(Var("IS_SWEDE")()) -> CTBoolean,
      OpaqueField(Var("NAME")()) -> CTString.nullable,
      OpaqueField(Var("NUM")()) -> CTInteger
    ))
  }

  it("can be registered and queried from SQL") {
    // Given
    CAPSRecords.create(personTable).toDF().createOrReplaceTempView("people")

    // When
    val df = session.sql("SELECT * FROM people")

    // Then
    df.collect() should equal(Array(
      Row(1L, true, "Mats", 23),
      Row(2L, false, "Martin", 42),
      Row(3L, false, "Max", 1337),
      Row(4L, false, "Stefan", 9)
    ))
  }

  test("verify CAPSRecords header") {
    val givenDF = session.createDataFrame(
          Seq(
            (1L, true, "Mats"),
            (2L, false, "Martin"),
            (3L, false, "Max"),
            (4L, false, "Stefan")
      )).toDF("ID", "IS_SWEDE", "NAME")

    val givenMapping = NodeMapping.on("ID")
      .withImpliedLabel("Person")
      .withOptionalLabel("Swedish" -> "IS_SWEDE")
      .withPropertyKey("name" -> "NAME")

    val nodeTable = CAPSNodeTable.fromMapping(givenMapping, givenDF)

    val records = CAPSRecords.create(nodeTable)

    val entityVar = Var(CAPSRecords.placeHolderVarName)(CTNode("Person"))

    records.header.slots.map(_.content).toVector should equal(
      Vector(
        OpaqueField(entityVar),
        ProjectedExpr(HasLabel(entityVar, Label("Swedish"))(CTBoolean)),
        ProjectedExpr(Property(entityVar, PropertyKey("name"))(CTString.nullable))
      ))
  }

  test("verify CAPSRecords header for relationship with a fixed type") {

    val givenDF = session.createDataFrame(
          Seq(
            (10L, 1L, 2L, "red"),
            (11L, 2L, 3L, "blue"),
            (12L, 3L, 4L, "green"),
            (13L, 4L, 1L, "yellow")
      )).toDF("ID", "FROM", "TO", "COLOR")

    val givenMapping = RelationshipMapping.on("ID")
      .from("FROM")
      .to("TO")
      .relType("NEXT")
      .withPropertyKey("color" -> "COLOR")

    val relTable = CAPSRelationshipTable.fromMapping(givenMapping, givenDF)

    val records = CAPSRecords.create(relTable)

    val entityVar = Var(CAPSRecords.placeHolderVarName)(CTRelationship("NEXT"))

    records.header.slots.map(_.content).toVector should equal(
      Vector(
        OpaqueField(entityVar),
        ProjectedExpr(StartNode(entityVar)(CTNode)),
        ProjectedExpr(EndNode(entityVar)(CTNode)),
        ProjectedExpr(Property(entityVar, PropertyKey("color"))(CTString.nullable))
      ))
  }

  test("contract relationships with a dynamic type") {
    val givenDF = session.createDataFrame(
          Seq(
            (10L, 1L, 2L, "RED"),
            (11L, 2L, 3L, "BLUE"),
            (12L, 3L, 4L, "GREEN"),
            (13L, 4L, 1L, "YELLOW")
      )).toDF("ID", "FROM", "TO", "COLOR").setNonNullable("COLOR")

    val givenMapping = RelationshipMapping.on("ID")
      .from("FROM")
      .to("TO")
      .withSourceRelTypeKey("COLOR", Set("RED", "BLUE", "GREEN", "YELLOW"))

    val relTable = CAPSRelationshipTable.fromMapping(givenMapping, givenDF)

    val records = CAPSRecords.create(relTable)

    val entityVar = Var(CAPSRecords.placeHolderVarName)(CTRelationship("RED", "BLUE", "GREEN", "YELLOW"))

    records.header.slots.map(_.content).toVector should equal(
      Vector(
        OpaqueField(entityVar),
        ProjectedExpr(StartNode(entityVar)(CTNode)),
        ProjectedExpr(EndNode(entityVar)(CTNode)),
        ProjectedExpr(Type(entityVar)(CTRelationship("RED", "BLUE", "GREEN", "YELLOW")))
      ))
  }

  test("can not construct records with data/header column name conflict") {
    val data = session.createDataFrame(Seq((1, "foo"), (2, "bar"))).toDF("int", "string")
    val header = RecordHeader.from(OpaqueField(Var("int")()), OpaqueField(Var("notString")()))

    a[InternalException] shouldBe thrownBy {
      CAPSRecords.verifyAndCreate(header, data)
    }
  }

  test("can construct records with matching data/header") {
    val data = session.createDataFrame(Seq((1L, "foo"), (2L, "bar"))).toDF("int", "string")
    val header = RecordHeader.from(OpaqueField(Var("int")(CTInteger)), OpaqueField(Var("string")(CTString)))

    val records = CAPSRecords.verifyAndCreate(header, data) // no exception is thrown
    records.data.select("int").collect() should equal(Array(Row(1), Row(2)))
  }

  test("toCypherMaps delegates to details") {
    val g = initGraph("CREATE (:Foo {p: 1})")

    val result = g.cypher("MATCH (n) WITH 5 - n.p + 1 AS b, n RETURN n, b")

    result.getRecords.collect.toBag should equal(
      Bag(
        CypherMap(
          "n" -> CAPSNode(0L, Set("Foo"), CypherMap("p" -> 1)),
          "b" -> 5
        )
      ))
  }
}
