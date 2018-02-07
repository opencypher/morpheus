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
import org.opencypher.caps.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.caps.api.schema.{CAPSNodeTable, CAPSRelationshipTable}
import org.opencypher.caps.api.types._
import org.opencypher.caps.api.value.CAPSNode
import org.opencypher.caps.api.value.CypherValue._
import org.opencypher.caps.impl.record._
import org.opencypher.caps.ir.api.expr._
import org.opencypher.caps.ir.api.{Label, PropertyKey}
import org.opencypher.caps.test.CAPSTestSuite
import org.opencypher.caps.test.fixture.GraphCreationFixture

import scala.collection.Bag

class CAPSRecordsTest extends CAPSTestSuite with GraphCreationFixture {

  it("can wrap a dataframe") {
    val givenDF = session.createDataFrame(
      Seq(
        (1L, true, "Mats"),
        (2L, false, "Martin"),
        (3L, false, "Max"),
        (4L, false, "Stefan")
      )).toDF("ID", "IS_SWEDE", "NAME")

    val records = CAPSRecords.wrap(givenDF)

    records.header.slots.map(s => s.content -> s.content.cypherType).toSet should equal(Set(
      OpaqueField(Var("ID")()) -> CTInteger,
      OpaqueField(Var("IS_SWEDE")()) -> CTBoolean,
      OpaqueField(Var("NAME")()) -> CTString.nullable
    ))
  }

  it("can be registered and queried from SQL") {
    val givenDF = session.createDataFrame(
      Seq(
        (1L, true, "Mats"),
        (2L, false, "Martin"),
        (3L, false, "Max"),
        (4L, false, "Stefan")
      )).toDF("ID", "IS_SWEDE", "NAME")

    CAPSRecords.wrap(givenDF).register("people")

    val df = session.sql("SELECT * FROM people")

    df.collect() should equal(Array(
      Row(1L, true, "Mats"),
      Row(2L, false, "Martin"),
      Row(3L, false, "Max"),
      Row(4L, false, "Stefan")
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

    val nodeTable = CAPSNodeTable(givenMapping, givenDF)

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

    val relTable = CAPSRelationshipTable(givenMapping, givenDF)

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
    import org.opencypher.caps.impl.spark.convert.SparkUtils.NullabilityOps
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

    val relTable = CAPSRelationshipTable(givenMapping, givenDF)

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

    a[CypherException] shouldBe thrownBy {
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

    result.records.iterator.toBag should equal(
      Bag(
        CypherMap(
          "n" -> CAPSNode(0L, Set("Foo"), CypherMap("p" -> 1)),
          "b" -> 5
        )
      ))
  }
}
