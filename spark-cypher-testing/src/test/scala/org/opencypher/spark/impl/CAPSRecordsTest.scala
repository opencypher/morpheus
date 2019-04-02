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
package org.opencypher.spark.impl

import org.apache.spark.sql.Row
import org.opencypher.okapi.api.io.conversion.{NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.impl.exception.InternalException
import org.opencypher.okapi.ir.api.PropertyKey
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.okapi.testing.Bag
import org.opencypher.okapi.testing.Bag._
import org.opencypher.spark.api.io.CAPSEntityTable
import org.opencypher.spark.api.value.CAPSEntity._
import org.opencypher.spark.api.value.CAPSNode
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.fixture.{GraphConstructionFixture, TeamDataFixture}

class CAPSRecordsTest extends CAPSTestSuite with GraphConstructionFixture with TeamDataFixture {

  describe("column naming") {

    it("creates column names for simple expressions") {
      caps.cypher("RETURN 1").records.asCaps.df.columns should equal(Array("1"))
      caps.cypher("RETURN '\u0099'").records.asCaps.df.columns should equal(Array("'\u0099'"))
      caps.cypher("RETURN 1 AS foo").records.asCaps.df.columns should equal(Array("foo"))
      caps.cypher("RETURN 1 AS foo, 2 AS bar").records.asCaps.df.columns.toSet should equal(Set("foo", "bar"))
      caps.cypher("RETURN true AND false").records.asCaps.df.columns.toSet should equal(Set("true AND false"))
      caps.cypher("RETURN true AND false AND false").records.asCaps.df.columns.toSet should equal(Set("true AND false AND false"))
      caps.cypher("RETURN 'foo' STARTS WITH 'f'").records.asCaps.df.columns.toSet should equal(Set("'foo' STARTS WITH 'f'"))
    }

    it("escapes property accessors") {
      caps.cypher("MATCH (n) RETURN n.foo").records.asCaps.df.columns.toSet should equal(Set("n_foo"))
    }

    it("creates column names for params") {
      caps.cypher("RETURN $x", parameters = CypherMap("x" -> 1)).records.asCaps.df.columns should equal(Array("$x"))
    }

    it("creates column names for node expressions") {
      val given = initGraph("CREATE (:L {val: 'a'})")
      caps.catalog.store("foo", given)

      val result = given.cypher("FROM GRAPH foo MATCH (n) RETURN n")

      result.records.asCaps.df.columns.toSet should equal(Set("n", "n:L", "n_val"))
    }

    it("creates column names for relationship expressions") {
      val given = initGraph("CREATE ({val: 'a'})-[:R {prop: 'b'}]->({val: 'c'})")
      caps.catalog.store("foo", given)

      val result = given.cypher("FROM GRAPH foo MATCH (n)-[r]->(m) RETURN r")

      result.records.asCaps.df.columns.toSet should equal(Set("r", "r:R", "source(r)", "target(r)", "r_prop"))
    }

    it("retains user-specified order of return items") {
      val given = initGraph("CREATE (:L {val: 'a'})")
      caps.catalog.store("foo", given)

      val result = given.cypher("FROM GRAPH foo MATCH (n) RETURN n.val AS bar, n, n.val AS foo")

      val dfColumns = result.records.asCaps.df.columns
      dfColumns.head should equal("bar")
      dfColumns.last should equal("foo")
      dfColumns.toSet should equal(Set("bar", "n", "n:L", "n_val", "foo"))
    }

    it("can handle ambiguous return items") {
      val given = initGraph("CREATE (:L {val: 'a'})")
      caps.catalog.store("foo", given)

      val result = given.cypher("FROM GRAPH foo MATCH (n) RETURN n, n.val")

      val dfColumns = result.records.asCaps.df.columns
      dfColumns.collect { case col if col == "n_val" => col }.length should equal(1)
      dfColumns.toSet should equal(Set("n", "n:L", "n_val"))
    }
  }

  it("can wrap a dataframe") {
    // Given (generally produced by a SQL query)
    val records = caps.records.wrap(personDF)

    records.header.expressions.map(s => s -> s.cypherType) should equal(Set(
      Var("ID")() -> CTInteger,
      Var("NAME")() -> CTString.nullable,
      Var("NUM")() -> CTInteger
    ))
  }

  it("can be registered and queried from SQL") {
    // Given
    caps.records.fromEntityTable(personTable).df.createOrReplaceTempView("people")

    // When
    val df = sparkSession.sql("SELECT * FROM people")

    // Then
    df.collect().toBag should equal(Bag(
      Row(1L.encodeAsCAPSId, "Mats", 23),
      Row(2L.encodeAsCAPSId, "Martin", 42),
      Row(3L.encodeAsCAPSId, "Max", 1337),
      Row(4L.encodeAsCAPSId, "Stefan", 9)
    ))
  }

  it("verify CAPSRecords header") {
    val givenDF = sparkSession.createDataFrame(
      Seq(
        (1L, "Mats"),
        (2L, "Martin"),
        (3L, "Max"),
        (4L, "Stefan")
      )).toDF("ID", "NAME")

    val givenMapping = NodeMappingBuilder.on("ID")
      .withImpliedLabel("Person")
      .withPropertyKey("name" -> "NAME")
      .build

    val nodeTable = CAPSEntityTable.create(givenMapping, givenDF)

    val records = caps.records.fromEntityTable(nodeTable)

    val entityVar = Var("node")(CTNode("Person"))
    records.header.expressions should equal(
      Set(
        entityVar,
        EntityProperty(entityVar, PropertyKey("name"))(CTString.nullable)
      ))
  }

  it("verify CAPSRecords header for relationship with a fixed type") {

    val givenDF = sparkSession.createDataFrame(
      Seq(
        (10L, 1L, 2L, "red"),
        (11L, 2L, 3L, "blue"),
        (12L, 3L, 4L, "green"),
        (13L, 4L, 1L, "yellow")
      )).toDF("ID", "FROM", "TO", "COLOR")

    val givenMapping = RelationshipMappingBuilder.on("ID")
      .from("FROM")
      .to("TO")
      .relType("NEXT")
      .withPropertyKey("color" -> "COLOR")
      .build

    val relTable = CAPSEntityTable.create(givenMapping, givenDF)

    val records = caps.records.fromEntityTable(relTable)

    val entityVar = Var("rel")(CTRelationship("NEXT"))

    records.header.expressions should equal(
      Set(
        entityVar,
        StartNode(entityVar)(CTNode),
        EndNode(entityVar)(CTNode),
        EntityProperty(entityVar, PropertyKey("color"))(CTString.nullable)
      )
    )
  }

  // Validation happens in operators instead
  ignore("can not construct records with data/header column name conflict") {
    val data = sparkSession.createDataFrame(Seq((1, "foo"), (2, "bar"))).toDF("int", "string")
    val header = RecordHeader.from(Var("int")(), Var("notString")())

    a[InternalException] shouldBe thrownBy {
      CAPSRecords(header, data)
    }
  }

  it("can construct records with matching data/header") {
    val data = sparkSession.createDataFrame(Seq((1L, "foo"), (2L, "bar"))).toDF("int", "string")
    val records = caps.records.wrap(data)

    val v = Var("int")(CTInteger)
    val columnName = records.header.column(v)
    records.df.select(columnName).collect() should equal(Array(Row(1), Row(2)))
  }

  it("toCypherMaps delegates to details") {
    val g = initGraph("CREATE (:Foo {p: 1})")

    val result = g.cypher("MATCH (n) WITH 5 - n.p + 1 AS b, n RETURN n, b")

    result.records.collect.toBag should equal(
      Bag(
        CypherMap(
          "n" -> CAPSNode(0L, Set("Foo"), CypherMap("p" -> 1)),
          "b" -> 5
        )
      ))
  }
}
