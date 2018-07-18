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
package org.opencypher.spark.impl.table

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DecimalType
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, PropertyKey, RelType}
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.api.io._
import org.opencypher.spark.api.value.CAPSNode
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.convert.SparkConversions
import org.opencypher.spark.schema.CAPSSchema._
import org.opencypher.spark.testing.CAPSTestSuite

case class Person(id: Long, name: String, age: Int) extends Node

@RelationshipType("FRIEND_OF")
case class Friend(id: Long, source: Long, target: Long, since: String) extends Relationship

class EntityTableTest extends CAPSTestSuite {

  private val nodeMapping: NodeMapping = NodeMapping
    .withSourceIdKey("ID")
    .withImpliedLabel("A")
    .withImpliedLabel("B")
    .withOptionalLabel("C" -> "IS_C")
    .withPropertyKey("foo" -> "FOO")
    .withPropertyKey("bar" -> "BAR")

  private val relMapping: RelationshipMapping = RelationshipMapping
    .withSourceIdKey("ID")
    .withSourceStartNodeKey("FROM")
    .withSourceEndNodeKey("TO")
    .withRelType("A")
    .withPropertyKey("foo" -> "FOO")
    .withPropertyKey("bar" -> "BAR")

  private val relMappingWithTypeColumn: RelationshipMapping = RelationshipMapping
    .withSourceIdKey("ID")
    .withSourceStartNodeKey("FROM")
    .withSourceEndNodeKey("TO")
    .withSourceRelTypeKey("REL_TYPES", Set("A", "B", "C"))
    .withPropertyKey("foo" -> "FOO")
    .withPropertyKey("bar" -> "BAR")

  it("mapping from scala classes") {
    val personTableScala = CAPSNodeTable(List(Person(0, "Alice", 15)))
    personTableScala.mapping should equal(NodeMapping
      .withSourceIdKey("id")
      .withImpliedLabel("Person")
      .withPropertyKey("name")
      .withPropertyKey("age"))

    val friends = List(Friend(0, 0, 1, "23/01/1987"), Friend(1, 1, 2, "12/12/2009"))
    val friendTableScala = CAPSRelationshipTable(friends)

    friendTableScala.mapping should equal(RelationshipMapping
      .withSourceIdKey("id")
      .withSourceStartNodeKey("source")
      .withSourceEndNodeKey("target")
      .withRelType("FRIEND_OF")
      .withPropertyKey("since"))
  }

  it("throws an IllegalArgumentException when a node table does not have the expected column ordering") {
    val df = sparkSession.createDataFrame(Seq((1L, true, "Mats", 23L))).toDF("ID", "IS_C", "FOO", "BAR")
    an[IllegalArgumentException] should be thrownBy {
      CAPSNodeTable(nodeMapping, df)
    }
  }

  it("throws an IllegalArgumentException when a relationship table does not have the expected column ordering") {
    val df = sparkSession.createDataFrame(Seq((1, 1, 1, true))).toDF("ID", "TARGET", "SOURCE", "TYPE")
    val relMapping = RelationshipMapping.on("ID").from("SOURCE").to("TARGET").withSourceRelTypeKey("TYPE", Set("A"))
    an[IllegalArgumentException] should be thrownBy {
      CAPSRelationshipTable(relMapping, df)
    }
  }

  it("NodeTable should create correct schema from given mapping") {
    val df = sparkSession.createDataFrame(Seq((1L, true, "Mats", 23L))).toDF("ID", "IS_C", "FOO", "BAR")

    val nodeTable = CAPSNodeTable.fromMapping(nodeMapping, df)

    nodeTable.schema should equal(
      Schema.empty
        .withNodePropertyKeys("A", "B")("foo" -> CTString.nullable, "bar" -> CTInteger)
        .withNodePropertyKeys("A", "B", "C")("foo" -> CTString.nullable, "bar" -> CTInteger)
        .asCaps)
  }

  it("NodeTable should create correct header from given mapping") {
    val df = sparkSession.createDataFrame(Seq((1L, true, "Mats", 23L))).toDF("ID", "IS_C", "FOO", "BAR")

    val nodeTable = CAPSNodeTable.fromMapping(nodeMapping, df)

    val v = Var("")(CTNode("A", "B"))

    nodeTable.header should equal(RecordHeader(Map(
      v -> "ID",
      HasLabel(v, Label("C"))(CTBoolean) -> "IS_C",
      Property(v, PropertyKey("foo"))(CTString) -> "FOO",
      Property(v, PropertyKey("bar"))(CTInteger) -> "BAR"
    )
    ))
  }

  it("Relationship table should create correct schema from given mapping") {
    val df = sparkSession
      .createDataFrame(Seq((1L, 2L, 3L, true, "Mats", 23L)))
      .toDF("ID", "FROM", "TO", "IS_A", "FOO", "BAR")

    val relationshipTable = CAPSRelationshipTable.fromMapping(relMapping, df)

    relationshipTable.schema should equal(
      Schema.empty
        .withRelationshipPropertyKeys("A")("foo" -> CTString.nullable, "bar" -> CTInteger)
        .asCaps)
  }

  it("Relationship table should create correct header from given mapping") {
    val df = sparkSession
      .createDataFrame(Seq((1L, 2L, 3L, true, "Mats", 23L)))
      .toDF("ID", "FROM", "TO", "IS_A", "FOO", "BAR")

    val relationshipTable = CAPSRelationshipTable.fromMapping(relMapping, df)

    val v = Var("")(CTRelationship("A"))

    relationshipTable.header should equal(RecordHeader(Map(
      v -> "ID",
      StartNode(v)(CTNode) -> "FROM",
      EndNode(v)(CTNode) -> "TO",
      Property(v, PropertyKey("foo"))(CTString) -> "FOO",
      Property(v, PropertyKey("bar"))(CTInteger) -> "BAR")
    ))
  }

  it("Relationship table should create correct header from given mapping with type column") {
    val df = sparkSession
      .createDataFrame(Seq((1L, 2L, 3L, "A", "Mats", 23L)))
      .toDF("ID", "FROM", "TO", "REL_TYPES", "FOO", "BAR")
      .setNonNullable("REL_TYPES")

    val relationshipTable = CAPSRelationshipTable.fromMapping(relMappingWithTypeColumn, df)

    val v = Var("")(CTRelationship("A", "B", "C"))

    val createdHeader = relationshipTable.header
    createdHeader should equal(RecordHeader(Map(
      v -> "ID",
      StartNode(v)(CTNode) -> "FROM",
      EndNode(v)(CTNode) -> "TO",
      HasType(v, RelType("A"))(CTBoolean) -> "A".toRelTypeColumnName,
      HasType(v, RelType("B"))(CTBoolean) -> "B".toRelTypeColumnName,
      HasType(v, RelType("C"))(CTBoolean) -> "C".toRelTypeColumnName,
      Property(v, PropertyKey("foo"))(CTString) -> "FOO",
      Property(v, PropertyKey("bar"))(CTInteger) -> "BAR")
    ))
  }

  it("NodeTable should cast compatible types in input DataFrame") {
    val df = sparkSession.createDataFrame(Seq((1, true, 10.toShort, 23.1f))).toDF("ID", "IS_C", "FOO", "BAR")

    val nodeTable = CAPSNodeTable.fromMapping(nodeMapping, df)

    nodeTable.schema should equal(
      Schema.empty
        .withNodePropertyKeys("A", "B")("foo" -> CTInteger, "bar" -> CTFloat)
        .withNodePropertyKeys("A", "B", "C")("foo" -> CTInteger, "bar" -> CTFloat)
        .asCaps)

    nodeTable.records.df.collect().toSet should equal(Set(Row(1L, true, (23.1f).toDouble, 10L)))
  }

  it("NodeTable can handle shuffled columns due to cast") {
    val df = sparkSession.createDataFrame(Seq((1L, true, 10.toShort, 23.1f))).toDF("ID", "IS_C", "FOO", "BAR")

    val nodeTable = CAPSNodeTable.fromMapping(nodeMapping, df)

    val graph = caps.graphs.create(nodeTable)
    graph.nodes("n").collect.toSet should equal(Set(
      CypherMap("n" -> CAPSNode(1, Set("A", "B"), CypherMap("bar" -> 23.1f, "foo" -> 10)))
    ))
  }

  it("NodeTable should not accept wrong source id key type (should be compatible to LongType)") {
    an[IllegalArgumentException] should be thrownBy {
      val df = sparkSession.createDataFrame(Seq(("1", true))).toDF("ID", "IS_A")
      val nodeMapping = NodeMapping.on("ID").withOptionalLabel("A" -> "IS_A")
      CAPSNodeTable.fromMapping(nodeMapping, df)
    }
  }

  it("NodeTable should not accept wrong optional label source key type (should be BooleanType") {
    an[IllegalArgumentException] should be thrownBy {
      val df = sparkSession.createDataFrame(Seq((1, "true"))).toDF("ID", "IS_A")
      val nodeMapping = NodeMapping.on("ID").withOptionalLabel("A" -> "IS_A")
      CAPSNodeTable.fromMapping(nodeMapping, df)
    }
  }

  it("RelationshipTable should not accept wrong sourceId, -StartNode, -EndNode key type (should be compatible to LongType)") {
    val relMapping = RelationshipMapping.on("ID").from("SOURCE").to("TARGET").relType("A")
    an[IllegalArgumentException] should be thrownBy {
      val df = sparkSession.createDataFrame(Seq(("1", 1, 1))).toDF("ID", "SOURCE", "TARGET")
      CAPSRelationshipTable.fromMapping(relMapping, df)
    }
    an[IllegalArgumentException] should be thrownBy {
      val df = sparkSession.createDataFrame(Seq((1, "1", 1))).toDF("ID", "SOURCE", "TARGET")
      CAPSRelationshipTable.fromMapping(relMapping, df)
    }
    an[IllegalArgumentException] should be thrownBy {
      val df = sparkSession.createDataFrame(Seq((1, 1, "1"))).toDF("ID", "SOURCE", "TARGET")
      CAPSRelationshipTable.fromMapping(relMapping, df)
    }
  }

  it("RelationshipTable should not accept wrong source relType key type (should be StringType)") {
    an[IllegalArgumentException] should be thrownBy {
      val relMapping = RelationshipMapping.on("ID").from("SOURCE").to("TARGET").withSourceRelTypeKey("TYPE", Set("A"))
      val df = sparkSession.createDataFrame(Seq((1, 1, 1, true))).toDF("ID", "SOURCE", "TARGET", "TYPE")
      CAPSRelationshipTable.fromMapping(relMapping, df)
    }
  }

  it("NodeTable should not accept wrong source property key type") {
    assert(!SparkConversions.supportedTypes.contains(DecimalType))
    an[IllegalArgumentException] should be thrownBy {
      val df = sparkSession.createDataFrame(Seq((1, true, BigDecimal(13.37)))).toDF("ID", "IS_A", "PROP")
      val nodeMapping = NodeMapping.on("ID").withOptionalLabel("A" -> "IS_A").withPropertyKey("PROP")
      CAPSNodeTable.fromMapping(nodeMapping, df)
    }
  }

  it("NodeTable should infer the correct schema") {
    val df = sparkSession.createDataFrame(Seq((1, "Alice", 1984, true, 13.37)))
      .toDF("id", "name", "birthYear", "isGood", "luckyNumber")

    val nodeTable = CAPSNodeTable(Set("Person"), df)

    nodeTable.schema should equal(Schema.empty
      .withNodePropertyKeys("Person")(
        "name" -> CTString.nullable,
        "birthYear" -> CTInteger,
        "isGood" -> CTBoolean,
        "luckyNumber" -> CTFloat)
      .asCaps)
  }

  it("NodeTable should infer the correct schema including optional labels") {
    val df = sparkSession.createDataFrame(Seq((1, "Alice", true))).toDF("id", "name", "IS_SWEDE")

    val nodeTable = CAPSNodeTable(Set("Person"), Map("Swede" -> "IS_SWEDE"), df)

    nodeTable.schema should equal(Schema.empty
      .withNodePropertyKeys("Person", "Swede")("name" -> CTString.nullable)
      .withNodePropertyKeys("Person")("name" -> CTString.nullable)
      .asCaps)
  }

  it("RelationshipTable should infer the correct schema") {
    val df = sparkSession.createDataFrame(Seq((1, 1, 1, "Alice", 1984, true, 13.37)))
      .toDF("id", "source", "target", "name", "birthYear", "isGood", "luckyNumber")

    val relationshipTable = CAPSRelationshipTable("KNOWS", df)

    relationshipTable.schema should equal(Schema.empty
      .withRelationshipPropertyKeys("KNOWS")(
        "name" -> CTString.nullable,
        "birthYear" -> CTInteger,
        "isGood" -> CTBoolean,
        "luckyNumber" -> CTFloat)
      .asCaps)
  }
}
