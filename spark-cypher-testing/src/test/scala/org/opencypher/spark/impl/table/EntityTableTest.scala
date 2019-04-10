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
package org.opencypher.spark.impl.table

import java.sql.Date

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DecimalType
import org.opencypher.okapi.api.graph.Pattern
import org.opencypher.okapi.api.io.conversion.{EntityMapping, NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.PropertyKey
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.api.io._
import org.opencypher.spark.api.value.CAPSEntity._
import org.opencypher.spark.api.value.CAPSNode
import org.opencypher.spark.impl.convert.SparkConversions
import org.opencypher.spark.testing.CAPSTestSuite

case class Person(id: Long, name: String, age: Int) extends Node

@RelationshipType("FRIEND_OF")
case class Friend(id: Long, source: Long, target: Long, since: String) extends Relationship

class EntityTableTest extends CAPSTestSuite {

  private val nodeMapping: EntityMapping = NodeMappingBuilder
    .withSourceIdKey("ID")
    .withImpliedLabel("A")
    .withImpliedLabel("B")
    .withPropertyKey("foo" -> "FOO")
    .withPropertyKey("bar" -> "BAR")
    .build

  private val relMapping: EntityMapping = RelationshipMappingBuilder
    .withSourceIdKey("ID")
    .withSourceStartNodeKey("FROM")
    .withSourceEndNodeKey("TO")
    .withRelType("A")
    .withPropertyKey("foo" -> "FOO")
    .withPropertyKey("bar" -> "BAR")
    .build

  it("mapping from scala classes") {
    val personTableScala = CAPSNodeTable(List(Person(0, "Alice", 15)))
    personTableScala.mapping should equal(NodeMappingBuilder
      .withSourceIdKey("id")
      .withImpliedLabel("Person")
      .withPropertyKey("name")
      .withPropertyKey("age")
      .build)


    val friends = List(Friend(0, 0, 1, "23/01/1987"), Friend(1, 1, 2, "12/12/2009"))
    val friendTableScala = CAPSRelationshipTable(friends)

    friendTableScala.mapping should equal(RelationshipMappingBuilder
      .withSourceIdKey("id")
      .withSourceStartNodeKey("source")
      .withSourceEndNodeKey("target")
      .withRelType("FRIEND_OF")
      .withPropertyKey("since")
      .build)
  }

  // TODO: What is the expected column ordering?
  ignore("throws an IllegalArgumentException when a relationship table does not have the expected column ordering") {
    val df = sparkSession.createDataFrame(Seq((1, 1, 1, true))).toDF("ID", "TARGET", "SOURCE", "TYPE")
    val relMapping = RelationshipMappingBuilder
      .on("ID")
      .from("SOURCE")
      .to("TARGET")
      .withRelType("A")
      .build

    an[IllegalArgumentException] should be thrownBy {
      CAPSEntityTable.create(relMapping, df)
    }
  }

  it("NodeTable should create correct schema from given mapping") {
    val df = sparkSession.createDataFrame(Seq((1L, "Mats", 23L))).toDF("ID", "FOO", "BAR")

    val nodeTable = CAPSEntityTable.create(nodeMapping, df)

    nodeTable.schema should equal(
      Schema.empty
        .withNodePropertyKeys("A", "B")("foo" -> CTString.nullable, "bar" -> CTInteger))
  }

  it("NodeTable should create correct header from given mapping") {
    val df = sparkSession.createDataFrame(Seq((1L, "Mats", 23L))).toDF("ID", "FOO", "BAR")

    val nodeTable = CAPSEntityTable.create(nodeMapping, df)

    val v = Var(Pattern.DEFAULT_NODE_NAME)(CTNode("A", "B"))

    nodeTable.header should equal(RecordHeader(Map(
      v -> "ID",
      EntityProperty(v, PropertyKey("foo"))(CTString) -> "FOO",
      EntityProperty(v, PropertyKey("bar"))(CTInteger) -> "BAR"
    )
    ))
  }

  it("Relationship table should create correct schema from given mapping") {
    val df = sparkSession
      .createDataFrame(Seq((1L, 2L, 3L, true, "Mats", 23L)))
      .toDF("ID", "FROM", "TO", "IS_A", "FOO", "BAR")

    val relationshipTable = CAPSEntityTable.create(relMapping, df)

    relationshipTable.schema should equal(
      Schema.empty
        .withRelationshipPropertyKeys("A")("foo" -> CTString.nullable, "bar" -> CTInteger))
  }

  it("Relationship table should create correct header from given mapping") {
    val df = sparkSession
      .createDataFrame(Seq((1L, 2L, 3L, "Mats", 23L)))
      .toDF("ID", "FROM", "TO", "FOO", "BAR")

    val relationshipTable = CAPSEntityTable.create(relMapping, df)

    val v = Var(Pattern.DEFAULT_REL_NAME)(CTRelationship("A"))

    relationshipTable.header should equal(RecordHeader(
      Map(
        v -> "ID",
        StartNode(v)(CTNode) -> "FROM",
        EndNode(v)(CTNode) -> "TO",
        EntityProperty(v, PropertyKey("foo"))(CTString) -> "FOO",
        EntityProperty(v, PropertyKey("bar"))(CTInteger) -> "BAR"
      )
    ))
  }

  it("NodeTable should cast compatible types in input DataFrame") {
    // The ASCII code of character `1` is 49
    val dfBinary = sparkSession.createDataFrame(Seq((Array[Byte](49.toByte), true, 10.toShort, 23.1f))).toDF("ID", "IS_C", "FOO", "BAR")
    val dfLong = sparkSession.createDataFrame(Seq((49L, true, 10.toShort, 23.1f))).toDF("ID", "IS_C", "FOO", "BAR")
    val dfInt = sparkSession.createDataFrame(Seq((49, true, 10.toShort, 23.1f))).toDF("ID", "IS_C", "FOO", "BAR")
    val dfString = sparkSession.createDataFrame(Seq(("1", 10.toShort, 23.1f))).toDF("ID", "FOO", "BAR")

    val dfs = Seq(dfBinary, dfString, dfLong, dfInt)

    dfs.foreach { df =>
      val nodeTable = CAPSEntityTable.create(nodeMapping, df)

      nodeTable.schema should equal(
        Schema.empty
          .withNodePropertyKeys("A", "B")("foo" -> CTInteger, "bar" -> CTFloat))

      nodeTable.records.df.collect().toSet should equal(Set(Row(49L.encodeAsCAPSId, 23.1f.toDouble, 10)))
    }
  }

  it("NodeTable can handle shuffled columns due to cast") {
    val df = sparkSession.createDataFrame(Seq((1L, 10.toShort, 23.1f))).toDF("ID", "FOO", "BAR")

    val nodeTable = CAPSEntityTable.create(nodeMapping, df)

    val graph = caps.graphs.create(nodeTable)
    graph.nodes("n").collect.toSet should equal(Set(
      CypherMap("n" -> CAPSNode(1, Set("A", "B"), CypherMap("bar" -> 23.1f, "foo" -> 10)))
    ))
  }

  it("NodeTable should not accept wrong source id key type (should be compatible to LongType)") {
    val e = the[IllegalArgumentException] thrownBy {
      val df = sparkSession.createDataFrame(Seq(Tuple1(Date.valueOf("1987-01-23")))).toDF("ID")
      val nodeMapping = NodeMappingBuilder.on("ID").withImpliedLabel("A").build
      CAPSEntityTable.create(nodeMapping, df)
    }
    e.getMessage should (include("Column `ID` should have a valid identifier data type") and include("Unsupported column type `DateType`"))
  }

  it("RelationshipTable should not accept wrong sourceId, -StartNode, -EndNode key type") {
    val relMapping = RelationshipMappingBuilder.on("ID").from("SOURCE").to("TARGET").relType("A").build
    an[IllegalArgumentException] should be thrownBy {
      val df = sparkSession.createDataFrame(Seq((1.toByte, 1, 1))).toDF("ID", "SOURCE", "TARGET")
      CAPSEntityTable.create(relMapping, df)
    }
    an[IllegalArgumentException] should be thrownBy {
      val df = sparkSession.createDataFrame(Seq((1, 1.toByte, 1))).toDF("ID", "SOURCE", "TARGET")
      CAPSEntityTable.create(relMapping, df)
    }
    an[IllegalArgumentException] should be thrownBy {
      val df = sparkSession.createDataFrame(Seq((1, 1, 1.toByte))).toDF("ID", "SOURCE", "TARGET")
      CAPSEntityTable.create(relMapping, df)
    }
  }

  it("NodeTable should infer the correct schema") {
    val df = sparkSession.createDataFrame(Seq((1L, "Alice", 1984, true, 13.37)))
      .toDF("id", "name", "birthYear", "isGood", "luckyNumber")

    val nodeTable = CAPSNodeTable(Set("Person"), df)

    nodeTable.schema should equal(Schema.empty
      .withNodePropertyKeys("Person")(
        "name" -> CTString.nullable,
        "birthYear" -> CTInteger,
        "isGood" -> CTBoolean,
        "luckyNumber" -> CTFloat))
  }

  it("RelationshipTable should infer the correct schema") {
    val df = sparkSession.createDataFrame(Seq((1L, 1L, 1L, "Alice", 1984, true, 13.37)))
      .toDF("id", "source", "target", "name", "birthYear", "isGood", "luckyNumber")

    val relationshipTable = CAPSRelationshipTable("KNOWS", df)

    relationshipTable.schema should equal(Schema.empty
      .withRelationshipPropertyKeys("KNOWS")(
        "name" -> CTString.nullable,
        "birthYear" -> CTInteger,
        "isGood" -> CTBoolean,
        "luckyNumber" -> CTFloat))
  }
}
