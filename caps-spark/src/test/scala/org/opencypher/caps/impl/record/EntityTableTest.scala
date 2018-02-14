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
package org.opencypher.caps.impl.record

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DecimalType
import org.opencypher.caps.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.caps.api.schema.{CAPSNodeTable, CAPSRelationshipTable, Schema}
import org.opencypher.caps.api.types.{CTFloat, CTInteger, CTString}
import org.opencypher.caps.api.value.CypherValue.CypherMap
import org.opencypher.caps.demo.SocialNetworkData.{Friend, Person}
import org.opencypher.caps.impl.exception.IllegalArgumentException
import org.opencypher.caps.impl.spark.DataFrameOps._
import org.opencypher.caps.impl.spark.{CAPSGraph, DataFrameOps}
import org.opencypher.caps.test.CAPSTestSuite

class EntityTableTest extends CAPSTestSuite {

  val nodeMapping = NodeMapping
    .withSourceIdKey("ID")
    .withImpliedLabel("A")
    .withImpliedLabel("B")
    .withOptionalLabel("C" -> "IS_C")
    .withPropertyKey("foo" -> "FOO")
    .withPropertyKey("bar" -> "BAR")

  test("mapping from scala classes") {
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

  test("NodeTable should create correct schema") {
    val df = session.createDataFrame(Seq((1L, true, "Mats", 23L))).toDF("ID", "IS_C", "FOO", "BAR")

    val nodeTable = CAPSNodeTable(nodeMapping, df)

    nodeTable.schema should equal(
      Schema.empty
        .withNodePropertyKeys("A", "B")("foo" -> CTString.nullable, "bar" -> CTInteger)
        .withNodePropertyKeys("A", "B", "C")("foo" -> CTString.nullable, "bar" -> CTInteger))
  }

  test("NodeTable should cast compatible types in input DataFrame") {
    val df = session.createDataFrame(Seq((1, true, 10.toShort, 23.1f))).toDF("ID", "IS_C", "FOO", "BAR")

    val nodeTable = CAPSNodeTable(nodeMapping, df)

    nodeTable.schema should equal(
      Schema.empty
        .withNodePropertyKeys("A", "B")("foo" -> CTInteger, "bar" -> CTFloat)
        .withNodePropertyKeys("A", "B", "C")("foo" -> CTInteger, "bar" -> CTFloat))

    nodeTable.records.toDF().collect().toSet should equal(Set(Row(true, 1L, 10L, (23.1f).toDouble)))
  }

  test("NodeTable can handle shuffled columns due to cast") {
    val df = session.createDataFrame(Seq((1, true, 10.toShort, 23.1f))).toDF("ID", "IS_C", "FOO", "BAR")

    val nodeTable = CAPSNodeTable(nodeMapping, df)

    val graph = CAPSGraph.create(nodeTable)
    graph.nodes("n").iterator.toSet {
      CypherMap("n" -> "1")
    }
  }

  test("NodeTable should not accept wrong source id key type (should be compatible to LongType)") {
    an[IllegalArgumentException] should be thrownBy {
      val df = session.createDataFrame(Seq(("1", true))).toDF("ID", "IS_A")
      val nodeMapping = NodeMapping.on("ID").withOptionalLabel("A" -> "IS_A")
      CAPSNodeTable(nodeMapping, df)
    }
  }

  test("NodeTable should not accept wrong optional label source key type (should be BooleanType") {
    an[IllegalArgumentException] should be thrownBy {
      val df = session.createDataFrame(Seq((1, "true"))).toDF("ID", "IS_A")
      val nodeMapping = NodeMapping.on("ID").withOptionalLabel("A" -> "IS_A")
      CAPSNodeTable(nodeMapping, df)
    }
  }

  test("RelationshipTable should not accept wrong sourceId, -StartNode, -EndNode key type (should be compatible to LongType)") {
    val relMapping = RelationshipMapping.on("ID").from("SOURCE").to("TARGET").relType("A")
    an[IllegalArgumentException] should be thrownBy {
      val df = session.createDataFrame(Seq(("1", 1, 1))).toDF("ID", "SOURCE", "TARGET")
      CAPSRelationshipTable(relMapping, df)
    }
    an[IllegalArgumentException] should be thrownBy {
      val df = session.createDataFrame(Seq((1, "1", 1))).toDF("ID", "SOURCE", "TARGET")
      CAPSRelationshipTable(relMapping, df)
    }
    an[IllegalArgumentException] should be thrownBy {
      val df = session.createDataFrame(Seq((1, 1, "1"))).toDF("ID", "SOURCE", "TARGET")
      CAPSRelationshipTable(relMapping, df)
    }
  }

  test("RelationshipTable should not accept wrong source relType key type (should be StringType") {
    an[IllegalArgumentException] should be thrownBy {
      val relMapping = RelationshipMapping.on("ID").from("SOURCE").to("TARGET").withSourceRelTypeKey("TYPE", Set("A"))
      val df = session.createDataFrame(Seq((1, 1, 1, true))).toDF("ID", "SOURCE", "TARGET", "TYPE")
      CAPSRelationshipTable(relMapping, df)
    }
  }

  test("NodeTable should not accept wrong source property key type") {
    assert(!supportedTypes.contains(DecimalType))
    an[IllegalArgumentException] should be thrownBy {
      val df = session.createDataFrame(Seq((1, true, BigDecimal(13.37)))).toDF("ID", "IS_A", "PROP")
      val nodeMapping = NodeMapping.on("ID").withOptionalLabel("A" -> "IS_A").withPropertyKey("PROP")
      CAPSNodeTable(nodeMapping, df)
    }
  }
}
