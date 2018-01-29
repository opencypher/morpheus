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
import org.opencypher.caps.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.{CTFloat, CTInteger, CTString}
import org.opencypher.caps.api.value.CAPSMap
import org.opencypher.caps.api.{NodeTable, RelationshipTable}
import org.opencypher.caps.demo.SocialNetworkData.{Friend, Person}
import org.opencypher.caps.impl.spark.CAPSGraph
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
    val persons = List(Person(0, "Alice"), Person(1, "Bob"), Person(2, "Carol"))
    val personTableScala = NodeTable(List(Person(0, "Alice")))
    personTableScala.mapping should equal(NodeMapping
      .withSourceIdKey("id")
      .withImpliedLabel("Person")
      .withPropertyKey("name"))

    val friends = List(Friend(0, 0, 1, "23/01/1987"), Friend(1, 1, 2, "12/12/2009"))
    val friendTableScala = RelationshipTable(friends)

    friendTableScala.mapping should equal(RelationshipMapping
      .withSourceIdKey("id")
      .withSourceStartNodeKey("from")
      .withSourceEndNodeKey("to")
      .withRelType("FRIEND_OF")
      .withPropertyKey("since"))
  }

  test("test schema creation") {
    val df = session.createDataFrame(Seq((1L, true, "Mats", 23L))).toDF(Seq("ID", "IS_C", "FOO", "BAR"): _*)

    val nodeTable = NodeTable(nodeMapping, df)

    nodeTable.schema should equal(
      Schema.empty
        .withNodePropertyKeys("A", "B")("foo" -> CTString.nullable, "bar" -> CTInteger)
        .withNodePropertyKeys("A", "B", "C")("foo" -> CTString.nullable, "bar" -> CTInteger))
  }

  test("test type casts when creating an EntityTable from a DataFrame") {
    val df = session.createDataFrame(Seq((1, true, 10.toShort, 23.1f))).toDF(Seq("ID", "IS_C", "FOO", "BAR"): _*)

    val nodeTable = NodeTable(nodeMapping, df)

    nodeTable.schema should equal(
      Schema.empty
        .withNodePropertyKeys("A", "B")("foo" -> CTInteger, "bar" -> CTFloat)
        .withNodePropertyKeys("A", "B", "C")("foo" -> CTInteger, "bar" -> CTFloat))

    nodeTable.records.toDF().collect().toSet should equal(Set(Row(true, 1L, 10L, (23.1f).toDouble)))
  }

  test("test ScanGraph can handle shuffled columns due to cast") {
    val df = session.createDataFrame(Seq((1, true, 10.toShort, 23.1f))).toDF(Seq("ID", "IS_C", "FOO", "BAR"): _*)

    val nodeTable = NodeTable(nodeMapping, df)

    val graph = CAPSGraph.create(nodeTable)
    graph.nodes("n").iterator.toSet {
      CAPSMap("n" -> "1")
    }
  }
}
