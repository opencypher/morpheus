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
package org.opencypher.caps.api.record

import org.apache.spark.sql.Row
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords, CAPSSession}
import org.opencypher.caps.api.types.{CTFloat, CTInteger, CTString}
import org.opencypher.caps.api.value.CypherMap
import org.opencypher.caps.demo.{Friend, Person}
import org.opencypher.caps.test.CAPSTestSuite

class NodeScanTest extends CAPSTestSuite {

  test("schema from scala classes") {
    val persons = List(Person(0, "Alice"), Person(1, "Bob"), Person(2, "Carol"))
    val personScanScala = GraphScan.nodesToScan(List(Person(0, "Alice")))
    val personsDf = sparkSession.createDataFrame(persons)
    val personScan = NodeScan.on("id") { builder =>
      builder.build.withImpliedLabel("Person").withPropertyKey("name")
    }.fromDf(personsDf)
    personScanScala.schema should equal(personScan.schema)


    val friends = List(Friend(0, 0, 1, "23/01/1987"), Friend(1, 1, 2, "12/12/2009"))
    val friendScanScala = GraphScan.relationshipsToScan(friends)
    val friendsDf = sparkSession.createDataFrame(friends)
    val friendScan = RelationshipScan.on("id") { builder =>
      builder.from("from").to("to").relType("FRIEND").build.withPropertyKey("since")
    }.fromDf(friendsDf)
    friendScanScala.schema should equal(friendScan.schema)
  }

  test("test schema creation") {
    val nodeScan = NodeScan.on("p" -> "ID") {
      _.build
      .withImpliedLabel("A")
      .withImpliedLabel("B")
      .withOptionalLabel("C" -> "IS_C")
      .withPropertyKey("foo" -> "FOO")
      .withPropertyKey("bar" -> "BAR")
    }.from(CAPSRecords.create(
      Seq("ID", "IS_C", "FOO", "BAR"),
      Seq(
        (1L, true, "Mats", 23L)
      )
    ))

    nodeScan.schema should equal (Schema.empty
      .withNodePropertyKeys("A","B")("foo" -> CTString.nullable, "bar" -> CTInteger)
      .withNodePropertyKeys("A","B","C")("foo" -> CTString.nullable, "bar" -> CTInteger)
    )
  }

  test("test type casts when creating a GraphScan from a DataFrame") {
    val nodeScan = NodeScan.on("p" -> "ID") {
      _.build
        .withImpliedLabel("A")
        .withImpliedLabel("B")
        .withOptionalLabel("C" -> "IS_C")
        .withPropertyKey("foo" -> "FOO")
        .withPropertyKey("bar" -> "BAR")
    }.from(CAPSRecords.create(
      Seq("ID", "IS_C", "FOO", "BAR"),
      Seq(
        (1, true, 10.toShort, 23.1f)
      )
    ))

    nodeScan.schema should equal(Schema.empty
      .withNodePropertyKeys("A","B")("foo" -> CTInteger, "bar" -> CTFloat)
      .withNodePropertyKeys("A","B", "C")("foo" -> CTInteger, "bar" -> CTFloat)
    )

    nodeScan.records.toDF().collect().toSet should equal(Set(
      Row(true, 1L, 10L, (23.1f).toDouble)
    ))
  }

  test("test ScanGraph can handle shuffled columns due to cast") {
    val nodeScan = NodeScan.on("p" -> "ID") {
      _.build
        .withImpliedLabel("A")
        .withImpliedLabel("B")
        .withOptionalLabel("C" -> "IS_C")
        .withPropertyKey("foo" -> "FOO")
        .withPropertyKey("bar" -> "BAR")
    }.from(CAPSRecords.create(
      Seq("ID", "IS_C", "FOO", "BAR"),
      Seq(
        (1, true, 10.toShort, 23.1f)
      )
    ))

    val graph = CAPSGraph.create(nodeScan)
    graph.nodes("n").toLocalScalaIterator.toSet {
      CypherMap("n" -> "1")
    }


  }

}
