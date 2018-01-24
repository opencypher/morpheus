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
import org.opencypher.caps.impl.record.{NodeScan, RelationshipScan}
import org.opencypher.caps.test.CAPSTestSuite

import scala.collection.Bag

class CAPSGraphOperationsTest extends CAPSTestSuite {

  val `:Person` =
    NodeScan.on("p" -> "ID") {
      _.build
        .withImpliedLabel("Person")
        .withOptionalLabel("Swedish" -> "IS_SWEDE")
        .withPropertyKey("name" -> "NAME")
        .withPropertyKey("lucky_number" -> "NUM")
    }
      .from(CAPSRecords.create(
        Seq("ID", "IS_SWEDE", "NAME", "NUM"),
        Seq(
          (1L, true, "Mats", 23L),
          (2L, false, "Martin", 42L),
          (3L, false, "Max", 1337L),
          (4L, false, "Stefan", 9L))
      ))

  val `:KNOWS` =
    RelationshipScan.on("k" -> "ID") {
      _.from("SRC").to("DST").relType("KNOWS")
        .build
        .withPropertyKey("since" -> "SINCE")
    }
      .from(CAPSRecords.create(
        Seq("SRC", "ID", "DST", "SINCE"),
        Seq(
          (1L, 1L, 2L, 2017L),
          (1L, 2L, 3L, 2016L),
          (1L, 3L, 4L, 2015L),
          (2L, 4L, 3L, 2016L),
          (2L, 5L, 4L, 2013L),
          (3L, 6L, 4L, 2016L))
      ))

  val `:Programmer` =
    NodeScan.on("p" -> "ID") {
      _.build
        .withImpliedLabel("Programmer")
        .withImpliedLabel("Person")
        .withPropertyKey("name" -> "NAME")
        .withPropertyKey("lucky_number" -> "NUM")
        .withPropertyKey("language" -> "LANG")
    }
      .from(CAPSRecords.create(
        Seq("ID", "NAME", "NUM", "LANG"),
        Seq(
          (100L, "Alice", 42L, "C"),
          (200L,   "Bob", 23L, "D"),
          (300L,   "Eve", 84L, "F"),
          (400L,  "Carl", 49L, "R")
        )
      ))

  val `:Book` =
    NodeScan.on("b" -> "ID") {
      _.build
        .withImpliedLabel("Book")
        .withPropertyKey("title" -> "NAME")
        .withPropertyKey("year" -> "YEAR")
    }
      .from(CAPSRecords.create(
        Seq("ID", "NAME", "YEAR"),
        Seq(
          (10L, "1984", 1949L),
          (20L, "Cryptonomicon", 1999L),
          (30L, "The Eye of the World", 1990L),
          (40L, "The Circle", 2013L))
      ))

  val `:READS` =
    RelationshipScan.on("r" -> "ID") {
      _.from("SRC").to("DST").relType("READS")
        .build
        .withPropertyKey("recommends" -> "RECOMMENDS")
    }
      .from(CAPSRecords.create(
        Seq("SRC", "ID", "DST", "RECOMMENDS"),
        Seq(
          (100L, 100L, 10L, true),
          (200L, 200L, 40L, true),
          (300L, 300L, 30L, true),
          (400L, 400L, 20L, false))
      ))

  test("union") {
    val graph1 = CAPSGraph.create(`:Person`, `:KNOWS`)
    val graph2 = CAPSGraph.create(`:Programmer`, `:Book`, `:READS`)

    val result = graph1 union graph2

    val nodes = result.nodes("n").toDF().collect.toBag
    nodes should equal(
      Bag(
        Row(1L, false, true, false, true, null, 23L, "Mats", null, null),
        Row(2L, false, true, false, false, null, 42L, "Martin", null, null),
        Row(3L, false, true, false, false, null, 1337L, "Max", null, null),
        Row(4L, false, true, false, false, null, 9L, "Stefan", null, null),
        Row(10L, true, false, false, false, null, null, null, "1984", 1949L),
        Row(20L, true, false, false, false, null, null, null, "Cryptonomicon", 1999L),
        Row(30L, true, false, false, false, null, null, null, "The Eye of the World", 1990L),
        Row(40L, true, false, false, false, null, null, null, "The Circle", 2013L),
        Row(100L, false, true, true, false, "C", 42L, "Alice", null, null),
        Row(200L, false, true, true, false, "D", 23L, "Bob", null, null),
        Row(300L, false, true, true, false, "F", 84L, "Eve", null, null),
        Row(400L, false, true, true, false, "R", 49L, "Carl", null, null)
      )
    )

    val rels = result.relationships("r").toDF().collect.toBag
    rels should equal(
      Bag(
        Row(1L, 1L, "KNOWS", 2L, null, 2017L),
        Row(1L, 2L, "KNOWS", 3L, null, 2016L),
        Row(1L, 3L, "KNOWS", 4L, null, 2015L),
        Row(2L, 4L, "KNOWS", 3L, null, 2016L),
        Row(2L, 5L, "KNOWS", 4L, null, 2013L),
        Row(3L, 6L, "KNOWS", 4L, null, 2016L),
        Row(100L, 100L, "READS", 10L, true, null),
        Row(200L, 200L, "READS", 40L, true, null),
        Row(300L, 300L, "READS", 30L, true, null),
        Row(400L, 400L, "READS", 20L, false, null)
      )
    )
  }
}


