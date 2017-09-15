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
import org.opencypher.caps.api.record.{NodeScan, RelationshipScan}
import org.opencypher.caps.test.CAPSTestSuite

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

    val nodes = result.nodes("n").details.toDF().collect.toSet
    nodes should equal(
      Set(
        Row(/* n */ 1L, /* ____n:Person */ true, /* ____n:Swedish */ true, /* ____n:Programmer */ false, /* ____n:Book */ false, /* ____n_dot_lucky_bar_numberINTEGER */ 23L, /* ____n_dot_nameSTRING */ "Mats", /* ____n_dot_yearINTEGER */ null, /* ____n_dot_languageSTRING */ null, /* ____n_dot_titleSTRING */ null),
        Row(/* n */ 2L, /* ____n:Person */ true, /* ____n:Swedish */ false, /* ____n:Programmer */ false, /* ____n:Book */ false, /* ____n_dot_lucky_bar_numberINTEGER */ 42L, /* ____n_dot_nameSTRING */ "Martin", /* ____n_dot_yearINTEGER */ null, /* ____n_dot_languageSTRING */ null, /* ____n_dot_titleSTRING */ null),
        Row(/* n */ 3L, /* ____n:Person */ true, /* ____n:Swedish */ false, /* ____n:Programmer */ false, /* ____n:Book */ false, /* ____n_dot_lucky_bar_numberINTEGER */ 1337L, /* ____n_dot_nameSTRING */ "Max", /* ____n_dot_yearINTEGER */ null, /* ____n_dot_languageSTRING */ null, /* ____n_dot_titleSTRING */ null),
        Row(/* n */ 4L, /* ____n:Person */ true, /* ____n:Swedish */ false, /* ____n:Programmer */ false, /* ____n:Book */ false, /* ____n_dot_lucky_bar_numberINTEGER */ 9L, /* ____n_dot_nameSTRING */ "Stefan", /* ____n_dot_yearINTEGER */ null, /* ____n_dot_languageSTRING */ null, /* ____n_dot_titleSTRING */ null),
        Row(/* n */ 10L, /* ____n:Person */ false, /* ____n:Swedish */ false, /* ____n:Programmer */ false, /* ____n:Book */ true, /* ____n_dot_lucky_bar_numberINTEGER */ null, /* ____n_dot_nameSTRING */ null, /* ____n_dot_yearINTEGER */ 1949L, /* ____n_dot_languageSTRING */ null, /* ____n_dot_titleSTRING */ "1984"),
        Row(/* n */ 20L, /* ____n:Person */ false, /* ____n:Swedish */ false, /* ____n:Programmer */ false, /* ____n:Book */ true, /* ____n_dot_lucky_bar_numberINTEGER */ null, /* ____n_dot_nameSTRING */ null, /* ____n_dot_yearINTEGER */ 1999L, /* ____n_dot_languageSTRING */ null, /* ____n_dot_titleSTRING */ "Cryptonomicon"),
        Row(/* n */ 30L, /* ____n:Person */ false, /* ____n:Swedish */ false, /* ____n:Programmer */ false, /* ____n:Book */ true, /* ____n_dot_lucky_bar_numberINTEGER */ null, /* ____n_dot_nameSTRING */ null, /* ____n_dot_yearINTEGER */ 1990L, /* ____n_dot_languageSTRING */ null, /* ____n_dot_titleSTRING */ "The Eye of the World"),
        Row(/* n */ 40L, /* ____n:Person */ false, /* ____n:Swedish */ false, /* ____n:Programmer */ false, /* ____n:Book */ true, /* ____n_dot_lucky_bar_numberINTEGER */ null, /* ____n_dot_nameSTRING */ null, /* ____n_dot_yearINTEGER */ 2013L, /* ____n_dot_languageSTRING */ null, /* ____n_dot_titleSTRING */ "The Circle"),
        Row(/* n */ 100L, /* ____n:Person */ true, /* ____n:Swedish */ false, /* ____n:Programmer */ true, /* ____n:Book */ false, /* ____n_dot_lucky_bar_numberINTEGER */ 42L, /* ____n_dot_nameSTRING */ "Alice", /* ____n_dot_yearINTEGER */ null, /* ____n_dot_languageSTRING */ "C", /* ____n_dot_titleSTRING */ null),
        Row(/* n */ 200L, /* ____n:Person */ true, /* ____n:Swedish */ false, /* ____n:Programmer */ true, /* ____n:Book */ false, /* ____n_dot_lucky_bar_numberINTEGER */ 23L, /* ____n_dot_nameSTRING */ "Bob", /* ____n_dot_yearINTEGER */ null, /* ____n_dot_languageSTRING */ "D", /* ____n_dot_titleSTRING */ null),
        Row(/* n */ 300L, /* ____n:Person */ true, /* ____n:Swedish */ false, /* ____n:Programmer */ true, /* ____n:Book */ false, /* ____n_dot_lucky_bar_numberINTEGER */ 84L, /* ____n_dot_nameSTRING */ "Eve", /* ____n_dot_yearINTEGER */ null, /* ____n_dot_languageSTRING */ "F", /* ____n_dot_titleSTRING */ null),
        Row(/* n */ 400L, /* ____n:Person */ true, /* ____n:Swedish */ false, /* ____n:Programmer */ true, /* ____n:Book */ false, /* ____n_dot_lucky_bar_numberINTEGER */ 49L, /* ____n_dot_nameSTRING */ "Carl", /* ____n_dot_yearINTEGER */ null, /* ____n_dot_languageSTRING */ "R", /* ____n_dot_titleSTRING */ null)
      )
    )

    val rels = result.relationships("r").details.toDF().collect.toSet
    rels should equal(
      Set(
        Row(/* ____source(r) */ 1L, /* r */ 1L, /* ____type(r) */ 0, /* ____target(r) */ 2L, /* ____r_dot_sinceINTEGER */ 2017L, /* ____r_dot_recommendsBOOLEAN */ null),
        Row(/* ____source(r) */ 1L, /* r */ 2L, /* ____type(r) */ 0, /* ____target(r) */ 3L, /* ____r_dot_sinceINTEGER */ 2016L, /* ____r_dot_recommendsBOOLEAN */ null),
        Row(/* ____source(r) */ 1L, /* r */ 3L, /* ____type(r) */ 0, /* ____target(r) */ 4L, /* ____r_dot_sinceINTEGER */ 2015L, /* ____r_dot_recommendsBOOLEAN */ null),
        Row(/* ____source(r) */ 2L, /* r */ 4L, /* ____type(r) */ 0, /* ____target(r) */ 3L, /* ____r_dot_sinceINTEGER */ 2016L, /* ____r_dot_recommendsBOOLEAN */ null),
        Row(/* ____source(r) */ 2L, /* r */ 5L, /* ____type(r) */ 0, /* ____target(r) */ 4L, /* ____r_dot_sinceINTEGER */ 2013L, /* ____r_dot_recommendsBOOLEAN */ null),
        Row(/* ____source(r) */ 3L, /* r */ 6L, /* ____type(r) */ 0, /* ____target(r) */ 4L, /* ____r_dot_sinceINTEGER */ 2016L, /* ____r_dot_recommendsBOOLEAN */ null),
        Row(/* ____source(r) */ 100L, /* r */ 100L, /* ____type(r) */ 1, /* ____target(r) */ 10L, /* ____r_dot_sinceINTEGER */ null, /* ____r_dot_recommendsBOOLEAN */ true),
        Row(/* ____source(r) */ 200L, /* r */ 200L, /* ____type(r) */ 1, /* ____target(r) */ 40L, /* ____r_dot_sinceINTEGER */ null, /* ____r_dot_recommendsBOOLEAN */ true),
        Row(/* ____source(r) */ 300L, /* r */ 300L, /* ____type(r) */ 1, /* ____target(r) */ 30L, /* ____r_dot_sinceINTEGER */ null, /* ____r_dot_recommendsBOOLEAN */ true),
        Row(/* ____source(r) */ 400L, /* r */ 400L, /* ____type(r) */ 1, /* ____target(r) */ 20L, /* ____r_dot_sinceINTEGER */ null, /* ____r_dot_recommendsBOOLEAN */ false)
      )
    )
  }
}


