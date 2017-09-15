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
import org.opencypher.caps.api.record._
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.test.CAPSTestSuite

class CAPSGraphTest extends CAPSTestSuite {

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
        (1, true, "Mats", 23),
        (2, false, "Martin", 42),
        (3, false, "Max", 1337),
        (4, false, "Stefan", 9))
    ))

  // required to test conflicting input data
  val `:Brogrammer` =
    NodeScan.on("p" -> "ID") {
      _.build
        .withImpliedLabel("Brogrammer")
        .withImpliedLabel("Person")
        .withPropertyKey("language" -> "LANG")
    }
    .from(CAPSRecords.create(
      Seq("ID", "LANG"),
      Seq(
        (100, "Node"),
        (200, "Coffeescript"),
        (300, "Javascript"),
        (400, "Typescript")
      )
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
          (100, "Alice", 42, "C"),
          (200,   "Bob", 23, "D"),
          (300,   "Eve", 84, "F"),
          (400,  "Carl", 49, "R")
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
          (10, "1984", 1949),
          (20, "Cryptonomicon", 1999),
          (30, "The Eye of the World", 1990),
          (40, "The Circle", 2013))
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
        (1, 1, 2, 2017),
        (1, 2, 3, 2016),
        (1, 3, 4, 2015),
        (2, 4, 3, 2016),
        (2, 5, 4, 2013),
        (3, 6, 4, 2016))
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
        (1, 100, 10, true),
        (2, 200, 40, true),
        (3, 300, 30, true),
        (4, 400, 20, false))
    ))

  val `:INFLUENCES` =
    RelationshipScan.on("i" -> "ID") {
      _.from("SRC").to("DST").relType("INFLUENCES")
        .build
    }
    .from(CAPSRecords.create(
      Seq("SRC", "ID", "DST"),
      Seq(
        (10, 1000, 20))
    ))

  test("Construct graph from single node scan") {
    val graph = CAPSGraph.create(`:Person`)
    val nodes = graph.nodes("n")

    nodes.details.toDF().columns should equal(Array(
      "n",
      "____n:Person",
      "____n:Swedish",
      "____n_dot_nameSTRING",
      "____n_dot_lucky_bar_numberINTEGER"
    ))

    nodes.details.toDF().collect().toSet should equal (Set(
      Row(1, true, true,    "Mats",   23),
      Row(2, true, false, "Martin",   42),
      Row(3, true, false,    "Max", 1337),
      Row(4, true, false, "Stefan",    9)
    ))
  }

  test("Construct graph from multiple node scans") {
    val graph = CAPSGraph.create(`:Person`, `:Book`)
    val nodes = graph.nodes("n")

    nodes.details.toDF().columns should equal(Array(
      "n",
      "____n:Person",
      "____n:Swedish",
      "____n:Book",
      "____n_dot_nameSTRING",
      "____n_dot_lucky_bar_numberINTEGER",
      "____n_dot_titleSTRING",
      "____n_dot_yearINTEGER"
    ))

    nodes.details.toDF().collect().toSet should equal(Set(
      Row( 1,  true,  true,  false,   "Mats",   23,                   null, null),
      Row( 2,  true,  false, false, "Martin",   42,                   null, null),
      Row( 3,  true,  false, false,    "Max", 1337,                   null, null),
      Row( 4,  true,  false, false, "Stefan",    9,                   null, null),
      Row(10, false,  false,  true,     null, null,                 "1984", 1949),
      Row(20, false,  false,  true,     null, null,        "Cryptonomicon", 1999),
      Row(30, false,  false,  true,     null, null, "The Eye of the World", 1990),
      Row(40, false,  false,  true,     null, null,           "The Circle", 2013)
    ))
  }

  test("Construct graph from single node and single relationship scan") {
    val graph = CAPSGraph.create(`:Person`, `:KNOWS`)
    val rels  = graph.relationships("e")

    rels.details.toDF().columns should equal(Array(
      "____source(e)",
      "e",
      "____type(e)",
      "____target(e)",
      "____e_dot_sinceINTEGER"
    ))

    rels.details.toDF().collect().toSet should equal(Set(
      Row(1, 1, 0, 2, 2017),
      Row(1, 2, 0, 3, 2016),
      Row(1, 3, 0, 4, 2015),
      Row(2, 4, 0, 3, 2016),
      Row(2, 5, 0, 4, 2013),
      Row(3, 6, 0, 4, 2016)
    ))
  }

  test("Extract all node scans") {
    val graph = CAPSGraph.create(`:Person`, `:Book`)

    val nodes = graph.nodes("n", CTNode())

    nodes.details.toDF().columns should equal(Array(
      "n",
      "____n:Person",
      "____n:Swedish",
      "____n:Book",
      "____n_dot_nameSTRING",
      "____n_dot_lucky_bar_numberINTEGER",
      "____n_dot_titleSTRING",
      "____n_dot_yearINTEGER"
    ))

    nodes.details.toDF().collect().toSet should equal(Set(
      Row( 1,  true,  true,  false,   "Mats",   23,                   null, null),
      Row( 2,  true,  false, false, "Martin",   42,                   null, null),
      Row( 3,  true,  false, false,    "Max", 1337,                   null, null),
      Row( 4,  true,  false, false, "Stefan",    9,                   null, null),
      Row(10, false,  false,  true,     null, null,                 "1984", 1949),
      Row(20, false,  false,  true,     null, null,        "Cryptonomicon", 1999),
      Row(30, false,  false,  true,     null, null, "The Eye of the World", 1990),
      Row(40, false,  false,  true,     null, null,           "The Circle", 2013)
    ))
  }

  test("Extract node scan subset") {
    val graph = CAPSGraph.create(`:Person`, `:Book`)

    val nodes = graph.nodes("n", CTNode("Person"))

    nodes.details.toDF().columns should equal(Array(
      "n",
      "____n:Person",
      "____n:Swedish",
      "____n_dot_nameSTRING",
      "____n_dot_lucky_bar_numberINTEGER"
    ))

    nodes.details.toDF().collect().toSet should equal (Set(
      Row(1, true, true,    "Mats",   23),
      Row(2, true, false, "Martin",   42),
      Row(3, true, false,    "Max", 1337),
      Row(4, true, false, "Stefan",    9)
    ))
  }

  test("Extract all relationship scans") {
    val graph = CAPSGraph.create(`:Person`, `:Book`, `:KNOWS`, `:READS`)

    val rels  = graph.relationships("e")

    rels.details.toDF().columns should equal(Array(
      "____source(e)",
      "e",
      "____type(e)",
      "____target(e)",
      "____e_dot_sinceINTEGER",
      "____e_dot_recommendsBOOLEAN"
    ))

    rels.details.toDF().collect().toSet should equal(Set(
      // :KNOWS
      Row(1, 1, 0, 2, 2017, null),
      Row(1, 2, 0, 3, 2016, null),
      Row(1, 3, 0, 4, 2015, null),
      Row(2, 4, 0, 3, 2016, null),
      Row(2, 5, 0, 4, 2013, null),
      Row(3, 6, 0, 4, 2016, null),
      // :READS
      Row(1, 100, 1, 10, null, true),
      Row(2, 200, 1, 40, null, true),
      Row(3, 300, 1, 30, null, true),
      Row(4, 400, 1, 20, null, false)
    ))
  }

  test("Extract relationship scan subset") {
    val graph = CAPSGraph.create(`:Person`, `:Book`, `:KNOWS`, `:READS`)

    val rels  = graph.relationships("e", CTRelationship("KNOWS"))

    rels.details.toDF().columns should equal(Array(
      "____source(e)",
      "e",
      "____type(e)",
      "____target(e)",
      "____e_dot_sinceINTEGER"
    ))

    rels.details.toDF().collect().toSet should equal(Set(
      Row(1, 1, 0, 2, 2017),
      Row(1, 2, 0, 3, 2016),
      Row(1, 3, 0, 4, 2015),
      Row(2, 4, 0, 3, 2016),
      Row(2, 5, 0, 4, 2013),
      Row(3, 6, 0, 4, 2016)
    ))
  }

  test("Extract relationship scan strict subset") {
    val graph = CAPSGraph.create(`:Person`, `:Book`, `:KNOWS`, `:READS`, `:INFLUENCES`)

    val rels  = graph.relationships("e", CTRelationship("KNOWS", "INFLUENCES"))

    rels.details.toDF().columns should equal(Array(
      "____source(e)",
      "e",
      "____type(e)",
      "____target(e)",
      "____e_dot_sinceINTEGER"
    ))

    rels.details.toDF().collect().toSet should equal(Set(
      // :KNOWS
      Row(1, 1, 0, 2, 2017),
      Row(1, 2, 0, 3, 2016),
      Row(1, 3, 0, 4, 2015),
      Row(2, 4, 0, 3, 2016),
      Row(2, 5, 0, 4, 2013),
      Row(3, 6, 0, 4, 2016),
      // :INFLUENCES
      Row(10, 1000, 2, 20, null)
    ))
  }

  test("Extract from scans with overlapping labels") {
    val graph = CAPSGraph.create(`:Person`, `:Programmer`)

    val nodes = graph.nodes("n", CTNode("Person"))

    nodes.details.toDF().columns should equal(Array(
      "n",
      "____n:Person",
      "____n:Swedish",
      "____n:Programmer",
      "____n_dot_languageSTRING",
      "____n_dot_nameSTRING",
      "____n_dot_lucky_bar_numberINTEGER"
    ))

    nodes.details.toDF().collect().toSet should equal (Set(
      Row(1,   true, true,  false, null,   "Mats",   23),
      Row(2,   true, false, false, null, "Martin",   42),
      Row(3,   true, false, false, null,    "Max", 1337),
      Row(4,   true, false, false, null, "Stefan",    9),
      Row(100, true, false, true,   "C",  "Alice",   42),
      Row(200, true, false, true,   "D",    "Bob",   23),
      Row(300, true, false, true,   "F",    "Eve",   84),
      Row(400, true, false, true,   "R",   "Carl",   49)
    ))
  }

  test("Extract from scans with implied label but missing keys") {
    val graph = CAPSGraph.create(`:Person`, `:Brogrammer`)

    val nodes = graph.nodes("n", CTNode("Person"))

    nodes.details.toDF().columns should equal(Array(
      "n",
      "____n:Person",
      "____n:Swedish",
      "____n:Brogrammer",
      "____n_dot_nameSTRING",
      "____n_dot_lucky_bar_numberINTEGER",
      "____n_dot_languageSTRING"
    ))

    nodes.details.toDF().collect().toSet should equal(Set(
      Row(1, true, true, false, "Mats", 23, null),
      Row(2, true, false, false, "Martin", 42, null),
      Row(3, true, false, false, "Max", 1337, null),
      Row(4, true, false, false, "Stefan", 9, null),
      Row(100, true, false, true, null, null, "Node"),
      Row(200, true, false, true, null, null, "Coffeescript"),
      Row(300, true, false, true, null, null, "Javascript"),
      Row(400, true, false, true, null, null, "Typescript")
    ))
  }
}
