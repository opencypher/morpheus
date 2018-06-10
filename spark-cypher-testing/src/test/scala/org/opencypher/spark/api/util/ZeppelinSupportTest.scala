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
package org.opencypher.spark.api.util

import org.opencypher.okapi.api.types.CTNode
import org.opencypher.okapi.api.util.ZeppelinSupport._
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.impl.syntax.RecordHeaderSyntax._
import org.opencypher.okapi.relational.impl.table.{OpaqueField, RecordHeader}
import org.opencypher.spark.impl.{CAPSGraph, CAPSRecords}
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.fixture.TeamDataFixture

//noinspection NameBooleanParameters
class ZeppelinSupportTest extends CAPSTestSuite with TeamDataFixture {

  it("unit table") {
    // Given
    val records = CAPSRecords.unit()

    // Then
    records.toJson should equal(ujson.read(
      s"""{
         |  "columns": [
         |  ],
         |  "rows": [
         |    {
         |    }
         |  ]
         |}""".stripMargin))
  }

  it("single column, no rows") {
    // Given
    val records = CAPSRecords.empty(headerOf('foo))

    // Then
    records.toJson should equal(ujson.read(
      s"""{
         |  "columns": [
         |    "foo"
         |  ],
         |  "rows": [
         |  ]
         |}""".stripMargin))
  }

  it("single column, three rows") {
    // Given
    val records = CAPSRecords.create(Seq(Row1("myString"), Row1("foo"), Row1(null)))

    // Then
    records.toJson should equal(ujson.read(
      """{
        |  "columns": [
        |    "foo"
        |  ],
        |  "rows": [
        |    {
        |      "foo": "myString"
        |    },
        |    {
        |      "foo": "foo"
        |    },
        |    {
        |      "foo": null
        |    }
        |  ]
        |}""".stripMargin))
  }

  it("three columns, three rows") {
    // Given
    val records = CAPSRecords.create(
      Seq(
        Row3("myString", 4L, false),
        Row3("foo", 99999999L, true),
        Row3(null, -1L, true)
      ))

    // Then
    records.toJson should equal(ujson.read(
        """{
          |  "columns": [
          |    "foo",
          |    "v",
          |    "veryLongColumnNameWithBoolean"
          |  ],
          |  "rows": [
          |    {
          |      "foo": "myString",
          |      "v": "4",
          |      "veryLongColumnNameWithBoolean": false
          |    },
          |    {
          |      "foo": "foo",
          |      "v": "99999999",
          |      "veryLongColumnNameWithBoolean": true
          |    },
          |    {
          |      "foo": null,
          |      "v": "-1",
          |      "veryLongColumnNameWithBoolean": true
          |    }
          |  ]
          |}""".stripMargin))
  }

  it("serialize lists") {
    // Given
    val records = CAPSRecords.create(
      Seq(
        ListRow(Seq("foo", "bar", "baz"), Seq(42, 23, 8), Seq(true, false, false)),
        ListRow(null, Seq.empty, Seq.empty)
      ))

    // Then
    records.toJson should equal(ujson.read(
      """{
        |  "columns": [
        |    "strings",
        |    "integers",
        |    "booleans"
        |  ],
        |  "rows": [
        |    {
        |      "strings": [
        |        "foo",
        |        "bar",
        |        "baz"
        |      ],
        |      "integers": [
        |        "42",
        |        "23",
        |        "8"
        |      ],
        |      "booleans": [
        |        true,
        |        false,
        |        false
        |      ]
        |    },
        |    {
        |      "strings": null,
        |      "integers": [
        |      ],
        |      "booleans": [
        |      ]
        |    }
        |  ]
        |}""".stripMargin))
  }

  it("graph serialization") {
    val graph = CAPSGraph.create(personTable, bookTable, readsTable, knowsTable, influencesTable)
    val asJson = graph.toZeppelinJson
    val expected = ujson.read(
      s"""
         |{
         |  "directed": true,
         |  "labels": {
         |    "Book": "#40c294",
         |    "Person": "#cbfe79",
         |    "Swedish": "#6f27a9"
         |  },
         |  "nodes": [
         |    {
         |      "id": "1",
         |      "label": "Person",
         |      "labels": [
         |        "Person",
         |        "Swedish"
         |      ],
         |      "data": {
         |        "luckyNumber": "23",
         |        "name": "Mats"
         |      }
         |    },
         |    {
         |      "id": "2",
         |      "label": "Person",
         |      "labels": [
         |        "Person"
         |      ],
         |      "data": {
         |        "luckyNumber": "42",
         |        "name": "Martin"
         |      }
         |    },
         |    {
         |      "id": "3",
         |      "label": "Person",
         |      "labels": [
         |        "Person"
         |      ],
         |      "data": {
         |        "luckyNumber": "1337",
         |        "name": "Max"
         |      }
         |    },
         |    {
         |      "id": "4",
         |      "label": "Person",
         |      "labels": [
         |        "Person"
         |      ],
         |      "data": {
         |        "luckyNumber": "9",
         |        "name": "Stefan"
         |      }
         |    },
         |    {
         |      "id": "10",
         |      "label": "Book",
         |      "labels": [
         |        "Book"
         |      ],
         |      "data": {
         |        "title": "1984",
         |        "year": "1949"
         |      }
         |    },
         |    {
         |      "id": "20",
         |      "label": "Book",
         |      "labels": [
         |        "Book"
         |      ],
         |      "data": {
         |        "title": "Cryptonomicon",
         |        "year": "1999"
         |      }
         |    },
         |    {
         |      "id": "30",
         |      "label": "Book",
         |      "labels": [
         |        "Book"
         |      ],
         |      "data": {
         |        "title": "The Eye of the World",
         |        "year": "1990"
         |      }
         |    },
         |    {
         |      "id": "40",
         |      "label": "Book",
         |      "labels": [
         |        "Book"
         |      ],
         |      "data": {
         |        "title": "The Circle",
         |        "year": "2013"
         |      }
         |    }
         |  ],
         |  "edges": [
         |    {
         |      "id": "100",
         |      "source": "100",
         |      "target": "10",
         |      "label": "READS",
         |      "data": {
         |        "recommends": true
         |      }
         |    },
         |    {
         |      "id": "200",
         |      "source": "200",
         |      "target": "40",
         |      "label": "READS",
         |      "data": {
         |        "recommends": true
         |      }
         |    },
         |    {
         |      "id": "300",
         |      "source": "300",
         |      "target": "30",
         |      "label": "READS",
         |      "data": {
         |        "recommends": true
         |      }
         |    },
         |    {
         |      "id": "400",
         |      "source": "400",
         |      "target": "20",
         |      "label": "READS",
         |      "data": {
         |        "recommends": false
         |      }
         |    },
         |    {
         |      "id": "1",
         |      "source": "1",
         |      "target": "2",
         |      "label": "KNOWS",
         |      "data": {
         |        "since": "2017"
         |      }
         |    },
         |    {
         |      "id": "2",
         |      "source": "1",
         |      "target": "3",
         |      "label": "KNOWS",
         |      "data": {
         |        "since": "2016"
         |      }
         |    },
         |    {
         |      "id": "3",
         |      "source": "1",
         |      "target": "4",
         |      "label": "KNOWS",
         |      "data": {
         |        "since": "2015"
         |      }
         |    },
         |    {
         |      "id": "4",
         |      "source": "2",
         |      "target": "3",
         |      "label": "KNOWS",
         |      "data": {
         |        "since": "2016"
         |      }
         |    },
         |    {
         |      "id": "5",
         |      "source": "2",
         |      "target": "4",
         |      "label": "KNOWS",
         |      "data": {
         |        "since": "2013"
         |      }
         |    },
         |    {
         |      "id": "6",
         |      "source": "3",
         |      "target": "4",
         |      "label": "KNOWS",
         |      "data": {
         |        "since": "2016"
         |      }
         |    },
         |    {
         |      "id": "1000",
         |      "source": "10",
         |      "target": "20",
         |      "label": "INFLUENCES",
         |      "data": {
         |
         |      }
         |    }
         |  ],
         |  "types": [
         |    "INFLUENCES",
         |    "KNOWS",
         |    "READS"
         |  ]
         |}""".stripMargin)

    asJson should equal(expected)
  }

  private case class Row1(foo: String)

  private case class Row3(foo: String, v: Long, veryLongColumnNameWithBoolean: Boolean)

  private case class ListRow(strings: Seq[String], integers: Seq[Long], booleans: Seq[Boolean])

  private case class MapRow(strings: Map[String, String], integers: Map[String, Long], booleans: Map[String, Boolean])

  private def headerOf(fields: Symbol*): RecordHeader = {
    val value1 = fields.map(f => OpaqueField(Var(f.name)(CTNode)))
    val (header, _) = RecordHeader.empty.update(addContents(value1))
    header
  }
}
