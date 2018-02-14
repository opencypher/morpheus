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
package org.opencypher.caps.web

import org.opencypher.caps.api.types.CTNode
import org.opencypher.caps.impl.table.{OpaqueField, RecordHeader}
import org.opencypher.caps.impl.spark.{CAPSGraph, CAPSRecords}
import org.opencypher.caps.impl.syntax.RecordHeaderSyntax._
import org.opencypher.caps.ir.api.expr.Var
import org.opencypher.caps.test.CAPSTestSuite
import org.opencypher.caps.test.fixture.TeamDataFixture
import org.opencypher.caps.web.CAPSJsonSerialiser.toJsonString

//noinspection NameBooleanParameters
class CAPSJsonSerialiserTest extends CAPSTestSuite with TeamDataFixture {

  test("unit table") {
    // Given
    val records = CAPSRecords.unit()

    // Then
    toJsonString(records) should equal(
      s"""{
         |  "columns" : [
         |  ],
         |  "rows" : [
         |    {
         |      ${""}
         |    }
         |  ]
         |}""".stripMargin
    )
  }

  test("single column, no rows") {

    // Given
    val records = CAPSRecords.empty(headerOf('foo))

    // Then
    toJsonString(records) should equal(
      s"""{
        |  "columns" : [
        |    "foo"
        |  ],
        |  "rows" : [
        |  ]
        |}""".stripMargin
    )
  }

  test("single column, three rows") {
    // Given
    val records = CAPSRecords.create(Seq(Row1("myString"), Row1("foo"), Row1(null)))

    // Then
    toJsonString(records) should equal(
      """{
        |  "columns" : [
        |    "foo"
        |  ],
        |  "rows" : [
        |    {
        |      "foo" : "myString"
        |    },
        |    {
        |      "foo" : "foo"
        |    },
        |    {
        |      "foo" : null
        |    }
        |  ]
        |}""".stripMargin
    )
  }

  test("three columns, three rows") {
    // Given
    val records = CAPSRecords.create(
      Seq(
        Row3("myString", 4L, false),
        Row3("foo", 99999999L, true),
        Row3(null, -1L, true)
      ))

    // Then
    toJsonString(records) should equal(
      """{
        |  "columns" : [
        |    "foo",
        |    "v",
        |    "veryLongColumnNameWithBoolean"
        |  ],
        |  "rows" : [
        |    {
        |      "foo" : "myString",
        |      "v" : 4,
        |      "veryLongColumnNameWithBoolean" : false
        |    },
        |    {
        |      "foo" : "foo",
        |      "v" : 99999999,
        |      "veryLongColumnNameWithBoolean" : true
        |    },
        |    {
        |      "foo" : null,
        |      "v" : -1,
        |      "veryLongColumnNameWithBoolean" : true
        |    }
        |  ]
        |}""".stripMargin
    )
  }

  test("serialize lists") {
    // Given
    val records = CAPSRecords.create(
      Seq(
        ListRow(Seq("foo", "bar", "baz"), Seq(42, 23, 8), Seq(true, false, false)),
        ListRow(null, Seq.empty, Seq.empty)
      ))

    // Then
    toJsonString(records) should equal(
      """{
        |  "columns" : [
        |    "strings",
        |    "integers",
        |    "booleans"
        |  ],
        |  "rows" : [
        |    {
        |      "strings" : [
        |        "foo",
        |        "bar",
        |        "baz"
        |      ],
        |      "integers" : [
        |        42,
        |        23,
        |        8
        |      ],
        |      "booleans" : [
        |        true,
        |        false,
        |        false
        |      ]
        |    },
        |    {
        |      "strings" : null,
        |      "integers" : [
        |      ],
        |      "booleans" : [
        |      ]
        |    }
        |  ]
        |}""".stripMargin
    )
  }

  ignore("serialize maps") {
    // Given
    val records = CAPSRecords.create(
      Seq(
        MapRow(
          Map("foo" -> "Alice", "bar" -> "Bob", "baz" -> "Carols"),
          Map("foo" -> 42, "bar" -> 23, "baz" -> 8),
          Map("foo" -> true, "bar" -> false, "baz" -> false)
        ),
        MapRow(null, Map.empty, Map.empty)
      ))

    // Then
    toJsonString(records) should equal(
      """{
        |  "columns" : [
        |    "strings",
        |    "integers",
        |    "booleans"
        |  ],
        |  "rows" : [
        |    {
        |      "booleans" : [
        |        true,
        |        false,
        |        false
        |      ],
        |      "integers" : [
        |        42,
        |        23,
        |        8
        |      ],
        |      "strings" : [
        |        "foo",
        |        "bar",
        |        "baz"
        |      ]
        |    },
        |    {
        |      "booleans" : [
        |      ],
        |      "integers" : [
        |      ],
        |      "strings" : null
        |    }
        |  ]
        |}""".stripMargin
    )
  }

  test("graph serialization") {
    val graph = CAPSGraph.create(personTable, bookTable, readsTable, knowsTable, influencesTable)
    val asJson = toJsonString(graph)
    val expected =
      s"""{
          |  "nodes" : [
          |    {
          |      "id" : 1,
          |      "labels" : [
          |        "Person",
          |        "Swedish"
          |      ],
          |      "properties" : {
         |        "luckyNumber" : 23,
          |        "name" : "Mats"
          |      }
          |    },
          |    {
          |      "id" : 2,
          |      "labels" : [
          |        "Person"
          |      ],
          |      "properties" : {
         |        "luckyNumber" : 42,
          |        "name" : "Martin"
          |      }
          |    },
          |    {
          |      "id" : 3,
          |      "labels" : [
          |        "Person"
          |      ],
          |      "properties" : {
         |        "luckyNumber" : 1337,
          |        "name" : "Max"
          |      }
          |    },
          |    {
          |      "id" : 4,
          |      "labels" : [
          |        "Person"
          |      ],
          |      "properties" : {
         |        "luckyNumber" : 9,
          |        "name" : "Stefan"
          |      }
          |    },
          |    {
          |      "id" : 10,
          |      "labels" : [
          |        "Book"
          |      ],
          |      "properties" : {
          |        "title" : "1984",
          |        "year" : 1949
          |      }
          |    },
          |    {
          |      "id" : 20,
          |      "labels" : [
          |        "Book"
          |      ],
          |      "properties" : {
          |        "title" : "Cryptonomicon",
          |        "year" : 1999
          |      }
          |    },
          |    {
          |      "id" : 30,
          |      "labels" : [
          |        "Book"
          |      ],
          |      "properties" : {
          |        "title" : "The Eye of the World",
          |        "year" : 1990
          |      }
          |    },
          |    {
          |      "id" : 40,
          |      "labels" : [
          |        "Book"
          |      ],
          |      "properties" : {
          |        "title" : "The Circle",
          |        "year" : 2013
          |      }
          |    }
          |  ],
          |  "edges" : [
          |    {
          |      "id" : 100,
         |      "source" : 100,
          |      "target" : 10,
          |      "type" : "READS",
          |      "properties" : {
          |        "recommends" : true
          |      }
          |    },
          |    {
          |      "id" : 200,
         |      "source" : 200,
          |      "target" : 40,
          |      "type" : "READS",
          |      "properties" : {
          |        "recommends" : true
          |      }
          |    },
          |    {
          |      "id" : 300,
         |      "source" : 300,
          |      "target" : 30,
          |      "type" : "READS",
          |      "properties" : {
          |        "recommends" : true
          |      }
          |    },
          |    {
          |      "id" : 400,
         |      "source" : 400,
          |      "target" : 20,
          |      "type" : "READS",
          |      "properties" : {
          |        "recommends" : false
          |      }
          |    },
          |    {
          |      "id" : 1,
          |      "source" : 1,
          |      "target" : 2,
          |      "type" : "KNOWS",
          |      "properties" : {
          |        "since" : 2017
          |      }
          |    },
          |    {
          |      "id" : 2,
          |      "source" : 1,
          |      "target" : 3,
          |      "type" : "KNOWS",
          |      "properties" : {
          |        "since" : 2016
          |      }
          |    },
          |    {
          |      "id" : 3,
          |      "source" : 1,
          |      "target" : 4,
          |      "type" : "KNOWS",
          |      "properties" : {
          |        "since" : 2015
          |      }
          |    },
          |    {
          |      "id" : 4,
          |      "source" : 2,
          |      "target" : 3,
          |      "type" : "KNOWS",
          |      "properties" : {
          |        "since" : 2016
          |      }
          |    },
          |    {
          |      "id" : 5,
          |      "source" : 2,
          |      "target" : 4,
          |      "type" : "KNOWS",
          |      "properties" : {
          |        "since" : 2013
          |      }
          |    },
          |    {
          |      "id" : 6,
          |      "source" : 3,
          |      "target" : 4,
          |      "type" : "KNOWS",
          |      "properties" : {
          |        "since" : 2016
          |      }
          |    },
          |    {
          |      "id" : 1000,
          |      "source" : 10,
          |      "target" : 20,
          |      "type" : "INFLUENCES",
          |      "properties" : {
          |        ${""}
          |      }
          |    }
          |  ],
          |  "labels" : [
          |    "Person",
          |    "Swedish",
          |    "Book"
          |  ],
          |  "types" : [
          |    "READS",
          |    "KNOWS",
          |    "INFLUENCES"
          |  ]
         |}""".stripMargin
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
