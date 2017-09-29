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
package org.opencypher.caps.web

import org.opencypher.caps.api.expr.Var
import org.opencypher.caps.api.record.{OpaqueField, RecordHeader}
import org.opencypher.caps.api.spark.CAPSRecords
import org.opencypher.caps.api.types.CTNode
import org.opencypher.caps.impl.syntax.header.{addContents, _}
import org.opencypher.caps.test.CAPSTestSuite
import org.opencypher.caps.web.RecordsSerialiser.toJsonString

// TODO: Lists, maps
//noinspection NameBooleanParameters
class RecordsSerialiserTest extends CAPSTestSuite {

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
        |      "foo" : "'myString'"
        |    },
        |    {
        |      "foo" : "'foo'"
        |    },
        |    {
        |      "foo" : "null"
        |    }
        |  ]
        |}""".stripMargin
    )
  }

  test("three columns, three rows") {
    // Given
    val records = CAPSRecords.create(Seq(
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
        |      "foo" : "'myString'",
        |      "v" : "4",
        |      "veryLongColumnNameWithBoolean" : "false"
        |    },
        |    {
        |      "foo" : "'foo'",
        |      "v" : "99999999",
        |      "veryLongColumnNameWithBoolean" : "true"
        |    },
        |    {
        |      "foo" : "null",
        |      "v" : "-1",
        |      "veryLongColumnNameWithBoolean" : "true"
        |    }
        |  ]
        |}""".stripMargin
    )
  }

  test("nodes and rels from cypher") {
    // Given
    val graph = TestGraph("(a:A {a: 1l, b: true})-[:T {t: 3.14d}]->(:B {b: 's'})-[:T]->(:X:Y:Z)-[:CIRCLE]->(a)")

    val records = graph.cypher("MATCH (n)-[r]->() RETURN n, r").recordsWithDetails

    // Then
    toJsonString(records) should equal(
      s"""|{
          |  "columns" : [
          |    "n",
          |    "r"
          |  ],
          |  "rows" : [
          |    {
          |      "n" : {
          |        "id" : 0,
          |        "labels" : [
          |          "A"
          |        ],
          |        "properties" : {
          |          "a" : "1",
          |          "b" : "true"
          |        }
          |      },
          |      "r" : {
          |        "id" : 0,
          |        "source" : 0,
          |        "target" : 1,
          |        "type" : "T",
          |        "properties" : {
          |          "t" : "3.14"
          |        }
          |      }
          |    },
          |    {
          |      "n" : {
          |        "id" : 1,
          |        "labels" : [
          |          "B"
          |        ],
          |        "properties" : {
          |          "b" : "'s'"
          |        }
          |      },
          |      "r" : {
          |        "id" : 1,
          |        "source" : 1,
          |        "target" : 2,
          |        "type" : "T",
          |        "properties" : {
          |          ${""}
          |        }
          |      }
          |    },
          |    {
          |      "n" : {
          |        "id" : 2,
          |        "labels" : [
          |          "Z",
          |          "Y",
          |          "X"
          |        ],
          |        "properties" : {
          |          ${""}
          |        }
          |      },
          |      "r" : {
          |        "id" : 2,
          |        "source" : 2,
          |        "target" : 0,
          |        "type" : "CIRCLE",
          |        "properties" : {
          |          ${""}
          |        }
          |      }
          |    }
          |  ]
          |}""".stripMargin
    )
  }

  private case class Row1(foo: String)
  private case class Row3(foo: String, v: Long, veryLongColumnNameWithBoolean: Boolean)

  private def headerOf(fields: Symbol*): RecordHeader = {
    val value1 = fields.map(f => OpaqueField(Var(f.name)(CTNode)))
    val (header, _) = RecordHeader.empty.update(addContents(value1))
    header
  }

}
