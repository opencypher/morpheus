/*
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
package org.opencypher.caps.impl.spark.io.hdfs

import org.opencypher.caps.test.BaseTestSuite

class CsvSchemaTest extends BaseTestSuite {
  test("read valid node schema") {
    val schema =
      """
        |{
        |  "idField": {
        |    "name": "id",
        |    "column": 0,
        |    "valueType": "LONG"
        |  },
        |  "implicitLabels": ["Person","Employee"],
        |  "optionalLabels": [
        |    {
        |      "name": "Swede",
        |      "column": 1,
        |      "valueType": "BOOLEAN"
        |    },
        |    {
        |      "name": "German",
        |      "column": 2,
        |      "valueType": "BOOLEAN"
        |    }
        |  ],
        |  "propertyFields": [
        |    {
        |      "name": "name",
        |      "column": 3,
        |      "valueType": "STRING"
        |    },
        |    {
        |      "name": "luckyNumber",
        |      "column": 4,
        |      "valueType": "INTEGER"
        |    }
        |  ]
        |}
      """.stripMargin

    val csvSchema = CsvNodeSchema(schema)

    csvSchema.idField should equal(CsvField("id", 0, "LONG"))
    csvSchema.implicitLabels should equal(List("Person","Employee"))
    csvSchema.optionalLabels should equal(List(
      CsvField("Swede", 1, "BOOLEAN"),
      CsvField("German", 2, "BOOLEAN")
    ))
    csvSchema.propertyFields should equal(List(
      CsvField("name", 3, "STRING"),
      CsvField("luckyNumber", 4, "INTEGER")
    ))
  }

  test("read node schema without optional labels") {
    val schema =
      """
        |{
        |  "idField": {
        |    "name": "id",
        |    "column": 0,
        |    "valueType": "LONG"
        |  },
        |  "implicitLabels": ["Person","Employee"],
        |  "propertyFields": [
        |    {
        |      "name": "name",
        |      "column": 3,
        |      "valueType": "STRING"
        |    },
        |    {
        |      "name": "luckyNumber",
        |      "column": 4,
        |      "valueType": "INTEGER"
        |    }
        |  ]
        |}
      """.stripMargin

    val csvSchema = CsvNodeSchema(schema)

    csvSchema.idField should equal(CsvField("id", 0, "LONG"))
    csvSchema.implicitLabels should equal(List("Person","Employee"))
    csvSchema.optionalLabels should equal(List())
    csvSchema.propertyFields should equal(List(
      CsvField("name", 3, "STRING"),
      CsvField("luckyNumber", 4, "INTEGER")
    ))
  }

  test("read valid relationship schema") {
    val schema =
      """
        |{
        |  "idField": {
        |     "name": "id",
        |     "column": 0,
        |     "valueType": "LONG"
        |  },
        |  "startIdField": {
        |     "name": "startId",
        |     "column": 1,
        |     "valueType": "LONG"
        |  },
        |  "endIdField":  {
        |     "name": "endId",
        |     "column": 2,
        |     "valueType": "LONG"
        |  },
        |  "relationshipType": "KNOWS",
        |  "propertyFields": [
        |    {
        |      "name": "since",
        |      "column": 3,
        |      "valueType": "INTEGER"
        |    }
        |  ]
        |}
      """.stripMargin

    val csvSchema = CsvRelSchema(schema)

    csvSchema.idField should equal(CsvField("id", 0, "LONG"))
    csvSchema.startIdField should equal(CsvField("startId", 1, "LONG"))
    csvSchema.endIdField should equal(CsvField("endId", 2, "LONG"))
    csvSchema.relType should equal("KNOWS")
    csvSchema.propertyFields should equal(List(
      CsvField("since", 3, "INTEGER")
    ))
  }

  test("read rel schema without fields") {
    val schema =
      """
        |{
        |  "idField": {
        |     "name": "id",
        |     "column": 0,
        |     "valueType": "LONG"
        |  },
        |  "startIdField": {
        |     "name": "startId",
        |     "column": 1,
        |     "valueType": "LONG"
        |  },
        |  "endIdField":  {
        |     "name": "endId",
        |     "column": 2,
        |     "valueType": "LONG"
        |  },
        |  "relationshipType": "KNOWS"
        |}
      """.stripMargin

    val csvSchema = CsvRelSchema(schema)

    csvSchema.idField should equal(CsvField("id", 0, "LONG"))
    csvSchema.startIdField should equal(CsvField("startId", 1, "LONG"))
    csvSchema.endIdField should equal(CsvField("endId", 2, "LONG"))
    csvSchema.relType should equal("KNOWS")
    csvSchema.propertyFields should equal(List())
  }
}
