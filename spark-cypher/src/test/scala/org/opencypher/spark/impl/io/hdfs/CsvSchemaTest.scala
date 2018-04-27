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
package org.opencypher.spark.impl.io.hdfs

import org.opencypher.okapi.testing.BaseTestSuite

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

  test("read node schema with nullable fields") {
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
        |      "valueType": "STRING",
        |      "nullable": true
        |    },
        |    {
        |      "name": "luckyNumber",
        |      "column": 4,
        |      "valueType": "INTEGER",
        |      "nullable": false
        |    }
        |  ]
        |}
      """.stripMargin

    val csvSchema = CsvNodeSchema(schema)

    csvSchema.idField should equal(CsvField("id", 0, "LONG"))
    csvSchema.implicitLabels should equal(List("Person","Employee"))
    csvSchema.optionalLabels should equal(List())
    csvSchema.propertyFields should equal(List(
      CsvField("name", 3, "STRING", Some(true)),
      CsvField("luckyNumber", 4, "INTEGER", Some(false))
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
    csvSchema.relationshipType should equal("KNOWS")
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
    csvSchema.relationshipType should equal("KNOWS")
    csvSchema.propertyFields should equal(List())
  }
}
