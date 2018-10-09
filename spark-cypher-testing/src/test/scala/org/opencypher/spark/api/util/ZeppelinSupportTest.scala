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

import org.opencypher.okapi.api.util.ZeppelinSupport._
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.fixture.TeamDataFixture

class ZeppelinSupportTest extends CAPSTestSuite with TeamDataFixture {
  // scalastyle:off line.contains.tab
  it("supports Zeppelin table representation") {
    val graph = caps.graphs.create(personTable)
    val result = graph.cypher("MATCH (p:Person) RETURN p.name, p.luckyNumber")
    val asTable = result.records.toZeppelinTable

    val expected = """p.name	p.luckyNumber
                     |'Mats'	23
                     |'Martin'	42
                     |'Max'	1337
                     |'Stefan'	9""".stripMargin

    asTable should equal(expected)
  }
  // scalastyle:on line.contains.tab

  it("can render a graph from records") {
    val graph = caps.graphs.create(personTable, knowsTable)
    val result = graph.cypher("MATCH (p:Person)-[k:KNOWS]->(p2:Person) RETURN p, k, p2")
    val asGraph = result.records.toZeppelinGraph

    val expected = """{
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
                     |    }
                     |  ],
                     |  "edges": [
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
                     |      "id": "2",
                     |      "source": "1",
                     |      "target": "3",
                     |      "label": "KNOWS",
                     |      "data": {
                     |        "since": "2016"
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
                     |      "id": "5",
                     |      "source": "2",
                     |      "target": "4",
                     |      "label": "KNOWS",
                     |      "data": {
                     |        "since": "2013"
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
                     |      "id": "6",
                     |      "source": "3",
                     |      "target": "4",
                     |      "label": "KNOWS",
                     |      "data": {
                     |        "since": "2016"
                     |      }
                     |    }
                     |  ],
                     |  "labels": {
                     |    "Person": "#cbfe79",
                     |    "Swedish": "#6f27a9"
                     |  },
                     |  "types": [
                     |    "KNOWS"
                     |  ],
                     |  "directed": true
                     |}""".stripMargin

    asGraph should equal(expected)
  }

  it("supports Zeppelin network representation") {
    val graph = caps.graphs.create(personTable, bookTable, readsTable, knowsTable, influencesTable)
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

}
