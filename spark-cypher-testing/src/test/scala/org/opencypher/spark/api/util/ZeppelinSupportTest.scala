/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.Format._
import org.opencypher.spark.impl.acceptance.ScanGraphInit
import org.opencypher.spark.testing.MorpheusTestSuite
import org.opencypher.spark.testing.fixture.TeamDataFixture

class ZeppelinSupportTest extends MorpheusTestSuite with TeamDataFixture with ScanGraphInit {
  // scalastyle:off line.contains.tab
  it("supports Zeppelin table representation") {
    val graph = morpheus.graphs.create(personTable)
    val result = graph.cypher("MATCH (p:Person) RETURN p.name, p.luckyNumber")
    val asTable = result.records.toZeppelinTable

    val expected =
      """p.name	p.luckyNumber
        |'Mats'	23
        |'Martin'	42
        |'Max'	1337
        |'Stefan'	9""".stripMargin

    asTable should equal(expected)
  }
  // scalastyle:on line.contains.tab

  it("can render a graph from records") {
    val graph = initGraph(
      """
        |CREATE (a:Person {val1: 1, val2: "foo"})
        |CREATE (b:Person:Swedish {val1: 2, val2: "bar"})
        |CREATE (c:Person {val1: 3, val2: "baz"})
        |CREATE (a)-[:KNOWS {since: 2015}]->(b)
        |CREATE (a)-[:KNOWS {since: 2016}]->(c)
        |CREATE (b)-[:KNOWS {since: 2017}]->(c)
      """.stripMargin)
    val result = graph.cypher("MATCH (p:Person)-[k:KNOWS]->(p2:Person) RETURN p, k, p2 ORDER BY p.val1, k.since")

    import org.opencypher.okapi.api.util.ZeppelinSupport._

    val asGraph = result.records.toZeppelinGraph

    val expected =
      """|{
         |  "nodes": [
         |    {
         |      "id": "01",
         |      "label": "Swedish",
         |      "labels": [
         |        "Person",
         |        "Swedish"
         |      ],
         |      "data": {
         |        "val1": "2",
         |        "val2": "bar"
         |      }
         |    },
         |    {
         |      "id": "00",
         |      "label": "Person",
         |      "labels": [
         |        "Person"
         |      ],
         |      "data": {
         |        "val1": "1",
         |        "val2": "foo"
         |      }
         |    },
         |    {
         |      "id": "02",
         |      "label": "Person",
         |      "labels": [
         |        "Person"
         |      ],
         |      "data": {
         |        "val1": "3",
         |        "val2": "baz"
         |      }
         |    }
         |  ],
         |  "edges": [
         |    {
         |      "id": "05",
         |      "source": "01",
         |      "target": "02",
         |      "label": "KNOWS",
         |      "data": {
         |        "since": "2017"
         |      }
         |    },
         |    {
         |      "id": "04",
         |      "source": "00",
         |      "target": "02",
         |      "label": "KNOWS",
         |      "data": {
         |        "since": "2016"
         |      }
         |    },
         |    {
         |      "id": "03",
         |      "source": "00",
         |      "target": "01",
         |      "label": "KNOWS",
         |      "data": {
         |        "since": "2015"
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

    val act = sorted(ujson.read(asGraph))
    val exp = sorted(ujson.read(expected))
    act shouldEqual exp
  }

  def sorted(v: ujson.Value): ujson.Value = v match {
    case ujson.Obj(x) =>
      val res = ujson.Obj()
      x.mapValues(sorted(_)).toSeq.sortBy(_._1).foreach(e => res.value.put(e._1, e._2))
      res
    case ujson.Arr(x) =>
      val res = ujson.Arr()
      x.map(sorted(_)).sortBy(_.toString).foreach(e => res.value.append(e))
      res
    case _ => v
  }

  it("supports Zeppelin network representation") {
    val graph = morpheus.graphs.create(personTable, bookTable, readsTable, knowsTable, influencesTable)
    val asJson = graph.toZeppelinJson()(CypherValue.Format.defaultValueFormatter)
    val expected = ujson.read(
      s"""
         |{
         |  "nodes": [
         |    {
         |      "id": "01",
         |      "label": "Person",
         |      "labels": [
         |        "Person"
         |      ],
         |      "data": {
         |        "luckyNumber": "23",
         |        "name": "Mats"
         |      }
         |    },
         |    {
         |      "id": "02",
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
         |      "id": "03",
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
         |      "id": "04",
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
         |      "id": "0A",
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
         |      "id": "14",
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
         |      "id": "1E",
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
         |      "id": "28",
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
         |      "id": "64",
         |      "source": "64",
         |      "target": "0A",
         |      "label": "READS",
         |      "data": {
         |        "recommends": true
         |      }
         |    },
         |    {
         |      "id": "C801",
         |      "source": "C801",
         |      "target": "28",
         |      "label": "READS",
         |      "data": {
         |        "recommends": true
         |      }
         |    },
         |    {
         |      "id": "AC02",
         |      "source": "AC02",
         |      "target": "1E",
         |      "label": "READS",
         |      "data": {
         |        "recommends": true
         |      }
         |    },
         |    {
         |      "id": "9003",
         |      "source": "9003",
         |      "target": "14",
         |      "label": "READS",
         |      "data": {
         |        "recommends": false
         |      }
         |    },
         |    {
         |      "id": "01",
         |      "source": "01",
         |      "target": "02",
         |      "label": "KNOWS",
         |      "data": {
         |        "since": "2017"
         |      }
         |    },
         |    {
         |      "id": "02",
         |      "source": "01",
         |      "target": "03",
         |      "label": "KNOWS",
         |      "data": {
         |        "since": "2016"
         |      }
         |    },
         |    {
         |      "id": "03",
         |      "source": "01",
         |      "target": "04",
         |      "label": "KNOWS",
         |      "data": {
         |        "since": "2015"
         |      }
         |    },
         |    {
         |      "id": "04",
         |      "source": "02",
         |      "target": "03",
         |      "label": "KNOWS",
         |      "data": {
         |        "since": "2016"
         |      }
         |    },
         |    {
         |      "id": "05",
         |      "source": "02",
         |      "target": "04",
         |      "label": "KNOWS",
         |      "data": {
         |        "since": "2013"
         |      }
         |    },
         |    {
         |      "id": "06",
         |      "source": "03",
         |      "target": "04",
         |      "label": "KNOWS",
         |      "data": {
         |        "since": "2016"
         |      }
         |    },
         |    {
         |      "id": "E807",
         |      "source": "0A",
         |      "target": "14",
         |      "label": "INFLUENCES",
         |      "data": {
         |
         |      }
         |    }
         |  ],
         |  "labels": {
         |    "Book": "#40c294",
         |    "Person": "#cbfe79"
         |  },
         |  "types": [
         |    "INFLUENCES",
         |    "KNOWS",
         |    "READS"
         |  ],
         |  "directed": true
         |}""".stripMargin)

    asJson should equal(expected)
  }

}
