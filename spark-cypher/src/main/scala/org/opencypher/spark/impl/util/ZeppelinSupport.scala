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
package org.opencypher.spark.impl.util

import io.circe.Json
import org.opencypher.spark.impl.{CAPSGraph, CAPSRecords, CAPSResult}
import org.opencypher.spark.web.JsonSerialiser

import scala.util.Random

/**
  * Provides helper methods for Apache Zeppelin integration
  */
object ZeppelinSupport {
  implicit class ResultVisualizer(result: CAPSResult) {

    /**
      * Visualizes the result in Zeppelin.
      * If the result contains a graph, it is shown as a network (see [[org.opencypher.spark.impl.util.ZeppelinSupport.GraphVisualizer#asZeppelinGraph]]).
      * If the result contains a tabular result, they are visualized as a table (see [[org.opencypher.spark.impl.util.ZeppelinSupport.RecordsVisualizer#asZeppelingTable]]).
      */
    def asZeppelin(): Unit = {
      result.graph match {
        case Some(g) => g.asZeppelinGraph()
        case None => result.records.get.asZeppelingTable()
      }
    }
  }

  implicit class RecordsVisualizer(records: CAPSRecords) {

    /**
      * Prints the records in the Zeppelin `%table` format
      * {{{
      *   MATCH (n:Person)
      *   RETURN n.name, n.age
      * }}}
      *
      * will print the following data
      *
      * {{{
      *   $table
      *   n.name\tn.age
      *   Alice\t20
      *   Bob\t42
      * }}}
      */
    def asZeppelingTable(): Unit = {
      val fields = records.header.fieldsInOrder
      val header = fields.mkString("\t")
      val rows = records.collect.map { data =>
        fields.map(field => data.get(field).get).mkString("\t")
      }.mkString("\n")

      print(s"""
               |%table
               |$header
               |$rows""".stripMargin)
    }
  }

  implicit class GraphVisualizer(graph: CAPSGraph) {

    /**
      * Prints the specified graph in Zeppelins `%network` format
      *
      * {{{
      *   g.cypher("""
      *     MATCH (p:Person)-[k:KNOWS]->(f)
      *     RETURN GRAPH friends of (p)-[k]->(f)
      *   """).asZeppelinTable("friends")
      * }}}
      *
      * will print the following data
      *
      * {{{
      *   $network
      *   {
      *     "nodes" : [
      *       {
      *         "id": 1,
      *         "label": "Person",
      *         "labels": ["Person"],
      *         "data": {
      *           "name": "Alice",
      *           "age": 20
      *         }
      *       },
      *       {
      *         "id": 2,
      *         "label": "Person",
      *         "labels": ["Person"],
      *         "data": {
      *           "name": "Bob",
      *           "age": 42
      *         }
      *       }
      *     ],
      *     "edges" : [
      *       {
      *         "id": 3,
      *         "source": 1,
      *         "target": 2,
      *         "label": "KNOWS",
      *         "data": {
      *           "since": 2000
      *         }
      *       }
      *     ],
      *     "labels": {"Person": "#abababa"},
      *     "types": [ "KNOWS"],
      *     "directed": true
      *   }
      * }}}
      */
    def asZeppelinGraph(): Unit = {
      val graphJson = ZeppelinJsonSerialiser.toJsonString(graph)
      print(s"""
           |%network
           |$graphJson
        """.stripMargin)
    }
  }

  object ZeppelinJsonSerialiser extends JsonSerialiser {
    override protected def formatNode(id: String, labels: Set[String], properties: Map[String, Json]): Json = {
      Json.obj(
        "id" -> Json.fromString(id),
        "label" -> Json.fromString(labels.headOption.getOrElse("")),
        "labels" -> Json.arr(
          labels.toSeq.map(Json.fromString): _*
        ),
        "data" -> Json.obj(
          properties.toSeq: _*
        )
      )
    }

    override protected def formatRel(
        id: String,
        source: String,
        target: String,
        typ: String,
        properties: Map[String, Json]): Json = {
      Json.obj(
        "id" -> Json.fromString(id),
        "source" -> Json.fromString(source),
        "target" -> Json.fromString(target),
        "label" -> Json.fromString(typ),
        "data" -> Json.obj(
          properties.toSeq: _*
        )
      )
    }

    override protected def formatGraph(graph: CAPSGraph, nodes: Seq[Json], rels: Seq[Json]): Json = {
      Json.obj(
        "nodes" -> Json.arr(nodes: _*),
        "edges" -> Json.arr(rels: _*),
        "labels" -> Json.obj(graph.schema.labels.map(l => l -> Json.fromString(randomColor)).toSeq: _*),
        "directed" -> Json.True,
        "types" -> Json.arr(graph.schema.relationshipTypes.map(Json.fromString).toSeq: _*)
      )
    }

    private def randomColor: String = {
      val rand = new Random()
      val r = rand.nextInt(255)
      val g = rand.nextInt(255)
      val b = rand.nextInt(255)
      s"#${r.toHexString}${g.toHexString}${b.toHexString}"
    }
  }
}
