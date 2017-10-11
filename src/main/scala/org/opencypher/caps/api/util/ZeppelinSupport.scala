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
package org.opencypher.caps.api.util

import io.circe.{Encoder, Json}
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSResult}
import org.opencypher.caps.web.JsonSerialiser

import scala.util.Random

/**
  * Provides helper methods for Apache Zeppelin integration
  */
object ZeppelinSupport {
  implicit class ResultVisualizer(result: CAPSResult) {

    /**
      * Prints the tabular part of the result in the Zeppelin %table format
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
    def asZeppelinTable(): Unit = {
      val fields = result.records.fieldsInOrder

      val header = fields.mkString("\t")
      val rows = result.records.toLocalScalaIterator.map { data =>
        fields.map(field => data.get(field).get).mkString("\t")
      }.mkString("\n")

      print(
        s"""
          |%table
          |$header
          |$rows""".stripMargin)
    }

    /**
      * Prints the specified graph in Zeppelins %network format
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
      * @param name the graphs name
      */
    def asZeppelinGraph(name: String): Unit = {
      val graph = result.graphs(name)
      val graphJson = ZeppelinJsonSerialiser.toJsonString(graph)
      print(
        s"""
           |%network
           |$graphJson
        """.stripMargin)
    }
  }

  object ZeppelinJsonSerialiser extends JsonSerialiser {
    override protected def formatNode(id: Long, labels: Seq[String], properties: Map[String, String]): Json = {
      Json.obj(
        "id" -> Json.fromLong(id),
        "label" -> Json.fromString(labels.headOption.getOrElse("")),
        "labels" -> Json.arr(
          labels.map(Json.fromString): _*
        ),
        "data" -> Json.obj(
          properties.mapValues(Json.fromString).toSeq: _*
        )
      )
    }

    override protected def formatRel(id: Long, source: Long, target: Long,
                                     typ: String, properties: Map[String, String]): Json = {
      Json.obj(
        "id" -> Json.fromLong(id),
        "source" -> Json.fromLong(source),
        "target" -> Json.fromLong(target),
        "label" -> Json.fromString(typ),
        "data" -> Json.obj(
          properties.mapValues(Json.fromString).toSeq: _*
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
