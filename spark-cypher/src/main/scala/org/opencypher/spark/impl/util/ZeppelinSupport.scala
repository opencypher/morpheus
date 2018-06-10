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
package org.opencypher.spark.impl.util

import org.opencypher.okapi.api.value.CypherValue.CypherEntity._
import org.opencypher.okapi.api.value.CypherValue.CypherNode._
import org.opencypher.okapi.api.value.CypherValue.CypherRelationship._
import org.opencypher.okapi.api.value.CypherValue.{CypherNode, CypherRelationship}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.spark.impl.{CAPSGraph, CAPSRecords, CAPSResult}
import upickle.Js

import scala.collection.JavaConverters._
import scala.util.Random

/**
  * Provides helper methods for Apache Zeppelin integration
  */
object ZeppelinSupport {

  implicit class ResultVisualizer(result: CAPSResult) {

    /**
      * Visualizes the result in Zeppelin.
      * If the result contains a graph, it is shown as a network (see [[org.opencypher.spark.impl.util.ZeppelinSupport.ZeppelinGraph#printGraph]]).
      * If the result contains a tabular result, they are visualized as a table (see [[org.opencypher.spark.impl.util.ZeppelinSupport.ZeppelinRecords#printTable]]).
      */
    def printZeppelin(): Unit = {
      result.graph match {
        case Some(g) => g.printGraph()
        case None => result.records.get.printTable()
      }
    }
  }

  implicit class ZeppelinRecords(r: CAPSRecords) {

    /**
      * Serialises CAPSRecords to JSON. The format is as follows:
      *
      * {{{
      * {
      *   "columns" : [ "key" ]   // array of columns
      *   "rows" : [              // array of rows
      *     {                     // each row is an object
      *       "key" : "value"     // each cell is a tuple
      *     }
      *   ]
      * }
      * }}}
      **/
    def toZeppelinJson: Js.Value = {
      val rows = Js.Arr.from(r.collect.map { row =>
        r.header.fieldsInOrder.map { field =>
          field -> row(field).toJson
        }
      })
      Js.Obj(
        "columns" -> r.header.fieldsInOrder.map(Js.Str),
        "rows" -> rows
      )
    }


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
    def printTable(): Unit = {
      val fields = r.header.fieldsInOrder
      val header = fields.mkString("\t")
      val rows = r.collect.map { data =>
        fields.map(field => data.get(field).get).mkString("\t")
      }.mkString("\n")

      print(
        s"""
           |%table
           |$header
           |$rows""".stripMargin)
    }
  }

  val labelJsonKey: String = "label"
  val dataJsonKey: String = "data"
  val sourceJsonKey: String = "source"
  val targetJsonKey: String = "target"

  implicit class ZeppelinNode(n: CypherNode[_]) {

    /**
      * Returns a Json formatted node:
      *
      * {{{
      *   "n" : {
      *     "id" : 0,           // id is a string
      *     "labels" : [        // labels is an array of strings
      *       "A",
      *       "B"
      *     ],
      *     "properties" : {    // properties is an object
      *       "key" : "value",  // key-value is a tuple
      *       "foo" : bar
      *     }
      *   }
      * }}}
      */
    def toZeppelinJson: Js.Value = {
      val default = n.toJson
      Js.Obj(
        idJsonKey -> default(idJsonKey),
        labelJsonKey -> Js.Str(n.labels.headOption.getOrElse("")),
        labelsJsonKey -> default(labelsJsonKey),
        dataJsonKey -> default(propertiesJsonKey)
      )
    }
  }

  implicit class ZeppelinRelationship(r: CypherRelationship[_]) {

    /**
      * Returns a Json formatted relationship:
      *
      * {{{
      *   "n" : {
      *     "id" : "0",           // id is a string
      *     "source" : "0",       // id of start node is a string
      *     "target" : "0",       // id of end node is a string
      *     "type" : "T"        // relationship type is a string
      *     "properties" : {    // properties is an object
      *       "key" : "value",
      *       "foo" : bar
      *     }
      *   }
      * }}}
      */
    def toZeppelinJson: Js.Value = {
      val default = r.toJson
      Js.Obj(
        idJsonKey -> default(idJsonKey),
        sourceJsonKey -> default(startIdJsonKey),
        targetJsonKey -> default(endIdJsonKey),
        labelJsonKey -> default(typeJsonKey),
        dataJsonKey -> default(propertiesJsonKey)
      )
    }
  }

  implicit class ZeppelinGraph(g: CAPSGraph) {

    /**
      * Prints the specified graph in Zeppelins `%network` format
      *
      * {{{
      *   g.cypher("""
      *     MATCH (p:Person)-[k:KNOWS]->(f)
      *     RETURN GRAPH friends of (p)-[k]->(f)
      *   """).printGraph
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
      *           "age": "20"
      *         }
      *       },
      *       {
      *         "id": "2",
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
    def printGraph(): Unit = {
      val graphJson = g.toZeppelinJson
      print(
        s"""
           |%network
           |${graphJson.render(2)}
        """.stripMargin)
    }

    /**
      * CAPSGraphs are serialized in the following format:
      *
      * {{{
      * {
      *   "nodes" : [ LIST_OF_NODES ]   // array of nodes
      *   "edges" : [ LIST_OF_EDGES ]   // array of relationships
      *   "labels": [ "Person", "Book"] // each label present in the graph
      *   "types": [ "KNOWS", "READS"]  // each relationship type present in the graph
      * }
      * }}}
      *
      * The format of scalar values follows the format of [[org.opencypher.okapi.api.value.CypherValue.CypherValue#toString]].
      */
    def toZeppelinJson: Js.Value = {
      val nodeJson: Js.Value = g.nodes("n").toCypherMaps.toLocalIterator.asScala.map { node =>
        node("n") match {
          case n: CypherNode[_] => n.toZeppelinJson
          case notANode => throw IllegalArgumentException("a node", notANode)
        }
      }

      val relJson: Js.Value = g.relationships("r").toCypherMaps.toLocalIterator.asScala.map { rel =>
        rel("r") match {
          case r: CypherRelationship[_] => r.toZeppelinJson
          case notARel => throw IllegalArgumentException("a relationship", notARel)
        }
      }

      Map[String, Js.Value](
        "nodes" -> nodeJson,
        "edges" -> relJson,
        "labels" -> g.schema.labels.toSeq.sorted.map(l => l -> Js.Str(colorForLabel(l))),
        "types" -> g.schema.relationshipTypes.toSeq.sorted.map(Js.Str),
        "directed" -> Js.True
      )
    }

    private def colorForLabel(label: String): String = {
      val rand = new Random(label.hashCode)
      val r = rand.nextInt(255)
      val g = rand.nextInt(255)
      val b = rand.nextInt(255)
      s"#${r.toHexString}${g.toHexString}${b.toHexString}"
    }
  }

}
