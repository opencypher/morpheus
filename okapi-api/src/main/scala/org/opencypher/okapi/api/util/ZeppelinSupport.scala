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
package org.opencypher.okapi.api.util

import org.opencypher.okapi.api.graph.{CypherResult, PropertyGraph}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.Element._
import org.opencypher.okapi.api.value.CypherValue.Node._
import org.opencypher.okapi.api.value.CypherValue.Relationship._
import org.opencypher.okapi.api.value.CypherValue.{Node, Relationship}
import ujson._

import scala.util.Random
/**
  * Provides helper methods for Apache Zeppelin integration
  */
object ZeppelinSupport {

  implicit class ResultVisualizer(result: CypherResult) {

    /**
      * Visualizes the result in Zeppelin.
      * If the result contains a graph, it is shown as a network (see [[ZeppelinSupport.ZeppelinGraph#printGraph]]).
      * If the result contains a tabular result, they are:
      *   - visualized as a graph if the result only contains nodes and relationships (see [[ZeppelinSupport.ZeppelinRecords#printGraph]])
      *   - TODO: visualized as a table if the result contains non element values (see [[ZeppelinSupport.ZeppelinRecords#printTable]])
      */
    def visualize()(implicit formatValue: Any => String = CypherValue.Format.defaultValueFormatter): Unit = {
      result.getGraph match {
        case Some(g) => g.printGraph()
        case None => result.records.printTable() // TODO find a way to identify results that could be printed as a graph
      }
    }
  }

  implicit class ZeppelinRecords(r: CypherRecords) {

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
      *   %table
      *   n.name\tn.age
      *   Alice\t20
      *   Bob\t42
      * }}}
      */
    def printTable()(implicit formatValue: Any => String = CypherValue.Format.defaultValueFormatter): Unit = {
      print(s"""
        |%table
        |$toZeppelinTable
        |""".stripMargin
      )
    }

    /**
      * Prints the records in the Zeppelin `%network` format
      * {{{
      *   MATCH (n:Person)-[b:BOUGHT]->(i:Item)
      *   RETURN n, b, i
      * }}}
      *
      * will print the following data
      *
      * {{{
      * {
      *   "nodes" : [ LIST_OF_NODES ]   // array of nodes
      *   "edges" : [ LIST_OF_EDGES ]   // array of relationships
      *   "labels": [ "Person", "Item"] // each label present in the graph
      *   "types": [ "BOUGHT"]  // each relationship type present in the graph
      *   "directed": true              // indicate that the graph has directed relationships
      * }
      * }}}
      */
    def printGraph()(implicit formatValue: Any => String = CypherValue.Format.defaultValueFormatter): Unit = {
      print(
        s"""
           |%network
           |$toZeppelinGraph
        """.stripMargin)
    }

    /**
      * Returns a Zeppelin compatible table representation of CypherRecords:
      *
      * {{{
      *   n.name\tn.age
      *   Alice\t20
      *   Bob\t42
      * }}}
      */
    def toZeppelinTable()(implicit formatValue: Any => String = CypherValue.Format.defaultValueFormatter): String = {
      val columns = r.logicalColumns.get
      s"""${columns.mkString("\t")}
         |${
        r.iterator.map { row =>
          columns.map(row(_).toCypherString).mkString("\t")
        }.mkString("\n")
      }""".stripMargin
    }

    /**
      * Returns a Zeppelin compatible Json representation of the result.
      * Only colums that are nodes or edges are represented.
      *
      * {{{
      * {
      *   "nodes" : [ LIST_OF_NODES ]   // array of nodes
      *   "edges" : [ LIST_OF_EDGES ]   // array of relationships
      *   "labels": [ "Person", "Book"] // each label present in the graph
      *   "types": [ "KNOWS", "READS"]  // each relationship type present in the graph
      *   "directed": true              // indicate that the graph has directed relationships
      * }
      * }}}
      */
    def toZeppelinGraph()(implicit formatValue: Any => String = CypherValue.Format.defaultValueFormatter): String = {
      val data = r.collect

      val nodeCols = data.headOption.map { row =>
        row.value.collect {
          case (key, _: Node[_]) => key
        }
      }.getOrElse(Seq.empty)

      val relCols = data.headOption.map { row =>
        row.value.collect {
          case (key, _: Relationship[_]) => key
        }
      }.getOrElse(Seq.empty).toSet

      val nodes = data
        .flatMap { row => nodeCols.map(row(_).cast[Node[_]]) }
        .groupBy(_.id)
        .values
        .map(_.head)

      val rels = data
        .flatMap { row => relCols.map(row(_).cast[Relationship[_]]) }
        .groupBy(_.id)
        .values
        .map(_.head)

      val labels = nodes.flatMap(_.labels).toSet
      val types = rels.map(_.relType).toSet

      ZeppelinGraph.toZeppelinJson(
        nodes.toIterator, rels.toIterator, labels, types
      ).render(2)
    }
  }

  private val labelJsonKey: String = "label"
  private val dataJsonKey: String = "data"
  private val sourceJsonKey: String = "source"
  private val targetJsonKey: String = "target"

  private implicit class ZeppelinNode(n: Node[_]) {

    /**
      * Returns a Zeppelin compatible Json representation of a node:
      *
      * {{{
      * {
      *   "id": 0,           // id is a string
      *   "label": "A"       // the main label is a string
      *   "labels": [        // labels is an array of strings
      *     "A",
      *     "B"
      *   ],
      *   "data" : {          // data is an object that contains the properties
      *     "key" : "value",  // key-value is a tuple
      *     "foo" : bar
      *   }
      * }
      * }}}
      */
    def toZeppelinJson()(implicit formatValue: Any => String): Value = {
      val default = n.toJson
      Obj(
        idJsonKey -> default(idJsonKey),
        labelJsonKey -> Str(n.labels.headOption.getOrElse("")),
        labelsJsonKey -> default(labelsJsonKey),
        dataJsonKey -> default(propertiesJsonKey)
      )
    }
  }

  private implicit class ZeppelinRelationship(r: Relationship[_]) {

    /**
      * Returns a Zeppelin compatible Json representation of a relationship:
      *
      * {{{
      * {
      *   "id" : "0",         // id is a string
      *   "source" : "0",     // id of start node is a string
      *   "target" : "0",     // id of end node is a string
      *   "label" : "T"       // relationship type is a string
      *   "data" : {          // data is an object that contains the properties
      *     "key" : "value",  // key-value is a tuple
      *     "foo" : bar
      *   }
      * }
      * }}}
      */
    def toZeppelinJson()(implicit formatValue: Any => String): Value = {
      val default = r.toJson
      Obj(
        idJsonKey -> default(idJsonKey),
        sourceJsonKey -> default(startIdJsonKey),
        targetJsonKey -> default(endIdJsonKey),
        labelJsonKey -> default(typeJsonKey),
        dataJsonKey -> default(propertiesJsonKey)
      )
    }
  }

  private object ZeppelinGraph {
    /**
      * Returns a Zeppelin compatible Json representation of a graph defined by a list of nodes and edges:
      *
      * {{{
      * {
      *   "nodes" : [ LIST_OF_NODES ]   // array of nodes
      *   "edges" : [ LIST_OF_EDGES ]   // array of relationships
      *   "labels": [ "Person", "Book"] // each label present in the graph
      *   "types": [ "KNOWS", "READS"]  // each relationship type present in the graph
      *   "directed": true              // indicate that the graph has directed relationships
      * }
      * }}}
      */
    def toZeppelinJson(
      nodes: Iterator[Node[_]],
      rels: Iterator[Relationship[_]],
      labels: Set[String],
      types: Set[String]
    )(implicit formatValue: Any => String): Value = {
      val nodeJsons = nodes.map(_.toZeppelinJson)
      val relJson = rels.map(_.toZeppelinJson)

      Obj(
        "nodes" -> nodeJsons,
        "edges" -> relJson,
        "labels" -> labels.toSeq.sorted.map(l => l -> Str(colorForLabel(l))),
        "types" -> types.toSeq.sorted.map(Str),
        "directed" -> True
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

  implicit class ZeppelinGraph(g: PropertyGraph) {

    /**
      * Prints the graph in Zeppelin's `%network` format
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
      *           "age": "42"
      *         }
      *       }
      *     ],
      *     "edges" : [
      *       {
      *         "id": "3",
      *         "source": "1",
      *         "target": "2",
      *         "label": "KNOWS",
      *         "data": {
      *           "since": "2000"
      *         }
      *       }
      *     ],
      *     "labels": {"Person": "#cbfe79"},
      *     "types": [ "KNOWS"],
      *     "directed": true
      *   }
      * }}}
      */
    def printGraph()(implicit formatValue: Any => String): Unit = {
      print(
        s"""
           |%network
           |${toZeppelinJson.render(2)}
        """.stripMargin)
    }

    /**
      * Returns a Zeppelin compatible Json representation of a PropertyGraph:
      *
      * {{{
      * {
      *   "nodes" : [ LIST_OF_NODES ]   // array of nodes
      *   "edges" : [ LIST_OF_EDGES ]   // array of relationships
      *   "labels": [ "Person", "Book"] // each label present in the graph
      *   "types": [ "KNOWS", "READS"]  // each relationship type present in the graph
      *   "directed": true              // indicate that the graph has directed relationships
      * }
      * }}}
      */
    def toZeppelinJson()(implicit formatValue: Any => String): Value = {
      val nodes = g.nodes("n").iterator.map(m => m("n").cast[Node[_]])
      val rels = g.relationships("r").iterator.map(m => m("r").cast[Relationship[_]])

      ZeppelinGraph.toZeppelinJson(
        nodes, rels, g.schema.labels, g.schema.relationshipTypes
      )
    }
  }
}
