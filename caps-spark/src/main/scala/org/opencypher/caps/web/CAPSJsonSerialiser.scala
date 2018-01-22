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

import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords}
import org.opencypher.caps.api.value._

trait JsonSerialiser {
  implicit val recordsEncoder: Encoder[CAPSRecords] = new Encoder[CAPSRecords] {
    override final def apply(records: CAPSRecords): Json = {
      val rows = records.iterator.map { map =>
        val unit = map.keys.map { key =>
          key -> constructValue(map.get(key))
        }
        Json.obj(unit.toSeq: _*)
      }

      Json.obj(
        "columns" -> Json.arr(records.sparkColumns.map(Json.fromString): _*),
        "rows" -> Json.arr(rows.toSeq: _*)
      )
    }
  }

  implicit val graphEncoder: Encoder[CAPSGraph] = new Encoder[CAPSGraph] {
    override final def apply(graph: CAPSGraph): Json = {
      val nodes = graph.nodes("n").iterator.map { map =>
        constructValue(map.get("n"))
      }.toSeq

      val rels = graph.relationships("rel").iterator.map { map =>
        constructValue(map.get("rel"))
      }.toSeq

      formatGraph(graph, nodes, rels)
    }
  }

  protected def constructValue(v: Option[CypherValue]): Json = {
    v match {
      case Some(n: CypherNode) =>
        CypherNode.contents(n) match {
          case Some(NodeContents(id, labels, properties)) =>
            formatNode(id.v, labels, properties.m.mapValues(p => constructValue(Some(p))))
          case None =>
            Json.Null
        }

      case Some(r: CypherRelationship) =>
        CypherRelationship.contents(r) match {
          case Some(RelationshipContents(id, source, target, typ, properties)) =>
            formatRel(id.v, source.v, target.v, typ, properties.m.mapValues(p => constructValue(Some(p))))
          case None =>
            Json.Null
        }

      case Some(CypherInteger(i)) => Json.fromLong(i)
      case Some(CypherFloat(f)) => Json.fromDouble(f).getOrElse(Json.fromString(f.toString))
      case Some(CypherBoolean(b)) => Json.fromBoolean(b)
      case Some(CypherString(s)) => Json.fromString(s)
      case Some(CypherList(contents)) => Json.arr(contents.map(v => constructValue(Some(v))): _*)
      case Some(CypherMap(contents)) => Json.obj(contents.properties.m.mapValues(p => constructValue(Some(p))).toSeq: _*)

      case _ =>
        Json.Null
    }
  }


  protected def formatNode(id: Long, labels: Seq[String], properties: Map[String, Json]) = {
    Json.obj(
      "id" -> Json.fromLong(id),
      "labels" -> Json.arr(
        labels.map(Json.fromString): _*
      ),
      "properties" -> Json.obj(
        properties.toSeq: _*
      )
    )
  }

  protected def formatRel(id: Long, source: Long, target: Long, typ: String, properties: Map[String, Json]) = {
    Json.obj(
      "id" -> Json.fromLong(id),
      "source" -> Json.fromLong(source),
      "target" -> Json.fromLong(target),
      "type" -> Json.fromString(typ),
      "properties" -> Json.obj(
        properties.toSeq: _*
      )
    )
  }

  protected def formatGraph(graph: CAPSGraph, nodes: Seq[Json], rels: Seq[Json]): Json = {
    Json.obj(
      "nodes" -> Json.arr(nodes: _*),
      "edges" -> Json.arr(rels: _*),
      "labels" -> Json.arr(graph.schema.labels.map(Json.fromString).toSeq: _*),
      "types" -> Json.arr(graph.schema.relationshipTypes.map(Json.fromString).toSeq: _*)
    )
  }

  def toJsonString(records: CAPSRecords): String = records.asJson.spaces2
  def toJsonString(graph: CAPSGraph): String = graph.asJson.spaces2
}

/**
  * Serialises CAPSRecords to a JSON string. The format is as follows:
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
  *
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
  * The format of scalar values follows the `toString()` format of [[org.opencypher.caps.api.value.CypherValue]].
  * The format of nodes is as follows:
  *
  * {{{
  *   "n" : {
  *     "id" : 0,           // id is an integer
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
  *
  * The format of relationships is as follows:
  *
  * {{{
  *   "n" : {
  *     "id" : 0,           // id is an integer
  *     "source" : 0,       // id of source node
  *     "target" : 0,       // id of target node
  *     "type" : "T"        // relationship type is a string
  *     "properties" : {    // properties is an object
  *       "key" : "value",  // key-value is a tuple
  *       "foo" : bar
  *     }
  *   }
  * }}}
  */
object CAPSJsonSerialiser extends JsonSerialiser
