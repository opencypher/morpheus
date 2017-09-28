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

import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.opencypher.caps.api.spark.CAPSRecords
import org.opencypher.caps.api.value._

import scala.collection.JavaConverters._

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
  * The format of scalar values follows the `toString()` format of [[org.opencypher.caps.api.value.CypherValue]]. The format of nodes is as follows:
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
  *       "foo" : "bar"
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
  *       "foo" : "bar"
  *     }
  *   }
  * }}}
  */
object RecordsSerialiser {

  implicit val recordsEncoder: Encoder[CAPSRecords] = new Encoder[CAPSRecords] {
    override final def apply(records: CAPSRecords): Json = {
      val rows = records.toCypherMaps.toLocalIterator().asScala.map { map =>
        val unit = map.keys.map { key =>
          key -> constructValue(map.get(key))
        }
        Json.obj(unit.toSeq: _*)
      }

      Json.obj(
        "columns" -> Json.arr(records.compact.columns.map(Json.fromString): _*),
        "rows" -> Json.arr(rows.toSeq: _*)
      )
    }
  }

  private def constructValue(v: Option[CypherValue]): Json = {
    v match {
      case Some(n: CypherNode) =>
        CypherNode.contents(n) match {
          case Some(NodeContents(id, labels, properties)) =>
            formatNode(id.v, labels, properties.m.mapValues(_.toString))
          case None =>
            Json.fromString("null")
        }

      case Some(r: CypherRelationship) =>
        CypherRelationship.contents(r) match {
          case Some(RelationshipContents(id, source, target, typ, properties)) =>
            formatRel(id.v, source.v, target.v, typ, properties.m.mapValues(_.toString))
          case None =>
            Json.fromString("null")
        }

      case Some(v: CypherValue) if !CypherValue.isNull(v) =>
        Json.fromString(v.toString)

      case _ =>
        Json.fromString("null")
    }
  }

  private def formatNode(id: Long, labels: Seq[String], properties: Map[String, String]) = {
    Json.obj(
      "id" -> Json.fromLong(id),
      "labels" -> Json.arr(
        labels.map(Json.fromString): _*
      ),
      "properties" -> Json.obj(
        properties.mapValues(Json.fromString).toSeq: _*
      )
    )
  }

  private def formatRel(id: Long, source: Long, target: Long, typ: String, properties: Map[String, String]) = {
    Json.obj(
      "id" -> Json.fromLong(id),
      "source" -> Json.fromLong(source),
      "target" -> Json.fromLong(target),
      "type" -> Json.fromString(typ),
      "properties" -> Json.obj(
        properties.mapValues(Json.fromString).toSeq: _*
      )
    )
  }

  def toJsonString(records: CAPSRecords): String = records.asJson.spaces2

}
