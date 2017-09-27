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
import org.apache.spark.sql.Row
import org.opencypher.caps.api.expr.Var
import org.opencypher.caps.api.record.RecordHeader
import org.opencypher.caps.api.spark.CAPSRecords
import org.opencypher.caps.api.types.{CTNode, CTRelationship}
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.impl.record.CAPSRecordsTokens
import org.opencypher.caps.impl.spark.SparkColumnName

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
      val rows = records.map { row =>
        val unit = records.compact.header.fields.map { field =>
          field.name -> constructValue(row, field, records.header, records.tokens)
        }
        Json.obj(unit.toSeq: _*)
      }

      Json.obj(
        "columns" -> Json.arr(records.compact.columns.map(Json.fromString): _*),
        "rows" -> Json.arr(rows.toSeq: _*)
      )
    }
  }

  private def constructValue(row: Row, field: Var, header: RecordHeader, tokens: CAPSRecordsTokens): Json = {
    field.cypherType match {
      case _: CTNode =>
        val (id, labels, properties) = explodeNode(row, field, header)
        formatNode(id, labels, properties)

      case _: CTRelationship =>
        val (id, typ, properties) = explodeRel(row, field, header, tokens)
        formatRel(id, typ, properties)

      case _ =>
        val raw = row.getAs(SparkColumnName.of(header.slotFor(field)))
        Json.fromString(CypherValue(raw).toString)
    }
  }

  private def explodeNode(row: Row, field: Var, header: RecordHeader): (Long, Seq[String], Map[String, String]) = {
    val id = row.getAs[Long](SparkColumnName.of(header.slotFor(field)))
    val labels = header.labelSlots(field).mapValues { s =>
      row.getAs[Boolean](SparkColumnName.of(s))
    }.collect {
      case (h, b) if b =>
        h.label.name
    }.toSeq
    val properties = header.propertySlots(field).mapValues { s =>
      CypherValue(row.getAs[Any](SparkColumnName.of(s)))
    }.collect {
      case (p, v) if !CypherValue.isNull(v) =>
        p.key.name -> v.toString
    }

    (id, labels, properties)
  }

  private def explodeRel(row: Row, field: Var, header: RecordHeader, tokens: CAPSRecordsTokens): (Long, String, Map[String, String]) = {
    val id = row.getAs[Long](SparkColumnName.of(header.slotFor(field)))
    val typ = tokens.relTypeName(row.getAs[Long](SparkColumnName.of(header.typeId(field))).toInt)
    val properties = header.propertySlots(field).mapValues { s =>
      CypherValue(row.getAs[Any](SparkColumnName.of(s)))
    }.collect {
      case (p, v) if !CypherValue.isNull(v) =>
        p.key.name -> v.toString
    }

    (id, typ, properties)
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

  private def formatRel(id: Long, typ: String, properties: Map[String, String]) = {
    Json.obj(
      "id" -> Json.fromLong(id),
      "type" -> Json.fromString(typ),
      "properties" -> Json.obj(
        properties.mapValues(Json.fromString).toSeq: _*
      )
    )
  }

  def toJsonString(records: CAPSRecords): String = {
    val str = records.asJson.spaces2
    println(str)
    str
  }

}
