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

import io.circe.{Encoder, Json}
import io.circe.syntax._
import org.opencypher.caps.api.spark.CAPSRecords

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
  * The format of values follows the `toString()` format of [[org.opencypher.caps.api.value.CypherValue]].
  */
object RecordsSerialiser {

  implicit val encodeFoo: Encoder[CAPSRecords] = new Encoder[CAPSRecords] {
    final def apply(records: CAPSRecords): Json = {

      val rows = records.toScalaIterator.map { map =>
        val unit = records.columns.map { key =>
          val maybeValue = map.get(key)
          key -> Json.fromString(maybeValue.map(_.toString).getOrElse("null"))
        }
        val list = unit.toList
        Json.obj(list: _*)
      }

      Json.obj(
        "columns" -> Json.arr(records.columns.map(Json.fromString): _*),
        "rows" -> Json.arr(rows.toList: _*)
      )
    }
  }

  def toJsonString(records: CAPSRecords): String = {
    val str = records.asJson.spaces2
    println(str)
    str
  }

}
