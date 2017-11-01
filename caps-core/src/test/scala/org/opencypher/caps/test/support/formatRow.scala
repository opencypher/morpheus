/*
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
package org.opencypher.caps.test.support

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField}

object formatRow extends (Row => String) {
  def apply(row: Row): String =
    s"Row(${row.schema.fields.zipWithIndex
      .map { case (field, i) => s"/* ${field.name} */ ${formatColumn(field, i, row)}" }
      .mkString(", ")})"

  private def formatColumn(field: StructField, pos: Int, row: Row) =
    if (row.isNullAt(pos))
      "null"
    else
      field.dataType match {
        case _: StringType => '"'.toString + row.getString(pos) + '"'.toString
        case _: LongType   => s"${row.getLong(pos)}L"
        case _             => row.get(pos).toString
      }
}
