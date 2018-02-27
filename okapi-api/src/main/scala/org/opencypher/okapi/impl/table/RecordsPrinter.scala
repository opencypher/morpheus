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
package org.opencypher.okapi.impl.table

import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.impl.util.PrintOptions

object RecordsPrinter {

  /**
    * Prints the given CypherRecords to stdout
    *
    * @param records the records to be printed.
    */
  def print(records: CypherRecords)(implicit options: PrintOptions): Unit = {
    val fieldContents = records.columns
    val factor = if (fieldContents.size > 1) fieldContents.size else 1

    val lineWidth = (options.columnWidth + options.margin) * factor + factor - 1
    val stream = options.stream
    val --- = "+" + repeat("-", lineWidth) + "+"

    stream.println(---)
    var sep = "| "
    if (fieldContents.isEmpty) {
      stream.print(sep)
      stream.print(fitToColumn("(no columns)"))
    } else
      fieldContents.foreach { field =>
        stream.print(sep)
        stream.print(fitToColumn(field))
        sep = " | "
      }
    stream.println(" |")
    stream.println(---)

    sep = "| "
    var count = 0
    records.collect.foreach { map =>
      if (fieldContents.isEmpty) {
        stream.print(sep)
        stream.print(fitToColumn("(empty row)"))
      } else {
        fieldContents.foreach { field =>
          map.get(field) match {
            case None =>
              stream.print(sep)
              stream.print(fitToColumn("null"))
              sep = " | "
            case Some(v) =>
              stream.print(sep)
              stream.print(fitToColumn(v.toCypherString))
              sep = " | "
          }
        }
      }
      stream.println(" |")
      sep = "| "
      count += 1
    }

    if (count == 0) {
      stream.println("(no rows)")
    } else {
      stream.println(---)
      stream.println(s"($count rows)")
    }

    stream.flush()
  }

  private def repeat(x: String, size: Int): String = (1 to size).map((_) => x).mkString

  private def fitToColumn(s: String)(implicit options: PrintOptions) = {
    val spaces = (1 until options.columnWidth).map(_ => " ").reduce(_ + _)
    val cell = s + spaces
    cell.take(options.columnWidth)
  }
}
