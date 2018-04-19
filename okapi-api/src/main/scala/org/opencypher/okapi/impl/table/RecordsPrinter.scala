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
package org.opencypher.okapi.impl.table

import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.impl.util.PrintOptions

object RecordsPrinter {

  /**
    * Prints the given CypherRecords to stdout
    *
    * @param records the records to be printed.
    */
  def print(records: CypherRecords)(implicit options: PrintOptions): Unit = {
    val columnNames = records.columns

    val rows: Seq[CypherValue.CypherMap] = records.collect

    // Dynamically sets the column width for each column to the longest string stored in the column
    val columnWidths: Map[String, Int] = {
      for {
        columnName <- columnNames
        headerWidth = columnName.length
        columnRowValueLengths = rows.map(row => row.getOrElse(columnName).toCypherString.length)
        maxColumnRowValueWidth = columnRowValueLengths.foldLeft(headerWidth)(math.max)
        columnWidth = math.min(options.maxColumnWidth, maxColumnRowValueWidth)
      } yield columnName -> columnWidth
    }.toMap

    val noColumnsHeader = "(no columns)"
    val emptyRow = "(empty row)"

    val noColumnsStringWidth = math.max(noColumnsHeader.length, emptyRow.length)

    val totalContentWidth = {
      if (columnNames.isEmpty) {
        noColumnsStringWidth + 3
      } else {
        columnWidths.values.sum
      }
    }

    val lineWidth: Int = totalContentWidth + 3 * columnWidths.size - 1

    val stream = options.stream
    val --- = "+" + repeat("-", lineWidth) + "+"

    stream.println(---)
    var sep = "| "
    if (columnNames.isEmpty) {
      stream.print(sep)
      stream.print(fitToColumn(noColumnsHeader, noColumnsStringWidth))
    } else
      columnNames.foreach { field =>
        stream.print(sep)
        stream.print(fitToColumn(field, columnWidths(field)))
        sep = " | "
      }
    stream.println(" |")
    stream.println(---)

    sep = "| "
    var count = 0
    rows.foreach { map =>
      if (columnNames.isEmpty) {
        stream.print(sep)
        stream.print(fitToColumn(emptyRow, noColumnsStringWidth))
      } else {
        columnNames.foreach { field =>
          val value = map.getOrElse(field)
          stream.print(sep)
          stream.print(fitToColumn(value.toCypherString, columnWidths(field)))
          sep = " | "
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

  private def fitToColumn(s: String, width: Int)(implicit options: PrintOptions) = {
    val spaces: String = (1 until width).map(_ => " ").foldLeft("")(_ + _)
    val cell = s + spaces
    cell.take(width)
  }
}
