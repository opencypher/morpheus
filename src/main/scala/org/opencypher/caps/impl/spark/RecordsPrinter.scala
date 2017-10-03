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
package org.opencypher.caps.impl.spark

import java.util.Objects

import org.opencypher.caps.api.spark.CAPSRecords
import org.opencypher.caps.api.util.PrintOptions

object RecordsPrinter {

  /**
    * Prints the given SparkCypherRecords to stdout
    * @param records the records to be printed.
    */
  def print(records: CAPSRecords)(implicit options: PrintOptions): Unit = {
    val fieldContents = records.header.slots.sortBy(_.index).map(_.content)
    val factor = if (fieldContents.size > 1) fieldContents.size else 1

    val lineWidth = (options.columnWidth + options.margin) * factor + factor - 1
    val stream = options.stream
    val --- = "+" + repeat("-", lineWidth) + "+"

    stream.println(---)
    var sep = "| "
    if (fieldContents.isEmpty) {
      stream.print(sep)
      stream.print(fitToColumn("(no columns)"))
    } else fieldContents.foreach { contents =>
      stream.print(sep)
      stream.print(fitToColumn(contents.key.withoutType))
      sep = " | "
    }
    stream.println(" |")
    stream.println(---)
    val values = records.toLocalScalaIterator
    sep = "| "
    if (fieldContents.isEmpty || values.isEmpty) {
      stream.print(sep)
      stream.print(fitToColumn("(no rows)"))
      stream.println(" |")
    } else values.foreach { map =>
      fieldContents.foreach { content =>
        map.get(SparkColumnName.of(content)) match {
          case None =>
            stream.print(sep)
            stream.print(fitToColumn("null"))
            sep = " | "
          case Some(v) =>
            stream.print(sep)
            stream.print(fitToColumn(Objects.toString(v)))
            sep = " | "
        }
      }
      stream.println(" |")
      sep = "| "
    }
    stream.println(---)
    stream.flush()
  }

  private def repeat(x: String, size: Int): String = (1 to size).map((_) => x).mkString
  private def fitToColumn(s: String)(implicit options: PrintOptions) = {
    val spaces = (1 until options.columnWidth).map(_ => " ").reduce(_ + _)
    val cell = s + spaces
    cell.take(options.columnWidth)
  }
}
