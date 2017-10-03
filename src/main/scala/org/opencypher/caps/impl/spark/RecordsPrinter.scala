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

import java.io.PrintStream
import java.util.Objects

import org.opencypher.caps.api.spark.CAPSRecords

object RecordsPrinter {

  val MARGIN = 2

  /**
    * Prints the given SparkCypherRecords to stdout
    * @param records the records to be printed.
    */
  def print(records: CAPSRecords, columnWidth: Int): Unit = {
    printTo(records, Console.out, columnWidth)
  }

  def printTo(records: CAPSRecords, stream: PrintStream, columnWidth: Int): Unit = {
    val fieldContents = records.header.slots.sortBy(_.index).map(_.content)
    val factor = if (fieldContents.size > 1) fieldContents.size else 1

    val lineWidth = (columnWidth + MARGIN) * factor + factor - 1
    val --- = "+" + repeat("-", lineWidth) + "+"

    stream.println(---)
    var sep = "| "
    if (fieldContents.isEmpty) {
      stream.print(sep)
      stream.print(fitToColumn("(no columns)", columnWidth))
    } else fieldContents.foreach { contents =>
      stream.print(sep)
      stream.print(fitToColumn(contents.key.withoutType, columnWidth))
      sep = " | "
    }
    stream.println(" |")
    stream.println(---)
    val values = records.toScalaIterator
    sep = "| "
    if (fieldContents.isEmpty || values.isEmpty) {
      stream.print(sep)
      stream.print(fitToColumn("(no rows)", columnWidth))
      stream.println(" |")
    } else values.foreach { map =>
      fieldContents.foreach { content =>
        map.get(SparkColumnName.of(content)) match {
          case None =>
            stream.print(sep)
            stream.print(fitToColumn("null", columnWidth))
            sep = " | "
          case Some(v) =>
            stream.print(sep)
            stream.print(fitToColumn(Objects.toString(v), columnWidth))
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
  private def fitToColumn(s: String, columnWidth: Int) = {
    val spaces = (1 until columnWidth).map(_ => " ").reduce(_ + _)
    val cell = s + spaces
    cell.take(columnWidth)
  }

}
