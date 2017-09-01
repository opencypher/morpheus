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
package org.opencypher.caps.api.spark

import java.io.PrintStream

import org.opencypher.caps.impl.spark.SparkColumnName

object RecordsPrinter {

  /**
    * Prints the given SparkCypherRecords to stdout
    * @param records the records to be printed.
    */
  def print(records: CAPSRecords): Unit = {
    print(records, Console.out)
  }

  def print(records: CAPSRecords, stream: PrintStream): Unit = {
    val fieldContents = records.header.slots.sortBy(_.index).map(_.content)
    val lineWidth = 20 * fieldContents.size + 5
    val --- = "+" + repeat("-", lineWidth) + "+"

    stream.println(---)
    var sep = "| "
    fieldContents.foreach { contents =>
      stream.print(sep)
      stream.print(fitTo20(contents.key.withoutType))
      sep = " | "
    }
    stream.println(" |")
    stream.println(---)
    records.toScalaIterator.foreach { map =>
      sep = "| "
      fieldContents.foreach { content =>
        map.get(SparkColumnName.of(content)) match {
          case None =>
            stream.print(sep)
            stream.print(fitTo20("null"))
            sep = " | "
          case Some(v) =>
            stream.print(sep)
            stream.print(fitTo20(v.toString))
            sep = " | "
        }
      }
      stream.println(" |")
    }
    stream.println(---)
    stream.flush()
  }

  private def repeat(x: String, size: Int): String = (1 to size).map((_) => x).mkString
  private def fitTo20(s: String) = (s + "                    ").take(20)

}
