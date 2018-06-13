/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.okapi.impl.util

object TablePrinter {

  private val emptyColumns = "(no columns)"
  private val emptyRow = "(empty row)"

  def toTable[T](header: Seq[String], data: Seq[Seq[T]])(implicit toString: T => String = (t: T) => t.toString): String = {
    val inputRows = header match {
      case Nil => Seq(Seq(emptyColumns),Seq(emptyRow)).toList
      case _ => header :: data.map(row => row.map(cell => toString(cell))).toList
    }
    val cellSizes = inputRows.map { row => row.map { cell => cell.length } }
    val colSizes = cellSizes.transpose.map { cellSizes => cellSizes.max }

    val rowSep = rowSeparator(colSizes)

    val rows = inputRows.map { row =>
      row.zip(colSizes).map {
        case (cell, colSize) => (" %" + (-1 * colSize) + "s ").format(cell)
      }.mkString("|", "|", "|")
    }

    val headerRows = Seq(rowSep, rows.head, rowSep)
    val bodyRows = if (rows.tail.nonEmpty) rows.tail :+ rowSep else Seq.empty
    val footerRow = rowCountFooter(data.size)

    (headerRows ++ bodyRows :+ footerRow).mkString("\n")
  }

  def rowSeparator(colSizes: Seq[Int]): String =
    colSizes.map { colSize => "-" * (colSize + 2) }.mkString("+", "+", "+")

  def rowCountFooter(rowCount: Int): String = rowCount match {
    case 0 => s"(no rows)"
    case 1 => s"(1 row)"
    case n => s"($n rows)"
  }

}
