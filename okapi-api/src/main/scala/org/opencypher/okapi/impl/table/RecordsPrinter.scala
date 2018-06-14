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
package org.opencypher.okapi.impl.table

import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.value.CypherValue.CypherValue
import org.opencypher.okapi.impl.util.{PrintOptions, TablePrinter}

object RecordsPrinter {

  /**
    * Prints the given CypherRecords to stdout
    *
    * @param records the records to be printed.
    */
  def print(records: CypherRecords)(implicit options: PrintOptions): Unit = {
    val columns = records.logicalColumns.getOrElse(records.physicalColumns)

    val rows: Seq[Seq[CypherValue]] = records.collect.map { row =>
      columns.foldLeft(Seq.empty[CypherValue]) {
        case (currentSeq, column) => currentSeq :+ row(column)
      }
    }

    options.stream
      .append(TablePrinter.toTable(columns, rows)(v => v.toCypherString))
      .flush()
  }
}
