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
package org.opencypher.caps.cosc.impl

import org.opencypher.caps.api.value.CypherValue.CypherMap
import org.opencypher.caps.impl.record.{CypherRecords, RecordHeader}
import org.opencypher.caps.impl.spark.RecordsPrinter
import org.opencypher.caps.impl.util.PrintOptions

object COSCRecords {
  def create(rows: List[CypherMap], header: RecordHeader): COSCRecords = new COSCRecords(rows, header) {}

  def unit: COSCRecords = {
    new COSCRecords(List.empty, RecordHeader.empty) {}
  }
}

sealed abstract class COSCRecords(
  val rows: List[CypherMap],
  val header: RecordHeader) extends CypherRecords {
  /**
    * Consume these records as an iterator.
    *
    * WARNING: This operation may be very expensive as it may have to materialise
    */
  override def iterator: Iterator[CypherMap] = rows.iterator

  /**
    * @return the number of records in this CypherRecords.
    */
  override def size: Long = rows.size

  override def print(implicit options: PrintOptions): Unit = RecordsPrinter.print(this)
}
