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

import org.opencypher.caps.api.types.CypherType
import org.opencypher.caps.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.caps.impl.record.{CypherRecords, CypherRecordsCompanion, RecordHeader, RecordsPrinter}
import org.opencypher.caps.impl.util.PrintOptions

object COSCRecords extends CypherRecordsCompanion[COSCRecords, COSCSession] {

  def create(rows: List[CypherMap], header: RecordHeader): COSCRecords = new COSCRecords(Embeddings(rows), header) {}

  override def unit()(implicit session: COSCSession): COSCRecords = {
    new COSCRecords(Embeddings.empty, RecordHeader.empty) {}
  }
}

sealed abstract class COSCRecords(
  val data: Embeddings,
  val header: RecordHeader) extends CypherRecords {

  /**
    * Iterator over the rows in this table.
    */
  override def rows: Iterator[String => CypherValue] = data.rows.map(_.value)

  override def columns: Set[String] = ???

  override def columnType: Map[String, CypherType] = ???

  /**
    * Consume these records as an iterator.
    *
    * WARNING: This operation may be very expensive as it may have to materialise
    */
  override def iterator: Iterator[CypherMap] = data.rows

  /**
    * @return the number of records in this CypherRecords.
    */
  override def size: Long = rows.size

  override def print(implicit options: PrintOptions): Unit = RecordsPrinter.print(this)

  /**
    * Registers these records as a table under the given name.
    *
    * @param name the name under which this table may be referenced.
    */
  override def register(name: String): Unit = ???
}


object Embeddings {
  def empty: Embeddings = Embeddings(List.empty)
}

case class Embeddings(data: List[CypherMap]) {

  def rows: Iterator[CypherMap] = data.iterator

}
