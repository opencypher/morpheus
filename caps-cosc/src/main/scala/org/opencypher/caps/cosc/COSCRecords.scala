package org.opencypher.caps.cosc

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
