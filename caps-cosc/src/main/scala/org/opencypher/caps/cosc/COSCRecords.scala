package org.opencypher.caps.cosc

import org.opencypher.caps.api.value.CypherMap
import org.opencypher.caps.impl.record.{CypherRecords, RecordHeader}
import org.opencypher.caps.impl.util.PrintOptions

sealed abstract class COSCRecords(val header: RecordHeader) extends CypherRecords {
  /**
    * Consume these records as an iterator.
    *
    * WARNING: This operation may be very expensive as it may have to materialise
    */
  override def iterator: Iterator[CypherMap] = ???

  /**
    * @return the number of records in this CypherRecords.
    */
  override def size: Long = ???

  override def print(implicit options: PrintOptions): Unit = ???
}

object COSCRecords {
  def unit: COSCRecords = {
    new COSCRecords(RecordHeader.empty) {}
  }
}
