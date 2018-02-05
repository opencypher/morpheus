package org.opencypher.caps.cosc

import org.opencypher.caps.api.value.CypherMap
import org.opencypher.caps.impl.record.{CypherRecordHeader, CypherRecords}
import org.opencypher.caps.impl.util.PrintOptions

class COSCRecords extends CypherRecords {
  /**
    * The header for this table, describing the slots stored.
    *
    * @return the header for this table.
    */
  override def header: CypherRecordHeader = ???

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
    ???
  }
}
