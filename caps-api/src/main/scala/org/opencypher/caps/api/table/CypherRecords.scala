package org.opencypher.caps.api.table

import org.opencypher.caps.api.value.CypherValue.CypherMap
import org.opencypher.caps.impl.table.CypherTable

/**
  * Represents a table of records containing Cypher values.
  * Each column (or slot) in this table represents an evaluated Cypher expression.
  */
trait CypherRecords extends CypherTable[String] with CypherPrintable {

  /**
    * Consume these records as an iterator.
    *
    * WARNING: This operation may be very expensive as it may have to materialise
    */
  def iterator: Iterator[CypherMap]

  /**
    * @return the number of records in this CypherRecords.
    */
  def size: Long

  /**
    * Registers these records as a table under the given name.
    *
    * @param name the name under which this table may be referenced.
    */
  def register(name: String): Unit
}
