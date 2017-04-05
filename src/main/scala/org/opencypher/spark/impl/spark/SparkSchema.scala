package org.opencypher.spark.impl.spark

import org.apache.spark.sql.types.{StructField, StructType}
import org.opencypher.spark.api.record.{RecordHeader, RecordSlot}

object SparkSchema {
  def from(header: RecordHeader): StructType = {
    val fields = header.slots.map(slot => structField(slot, header.mandatory(slot)))
    StructType(fields)
  }

  private def structField(slot: RecordSlot, nullable: Boolean): StructField = {
    val name = SparkColumnName.of(slot.content)
    val dataType = sparkType(slot.content.cypherType)
    StructField(name, dataType, nullable)
  }
}
