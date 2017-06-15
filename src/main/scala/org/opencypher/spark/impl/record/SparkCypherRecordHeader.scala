package org.opencypher.spark.impl.record

import org.apache.spark.sql.types.{StructField, StructType}
import org.opencypher.spark.api.expr.Var
import org.opencypher.spark.api.record.{OpaqueField, RecordHeader, RecordSlot}
import org.opencypher.spark.impl.spark.{SparkColumnName, fromSparkType, toSparkType}

object SparkCypherRecordHeader {

  def fromSparkStructType(structType: StructType): RecordHeader = RecordHeader.from(structType.fields.map {
    field =>
      OpaqueField(Var(field.name)(fromSparkType(field.dataType, field.nullable)))
  }: _*)

  def asSparkStructType(header: RecordHeader): StructType = {
    val fields = header.slots.map(slot => structField(slot, header.mandatory(slot)))
    StructType(fields)
  }

  private def structField(slot: RecordSlot, nullable: Boolean): StructField = {
    val name = SparkColumnName.of(slot.content)
    val dataType = toSparkType(slot.content.cypherType)
    StructField(name, dataType, nullable)
  }
}
