package org.opencypher.caps.test.support

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField}

object formatRow extends (Row => String) {
  def apply(row: Row): String =
    s"Row(${
      row.schema.fields.zipWithIndex.map { case (field, i) => s"/* ${field.name} */ ${formatColumn (field, i, row)}" }.mkString(", ")
    })"

  private def formatColumn(field: StructField, pos: Int, row: Row) =
    if (row.isNullAt(pos))
      "null"
    else field.dataType match {
      case _: StringType => '"'.toString + row.getString(pos) + '"'.toString
      case _: LongType => s"${row.getLong(pos)}L"
      case _ => row.get(pos).toString
    }
}
