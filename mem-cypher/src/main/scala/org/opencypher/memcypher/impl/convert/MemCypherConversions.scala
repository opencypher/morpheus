package org.opencypher.memcypher.impl.convert

import org.opencypher.memcypher.impl.table.{ColumnSchema, Schema}
import org.opencypher.okapi.relational.impl.table.RecordHeader

object MemCypherConversions {

  implicit class RichRecordHeader(val header: RecordHeader) {
    def toSchema: Schema = {
      val columnSchemas = header.columns.toSeq.sorted.map { column =>
        val expressions = header.expressionsFor(column)
        val commonType = expressions.map(_.cypherType).reduce(_ join _)
        ColumnSchema(column, commonType)
      }
      Schema(columnSchemas.toArray)
    }
  }

}
