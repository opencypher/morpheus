package org.opencypher.memcypher.impl.records

import org.opencypher.memcypher.api.MemCypherSession
import org.opencypher.memcypher.impl.table.Table
import org.opencypher.okapi.relational.api.io.EntityTable
import org.opencypher.okapi.relational.api.table.RelationalCypherRecordsFactory
import org.opencypher.okapi.relational.impl.table.RecordHeader

case class MemCypherRecordsFactory(implicit val session: MemCypherSession)
  extends RelationalCypherRecordsFactory[Table] {

  override type Records = MemCypherRecords

  override def unit(): MemCypherRecords = ???

  override def empty(initialHeader: RecordHeader): MemCypherRecords = ???

  override def fromEntityTable(entityTable: EntityTable[Table]): MemCypherRecords = ???

  override def from(
    header: RecordHeader,
    table: Table,
    maybeLogicalColumns: Option[Seq[String]]
  ): MemCypherRecords = {
    val logicalColumns = maybeLogicalColumns match {
      case s@Some(_) => s
      case None => Some(header.vars.map(_.withoutType).toSeq)
    }
    MemCypherRecords(header, table, logicalColumns)
  }
}
