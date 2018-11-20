package org.opencypher.memcypher.impl.records

import org.opencypher.memcypher.impl.convert.MemRowToCypherMap
import org.opencypher.memcypher.impl.table.Table
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.relational.api.table.RelationalCypherRecords
import org.opencypher.okapi.relational.impl.table.RecordHeader

case class MemCypherRecords(
  header: RecordHeader,
  table: Table,
  override val logicalColumns: Option[Seq[String]]
) extends RelationalCypherRecords[Table] with MemCypherRecordsBehaviour {

  override type Records = MemCypherRecords

  override def cache(): MemCypherRecords = this
}

trait MemCypherRecordsBehaviour extends RelationalCypherRecords[Table] {

  override type Records <: MemCypherRecordsBehaviour

  override lazy val columnType: Map[String, CypherType] =
    table.schema.columns.map(columnMeta => columnMeta.name -> columnMeta.dataType).toMap

  override def rows: Iterator[String => CypherValue] =
    iterator.map(_.value)

  override def iterator: Iterator[CypherMap] =
    toCypherMaps.iterator

  override def collect: Array[CypherMap] =
    toCypherMaps.toArray

  def toCypherMaps: Seq[CypherMap] =
    table.data.map(MemRowToCypherMap(header, table.schema))
}
