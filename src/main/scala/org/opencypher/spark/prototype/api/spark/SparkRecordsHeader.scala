package org.opencypher.spark.prototype.api.spark

import org.opencypher.spark.api.CypherType
import org.opencypher.spark.prototype.api.expr.{Expr, Var}
import org.opencypher.spark.prototype.api.record.{RecordSlot, RecordsHeader, SlotContent}
import org.opencypher.spark.prototype.impl.spark.SparkInternalHeader

final case class SparkRecordsHeader(internalHeader: SparkInternalHeader)
  extends RecordsHeader {

  override def slots: Traversable[RecordSlot] = internalHeader.slots
  override def fields: Traversable[Var] = internalHeader.fields

  override def slotsFor(expr: Expr, cypherType: CypherType): Traversable[RecordSlot] =
    internalHeader.slotsFor(expr, cypherType)

  override def slotsFor(expr: Expr): Traversable[RecordSlot] =
    internalHeader.slotsFor(expr)
}

object SparkRecordsHeader {

  def empty: SparkRecordsHeader =
    SparkRecordsHeader(SparkInternalHeader.empty)

  def from(contents: SlotContent*): SparkRecordsHeader =
    SparkRecordsHeader(contents.foldLeft(SparkInternalHeader.empty) { case (header, slot) => header + slot })
}
