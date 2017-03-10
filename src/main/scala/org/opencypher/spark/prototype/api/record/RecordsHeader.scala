package org.opencypher.spark.prototype.api.record

import org.opencypher.spark.api.CypherType
import org.opencypher.spark.prototype.api.expr.{Expr, Var}

trait RecordsHeader {

  def slots: Traversable[RecordSlot]
  def fields: Traversable[Var]

  def slotsFor(expr: Expr, cypherType: CypherType): Traversable[RecordSlot]
  def slotsFor(expr: Expr): Traversable[RecordSlot]
}



