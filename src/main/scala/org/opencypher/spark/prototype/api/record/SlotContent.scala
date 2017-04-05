package org.opencypher.spark.prototype.api.record

import org.opencypher.spark.prototype.api.types._
import org.opencypher.spark.prototype.api.expr._

final case class RecordSlot(index: Int, content: SlotContent)

object RecordSlot {
  def from(pair: (Int, SlotContent)) = RecordSlot(pair._1, pair._2)
}

sealed trait SlotContent {
  def key: Expr
  def alias: Option[Var]
  def owner: Option[Var]
  def cypherType: CypherType

  def support: Traversable[Expr]
}

sealed trait ProjectedSlotContent extends SlotContent {
  def expr: Expr

  override def owner = expr match {
    case Property(v: Var, _, _) => Some(v)
    case HasLabel(v: Var, _, _) => Some(v)
    case HasType(v: Var, _, _) => Some(v)
    case _ => None
  }
}

sealed trait FieldSlotContent extends SlotContent {
  override def alias = Some(field)
  override def key: Var = field
  def field: Var
}

final case class ProjectedExpr(expr: Expr, cypherType: CypherType) extends ProjectedSlotContent {
  override def key = expr
  override def alias = None

  override def support = Seq(expr)
}

final case class OpaqueField(field: Var, cypherType: CypherType) extends FieldSlotContent {
  def expr = field
  override def owner = Some(field)

  override def support = Seq(field)
}

final case class ProjectedField(field: Var, expr: Expr, cypherType: CypherType)
  extends ProjectedSlotContent with FieldSlotContent {

  override def support = Seq(field, expr)
}
