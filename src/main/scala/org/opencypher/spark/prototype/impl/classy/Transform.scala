package org.opencypher.spark.prototype.impl.classy

import org.opencypher.spark.prototype.api.expr.Expr
import org.opencypher.spark.prototype.api.record.{ProjectedSlotContent, RecordSlot}
import org.opencypher.spark.prototype.impl.physical.RuntimeContext

import scala.language.implicitConversions

trait Transform[T] {
  def filter(subject: T, expr: Expr)(implicit context: RuntimeContext): T
  def select(subject: T, fields: Map[Expr, String]): T
  def project(subject: T, it: ProjectedSlotContent): T
  def join(subject: T, other: T)(lhs: RecordSlot, rhs: RecordSlot): T
}

object Transform {
  @inline final def apply[T](implicit instance: Transform[T]): Transform[T] = instance
}
