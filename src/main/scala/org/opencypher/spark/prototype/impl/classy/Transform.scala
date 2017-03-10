package org.opencypher.spark.prototype.impl.classy

import org.opencypher.spark.prototype.api.expr.Expr
import org.opencypher.spark.prototype.api.record.ProjectedSlotContent

import scala.language.implicitConversions

trait Transform[T] {
  def filter(subject: T, expr: Expr): T
  def select(subject: T, fields: Map[Expr, String]): T
  def project(subject: T, it: ProjectedSlotContent): T
}

object Transform {
  @inline final def apply[T](implicit instance: Transform[T]) = instance
}
