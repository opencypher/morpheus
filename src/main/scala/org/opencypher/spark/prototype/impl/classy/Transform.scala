package org.opencypher.spark.prototype.impl.classy

import org.opencypher.spark.prototype.api.expr.Expr

import scala.language.implicitConversions

trait Transform[T] {
  def filter(subject: T, expr: Expr): T
}

object Transform {
  @inline final def apply[T](implicit instance: Transform[T]) = instance
}
