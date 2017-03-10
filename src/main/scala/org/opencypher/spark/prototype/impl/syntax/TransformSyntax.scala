package org.opencypher.spark.prototype.impl.syntax

import org.opencypher.spark.prototype.api.expr.Expr
import org.opencypher.spark.prototype.impl.classy.Transform

import scala.language.implicitConversions

trait TransformSyntax {
  implicit def transformSyntax[T : Transform](subject: T): TransformOps[T] = new TransformOps(subject)
}

final class TransformOps[T](subject: T)(implicit transform: Transform[T]) {
  def filter(expr: Expr): T = transform.filter(subject, expr)
}
