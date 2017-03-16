package org.opencypher.spark.prototype.impl.syntax

import org.opencypher.spark.prototype.api.expr.Expr
import org.opencypher.spark.prototype.api.record.{ProjectedSlotContent, RecordSlot}
import org.opencypher.spark.prototype.impl.classy.Transform
import org.opencypher.spark.prototype.impl.physical.RuntimeContext

import scala.language.implicitConversions

trait TransformSyntax {
  implicit def transformSyntax[T : Transform](subject: T): TransformOps[T] = new TransformOps(subject)
}

final class TransformOps[T](subject: T)(implicit transform: Transform[T]) {
  def filter(expr: Expr): T = transform.filter(subject, expr)
  def select(fields: Map[Expr, String]): T = transform.select(subject, fields)
  def project(it: ProjectedSlotContent): T = transform.project(subject, it)
  def join(other: T)(lhs: RecordSlot, rhs: RecordSlot): T = transform.join(subject, other)(lhs, rhs)
}
