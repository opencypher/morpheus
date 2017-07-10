package org.opencypher.spark.impl.syntax

import org.opencypher.spark.api.expr.{Expr, Var}
import org.opencypher.spark.api.record.{ProjectedSlotContent, RecordHeader, RecordSlot}
import org.opencypher.spark.impl.classes.Transform

import scala.language.implicitConversions

trait TransformSyntax {
  implicit def transformSyntax[T : Transform](subject: T): TransformOps[T] = new TransformOps(subject)
}

final class TransformOps[T](subject: T)(implicit transform: Transform[T]) {
  def select(fields: IndexedSeq[Var], header: RecordHeader): T = transform.select(subject, fields, header)
  def reorder(header: RecordHeader): T = transform.reorder(subject, header)
  def project(expr: Expr, header: RecordHeader): T = transform.project(subject, expr, header)
  def alias2(expr: Expr, v: Var, nextHeader: RecordHeader): T = transform.alias2(subject, expr, v, nextHeader)
  def join(other: T)(lhs: RecordSlot, rhs: RecordSlot): T = transform.join(subject, other)(lhs, rhs)
  def join(other: T, header: RecordHeader)(lhs: RecordSlot, rhs: RecordSlot): T = transform.join(subject, other, header)(lhs, rhs)
}
