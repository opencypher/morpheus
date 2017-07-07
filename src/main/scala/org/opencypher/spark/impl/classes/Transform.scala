package org.opencypher.spark.impl.classes

import org.opencypher.spark.api.expr.{Expr, Var}
import org.opencypher.spark.api.record.{RecordHeader, RecordSlot}

import scala.language.implicitConversions

trait Transform[T] {
  def reorder(subject: T, nextHeader: RecordHeader): T
  def alias2(subject: T, expr: Expr, v: Var, nextHeader: RecordHeader): T
  def join(subject: T, other: T)(lhs: RecordSlot, rhs: RecordSlot): T
  def join(subject: T, other: T, header: RecordHeader)(lhs: RecordSlot, rhs: RecordSlot): T
  def varExpand(lhs: T, rels: T, lower: Int, upper: Int, header: RecordHeader)
               (nodeSlot: RecordSlot, startSlot: RecordSlot, rel: Var, path: Var): T
}

object Transform {
  @inline final def apply[T](implicit instance: Transform[T]): Transform[T] = instance
}
