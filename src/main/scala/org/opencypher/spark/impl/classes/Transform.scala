package org.opencypher.spark.impl.classes

import org.opencypher.spark.api.expr.{Expr, Var}
import org.opencypher.spark.api.record.{ProjectedSlotContent, RecordHeader, RecordSlot}

import scala.language.implicitConversions

trait Transform[T] {
  def filter(subject: T, expr: Expr, nextHeader: RecordHeader): T
  def select(subject: T, fields: IndexedSeq[Var], nextHeader: RecordHeader): T
  def reorder(subject: T, nextHeader: RecordHeader): T
  def project(subject: T, expr: Expr, header: RecordHeader): T
  def alias2(subject: T, expr: Expr, v: Var, nextHeader: RecordHeader): T
  def join(subject: T, other: T)(lhs: RecordSlot, rhs: RecordSlot): T
  def join(subject: T, other: T, header: RecordHeader)(lhs: RecordSlot, rhs: RecordSlot): T
}

object Transform {
  @inline final def apply[T](implicit instance: Transform[T]): Transform[T] = instance
}
