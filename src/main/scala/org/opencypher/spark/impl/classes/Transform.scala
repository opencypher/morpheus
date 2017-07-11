package org.opencypher.spark.impl.classes

import org.opencypher.spark.api.expr.{Expr, Var}
import org.opencypher.spark.api.record.{RecordHeader, RecordSlot}

import scala.language.implicitConversions

trait Transform[T] {
  def reorder(subject: T, nextHeader: RecordHeader): T
  def join(subject: T, other: T)(lhs: RecordSlot, rhs: RecordSlot): T
  def join(subject: T, other: T, header: RecordHeader)(lhs: RecordSlot, rhs: RecordSlot): T
  def initVarExpand(subject: T, sourceSlot: RecordSlot, edgeList: RecordSlot, lastEdge: RecordSlot, header: RecordHeader): T
  def varExpand(lhs: T, rels: T, lower: Int, upper: Int, header: RecordHeader)
               (edgeList: Var, endNode: RecordSlot, rel: Var, relStartNode: RecordSlot): T
}

object Transform {
  @inline final def apply[T](implicit instance: Transform[T]): Transform[T] = instance
}
