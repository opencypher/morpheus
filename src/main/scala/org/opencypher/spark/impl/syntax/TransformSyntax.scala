package org.opencypher.spark.impl.syntax

import org.opencypher.spark.api.expr.{Expr, Var}
import org.opencypher.spark.api.record.{RecordHeader, RecordSlot}
import org.opencypher.spark.impl.classes.Transform

import scala.language.implicitConversions

trait TransformSyntax {
  implicit def transformSyntax[T : Transform](subject: T): TransformOps[T] = new TransformOps(subject)
}

final class TransformOps[T](subject: T)(implicit transform: Transform[T]) {
  def join(other: T)(lhs: RecordSlot, rhs: RecordSlot): T = transform.join(subject, other)(lhs, rhs)
  def join(other: T, header: RecordHeader)(lhs: RecordSlot, rhs: RecordSlot): T = transform.join(subject, other, header)(lhs, rhs)
  def initVarExpand(sourceSlot: RecordSlot, edgeList: RecordSlot, lastEdge: RecordSlot, header: RecordHeader): T = transform.initVarExpand(subject, sourceSlot, edgeList, lastEdge, header)
  def varExpand(rels: T, lower: Int, upper: Int, header: RecordHeader)
               (edgeList: Var, endNode: RecordSlot, rel: Var, relStartNode: RecordSlot): T =
    transform.varExpand(subject, rels, lower, upper, header)(edgeList, endNode, rel, relStartNode)
}
