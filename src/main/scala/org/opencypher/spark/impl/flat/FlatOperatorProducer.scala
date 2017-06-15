package org.opencypher.spark.impl.flat

import cats.Monoid
import org.opencypher.spark.api.exception.SparkCypherException
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.global.{Global, GlobalsRegistry, Label}
import org.opencypher.spark.api.ir.pattern.{AllGiven, EveryNode, EveryRelationship}
import org.opencypher.spark.api.record._
import org.opencypher.spark.api.types._
import org.opencypher.spark.impl.logical.{GraphSource, NamedLogicalGraph}
import org.opencypher.spark.impl.syntax.header._
import org.opencypher.spark.impl.syntax.util.traversable._
import org.opencypher.spark.impl.util.{Added, FailedToAdd, Found, Replaced}

class FlatOperatorProducer(implicit context: FlatPlannerContext) {

  private val schema = context.schema

  import context.tokens._
  import context.constants._

  private implicit val typeVectorMonoid = new Monoid[Vector[CypherType]] {
    override def empty: Vector[CypherType] = Vector.empty
    override def combine(x: Vector[CypherType], y: Vector[CypherType]): Vector[CypherType] = x ++ y
  }

  def select(fields: IndexedSeq[Var], in: FlatOperator): Select = {
    val slotContents = fields.map(in.header.slotsFor(_).head.content)

    val (nextHeader, _) = RecordHeader.empty.update(addContents(slotContents))

    Select(fields, in, nextHeader)
  }

  def filter(expr: Expr, in: FlatOperator): Filter = {
    in.header

//    expr match {
//      case HasLabel(n, label) =>
//        in.header.contents.map { c =>
//
//        }
//      case _ => in.header
//    }

    // TODO: Should replace SlotContent expressions with detailed type of entity
    // TODO: Should reduce width of header due to more label information

    Filter(expr, in, in.header)
  }

  def nodeScan(node: Var, _nodeDef: EveryNode, prev: FlatOperator): NodeScan = {

    val header = if (_nodeDef.labels.elts.isEmpty) RecordHeader.nodeFromSchema(node, schema, globals)
    else RecordHeader.nodeFromSchema(node, schema, globals, _nodeDef.labels.elts.map(_.name))

    val nodeDef = if (_nodeDef.labels.elts.isEmpty) EveryNode(AllGiven(schema.labels.map(Label))) else _nodeDef

    NodeScan(node, nodeDef, prev, header)
  }

  // TODO: Specialize per kind of slot content
  def project(it: ProjectedSlotContent, in: FlatOperator): FlatOperator = {
    val (newHeader, result) = in.header.update(addContent(it))

    result match {
      case _: Found[_] => in
      case _: Replaced[_] => Alias(it.expr, it.alias.get, in, newHeader)
      case _: Added[_] => Project(it.expr, in, newHeader)
      case f: FailedToAdd[_] => throw SparkCypherException(s"Failed to add new slot: $f")
    }
  }

  // TODO: Specialize per kind of slot content
  def expandSource(source: Var, rel: Var, types: EveryRelationship, target: Var,
                   sourceOp: FlatOperator, targetOp: FlatOperator): FlatOperator = {
    val relHeader = if (types.relTypes.elts.isEmpty) RecordHeader.relationshipFromSchema(rel, schema, globals)
    else RecordHeader.relationshipFromSchema(rel, schema, globals, types.relTypes.elts.map(_.name))

    val expandHeader = sourceOp.header ++ relHeader ++ targetOp.header

    ExpandSource(source, rel, types, target, sourceOp, targetOp, expandHeader, relHeader)
  }

  def planLoadGraph(logicalGraph: NamedLogicalGraph, source: GraphSource): LoadGraph = {

    LoadGraph(logicalGraph, source)
  }
}
