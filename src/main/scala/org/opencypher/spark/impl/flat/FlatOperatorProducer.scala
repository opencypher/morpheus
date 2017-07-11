package org.opencypher.spark.impl.flat

import cats.Monoid
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.global.Label
import org.opencypher.spark.api.ir.pattern.{AllGiven, EveryNode, EveryRelationship}
import org.opencypher.spark.api.record._
import org.opencypher.spark.api.types._
import org.opencypher.spark.impl.exception.Raise
import org.opencypher.spark.impl.logical.{GraphSource, NamedLogicalGraph}
import org.opencypher.spark.impl.syntax.header._
import org.opencypher.spark.impl.util.{Added, FailedToAdd, Found, Replaced}
import org.opencypher.spark.impl.syntax.expr._

class FlatOperatorProducer(implicit context: FlatPlannerContext) {

  private val tokens = context.tokens
  private val schema = context.schema

  private implicit val typeVectorMonoid = new Monoid[Vector[CypherType]] {
    override def empty: Vector[CypherType] = Vector.empty
    override def combine(x: Vector[CypherType], y: Vector[CypherType]): Vector[CypherType] = x ++ y
  }

  def sanitize(in: FlatOperator): Sanitize = {
    val fieldContents = in.header.contents.collect { case content: FieldSlotContent => content }.toSeq

    val (nextHeader, _) = RecordHeader.empty.update(addContents(fieldContents))

    Sanitize(in, nextHeader)
  }

  def select(fields: IndexedSeq[Var], in: FlatOperator): Select = {
    val fieldContents = fields.map { field => in.header.slotsFor(field).head.content }
    val exprContents = in.header.contents.collect {
      case content@ProjectedExpr(expr) if (expr.dependencies -- fields).isEmpty => content
    }
    val finalContents = fieldContents ++ exprContents

    val (nextHeader, _) = RecordHeader.empty.update(addContents(finalContents))

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

  def nodeScan(node: Var, nodeDef: EveryNode, prev: FlatOperator): NodeScan = {

    val header = if (nodeDef.labels.elts.isEmpty) RecordHeader.nodeFromSchema(node, schema, tokens)
    else RecordHeader.nodeFromSchema(node, schema, tokens, nodeDef.labels.elts.map(_.name))

    val _nodeDef = if (nodeDef.labels.elts.isEmpty) EveryNode(AllGiven(schema.labels.map(Label))) else nodeDef

    new NodeScan(node, _nodeDef, prev, header)
  }

  def edgeScan(edge: Var, edgeDef: EveryRelationship, prev: FlatOperator): EdgeScan = {
    val edgeHeader = if (edgeDef.relTypes.elts.isEmpty) RecordHeader.relationshipFromSchema(edge, schema, tokens)
    else RecordHeader.relationshipFromSchema(edge, schema, tokens, edgeDef.relTypes.elts.map(_.name))

    EdgeScan(edge, edgeDef, prev, edgeHeader)
  }

  def varLengthEdgeScan(edgeList: Var, edgeDef: EveryRelationship, prev: FlatOperator): EdgeScan = {
    val edge = FreshVariableNamer(edgeList.name + "extended", CTRelationship)
    edgeScan(edge, edgeDef, prev)
  }

  // TODO: Specialize per kind of slot content
  def project(it: ProjectedSlotContent, in: FlatOperator): FlatOperator = {
    val (newHeader, result) = in.header.update(addContent(it))

    result match {
      case _: Found[_] => in
      case _: Replaced[_] => Alias(it.expr, it.alias.get, in, newHeader)
      case _: Added[_] => Project(it.expr, in, newHeader)
      case f: FailedToAdd[_] => Raise.slotNotAdded(f.toString)
    }
  }

  // TODO: Specialize per kind of slot content
  // TODO: Remove types parameter and read rel-types from the rel variable
  def expandSource(source: Var, rel: Var, types: EveryRelationship, target: Var,
                   sourceOp: FlatOperator, targetOp: FlatOperator): FlatOperator = {
    val relHeader = if (types.relTypes.elts.isEmpty) RecordHeader.relationshipFromSchema(rel, schema, tokens)
    else RecordHeader.relationshipFromSchema(rel, schema, tokens, types.relTypes.elts.map(_.name))

    val expandHeader = sourceOp.header ++ relHeader ++ targetOp.header

    ExpandSource(source, rel, types, target, sourceOp, targetOp, expandHeader, relHeader)
  }

  def planStart(logicalGraph: NamedLogicalGraph, source: GraphSource, fields: Set[Var]): Start = {
    Start(logicalGraph, source, fields)
  }

  def initVarExpand(source: Var, edgeList: Var, in: FlatOperator): InitVarExpand = {
    val endNodeId = FreshVariableNamer(edgeList.name + "endNode", CTNode)
    val (header, _) = in.header.update(addContents(Seq(OpaqueField(edgeList), OpaqueField(endNodeId))))

    InitVarExpand(source, edgeList, endNodeId, in, header)
  }

  def boundedVarExpand(edge: Var, edgeList: Var, target: Var, lower: Int, upper: Int,
                       sourceOp: InitVarExpand, edgeOp: FlatOperator, targetOp: FlatOperator) : FlatOperator = {

    val (initHeader, _) = sourceOp.in.header.update(addContent(OpaqueField(edgeList)))
    val header = initHeader ++ targetOp.header

    BoundedVarExpand(edge, edgeList, target, lower, upper, sourceOp, edgeOp, targetOp, header)
  }
}
