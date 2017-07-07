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

class FlatOperatorProducer(implicit context: FlatPlannerContext) {

  private val tokens = context.tokens
  private val schema = context.schema

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

    val header = if (_nodeDef.labels.elts.isEmpty) RecordHeader.nodeFromSchema(node, schema, tokens)
    else RecordHeader.nodeFromSchema(node, schema, tokens, _nodeDef.labels.elts.map(_.name))

    val nodeDef = if (_nodeDef.labels.elts.isEmpty) EveryNode(AllGiven(schema.labels.map(Label))) else _nodeDef

    new NodeScan(node, nodeDef, prev, header)
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

  def boundedVarLength(source: Var, rel: Var, target: Var,
                       lower: Int, upper: Int, sourceOp: FlatOperator, targetOp: FlatOperator): FlatOperator = {
    val (listEntry, _) = RecordHeader.empty.update(addContent(OpaqueField(rel)))

    val types = relTypes(rel)
    val relHeader = if (types.isEmpty) RecordHeader.relationshipFromSchema(rel, schema, tokens)
    else RecordHeader.relationshipFromSchema(rel, schema, tokens, types)

    val expandHeader = sourceOp.header ++ listEntry ++ targetOp.header

    val lastRel = Var("inventedName")(CTRelationship)
    val tempJoinHeader = sourceOp.header ++ listEntry.update(addContent(ProjectedExpr(EndNode(lastRel)(CTNode))))._1

    BoundedVarLength(source, rel, target, lower, upper, sourceOp, targetOp, expandHeader, relHeader, tempJoinHeader, lastRel)
  }

  private def relTypes(r: Var): Set[String] = r.cypherType match {
    case t: CTRelationship => t.types
    case _ => Set.empty
  }
}
