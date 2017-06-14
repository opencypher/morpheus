package org.opencypher.spark.impl.flat

import cats.Monoid
import org.opencypher.spark.api.exception.SparkCypherException
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.pattern.{AllGiven, EveryNode, EveryRelationship}
import org.opencypher.spark.api.record._
import org.opencypher.spark.api.types._
import org.opencypher.spark.impl.logical.{GraphSource, NamedLogicalGraph}
import org.opencypher.spark.impl.syntax.header._
import org.opencypher.spark.impl.syntax.util.traversable._
import org.opencypher.spark.impl.util.{Added, FailedToAdd, Found, Replaced}

class FlatOperatorProducer(implicit context: FlatPlannerContext) {

  private val globals = context.globalsRegistry
  private val schema = context.schema

  import globals._

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
    val nodeDef = if (_nodeDef.labels.elts.isEmpty) EveryNode(AllGiven(schema.labels.map(globals.labelRefByName))) else _nodeDef

    val givenLabels = nodeDef.labels.elts.map(ref => label(ref).name)

    val header = constructHeaderFromKnownLabels(node, givenLabels)

    NodeScan(node, nodeDef, prev, header)
  }

  private def constructHeaderFromKnownLabels(node: Var, labels: Set[String]) = {

    val impliedLabels = schema.impliedLabels.transitiveImplicationsFor(labels)
    val impliedKeys = impliedLabels.flatMap(label => schema.nodeKeyMap.keysFor(label).toSet)
    val possibleLabels = impliedLabels.flatMap(label => schema.optionalLabels.combinationsFor(label))
    val optionalKeys = possibleLabels.flatMap(label => schema.nodeKeyMap.keysFor(label).toSet)
    val optionalNullableKeys = optionalKeys.map { case (k, v) => k -> v.nullable }
    val allKeys = (impliedKeys ++ optionalNullableKeys).toSeq.map { case (k, v) => k -> Vector(v) }
    val keyGroups = allKeys.groups[String, Vector[CypherType]]

    val labelHeaderContents = (impliedLabels ++ possibleLabels).map {
      labelName => ProjectedExpr(HasLabel(node, labelRefByName(labelName))(CTBoolean))
    }.toSeq

    // TODO: This should consider multiple types per property
    val keyHeaderContents = keyGroups.toSeq.flatMap {
      case (k, types) => types.map { t => ProjectedExpr(Property(node, propertyKeyRefByName(k))(t)) }
    }

    // TODO: Add is null column(?)

    // TODO: Check results for errors
    val (header, _) = RecordHeader.empty
      .update(addContents(OpaqueField(node) +: (labelHeaderContents ++ keyHeaderContents)))

    header
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
    val relKeyHeaderProperties = if (types.relTypes.elts.isEmpty) schema.relationshipTypes.flatMap(t => schema.relationshipKeys(t).toSeq)
    else types.relTypes.elts.flatMap(t => schema.relationshipKeys(globals.relType(t).name).toSeq)

    val relKeyHeaderContents = relKeyHeaderProperties.map {
      case ((k, t)) => ProjectedExpr(Property(rel, propertyKeyRefByName(k))(t))
    }

    val startNode = ProjectedExpr(StartNode(rel)(CTNode))
    val typeIdContent = ProjectedExpr(TypeId(rel)(CTInteger))
    val endNode = ProjectedExpr(EndNode(rel)(CTNode))

    val relHeaderContents = Seq(startNode, OpaqueField(rel), typeIdContent, endNode) ++ relKeyHeaderContents
    // this header is necessary on its own to get the type filtering right
    val (relHeader, _) = RecordHeader.empty.update(addContents(relHeaderContents))

    val expandHeader = sourceOp.header ++ relHeader ++ targetOp.header

    ExpandSource(source, rel, types, target, sourceOp, targetOp, expandHeader, relHeader)
  }

  def planLoadGraph(logicalGraph: NamedLogicalGraph, source: GraphSource): LoadGraph = {

    LoadGraph(logicalGraph, source)
  }
}
