package org.opencypher.spark.prototype.impl.flat

import cats.Monoid
import org.opencypher.spark.api.CypherType
import org.opencypher.spark.api.types.{CTBoolean, CTInteger}
import org.opencypher.spark.prototype.api.expr._
import org.opencypher.spark.prototype.api.ir.pattern.{AllGiven, EveryNode, EveryRelationship}
import org.opencypher.spark.prototype.api.record._
import org.opencypher.spark.prototype.impl.syntax.header._
import org.opencypher.spark.prototype.impl.syntax.util.traversable._
import org.opencypher.spark.prototype.impl.util.{Found, Replaced}

class FlatOperatorProducer(implicit context: FlatPlannerContext) {

  private val globals = context.globalsRegistry
  private val schema = context.schema

  import globals._

  private implicit val typeVectorMonoid = new Monoid[Vector[CypherType]] {
    override def empty: Vector[CypherType] = Vector.empty
    override def combine(x: Vector[CypherType], y: Vector[CypherType]): Vector[CypherType] = x ++ y
  }

  // TODO: Unalias dependencies MATCH (n) WITH n.prop AS m, n WITH n // frees up m, don't lose n.prop
  def select(fields: Set[Var], in: FlatOperator) = {
    // TODO: Error handling
    val (newHeader, removed) = in.header.update(selectFields {
      case RecordSlot(_, content: FieldSlotContent) => fields(content.field)
      case _ => false
    })
    Select(fields, in, newHeader)
  }

  def filter(expr: Expr, in: FlatOperator): Filter = {
    Filter(expr, in, in.header)
  }

  def nodeScan(node: Var, _nodeDef: EveryNode): NodeScan = {
    val nodeDef = if (_nodeDef.labels.elts.isEmpty) EveryNode(AllGiven(schema.labels.map(globals.label))) else _nodeDef

    val givenLabels = nodeDef.labels.elts.map(ref => label(ref).name)
    val impliedLabels = schema.impliedLabels.transitiveImplicationsFor(givenLabels)
    val impliedKeys = impliedLabels.flatMap(label => schema.nodeKeyMap.keysFor(label).toSet)
    val possibleLabels = impliedLabels.flatMap(label => schema.optionalLabels.combinationsFor(label))
    val optionalKeys = possibleLabels.flatMap(label => schema.nodeKeyMap.keysFor(label).toSet)
    val optionalNullableKeys = optionalKeys.map { case (k, v) => k -> v.nullable }
    val allKeys = (impliedKeys ++ optionalNullableKeys).toSeq.map { case (k, v) => k -> Vector(v) }
    val keyGroups = allKeys.groups[String, Vector[CypherType]]

    val labelHeaderContents = (impliedLabels ++ possibleLabels).map {
      labelName => ProjectedExpr(HasLabel(node, label(labelName)), CTBoolean)
    }.toSeq

    val keyHeaderContents = keyGroups.toSeq.flatMap {
      case (k, types) => types.map { t => ProjectedExpr(Property(node, propertyKey(k)), t) }
    }

    // TODO: Add is null column(?)

    // TODO: Check results for errors
    val (header, _) = RecordHeader.empty.update(addContents(labelHeaderContents ++ keyHeaderContents))

    NodeScan(node, nodeDef, header)
  }

  // TODO: Specialize per kind of slot content
  def project(it: ProjectedSlotContent, in: FlatOperator): FlatOperator = {
    val (newHeader, result) = in.header.update(addContent(it))

    result match {
      case _: Found[_] => in
      case _: Replaced[_] => Alias(it.expr, it.alias.get, in, newHeader)
      case _ => throw new NotImplementedError("No support yet for projecting non-attribute expressions") // TODO: Error handling
    }
  }

  // TODO: Specialize per kind of slot content
  def expandSource(source: Var, rel: Var, types: EveryRelationship, target: Var, in: FlatOperator): FlatOperator = {
    // TODO: This should consider multiple types per property
    val allNodeProperties = schema.nodeKeyMap.m.values.reduce(_ ++ _).toSeq.distinct
    val allLabels = schema.labels

    val targetLabelHeaderContents = allLabels.map {
      labelName => ProjectedExpr(HasLabel(target, label(labelName)), CTBoolean)
    }

    // TODO: This should consider multiple types per property
    val relKeyHeaderProperties = types.relTypes.elts.flatMap(t => schema.relationshipKeys(globals.relType(t).name).toSeq)
    val relKeyHeaderContents = relKeyHeaderProperties.map {
      case ((k, t)) => ProjectedExpr(Property(rel, propertyKey(k)), t)
    }

    val targetKeyHeaderContents = allNodeProperties.map {
      case ((k, t)) => ProjectedExpr(Property(target, propertyKey(k)), t)
    }

    val typeIdContent = ProjectedExpr(TypeId(rel), CTInteger)

    val (newHeader, _) = in.header.update(addContents(
      Seq(typeIdContent) ++ relKeyHeaderContents ++ targetLabelHeaderContents ++ targetKeyHeaderContents
    ))
    ExpandSource(source, rel, types, target, in, newHeader)
  }
}
