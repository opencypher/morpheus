package org.opencypher.spark.prototype.impl.planner

import cats.Monoid
import org.opencypher.spark.api.CypherType
import org.opencypher.spark.api.types.CTBoolean
import org.opencypher.spark.prototype.api.expr._
import org.opencypher.spark.prototype.api.ir.pattern.EveryNode
import org.opencypher.spark.prototype.api.record.{ProjectedExpr, RecordHeader}
import org.opencypher.spark.prototype.impl.physical._
import org.opencypher.spark.prototype.impl.syntax.header._
import org.opencypher.spark.prototype.impl.syntax.util.traversable._

class PhysicalOperatorProducer(implicit context: PhysicalPlannerContext) {

  private val globals = context.globalsRegistry
  private val schema = context.schema

  import globals._

  private implicit val typeVectorMonoid = new Monoid[Vector[CypherType]] {
    override def empty: Vector[CypherType] = Vector.empty
    override def combine(x: Vector[CypherType], y: Vector[CypherType]): Vector[CypherType] = x ++ y
  }

  def select(fields: Seq[(Expr, String)], in: PhysicalOperator) = {
    val remaining = fields.collect { case (k, v) => Var(v) }.toSet
    Select(fields, in, in.header)
  }

  def filter(expr: Expr, in: PhysicalOperator): Filter = {
    Filter(expr, in, in.header)
  }

  def nodeScan(node: Var, nodeDef: EveryNode): NodeScan = {
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

    NodeScan(node, nodeDef)(header)
  }
}
