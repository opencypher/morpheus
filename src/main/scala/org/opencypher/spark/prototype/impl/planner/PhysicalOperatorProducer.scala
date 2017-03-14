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

import scala.annotation.tailrec

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
    val keyMaps = keyGroups.map {
      case (k, types) if types.exists(_.isNullable) => k -> types.map(_.nullable)
      case (k, types) => k -> types
    }

    val labelHeaderContents = (impliedLabels ++ possibleLabels).map {
      labelName => ProjectedExpr(HasLabel(node, label(labelName)), CTBoolean)
    }.toSeq

    val keyHeaderContents = allKeys.toSeq.flatMap {
      case (k, types) => types.map { t => ProjectedExpr(Property(node, propertyKey(k)), t) }
    }.toSeq

    // TODO: Check results for errors
    val (header, _) = RecordHeader.empty.update(addContents(labelHeaderContents ++ keyHeaderContents))

    NodeScan(node, nodeDef)(header)
  }


  // TODO: Test and move this some place proper
  @tailrec
  private def dependencies(remaining: List[Expr], result: Set[Var]): Set[Var] = remaining match {
    case (expr: Var) :: tl => dependencies(tl, result + expr)
    case Equals(l, r) :: tl => dependencies(l :: r :: tl, result)
    case StartNode(expr) :: tl => dependencies(expr :: tl, result)
    case EndNode(expr) :: tl => dependencies(expr :: tl, result)
    case TypeId(expr) :: tl => dependencies(expr :: tl, result)
    case HasLabel(expr, _) :: tl => dependencies(expr :: tl, result)
    case HasType(expr, _) :: tl => dependencies(expr :: tl, result)
    case Property(expr, _) :: tl => dependencies(expr :: tl, result)
    case (expr: Ands) :: tl => dependencies(expr.exprs.toList ++ tl, result)
    case (expr: Ors) :: tl => dependencies(expr.exprs.toList ++ tl, result)
    case (expr: Lit[_]) :: tl => dependencies(tl, result)
    case (expr: Const) :: tl => dependencies(tl, result)
    case _ => result
  }
}
