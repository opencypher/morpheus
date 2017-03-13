package org.opencypher.spark.prototype.impl.planner

import org.opencypher.spark.prototype.api.expr._
import org.opencypher.spark.prototype.api.ir.global.GlobalsRegistry
import org.opencypher.spark.prototype.api.ir.pattern.AllGiven
import org.opencypher.spark.prototype.api.spark.{SparkCypherGraph, SparkCypherView}
import org.opencypher.spark.prototype.impl.logical
import org.opencypher.spark.prototype.impl.physical.RecordsViewProducer._

case class PhysicalPlannerContext(graph: SparkCypherGraph, globals: GlobalsRegistry)

class PhysicalPlanner extends Stage[logical.LogicalOperator, SparkCypherView, PhysicalPlannerContext] {

  def plan(logicalPlan: logical.LogicalOperator)(implicit context: PhysicalPlannerContext): SparkCypherView =
    logicalPlan match {
      case logical.Select(fields, in) =>
        plan(in).select(fields.toMap)

      case logical.NodeScan(v, every) =>
        context.graph.allNodes(v)

      case logical.Project(it, in) =>
        plan(in).project(it)

      case logical.Filter(expr, in) => expr match {
        case HasLabel(n: Var, ref) =>
          plan(in).labelFilter(n, AllGiven(Set(ref)))
        case _ => ???
      }
        //      if (in.signature.items.exists(_.exprs.contains(expr))) {
        //        planExpr(planOp(in), expr)
        //      }
  //      planExpr(planOp(in), expr)
      case logical.ExpandSource(source, rel, target, in) =>
        // TODO: where is the rel-type info?
  //      val rels = allRelationships(rel).asProduct
  //        .relationshipStartId(rel)(relStart(rel))
  //        .relationshipEndId(rel)(relEnd(rel))
  //        .asRow
  //      // TODO: where is the node label info?
  //      val rhs = allNodes(target).asProduct.nodeId(target)(nodeId(target)).asRow
  //      val lhs = planOp(in).nodeId(source)(nodeId(source))
  //
  //      lhs.asRow.join(rels).on(nodeId(source))(relStart(rel)).join(rhs).on(relEnd(rel))(nodeId(target)).asProduct
        ???
      case x => throw new NotImplementedError(s"Can't plan operator $x yet")
    }
}
