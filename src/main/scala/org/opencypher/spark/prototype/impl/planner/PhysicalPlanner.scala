package org.opencypher.spark.prototype.impl.planner

import org.opencypher.spark.prototype.api.expr.{Ands, Expr, HasLabel, Var}
import org.opencypher.spark.prototype.api.graph.{SparkCypherGraph, SparkCypherView}
import org.opencypher.spark.prototype.api.ir.QueryModel
import org.opencypher.spark.prototype.api.ir.global.LabelRef
import org.opencypher.spark.prototype.api.ir.pattern.AllGiven
import org.opencypher.spark.prototype.api.record.SparkCypherRecords
import org.opencypher.spark.prototype.impl.logical

class PhysicalPlanner {

  def plan(logicalPlan: logical.LogicalOperator)(implicit context: PhysicalPlanningContext): SparkCypherView = logicalPlan match {
    case logical.Select(fields, in) =>
      Select(fields, plan(in))
//      plan(in)
//      val frame = planOp(in).selectFields(fields.map(t => Symbol(t._2.replaceAllLiterally(".", "_"))): _*)
//      frame
    case logical.NodeScan(v, every) =>
      AllNodesScan(v, context.graph)
      context.graph.nodes
//      labelScan(v)(every.labels.elts.map(globals.label(_).name).toIndexedSeq).asProduct
    case logical.Project(expr, in) =>
//      planExpr(planOp(in), expr)
      ???
    case logical.Filter(expr, in) =>
      //      if (in.signature.items.exists(_.exprs.contains(expr))) {
      //        planExpr(planOp(in), expr)
      //      }
//      planExpr(planOp(in), expr)
      ???
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

case class AllNodesScan(node: Var, domain: SparkCypherGraph) extends SparkCypherView {
  override def graph: SparkCypherGraph = ???
  override def records: SparkCypherRecords = domain.nodes.records

  override def model: QueryModel[Expr] = ???
}

case class LabelFilter(node: Var, labels: AllGiven[LabelRef], in: SparkCypherView) extends SparkCypherView {
  override def domain: SparkCypherGraph = in.domain
  override def graph: SparkCypherGraph = ???
  override def records: SparkCypherRecords = {
    val labelExprs: Set[Expr] = labels.elts.map { ref => HasLabel(node, ref) }
    in.records.filter(Ands(labelExprs))
  }

  override def model: QueryModel[Expr] = ???
}

case class Select(fields: Seq[(Expr, String)], in: SparkCypherView) extends SparkCypherView {
  override def domain: SparkCypherGraph = in.domain
  override def graph: SparkCypherGraph = ???

//  override def records: SparkCypherRecords = {
//    val records = in.records
//    records.select(fields.toSet)
//  }

  override def model: QueryModel[Expr] = ???

  override def records: SparkCypherRecords = ???
}

case class PhysicalPlanningContext(graph: SparkCypherGraph)
