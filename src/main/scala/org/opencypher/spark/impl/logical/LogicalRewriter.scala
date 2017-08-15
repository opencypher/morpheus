package org.opencypher.spark.impl.logical

import org.opencypher.spark.api.expr.{HasLabel, Var}
import org.opencypher.spark.api.ir.global.Label
import org.opencypher.spark.api.ir.pattern.{AllGiven, EveryNode}
import org.opencypher.spark.impl.DirectCompilationStage

class LogicalRewriter(producer: LogicalOperatorProducer)
  extends DirectCompilationStage[LogicalOperator, LogicalOperator, LogicalPlannerContext]{

  override def process(input: LogicalOperator)(implicit context: LogicalPlannerContext): LogicalOperator = {
    moveLabelPredicatesToNodeScans(input)
  }

  /**
    * This rewriter removes node label filters from the plan and pushes the predicate down to the NodeScan operations.
    *
    * @param input logical plan
    * @return rewritten plan
    */
  private def moveLabelPredicatesToNodeScans(input: LogicalOperator): LogicalOperator = {

    def extractLabels(op: LogicalOperator): Set[(Var, Label)] = {
      op match {
        case Filter(expr, in) =>
          val res = expr match {
            case HasLabel(v: Var, label) => Set(v -> label)
            case _ => Set.empty
          }
          res ++ extractLabels(in)
        case s:StackingLogicalOperator =>
          Set.empty ++ extractLabels(s.in)
        case b:BinaryLogicalOperator =>
          Set.empty ++ extractLabels(b.lhs) ++ extractLabels(b.rhs)
        case _ => Set.empty
      }
    }

    val labelMap = extractLabels(input).groupBy(_._1).mapValues(_.map(_._2))

    def rewrite(root: LogicalOperator): LogicalOperator = {
      root match {
        case NodeScan(node, nodeDef, in) =>
          NodeScan(node,
            EveryNode(AllGiven[Label](labelMap.getOrElse(node, nodeDef.labels.elements))),
            rewrite(in))(in.solved)
        case f@Filter(expr, in) =>
          expr match {
            case _:HasLabel => rewrite(in)
            case _ => f.clone(rewrite(in))
          }
        case s:StackingLogicalOperator =>
          s.clone(rewrite(s.in))
        case b:BinaryLogicalOperator =>
          b.clone(rewrite(b.lhs), rewrite(b.rhs))
        case l:LogicalLeafOperator =>
          l
      }
    }
    rewrite(input)
  }
}
