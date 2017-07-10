package org.opencypher.spark.impl.logical

import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.pattern.{EveryNode, EveryRelationship}
import org.opencypher.spark.api.ir.{Field, SolvedQueryModel}
import org.opencypher.spark.api.record.{ProjectedExpr, ProjectedField}
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.types._
import org.opencypher.spark.impl.util._

class LogicalOperatorProducer {

  def planTargetExpand(source: Field, rel: Field, target: Field, sourcePlan: LogicalOperator, targetPlan: LogicalOperator): ExpandTarget = {
    val prevSolved = sourcePlan.solved ++ targetPlan.solved

    val solved = prevSolved.withField(rel)

    ExpandTarget(source, rel, target, sourcePlan, targetPlan)(solved)
  }

  def planSourceExpand(source: Field, r: (Field, EveryRelationship), target: Field, sourcePlan: LogicalOperator, targetPlan: LogicalOperator): ExpandSource = {
    val (rel, types) = r

    val prevSolved = sourcePlan.solved ++ targetPlan.solved

    val solved = types.relTypes.elts.foldLeft(prevSolved.withField(rel)) {
      case (acc, next) => acc.withPredicate(HasType(rel, next)(CTBoolean))
    }

    ExpandSource(source, rel, types, target, sourcePlan, targetPlan)(solved)
  }

  def planNodeScan(node: Field, everyNode: EveryNode, prev: LogicalOperator): NodeScan = {
    val solved = everyNode.labels.elts.foldLeft(SolvedQueryModel.empty[Expr].withField(node)) {
      case (acc, label) => acc.withPredicate(HasLabel(node, label)(CTBoolean))
    }

    NodeScan(node, everyNode, prev)(solved)
  }

  def planFilter(expr: Expr, prev: LogicalOperator): Filter = {
    Filter(expr, prev)(prev.solved.withPredicate(expr))
  }

  def projectField(field: Field, expr: Expr, prev: LogicalOperator): Project = {
    val projection = ProjectedField(field, expr)

    Project(projection, prev)(prev.solved.withField(field))
  }

  def projectExpr(expr: Expr, prev: LogicalOperator): Project = {
    val projection = ProjectedExpr(expr)

    Project(projection, prev)(prev.solved)
  }

  def planSelect(fields: IndexedSeq[Var], prev: LogicalOperator): Select = {
    Select(fields, prev)(prev.solved)
  }

  def planLoadDefaultGraph(schema: Schema, fields: Set[Var]): LoadGraph = {
    val irFields = fields.map { v => Field(v.name)(v.cypherType) }
    LoadGraph(NamedLogicalGraph("default", schema), DefaultGraphSource, fields)(SolvedQueryModel(irFields, Set.empty))
  }
}
