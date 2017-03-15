package org.opencypher.spark.prototype.impl.planner

import org.opencypher.spark.api.CypherType
import org.opencypher.spark.api.types.CTAny
import org.opencypher.spark.prototype.api.expr._
import org.opencypher.spark.prototype.api.ir.{Field, SolvedQueryModel}
import org.opencypher.spark.prototype.api.ir.pattern.{EveryNode, EveryRelationship}
import org.opencypher.spark.prototype.api.record.{ProjectedExpr, ProjectedField, RecordHeader}
import org.opencypher.spark.prototype.impl.logical._

class LogicalOperatorProducer {

  def planTargetExpand(source: Field, rel: Field, target: Field, prev: LogicalOperator): ExpandTarget = {
    val signature = RecordHeader.empty
    val solved = prev.solved.withFields(rel, source)

    ExpandTarget(Var(source.name), Var(rel.name), Var(target.name), prev, signature)(solved)
  }

  def planSourceExpand(source: Field, r: (Field, EveryRelationship), target: Field, prev: LogicalOperator): ExpandSource = {
    val signature = RecordHeader.empty

    val (rel, types) = r
    val relVar = Var(rel.name)

    val solved = types.relTypes.elts.foldLeft(prev.solved.withFields(rel, target)) {
      case (acc, next) => acc.withPredicate(HasType(relVar, next))
    }

    ExpandSource(Var(source.name), relVar, types, Var(target.name), prev, signature)(solved)
  }

  def planNodeScan(node: Field, everyNode: EveryNode): NodeScan = {
    val signature = RecordHeader.empty

    val nodeVar = Var(node.name)
    val solved = everyNode.labels.elts.foldLeft(SolvedQueryModel.empty[Expr].withField(node)) {
      case (acc, ref) => acc.withPredicate(HasLabel(nodeVar, ref))
    }

    NodeScan(nodeVar, everyNode, signature)(solved)
  }


  def planFilter(expr: Expr, prev: LogicalOperator): Filter = {
    val signature = RecordHeader.empty

    Filter(expr, prev, signature)(prev.solved.withPredicate(expr))
  }

  def projectField(field: Field, expr: Expr, typ: Option[CypherType], prev: LogicalOperator): Project = {
    val signature = RecordHeader.empty
    val projection = ProjectedField(Var(field.name), expr, typ.getOrElse(CTAny.nullable))

    Project(projection, prev, signature)(prev.solved.withField(field))
  }

  def projectExpr(expr: Expr, typ: Option[CypherType], prev: LogicalOperator): Project = {
    val signature = RecordHeader.empty
    val projection = ProjectedExpr(expr, typ.getOrElse(CTAny.nullable))

    Project(projection, prev, signature)(prev.solved)
  }

  def planSelect(fields: Seq[(Var, String)], prev: LogicalOperator): Select = {
    val signature = RecordHeader.empty

    Select(fields, prev, signature)(prev.solved)
  }
}
