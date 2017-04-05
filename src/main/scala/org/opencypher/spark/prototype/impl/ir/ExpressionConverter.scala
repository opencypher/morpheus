package org.opencypher.spark.prototype.impl.ir

import org.neo4j.cypher.internal.frontend.v3_2.ast
import org.opencypher.spark.api.CypherType
import org.opencypher.spark.prototype.api.expr._
import org.opencypher.spark.prototype.api.ir.global.GlobalsRegistry

final class ExpressionConverter(val tokenDefs: GlobalsRegistry) extends AnyVal {

  def convert(e: ast.Expression)(implicit typings: Map[ast.Expression, CypherType]): Expr = e match {
    case ast.Variable(name) => Var(name, typings(e))
    case astExpr: ast.IntegerLiteral => IntegerLit(astExpr.value, typings(e))
    case ast.StringLiteral(value) => StringLit(value, typings(e))
    case ast.Property(m, ast.PropertyKeyName(name)) => Property(convert(m), tokenDefs.propertyKey(name), typings(e))
    case ast.Equals(lhs, rhs) => Equals(convert(lhs), convert(rhs), typings(e))
    case ast.In(lhs, ast.ListLiteral(elems)) if elems.size == 1 =>
      Equals(convert(lhs), convert(elems.head), typings(e))
    case ast.HasLabels(node, labels) =>
      val exprs = labels.map { (l: ast.LabelName) => HasLabel(convert(node), tokenDefs.label(l.name), typings(e)) }
      if (exprs.size == 1) exprs.head else new Ands(exprs.toSet, typings(e))
    case ast.Ands(exprs) => new Ands(exprs.map(convert), typings(e))
    case ast.Parameter(name, _) => Const(tokenDefs.constant(name), typings(e))
    case ast.Not(expr) => Not(convert(expr), typings(e))
    case _ => throw new NotImplementedError(s"Not yet able to convert expression: $e")
  }

}
