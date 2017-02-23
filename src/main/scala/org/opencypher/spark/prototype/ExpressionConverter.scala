package org.opencypher.spark.prototype

import org.neo4j.cypher.internal.frontend.v3_2.ast
import org.neo4j.cypher.internal.frontend.v3_2.ast.LabelName
import org.opencypher.spark.prototype.ir.token.TokenRegistry

final class ExpressionConverter(val tokenDefs: TokenRegistry) extends AnyVal {

  def convert(e: ast.Expression): Expr = e match {
    case ast.Variable(name) => Var(name)
    case astExpr: ast.IntegerLiteral => IntegerLit(astExpr.value)
    case ast.StringLiteral(value) => StringLit(value)
    case ast.Property(m, ast.PropertyKeyName(name)) => Property(convert(m), tokenDefs.propertyKey(name))
    case ast.Equals(lhs, rhs) => Equals(convert(lhs), convert(rhs))
    case ast.In(lhs, ast.ListLiteral(elems)) if elems.size == 1 =>
      Equals(convert(lhs), convert(elems.head))
    case ast.HasLabels(node, labels) =>
      val exprs = labels.map { (l: LabelName) => HasLabel(convert(node), tokenDefs.label(l.name)) }
      if (exprs.size == 1) exprs.head else Ands(exprs: _*)
    case ast.Ands(exprs) => Ands(exprs.map(convert))
    case ast.Parameter(name, _) => Param(tokenDefs.param(name))
    case _ => throw new UnsupportedOperationException(s"No mapping defined for $e")
  }

}
