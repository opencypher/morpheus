package org.opencypher.spark.impl.prototype

import org.neo4j.cypher.internal.frontend.v3_2.ast
import org.neo4j.cypher.internal.frontend.v3_2.ast.LabelName

class ExpressionConverter(val tokenDefs: TokenDefs) {

  def convert(e: ast.Expression): Expr = e match {
    case ast.Variable(name) => Var(name)
    case astExpr: ast.IntegerLiteral => IntegerLit(astExpr.value)
    case ast.StringLiteral(value) => StringLit(value)
    case ast.Property(m, ast.PropertyKeyName(name)) => Property(convert(m), tokenDefs.propertyKey(name))
    case ast.Equals(lhs, rhs) => Equals(convert(lhs), convert(rhs))
    case ast.HasLabels(node, labels) =>
      val exprs = labels.map { (l: LabelName) => HasLabel(convert(node), tokenDefs.label(l.name)) }
      if (exprs.size == 1) exprs.head else Ands(exprs: _*)
    case ast.Ands(exprs) => Ands(exprs.map(convert))
    case _ => throw new UnsupportedOperationException(s"No mapping defined for $e")
  }
}
