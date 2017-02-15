package org.opencypher.spark.impl.prototype

import org.neo4j.cypher.internal.frontend.v3_2.ast
import org.neo4j.cypher.internal.frontend.v3_2.ast.LabelName

class ExpressionConverter(val tokenDefs: TokenDefs) {

  def convert(e: ast.Expression): Expr = e match {
    case ast.Variable(name) => Var(name)
    case astExpr: ast.IntegerLiteral => IntegerLit(astExpr.value)
    case ast.StringLiteral(value) => StringLit(value)
    case ast.Property(m, ast.PropertyKeyName(name)) => Property(convert(m), tokenDefs.propertyKey(name).get)
    case ast.Equals(lhs, rhs) => Equals(convert(lhs), convert(rhs))
    case ast.HasLabels(node, labels) =>
      val expr = convert(node)
      val map: Set[Expr] = labels.toSet.map { (l: LabelName) => HasLabel(expr, tokenDefs.label(l.name).get) }
      Ands(map)
    case _ => throw new UnsupportedOperationException(s"No mapping defined for $e")
  }
}
