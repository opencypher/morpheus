package org.opencypher.spark.prototype

import org.neo4j.cypher.internal.frontend.v3_2.ast
import org.opencypher.spark.prototype.ir.token._

object TokenCollector {

  def apply(expr: ast.ASTNode, tokens: TokenRegistry = TokenRegistry.none): TokenRegistry = {
    expr.fold(tokens) {
      case ast.LabelName(name) => _.withLabel(Label(name))
      case ast.RelTypeName(name) => _.withRelType(RelType(name))
      case ast.PropertyKeyName(name) => _.withPropertyKey(PropertyKey(name))
      case ast.Parameter(name, _) => _.withParameter(Parameter(name))
    }
  }
}
