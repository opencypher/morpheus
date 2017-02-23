package org.opencypher.spark.prototype

import org.neo4j.cypher.internal.frontend.v3_2.ast
import org.opencypher.spark.prototype.ir.token.{Label, PropertyKey, RelType, TokenRegistry}

object TokenCollector {

  def apply(expr: ast.ASTNode, tokens: TokenRegistry = TokenRegistry.none): TokenRegistry = {
    expr.fold(tokens) {
      case ast.LabelName(name) => _.withLabel(Label(name))
      case ast.RelTypeName(name) => _.withRelType(RelType(name))
      case ast.PropertyKeyName(name) => _.withPropertyKey(PropertyKey(name))
    }
  }
}
