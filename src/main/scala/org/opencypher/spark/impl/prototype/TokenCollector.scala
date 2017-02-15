package org.opencypher.spark.impl.prototype

import org.neo4j.cypher.internal.frontend.v3_2.ast

object TokenCollector {

  def apply(expr: ast.ASTNode, tokens: TokenDefs = TokenDefs.empty): TokenDefs = {
    expr.fold(tokens) {
      case ast.LabelName(name) => _.withLabel(LabelDef(name))
      case ast.RelTypeName(name) => _.withRelType(RelTypeDef(name))
      case ast.PropertyKeyName(name) => _.withPropertyKey(PropertyKeyDef(name))
    }
  }
}
