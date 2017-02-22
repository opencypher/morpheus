package org.opencypher.spark.prototype

import org.neo4j.cypher.internal.frontend.v3_2.ast
import org.opencypher.spark.prototype.ir.{LabelDef, PropertyKeyDef, RelTypeDef, TokenRegistry}

object TokenCollector {

  def apply(expr: ast.ASTNode, tokens: TokenRegistry = TokenRegistry.none): TokenRegistry = {
    expr.fold(tokens) {
      case ast.LabelName(name) => _.withLabel(LabelDef(name))
      case ast.RelTypeName(name) => _.withRelType(RelTypeDef(name))
      case ast.PropertyKeyName(name) => _.withPropertyKey(PropertyKeyDef(name))
    }
  }
}
