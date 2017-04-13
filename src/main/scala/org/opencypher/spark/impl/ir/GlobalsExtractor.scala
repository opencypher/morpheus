package org.opencypher.spark.impl.ir

import org.neo4j.cypher.internal.frontend.v3_2.ast
import org.opencypher.spark.api.ir.global._
import org.opencypher.spark.api.types.CypherType
import org.opencypher.spark.impl.typer.fromFrontendType

object GlobalsExtractor {

  def apply(expr: ast.ASTNode, tokens: GlobalsRegistry = GlobalsRegistry.none): GlobalsRegistry = {
    expr.fold(tokens) {
      case ast.LabelName(name) => _.withLabel(Label(name))
      case ast.RelTypeName(name) => _.withRelType(RelType(name))
      case ast.PropertyKeyName(name) => _.withPropertyKey(PropertyKey(name))
      case ast.Parameter(name, _) => _.withConstant(Constant(name))
    }
  }

  def paramWithTypes(expr: ast.ASTNode): Map[ast.Expression, CypherType] = {
    expr.fold(Map.empty[ast.Expression, CypherType]) {
      case p: ast.Parameter => _.updated(p, fromFrontendType(p.parameterType))
    }
  }
}
