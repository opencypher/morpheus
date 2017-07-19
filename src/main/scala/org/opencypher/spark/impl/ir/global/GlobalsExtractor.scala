/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.spark.impl.ir.global

import org.neo4j.cypher.internal.frontend.v3_2.ast
import org.opencypher.spark.api.ir.global._
import org.opencypher.spark.api.types.CypherType
import org.opencypher.spark.impl.typer.fromFrontendType

object GlobalsExtractor {

  def apply(expr: ast.ASTNode, tokens: GlobalsRegistry = GlobalsRegistry.empty): GlobalsRegistry = {
    expr.fold(tokens) {
      case ast.LabelName(name) => _.mapTokens(_.withLabel(Label(name)))
      case ast.RelTypeName(name) => _.mapTokens(_.withRelType(RelType(name)))
      case ast.PropertyKeyName(name) => _.mapTokens(_.withPropertyKey(PropertyKey(name)))
      case ast.Parameter(name, _) => _.mapConstants(_.withConstant(Constant(name)))
    }
  }

  def paramWithTypes(expr: ast.ASTNode): Map[ast.Expression, CypherType] = {
    expr.fold(Map.empty[ast.Expression, CypherType]) {
      case p: ast.Parameter => _.updated(p, fromFrontendType(p.parameterType))
    }
  }
}
