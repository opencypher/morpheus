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
package org.opencypher.caps.ir.impl.global

import org.neo4j.cypher.internal.frontend.v3_3.ast
import org.opencypher.caps.ir.api.global._
import org.opencypher.caps.api.types.CypherType
import org.opencypher.caps.impl.typer.fromFrontendType

object ConstantsExtractor {

  def apply(expr: ast.ASTNode, constants: ConstantRegistry = ConstantRegistry.empty): ConstantRegistry = {
    expr.fold(constants) {
      case ast.Parameter(name, _) => _.withConstant(Constant(name))
    }
  }

  def paramWithTypes(expr: ast.ASTNode): Map[ast.Expression, CypherType] = {
    expr.fold(Map.empty[ast.Expression, CypherType]) {
      case p: ast.Parameter => _.updated(p, fromFrontendType(p.parameterType))
    }
  }
}
