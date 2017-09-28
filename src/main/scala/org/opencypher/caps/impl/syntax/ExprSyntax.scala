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
package org.opencypher.caps.impl.syntax

import org.opencypher.caps.api.expr._

import scala.annotation.tailrec
import scala.language.implicitConversions

trait ExprSyntax {
  implicit def exprOps(e: Expr): ExprOps = new ExprOps(e)
}

final class ExprOps(val e: Expr) extends AnyVal {

  def dependencies: Set[Var] = computeDependencies(List(e), Set.empty)

  // TODO: Test this, possible consolidate into better hierarchy
  @tailrec
  private def computeDependencies(remaining: List[Expr], result: Set[Var]): Set[Var] =
    remaining match {
      case Nil => result
      case (expr: Var) :: tl => computeDependencies(tl, result + expr)
      case Equals(l, r) :: tl => computeDependencies(l :: r :: tl, result)
      case StartNode(expr) :: tl => computeDependencies(expr :: tl, result)
      case EndNode(expr) :: tl => computeDependencies(expr :: tl, result)
      case TypeId(expr) :: tl => computeDependencies(expr :: tl, result)
      case HasLabel(expr, _) :: tl => computeDependencies(expr :: tl, result)
      case HasType(expr, _) :: tl => computeDependencies(expr :: tl, result)
      case OfType(expr) :: tl => computeDependencies(expr :: tl, result)
      case Property(expr, _) :: tl => computeDependencies(expr :: tl, result)
      case (expr: Not) :: tl => computeDependencies(expr.expr :: tl, result)
      case (expr: IsNull) :: tl => computeDependencies(expr.expr :: tl, result)
      case (expr: IsNotNull) :: tl => computeDependencies(expr.expr :: tl, result)
      case (expr: Ands) :: tl => computeDependencies(expr.exprs.toList ++ tl, result)
      case (expr: Ors) :: tl => computeDependencies(expr.exprs.toList ++ tl, result)
      case (expr: Lit[_]) :: tl => computeDependencies(tl, result)
      case (expr: Const) :: tl => computeDependencies(tl, result)
      case (expr: BinaryExpr) :: tl => computeDependencies(expr.lhs :: expr.rhs :: tl, result)
      case (expr: FunctionExpr) :: tl => computeDependencies(expr.expr :: tl, result)
      case (expr: Aggregator) :: tl => computeDependencies(expr.inner.map(_ :: tl).getOrElse(tl), result)
    }
}
