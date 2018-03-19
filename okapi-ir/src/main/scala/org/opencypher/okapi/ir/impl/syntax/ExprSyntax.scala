/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.okapi.ir.impl.syntax

import org.opencypher.okapi.ir.api.expr._

import scala.annotation.tailrec
import scala.language.implicitConversions

object ExprSyntax {
  implicit def exprOps(e: Expr): ExprOps = new ExprOps(e)
}

final class ExprOps(val e: Expr) extends AnyVal {

  def evaluable(given: Set[Var]): Boolean =
    (dependencies -- given).isEmpty

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
      case Type(expr) :: tl => computeDependencies(expr :: tl, result)
      case Property(expr, _) :: tl => computeDependencies(expr :: tl, result)
      case ExistsPatternExpr(_, _) :: tl => computeDependencies(tl, result)
      case (expr: PredicateExpression) :: tl => computeDependencies(expr.inner :: tl, result)
      case (expr: Ands) :: tl => computeDependencies(expr.exprs.toList ++ tl, result)
      case (expr: Ors) :: tl => computeDependencies(expr.exprs.toList ++ tl, result)
      case (expr: Lit[_]) :: tl => computeDependencies(tl, result)
      case (expr: Param) :: tl => computeDependencies(tl, result)
      case (expr: BinaryExpr) :: tl => computeDependencies(expr.lhs :: expr.rhs :: tl, result)
      case (expr: FunctionExpr) :: tl => computeDependencies(expr.exprs.toList ++ tl, result)
      case (expr: Aggregator) :: tl => computeDependencies(expr.inner.map(_ :: tl).getOrElse(tl), result)
      case (expr: CaseExpr) :: tl => computeDependencies(expr.alternatives.map(_._1).toList ++ tl, result)
    }
}
