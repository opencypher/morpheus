/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.okapi.ir.impl

import org.neo4j.cypher.internal.v4_0.expressions.{RegexMatch, functions}
import org.neo4j.cypher.internal.v4_0.util.Ref
import org.neo4j.cypher.internal.v4_0.{expressions => ast}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.impl.FunctionUtils._

import scala.language.implicitConversions

final class ExpressionConverter(implicit context: IRBuilderContext) {

  implicit def toRef(e: ast.Expression): Ref[ast.Expression] = Ref(e)

  def convert(e: ast.Expression)(implicit typings: Ref[ast.Expression] => CypherType): Expr = e match {
    case ast.Variable(name) =>
      Var(name)(typings(e))
    case ast.Parameter(name, _) =>
      Param(name)(typings(e))

    // Literals
    case astExpr: ast.IntegerLiteral =>
      IntegerLit(astExpr.value)(typings(e))
    case ast.StringLiteral(value) =>
      StringLit(value)(typings(e))
    case _: ast.True =>
      TrueLit
    case _: ast.False =>
      FalseLit
    case ast.ListLiteral(exprs) =>
      ListLit(exprs.map(convert).toIndexedSeq)(typings(e))
    case ast.ContainerIndex(container, index) =>
      ContainerIndex(convert(container), convert(index))(typings(e))

    case ast.Property(m, ast.PropertyKeyName(name)) => Property(convert(m), PropertyKey(name))(typings(e))

    // Predicates
    case ast.Ands(exprs) =>
      Ands(exprs.map(convert))
    case ast.Ors(exprs) =>
      Ors(exprs.map(convert))
    case ast.HasLabels(node, labels) =>
      val exprs = labels.map { l: ast.LabelName =>
        HasLabel(convert(node), Label(l.name))(typings(e))
      }
      if (exprs.size == 1) exprs.head else Ands(exprs.toSet)
    case ast.Not(expr) =>
      Not(convert(expr))(typings(e))
    case ast.Equals(f: ast.FunctionInvocation, s: ast.StringLiteral) if f.function == functions.Type =>
      HasType(convert(f.args.head), RelType(s.value))(CTBoolean)
    case ast.Equals(lhs, rhs) =>
      Equals(convert(lhs), convert(rhs))(typings(e))
    case ast.LessThan(lhs, rhs) =>
      LessThan(convert(lhs), convert(rhs))(typings(e))
    case ast.LessThanOrEqual(lhs, rhs) =>
      LessThanOrEqual(convert(lhs), convert(rhs))(typings(e))
    case ast.GreaterThan(lhs, rhs) =>
      GreaterThan(convert(lhs), convert(rhs))(typings(e))
    case ast.GreaterThanOrEqual(lhs, rhs) =>
      GreaterThanOrEqual(convert(lhs), convert(rhs))(typings(e))
    // if the list only contains a single element, convert to simple equality to avoid list construction
    case ast.In(lhs, ast.ListLiteral(elems)) if elems.size == 1 =>
      Equals(convert(lhs), convert(elems.head))(typings(e))
    case ast.In(lhs, rhs) =>
      In(convert(lhs), convert(rhs))(typings(e))
    case ast.IsNull(expr) =>
      IsNull(convert(expr))(typings(e))
    case ast.IsNotNull(expr) =>
      IsNotNull(convert(expr))(typings(e))
    case ast.StartsWith(lhs, rhs) =>
      StartsWith(convert(lhs), convert(rhs))
    case ast.EndsWith(lhs, rhs) =>
      EndsWith(convert(lhs), convert(rhs))
    case ast.Contains(lhs, rhs) =>
      Contains(convert(lhs), convert(rhs))

    // Arithmetics
    case ast.Add(lhs, rhs) =>
      Add(convert(lhs), convert(rhs))(typings(e))
    case ast.Subtract(lhs, rhs) =>
      Subtract(convert(lhs), convert(rhs))(typings(e))
    case ast.Multiply(lhs, rhs) =>
      Multiply(convert(lhs), convert(rhs))(typings(e))
    case ast.Divide(lhs, rhs) =>
      Divide(convert(lhs), convert(rhs))(typings(e))

    case funcInv: ast.FunctionInvocation =>
      funcInv.convertFunction(funcInv.args.map(convert), typings(e))
    case _: ast.CountStar =>
      CountStar(typings(e))

    // Exists (rewritten Pattern Expressions)
    case org.opencypher.okapi.ir.impl.parse.rewriter.ExistsPattern(subquery, trueVar) =>
      val innerModel = IRBuilder(subquery)(context) match {
        case sq: SingleQuery => sq
        case _ => throw new IllegalArgumentException("ExistsPattern only accepts SingleQuery")
      }
      ExistsPatternExpr(
        Var(trueVar.name)(CTBoolean),
        innerModel
      )(typings(e))

    // Case When .. Then .. [Else ..] End
    case ast.CaseExpression(None, alternatives, default) =>
      val alternativeExprs = alternatives.map { case (left, right) => convert(left) -> convert(right) }
      val defaultExpr = default.flatMap(expr => Some(convert(expr)))
      CaseExpr(alternativeExprs, defaultExpr)(typings(e))

    case ast.MapExpression(items) =>
      val convertedItems: Map[String, Expr] = items.map {
        case (key, value) => key.name -> convert(value)
      }.toMap
      MapExpression(convertedItems)(typings(e))

    case ast.Null() => NullLit(typings(e))

    case RegexMatch(lhs, rhs) => expr.RegexMatch(convert(lhs), convert(rhs))(typings(e))

    case _ =>
      throw NotImplementedException(s"Not yet able to convert expression: $e")
  }
}
