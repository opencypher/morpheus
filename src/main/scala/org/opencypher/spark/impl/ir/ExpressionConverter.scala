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
package org.opencypher.spark.impl.ir

import org.neo4j.cypher.internal.frontend.v3_2.ast.functions
import org.neo4j.cypher.internal.frontend.v3_2.{Ref, ast}
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.global.GlobalsRegistry
import org.opencypher.spark.api.types._
import org.opencypher.spark.impl.parse.RetypingPredicate

import org.opencypher.spark.impl.ir.FunctionUtils._

import scala.language.implicitConversions

final class ExpressionConverter(val globals: GlobalsRegistry) extends AnyVal {

  import globals.constants
  import globals.tokens

  implicit def toRef(e: ast.Expression): Ref[ast.Expression] = Ref(e)

  def convert(e: ast.Expression)(implicit typings: (Ref[ast.Expression]) => CypherType): Expr = e match {
    case ast.Variable(name) =>
      Var(name)(typings(e))
    case ast.Parameter(name, _) =>
      Const(constants.constantByName(name))(typings(e))

    // Literals
    case astExpr: ast.IntegerLiteral =>
      IntegerLit(astExpr.value)(typings(e))
    case ast.StringLiteral(value) =>
      StringLit(value)(typings(e))
    case _: ast.True =>
      TrueLit()
    case _: ast.False =>
      FalseLit()
    case ast.ListLiteral(exprs) =>
      ListLit(exprs.map(convert).toIndexedSeq)(typings(e))

    case ast.Property(m, ast.PropertyKeyName(name)) => Property(convert(m), tokens.propertyKeyByName(name))(typings(e))

    // Predicates
    case ast.Ands(exprs) =>
      new Ands(exprs.map(convert))(typings(e))
    case ast.Ors(exprs) =>
      new Ors(exprs.map(convert))(typings(e))
    case ast.HasLabels(node, labels) =>
      val exprs = labels.map { (l: ast.LabelName) => HasLabel(convert(node), tokens.labelByName(l.name))(typings(e)) }
      if (exprs.size == 1) exprs.head else new Ands(exprs.toSet)(typings(e))
    case ast.Not(expr) =>
      Not(convert(expr))(typings(e))
      // TODO: Does this belong here still?
    case ast.Equals(f: ast.FunctionInvocation, s: ast.StringLiteral) if f.function == functions.Type =>
      HasType(convert(f.args.head), tokens.relTypeByName(s.value))(CTBoolean)
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
    case RetypingPredicate(lhs, rhs) =>
      new Ands(lhs.map(convert) + convert(rhs))(typings(e))
    // if the list only contains a single element, convert to simple equality to avoid list construction
    case ast.In(lhs, ast.ListLiteral(elems)) if elems.size == 1 =>
      Equals(convert(lhs), convert(elems.head))(typings(e))
    case ast.In(lhs, rhs) =>
      In(convert(lhs), convert(rhs))(typings(e))
    case ast.IsNull(expr) =>
      IsNull(convert(expr))(typings(e))
    case ast.IsNotNull(expr) =>
      IsNotNull(convert(expr))(typings(e))

    // Arithmetics
    case ast.Add(lhs, rhs) =>
      Add(convert(lhs), convert(rhs))(typings(e))
    case ast.Subtract(lhs, rhs) =>
      Subtract(convert(lhs), convert(rhs))(typings(e))
    case ast.Multiply(lhs, rhs) =>
      Multiply(convert(lhs), convert(rhs))(typings(e))
    case ast.Divide(lhs, rhs) =>
      Divide(convert(lhs), convert(rhs))(typings(e))

    // Functions
    case funcInv: ast.FunctionInvocation =>
      funcInv.toCAPSFunction(funcInv.args.map(convert), typings(e))
    case _: ast.CountStar =>
      CountStar()(typings(e))

    case _ =>
      throw new NotImplementedError(s"Not yet able to convert expression: $e")
  }
}
