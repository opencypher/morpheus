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

import org.opencypher.okapi.api.types.{CTInteger, CypherType}
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.impl.parse.{functions => f}
import org.opencypher.v9_0.expressions.{FunctionInvocation, functions}

object FunctionUtils {

  implicit class RichFunctionInvocation(functionInvocation: FunctionInvocation) {
    def convertFunction(expr: IndexedSeq[Expr], cypherType: CypherType): Expr = {
      val distinct = functionInvocation.distinct

      functionInvocation.function match {
        case functions.Id => Id(expr.head)
        case functions.Labels => Labels(expr.head)
        case functions.Type => Type(expr.head)
        case functions.Avg => Avg(expr.head)
        case functions.Count => Count(expr.head, distinct)
        case functions.Max => Max(expr.head)(cypherType)
        case functions.Min => Min(expr.head)(cypherType)
        case functions.Sum => Sum(expr.head)(cypherType)
        case functions.Exists => Exists(expr.head)
        case functions.Size => Size(expr.head)
        case functions.Keys => Keys(expr.head)(cypherType)
        case functions.StartNode => StartNodeFunction(expr.head)(cypherType)
        case functions.EndNode => EndNodeFunction(expr.head)(cypherType)
        case functions.ToFloat => ToFloat(expr.head)
        case functions.ToInteger => ToInteger(expr.head)
        case functions.Collect => Collect(expr.head, distinct)
        case functions.Coalesce => Coalesce(expr)(cypherType)
        case functions.ToString => ToString(expr.head)
        case functions.ToBoolean => ToBoolean(expr.head)
        case functions.Range => Range(expr(0), expr(1), expr.lift(2))
        case functions.Substring => Substring(expr(0), expr(1), expr.lift(2))
        case functions.Left => Substring(expr(0), IntegerLit(0), expr.lift(1))
        case functions.Right => Substring(expr(0), Subtract(Multiply(IntegerLit(-1), expr(1))(CTInteger), IntegerLit(1))(CTInteger), None)
        case functions.Replace => Replace(expr(0), expr(1), expr(2))
        case functions.Trim => Trim(expr.head)
        case functions.LTrim => LTrim(expr.head)
        case functions.RTrim => RTrim(expr.head)
        case functions.ToUpper => ToUpper(expr.head)
        case functions.ToLower => ToLower(expr.head)
        case functions.Properties => Properties(expr.head)(cypherType)

        // Logarithmic functions
        case functions.Sqrt => Sqrt(expr.head)
        case functions.Log => Log(expr.head)
        case functions.Log10 => Log10(expr.head)
        case functions.Exp => Exp(expr.head)
        case functions.E => E
        case functions.Pi => Pi

        // Numeric functions
        case functions.Abs => Abs(expr.head)(cypherType)
        case functions.Ceil => Ceil(expr.head)
        case functions.Floor => Floor(expr.head)
        case functions.Rand => Rand
        case functions.Round => Round(expr.head)
        case functions.Sign => Sign(expr.head)

        // Trigonometric functions
        case functions.Acos => Acos(expr.head)
        case functions.Asin => Asin(expr.head)
        case functions.Atan => Atan(expr.head)
        case functions.Atan2 => Atan2(expr(0),expr(1))
        case functions.Cos => Cos(expr.head)
        case functions.Cot => Cot(expr.head)
        case functions.Degrees => Degrees(expr.head)
        case functions.Haversin => Haversin(expr.head)
        case functions.Radians => Radians(expr.head)
        case functions.Sin => Sin(expr.head)
        case functions.Tan => Tan(expr.head)

        // Match by name
        case functions.UnresolvedFunction => functionInvocation.name match {
          // Time functions
          case f.Timestamp.name => Timestamp
          case f.LocalDateTime.name => LocalDateTime(expr.headOption)
          case f.Date.name => Date(expr.headOption)
          case f.Duration.name => Duration(expr.head)

          case name => throw NotImplementedException(s"Support for converting ${name} function not yet implemented")
        }

        case a: functions.Function =>
          throw NotImplementedException(s"Support for converting ${a.name} function not yet implemented")
      }
    }
  }
}
