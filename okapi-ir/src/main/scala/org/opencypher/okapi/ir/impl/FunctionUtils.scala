/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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

import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.v9_0.expressions.{FunctionInvocation, functions}

object FunctionUtils {

  implicit class RichFunctionInvocation(functionInvocation: FunctionInvocation) {
    def convertFunction(expr: IndexedSeq[Expr], cypherType: CypherType): Expr = {
      val distinct = functionInvocation.distinct

      functionInvocation.function match {
        case functions.Id => Id(expr.head)(cypherType)
        case functions.Labels => Labels(expr.head)(cypherType)
        case functions.Type => Type(expr.head)(cypherType)
        case functions.Avg => Avg(expr.head)(cypherType)
        case functions.Count => Count(expr.head, distinct)(cypherType)
        case functions.Max => Max(expr.head)(cypherType)
        case functions.Min => Min(expr.head)(cypherType)
        case functions.Sum => Sum(expr.head)(cypherType)
        case functions.Exists => Exists(expr.head)(cypherType)
        case functions.Size => Size(expr.head)(cypherType)
        case functions.Keys => Keys(expr.head)(cypherType)
        case functions.StartNode => StartNodeFunction(expr.head)(cypherType)
        case functions.EndNode => EndNodeFunction(expr.head)(cypherType)
        case functions.ToFloat => ToFloat(expr.head)(cypherType)
        case functions.ToInteger => ToInteger(expr.head)(cypherType)
        case functions.Collect => Collect(expr.head, distinct)(cypherType)
        case functions.Coalesce => Coalesce(expr)(cypherType)
        case functions.ToString => ToString(expr.head)(cypherType)
        case functions.ToBoolean => ToBoolean(expr.head)(cypherType)
        // Logarithmic functions
        case functions.Sqrt => Sqrt(expr.head)(cypherType)
        case functions.Log => Log(expr.head)(cypherType)
        case functions.Log10 => Log10(expr.head)(cypherType)
        case functions.Exp => Exp(expr.head)(cypherType)
        case functions.E => E()(cypherType)
        // Numeric functions
        case functions.Abs => Abs(expr.head)(cypherType)
        case functions.Ceil => Ceil(expr.head)(cypherType)
        case functions.Floor => Floor(expr.head)(cypherType)
        case functions.Rand => Rand()(cypherType)
        case functions.Round => Round(expr.head)(cypherType)

        case a: functions.Function =>
          throw NotImplementedException(s"Support for converting ${a.name} function not yet implemented")
      }
    }
  }
}
