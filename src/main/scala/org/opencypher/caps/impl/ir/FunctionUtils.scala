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
package org.opencypher.caps.impl.ir

import org.neo4j.cypher.internal.frontend.v3_3.ast.{FunctionInvocation, _}
import org.opencypher.caps.api.expr._
import org.opencypher.caps.api.types.CypherType
import org.opencypher.caps.impl.spark.exception.Raise

object FunctionUtils {

  implicit class RichFunctionInvocation(functionInvocation: FunctionInvocation) {
    def toCAPSFunction(expr: IndexedSeq[Expr], cypherType: CypherType): Expr = {
      functionInvocation.function match {
        case functions.Id => Id(expr.head)(cypherType)
        case functions.Labels => Labels(expr.head)(cypherType)
        case functions.Type => Type(expr.head)(cypherType)
        case functions.Avg => Avg(expr.head)(cypherType)
        case functions.Count => Count(expr.head)(cypherType)
        case functions.Max => Max(expr.head)(cypherType)
        case functions.Min => Min(expr.head)(cypherType)
        case functions.Sum => Sum(expr.head)(cypherType)
        case functions.Exists => Exists(expr.head)(cypherType)
        case a: Function => Raise.notYetImplemented(s"converting ${a.name} function")
      }
    }
  }
}
