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
 */
package org.opencypher.caps.cosc.impl.value

import org.opencypher.caps.api.value.CypherValue.{CypherBoolean, CypherMap, CypherNode, CypherValue}
import org.opencypher.caps.cosc.impl.COSCRuntimeContext
import org.opencypher.caps.cosc.impl.value.CypherValueOps._
import org.opencypher.caps.impl.exception.IllegalArgumentException
import org.opencypher.caps.impl.table.RecordHeader
import org.opencypher.caps.ir.api.PropertyKey
import org.opencypher.caps.ir.api.expr._

object CypherMapOps {

  implicit class RichCypherMap(map: CypherMap) {

    def evaluate(expr: Expr)(implicit header: RecordHeader, context: COSCRuntimeContext): CypherValue = expr match {

      case Param(name) =>
        println(s"Parameter lookup: $name in ${context.parameters}")

        context.parameters(name)

      case Property(Var(v), PropertyKey(k)) =>
        println(s"Property lookup: $v.$k in $map")

        map(v) match {
          case CypherNode(_, _, props) => props(k)
        }

      case Equals(lhs, rhs) =>
        println(s"Equals check: $expr in: $map")

        evaluate(lhs) == evaluate(rhs)

      case Not(innerExpr) =>
        println(s"Not: $innerExpr in: $map")
        !evaluate(innerExpr)

      case Ands(exprs) =>
        exprs.map(evaluate).reduce(_ && _)

      case Ors(exprs) =>
        exprs.map(evaluate).reduce(_ || _)

      case GreaterThan(lhs, rhs) =>
        println(s"GreaterThan check: $expr in: $map")

        evaluate(lhs) > evaluate(rhs)

      case _: TrueLit =>
        true

      case _: FalseLit =>
        false

      case _ =>
        throw IllegalArgumentException("supported expression", expr.getClass.getSimpleName)
    }
  }

}
