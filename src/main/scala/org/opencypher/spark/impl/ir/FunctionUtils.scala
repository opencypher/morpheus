package org.opencypher.spark.impl.ir

import org.neo4j.cypher.internal.frontend.v3_2.ast.{FunctionInvocation, _}
import org.opencypher.spark.api.expr.{Expr, FunctionExpr, Id}
import org.opencypher.spark.api.types.CypherType

object FunctionUtils {

  implicit class RichFunctionInvocation(functionInvocation: FunctionInvocation) {
    def toCAPSFunction(expr: IndexedSeq[Expr], cypherType: CypherType): FunctionExpr = {
      functionInvocation.function match {
        case functions.Id => Id(expr.head)(cypherType)
      }
    }
  }
}
