package org.opencypher.okapi.ir.impl.parse.rewriter

import org.opencypher.v9_0.expressions.{FunctionInvocation, Property}
import org.opencypher.v9_0.util.{Rewriter, topDown}

case object unwrapFunctionInvocationInProperties extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance.apply(that)

  private val instance = topDown(Rewriter.lift {
    case p: Property => p.map match {
      case f: FunctionInvocation => rewriteProperty(p)
    }
  })

  private def rewriteProperty: Property => FunctionInvocation = {
    case expr@Property(map, key) => map match {
      case func@FunctionInvocation(_, _, _, _) => func
    }
  }
}
