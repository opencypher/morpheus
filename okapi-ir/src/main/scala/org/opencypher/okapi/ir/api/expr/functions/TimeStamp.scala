package org.opencypher.okapi.ir.api.expr.functions

import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.expressions.functions.Function
import org.opencypher.v9_0.util.symbols._

case object TimeStamp extends Function with TypeSignatures {
  def name = "timestamp"

  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(), outputType = CTMap)
  )
}
