package org.opencypher.spark.impl

import org.opencypher.spark.api.expr.Var
import org.opencypher.spark.api.ir.Field

import scala.language.implicitConversions

package object util {
  type SuccessfulAdditiveUpdateResult[T] = SuccessfulUpdateResult[T] with AdditiveUpdateResult[T]

  implicit def toVar(f: Field): Var = Var(f.name)(f.cypherType)
}
