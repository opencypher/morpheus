package org.opencypher

import org.opencypher.spark.api.expr.Var

import scala.language.implicitConversions

package object spark {

  implicit def toVar(s: Symbol): Var = Var(s.name)()
}
