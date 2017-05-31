package org.opencypher

import org.opencypher.spark.api.expr.Var
import org.opencypher.spark.api.ir.Field
import org.opencypher.spark.api.types.CypherType

import scala.language.implicitConversions

package object spark {

  implicit def toVar(s: Symbol): Var = Var(s.name)()
  implicit def toField(s: Symbol): Field = Field(s.name)()
  implicit def toField(t: (Symbol, CypherType)): Field = Field(t._1.name)(t._2)

}
