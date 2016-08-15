package org.opencypher.spark.impl

import org.opencypher.spark.api.{CypherField, CypherType}

object StdField {

  def apply(pair: (Symbol, CypherType)): StdField = new StdField(pair._1, pair._2)
}

case class StdField(sym: Symbol, cypherType: CypherType) extends CypherField {
  // TODO: Properly escape here
  val columnSym: Symbol = Symbol(sym.name.filterNot(_ == '.').filterNot( _ == '(').filterNot(_ == ')'))
}
