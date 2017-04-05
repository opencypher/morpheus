package org.opencypher.spark.impl

import org.opencypher.spark.api.frame.CypherField
import org.opencypher.spark.impl.util.SparkIdentifier
import org.opencypher.spark.prototype.api.types.CypherType

object StdField {
  def apply(pair: (Symbol, CypherType)): StdField = new StdField(pair._1, pair._2)
}

final case class StdField(sym: Symbol, cypherType: CypherType) extends CypherField {
  val column = SparkIdentifier.fromString(sym.name)
}
