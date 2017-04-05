package org.opencypher.spark.api.ir

import org.opencypher.spark.api.types._

final case class Field(name: String)(val cypherType: CypherType = CTWildcard) {
  def escapedName: String = name.replaceAll("`", "``")
  override def toString = s"$name :: $cypherType"

  def toTypedTuple: (String, CypherType) = name -> cypherType
}
