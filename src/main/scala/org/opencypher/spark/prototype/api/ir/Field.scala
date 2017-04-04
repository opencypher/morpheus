package org.opencypher.spark.prototype.api.ir

import org.opencypher.spark.api.CypherType
import org.opencypher.spark.api.types.CTWildcard

final case class Field(name: String)(cypherType: CypherType = CTWildcard)
{
  def escapedName: String = name.replaceAll("`", "``")
  override def toString = s"$name :: $cypherType"
}
