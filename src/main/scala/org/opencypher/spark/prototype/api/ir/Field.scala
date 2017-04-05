package org.opencypher.spark.prototype.api.ir

import org.opencypher.spark.prototype.api.types._

final case class Field(name: String)(val cypherType: CypherType = CTWildcard) {
  def escapedName: String = name.replaceAll("`", "``")
  override def toString = s"$name :: $cypherType"
}
