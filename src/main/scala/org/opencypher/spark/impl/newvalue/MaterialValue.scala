package org.opencypher.spark.impl.newvalue

final case class MaterialValue[V <: CypherValue] private[newvalue](val v: V) extends AnyVal {
  def isIndeterminate = v == null
}
