package org.opencypher.spark.prototype.api

package object value {
  def cypherNull[T <: CypherValue] = null.asInstanceOf[T]
}
