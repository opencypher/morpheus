package org.opencypher.spark.api

package object value {
  def cypherNull[T <: CypherValue] = null.asInstanceOf[T]
}
