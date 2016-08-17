package org.opencypher.spark.impl

package object newvalue {
  def cypherNull[T <: CypherValue] = null.asInstanceOf[T]
}
