package org.opencypher.spark.api.value

import scala.collection.immutable.SortedMap

import scala.language.implicitConversions

object Properties {

  val empty = new Properties(SortedMap.empty)

  def apply(elts: (String, CypherValue)*) =
    fromMap(SortedMap(elts: _*))

  implicit def fromMap(v: Map[String, CypherValue]): Properties = {
    if (v == null) throw new IllegalArgumentException("Property map must not be null")
    v match {
      case m: SortedMap[String, CypherValue] if m.ordering eq Ordering.String =>
        new Properties(m)

      case _ =>
        new Properties(SortedMap(v.toSeq: _*)(Ordering.String))
    }
  }
}

final class Properties private (val m: SortedMap[String, CypherValue]) extends AnyVal with Serializable {

  def apply(key: String) = m.getOrElse(key, cypherNull)
  def get(key: String) = m.get(key)

  def containsNullValue: Boolean = m.values.exists(CypherValue.isComparable)
}
