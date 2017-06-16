package org.opencypher.spark.impl.convert

import org.opencypher.spark.api.types._

object fromJavaType extends Serializable {

  def apply(v: AnyRef): CypherType = v match {
    case null => CTVoid
    case _: String => CTString
    case _: java.lang.Byte => CTInteger
    case _: java.lang.Short => CTInteger
    case _: java.lang.Integer => CTInteger
    case _: java.lang.Long => CTInteger
    case _: java.lang.Float => CTFloat
    case _: java.lang.Double => CTFloat
    case _: java.lang.Boolean => CTBoolean
    case x => throw new IllegalArgumentException(s"Expected a (representation of a) Cypher value, but was $x (${x.getClass})")
  }
}
