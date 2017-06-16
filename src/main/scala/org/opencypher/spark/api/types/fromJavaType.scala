package org.opencypher.spark.api.types

object fromJavaType extends Serializable {

  def apply(v: AnyRef): CypherType = v match {
    case null => CTVoid
    case _: String => CTString
    case _: java.lang.Integer => CTInteger
    case _: java.lang.Long => CTInteger
    case _: java.lang.Float => CTFloat
    case _: java.lang.Double => CTFloat
    case _: java.lang.Boolean => CTBoolean
    case x => throw new IllegalArgumentException(s"Expected a (representation of a) Cypher value, but was $x")
  }
}
