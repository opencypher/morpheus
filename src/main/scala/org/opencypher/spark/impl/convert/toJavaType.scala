package org.opencypher.spark.impl.convert

import org.opencypher.spark.api.value.{CypherInteger, CypherValue}

object toJavaType extends Serializable {

  // TODO: Replace with CypherValue.apply later (in other PR)
  def apply(v: CypherValue): Any = v match {
    case CypherInteger(long) => long
    case x => throw new IllegalArgumentException(s"Expected a (representation of a) Cypher value, but was $x (${x.getClass})")
  }
}
