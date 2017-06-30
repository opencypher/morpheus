package org.opencypher.spark.impl.convert

import org.opencypher.spark.api.value.{CypherInteger, CypherValue}

object toJavaType extends Serializable {

  // TODO: Test and complete
  def apply(v: CypherValue): Any = v match {
    case CypherInteger(long) => long
    case x => throw new IllegalArgumentException(s"Expected a (representation of a) Cypher value, but was $x (${x.getClass})")
  }
}
