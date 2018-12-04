package org.opencypher.memcypher.impl.types

import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.IllegalArgumentException

object CypherTypeOps {

  implicit class ScalaOrdering(ct: CypherType) {
    def ordering: Ordering[_] = ct.material match {
      case CTBoolean => Ordering.Boolean
      case CTFloat => Ordering.Float
      case CTInteger => Ordering.Long
      case CTString => Ordering.String
      case _: CTNode | _: CTRelationship => Ordering.Long
      case _ => throw IllegalArgumentException("Cypher type with ordering support", ct)
    }
  }

  implicit class ScalaEquivalence(ct: CypherType) {
    def equivalence: Equiv[_] = ct.material match {
      case CTBoolean => Equiv[Boolean]
      case CTFloat => Equiv[Float]
      case CTInteger => Equiv[Long]
      case CTString => Equiv[String]
      case _: CTNode | _: CTRelationship => Equiv[Long]
      case _ => throw IllegalArgumentException("Cypher type with equivalence support", ct)
    }
  }
}
