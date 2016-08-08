package org.opencypher

import scala.collection.immutable.ListMap

package object spark {

  type CypherNumberValue = CypherValue with ToNumber
  type CypherMapValue = CypherValue with HasProperties
  type CypherEntityValue = CypherValue with HasEntityId with HasProperties

  // Keys guaranteed to be in column order
  type CypherRecord = Map[String, CypherValue]

  object CypherRecord {
    def apply(elts: (String, CypherValue)*): CypherRecord = ListMap(elts:_*)
  }
}
