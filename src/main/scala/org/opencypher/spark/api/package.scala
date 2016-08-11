package org.opencypher.spark

import scala.collection.immutable.ListMap

package object api {

  object implicits extends CypherImplicits

  object CypherRecord {
    def apply(elts: (String, CypherValue)*): CypherRecord =
      ListMap(elts: _*)
  }

  // Keys guaranteed to be in column order
  type CypherRecord = Map[String, CypherValue]

  type CypherNumberValue = CypherValue with IsNumber
  type CypherMapValue = CypherValue with HasProperties
  type CypherEntityValue = CypherValue with HasEntityId with HasProperties
}
