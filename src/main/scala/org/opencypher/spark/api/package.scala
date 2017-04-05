package org.opencypher.spark

import org.opencypher.spark.prototype.api.types.CypherType
import org.opencypher.spark.prototype.api.value.CypherValue

import scala.collection.immutable.ListMap

package object api {

  object implicits extends CypherImplicits

  type TypedSymbol = (Symbol, CypherType)
  type Alias = (Symbol, Symbol)

  object CypherRecord {
    def apply(elts: (String, CypherValue)*): CypherRecord =
      ListMap(elts: _*)
  }

  // Keys guaranteed to be in column order
  type CypherRecord = Map[String, CypherValue]

}
