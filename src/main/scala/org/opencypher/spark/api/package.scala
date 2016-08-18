package org.opencypher.spark

import org.opencypher.spark.api.types.CTNull

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

  def cypherType(v: CypherValue) = v match {
    case m: MaterialCypherValue => m.cypherType
    case _                      => CTNull
  }

  object MaterialCypherValue {
    def unapply(v: Any) = v match {
      case m: MaterialCypherValue => Some(m.value)
      case _                      => None
    }
  }

  def cypherNull[T <: CypherValue] = null.asInstanceOf[T]

  type MaterialCypherValue = CypherValue with IsMaterial

  type CypherBoolean = MaterialCypherValue with IsBoolean
  type CypherNumber = MaterialCypherValue with IsNumber

  // TODO: Should this move into the Cypher type system?
  type CypherAnyMap = MaterialCypherValue with HasProperties
  type CypherEntity = CypherAnyMap with HasEntityId
}
