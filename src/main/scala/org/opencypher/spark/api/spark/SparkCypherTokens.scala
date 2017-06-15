package org.opencypher.spark.api.spark

import org.opencypher.spark.api.ir.global._

// Lightweight wrapper around token registry for exposure to users
final case class SparkCypherTokens(registry: TokenRegistry) {

  def labelName(id: Int): String = registry.label(LabelRef(id)).name
  def labelId(name: String): Int = registry.labelRefByName(name).id

  def propertyKeyName(id: Int): String = registry.propertyKey(PropertyKeyRef(id)).name
  def propertyKeyId(name: String): Int = registry.propertyKeyRefByName(name).id

  def relTypeName(id: Int): String = registry.relType(RelTypeRef(id)).name
  def relTypeId(name: String): Int = registry.relTypeRefByName(name).id

  def withLabel(name: String) = copy(registry = registry.withLabel(Label(name)))
  def withRelType(name: String) = copy(registry = registry.withRelType(RelType(name)))
  def withPropertyKey(name: String) = copy(registry = registry.withPropertyKey(PropertyKey(name)))
}

object SparkCypherTokens {
  val empty = SparkCypherTokens(TokenRegistry.empty)
}
