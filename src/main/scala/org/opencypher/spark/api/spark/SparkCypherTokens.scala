package org.opencypher.spark.api.spark

import org.opencypher.spark.api.ir.global._

// Lightweight wrapper around globals registry for exposure to users
final case class SparkCypherTokens(globals: GlobalsRegistry) {

  def labelName(id: Int): String = globals.label(LabelRef(id)).name
  def labelId(name: String): Int = globals.labelRefByName(name).id

  def propertyKeyName(id: Int): String = globals.propertyKey(PropertyKeyRef(id)).name
  def propertyKeyId(name: String): Int = globals.propertyKeyRefByName(name).id

  def relTypeName(id: Int): String = globals.relType(RelTypeRef(id)).name
  def relTypeId(name: String): Int = globals.relTypeRefByName(name).id

  def withLabel(name: String) = copy(globals = globals.withLabel(Label(name)))
  def withRelType(name: String) = copy(globals = globals.withRelType(RelType(name)))
  def withPropertyKey(name: String) = copy(globals = globals.withPropertyKey(PropertyKey(name)))
}


object SparkCypherTokens {
  val empty = SparkCypherTokens(GlobalsRegistry.empty)
}
