package org.opencypher.spark.prototype.api.ir.global

import org.opencypher.spark.prototype.api.ir.global.GlobalsCollector._
import org.opencypher.spark.prototype.api.schema.VerifiedSchema

// TODO: Use Register typeclass
object GlobalsRegistry {
  val none = GlobalsRegistry(
    labels = GlobalsCollector[LabelRef, Label].empty,
    relTypes = GlobalsCollector[RelTypeRef, RelType].empty,
    propertyKeys = GlobalsCollector[PropertyKeyRef, PropertyKey].empty,
    constants = GlobalsCollector[ConstantRef, Constant].empty
  )

  def fromSchema(verified: VerifiedSchema): GlobalsRegistry = {
    val schema = verified.schema
    val withLabels = schema.labels.foldLeft(GlobalsRegistry.none) { case (acc, l) => acc.withLabel(Label(l)) }
    val withKeys = schema.keys.foldLeft(withLabels) { case (acc, name) => acc.withPropertyKey(PropertyKey(name)) }
    val withTypes = schema.relationshipTypes.foldLeft(withKeys) { case (acc, name) => acc.withRelType(RelType(name)) }
    withTypes
  }
}

final case class GlobalsRegistry(
  labels: GlobalsCollection[Label],
  relTypes: GlobalsCollection[RelType],
  propertyKeys: GlobalsCollection[PropertyKey],
  constants: GlobalsCollection[Constant]
) {

  def label(name: String): LabelRef = labels.ref(name)
  def label(ref: LabelRef): Label = labels.get(ref)

  def relType(name: String): RelTypeRef = relTypes.ref(name)
  def relType(ref: RelTypeRef): RelType = relTypes.get(ref)

  def propertyKey(name: String): PropertyKeyRef = propertyKeys.ref(name)
  def propertyKey(ref: PropertyKeyRef): PropertyKey = propertyKeys.get(ref)

  def constant(name: String): ConstantRef = constants.ref(name)
  def constant(ref: ConstantRef): Constant = constants.get(ref)

  def withLabel(defn: Label): GlobalsRegistry = {
    val merged = labels.merge(defn)
    if (merged eq labels) this else copy(labels = merged)
  }

  def withRelType(defn: RelType): GlobalsRegistry = {
    val merged = relTypes.merge(defn)
    if (merged eq relTypes) this else copy(relTypes = merged)
  }

  def withPropertyKey(defn: PropertyKey): GlobalsRegistry = {
    val merged = propertyKeys.merge(defn)
    if (merged eq propertyKeys) this else copy(propertyKeys = merged)
  }

  def withConstant(defn: Constant): GlobalsRegistry = {
    val merged = constants.merge(defn)
    if (merged eq constants) this else copy(constants = merged)
  }
}
