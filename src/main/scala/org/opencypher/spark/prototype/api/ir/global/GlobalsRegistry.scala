package org.opencypher.spark.prototype.api.ir.global

import org.opencypher.spark.prototype.api.ir.global.GlobalsCollector._

object GlobalsRegistry {
  val none = GlobalsRegistry(
    labels = GlobalsCollector[LabelRef, Label].empty,
    relTypes = GlobalsCollector[RelTypeRef, RelType].empty,
    propertyKeys = GlobalsCollector[PropertyKeyRef, PropertyKey].empty,
    constants = GlobalsCollector[ConstantRef, Constant].empty
  )
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
