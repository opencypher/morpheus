package org.opencypher.spark.impl.prototype

import TokenRegistration._

object TokenDefs {
  val none = TokenDefs(
    labels = TokenRegistration[LabelRef, LabelDef].empty,
    relTypes = TokenRegistration[RelTypeRef, RelTypeDef].empty,
    propertyKeys = TokenRegistration[PropertyKeyRef, PropertyKeyDef].empty
  )
}

final case class TokenDefs(
  labels: Registry[LabelDef],
  relTypes: Registry[RelTypeDef],
  propertyKeys: Registry[PropertyKeyDef]
) {

  def label(name: String): LabelRef = labels.ref(name)
  def label(ref: LabelRef): LabelDef = labels.get(ref)

  def relType(name: String): RelTypeRef = relTypes.ref(name)
  def relType(ref: RelTypeRef): RelTypeDef = relTypes.get(ref)

  def propertyKey(name: String): PropertyKeyRef = propertyKeys.ref(name)
  def propertyKey(ref: PropertyKeyRef): PropertyKeyDef = propertyKeys.get(ref)

  def withLabel(defn: LabelDef): TokenDefs = {
    val merged = labels.merge(defn)
    if (merged eq labels) this else copy(labels = merged)
  }

  def withRelType(defn: RelTypeDef): TokenDefs = {
    val merged = relTypes.merge(defn)
    if (merged eq relTypes) this else copy(relTypes = merged)
  }

  def withPropertyKey(defn: PropertyKeyDef): TokenDefs = {
    val merged = propertyKeys.merge(defn)
    if (merged eq propertyKeys) this else copy(propertyKeys = merged)
  }
}
