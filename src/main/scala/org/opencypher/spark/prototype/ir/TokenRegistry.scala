package org.opencypher.spark.prototype.ir

import org.opencypher.spark.prototype.ir.TokenCollector._

object TokenRegistry {
  val none = TokenRegistry(
    labels = TokenCollector[LabelRef, LabelDef].empty,
    relTypes = TokenCollector[RelTypeRef, RelTypeDef].empty,
    propertyKeys = TokenCollector[PropertyKeyRef, PropertyKeyDef].empty
  )
}

final case class TokenRegistry(labels: TokenCollection[LabelDef],
                               relTypes: TokenCollection[RelTypeDef],
                               propertyKeys: TokenCollection[PropertyKeyDef]
) {

  def label(name: String): LabelRef = labels.ref(name)
  def label(ref: LabelRef): LabelDef = labels.get(ref)

  def relType(name: String): RelTypeRef = relTypes.ref(name)
  def relType(ref: RelTypeRef): RelTypeDef = relTypes.get(ref)

  def propertyKey(name: String): PropertyKeyRef = propertyKeys.ref(name)
  def propertyKey(ref: PropertyKeyRef): PropertyKeyDef = propertyKeys.get(ref)

  def withLabel(defn: LabelDef): TokenRegistry = {
    val merged = labels.merge(defn)
    if (merged eq labels) this else copy(labels = merged)
  }

  def withRelType(defn: RelTypeDef): TokenRegistry = {
    val merged = relTypes.merge(defn)
    if (merged eq relTypes) this else copy(relTypes = merged)
  }

  def withPropertyKey(defn: PropertyKeyDef): TokenRegistry = {
    val merged = propertyKeys.merge(defn)
    if (merged eq propertyKeys) this else copy(propertyKeys = merged)
  }
}
