package org.opencypher.spark.prototype.ir.token

import org.opencypher.spark.prototype.ir.token.TokenCollector._

object TokenRegistry {
  val none = TokenRegistry(
    labels = TokenCollector[LabelRef, Label].empty,
    relTypes = TokenCollector[RelTypeRef, RelType].empty,
    propertyKeys = TokenCollector[PropertyKeyRef, PropertyKey].empty,
    params = TokenCollector[ParameterRef, Parameter].empty
  )
}

final case class TokenRegistry(
  labels: TokenCollection[Label],
  relTypes: TokenCollection[RelType],
  propertyKeys: TokenCollection[PropertyKey],
  params: TokenCollection[Parameter]
) {

  def label(name: String): LabelRef = labels.ref(name)
  def label(ref: LabelRef): Label = labels.get(ref)

  def relType(name: String): RelTypeRef = relTypes.ref(name)
  def relType(ref: RelTypeRef): RelType = relTypes.get(ref)

  def propertyKey(name: String): PropertyKeyRef = propertyKeys.ref(name)
  def propertyKey(ref: PropertyKeyRef): PropertyKey = propertyKeys.get(ref)

  def param(name: String): ParameterRef = params.ref(name)
  def param(ref: ParameterRef): Parameter = params.get(ref)

  def withLabel(defn: Label): TokenRegistry = {
    val merged = labels.merge(defn)
    if (merged eq labels) this else copy(labels = merged)
  }

  def withRelType(defn: RelType): TokenRegistry = {
    val merged = relTypes.merge(defn)
    if (merged eq relTypes) this else copy(relTypes = merged)
  }

  def withPropertyKey(defn: PropertyKey): TokenRegistry = {
    val merged = propertyKeys.merge(defn)
    if (merged eq propertyKeys) this else copy(propertyKeys = merged)
  }

  def withParam(defn: Parameter): TokenRegistry = {
    val merged = params.merge(defn)
    if (merged eq params) this else copy(params = merged)
  }
}
