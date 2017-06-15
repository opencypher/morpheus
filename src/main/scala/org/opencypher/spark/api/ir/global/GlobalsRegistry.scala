package org.opencypher.spark.api.ir.global

import org.opencypher.spark.api.schema.VerifiedSchema
import org.opencypher.spark.impl.syntax.register._
import org.opencypher.spark.impl.util.RefCollection

object GlobalsRegistry {
  val empty = GlobalsRegistry(
    labels = RefCollection.empty[Label],
    relTypes = RefCollection.empty[RelType],
    propertyKeys = RefCollection.empty[PropertyKey],
    constants = RefCollection.empty[Constant]
  )

  def fromSchema(verified: VerifiedSchema): GlobalsRegistry = {
    val schema = verified.schema
    val withLabels = schema.labels.foldLeft(GlobalsRegistry.empty) { case (acc, l) => acc.withLabel(Label(l)) }
    val withKeys = schema.keys.foldLeft(withLabels) { case (acc, name) => acc.withPropertyKey(PropertyKey(name)) }
    val withTypes = schema.relationshipTypes.foldLeft(withKeys) { case (acc, name) => acc.withRelType(RelType(name)) }
    withTypes
  }
}

final case class GlobalsRegistry(
  labels: RefCollection[Label],
  relTypes: RefCollection[RelType],
  propertyKeys: RefCollection[PropertyKey],
  constants: RefCollection[Constant]
) {

  self =>

  def labelByName(name: String): Label = label(labelRefByName(name))
  def label(ref: LabelRef): Label = labels.lookup(ref).get

  def labelRefByName(name: String): LabelRef = labels.findByKey(name).get
  def labelRef(defn: Label): LabelRef = labelRefByName(defn.name)

  def relTypeByName(name: String): RelType = relType(relTypeRefByName(name))
  def relType(ref: RelTypeRef): RelType = relTypes.lookup(ref).get

  def relTypeRefByName(name: String): RelTypeRef = relTypes.findByKey(name).get
  def relTypeRef(defn: RelType): RelTypeRef = relTypeRefByName(defn.name)

  def propertyKeyByName(name: String): PropertyKey = propertyKey(propertyKeyRefByName(name))
  def propertyKey(ref: PropertyKeyRef): PropertyKey = propertyKeys.lookup(ref).get

  def propertyKeyRefByName(name: String): PropertyKeyRef = propertyKeys.findByKey(name).get
  def propertyKeyRef(defn: PropertyKey): PropertyKeyRef = propertyKeyRefByName(defn.name)

  def constantByName(name: String): Constant = constant(constantRefByName(name))
  def constant(ref: ConstantRef): Constant = constants.lookup(ref).get

  def constantRefByName(name: String): ConstantRef = constants.findByKey(name).get
  def constantRef(defn: Constant): ConstantRef = constantRefByName(defn.name)

  def withLabel(defn: Label): GlobalsRegistry = {
    labels.insert(defn) match {
      case Right((Some(newLabels), _)) => copy(labels = newLabels)
      case _ => self
    }
  }

  def withRelType(defn: RelType): GlobalsRegistry = {
    relTypes.insert(defn) match {
      case Right((Some(newTypes), _)) => copy(relTypes = newTypes)
      case _ => self
    }
  }

  def withPropertyKey(defn: PropertyKey): GlobalsRegistry = {
    propertyKeys.insert(defn) match {
      case Right((Some(newKeys), _)) => copy(propertyKeys = newKeys)
      case _ => self
    }
  }

  def withConstant(defn: Constant): GlobalsRegistry = {
    constants.insert(defn) match {
      case Right((Some(newConstants), _)) => copy(constants = newConstants)
      case _ => self
    }
  }
}
