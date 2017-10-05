/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.caps.ir.api.global

import org.opencypher.caps.api.schema.VerifiedSchema
import org.opencypher.caps.common.RefCollection
import org.opencypher.caps.impl.syntax.register._

import scala.util.Try

// TODO: Get rid of this
object GlobalsRegistry {
  val empty = GlobalsRegistry()

  def fromTokens(tokens: TokenRegistry) = GlobalsRegistry(tokens)
  def fromSchema(verified: VerifiedSchema) = GlobalsRegistry(TokenRegistry.fromSchema(verified))
}

final case class GlobalsRegistry(
  tokens: TokenRegistry = TokenRegistry.empty,
  constants: ConstantRegistry = ConstantRegistry.empty
) {
  def mapTokens(f: TokenRegistry => TokenRegistry) = copy(tokens = f(tokens))
  def mapConstants(f: ConstantRegistry => ConstantRegistry) = copy(constants = f(constants))
}

object ConstantRegistry {
  val empty = ConstantRegistry()
}

// Constants are globals that are provided as part of invoking a query
final case class ConstantRegistry(constants: RefCollection[Constant] = RefCollection.empty[Constant]) {

  self =>

  def constantByName(name: String): Constant = constant(constantRefByName(name))
  def constant(ref: ConstantRef): Constant = constants.lookup(ref).get

  def constantRefByName(name: String): ConstantRef = constants.findByKey(name).get
  def constantRef(defn: Constant): ConstantRef = constantRefByName(defn.name)

  def withConstant(defn: Constant): ConstantRegistry = {
    constants.insert(defn) match {
      case Right((Some(newConstants), _)) => copy(constants = newConstants)
      case _ => self
    }
  }

  def contains(name: String): Boolean = Try(constantRefByName(name)).isSuccess
}

object TokenRegistry {
  val empty = TokenRegistry(
    labels = RefCollection.empty[Label],
    relTypes = RefCollection.empty[RelType],
    propertyKeys = RefCollection.empty[PropertyKey]
  )

  def fromSchema(verified: VerifiedSchema): TokenRegistry = {
    fromSchema(TokenRegistry.empty, verified)
  }

  def fromSchema(existing: TokenRegistry, verified: VerifiedSchema): TokenRegistry = {
    val schema = verified.schema
    val withLabels = schema.labels.foldLeft(existing) { case (acc, l) => acc.withLabel(Label(l)) }
    val withKeys = schema.keys.foldLeft(withLabels) { case (acc, name) => acc.withPropertyKey(PropertyKey(name)) }
    val withTypes = schema.relationshipTypes.foldLeft(withKeys) { case (acc, name) => acc.withRelType(RelType(name)) }
    withTypes
  }
}

// Tokens are all globals that materially occur in the data graph
// (even though they may initially be registered based on a query)
final case class TokenRegistry(
  labels: RefCollection[Label],
  relTypes: RefCollection[RelType],
  propertyKeys: RefCollection[PropertyKey]
) {

  self =>

  def ++(other: TokenRegistry) = {
    val newLabels = labels ++ other.labels
    val newRelTypes = relTypes ++ other.relTypes
    val newPropertyKeys = propertyKeys ++ other.propertyKeys

    copy(newLabels,
      newRelTypes,
      newPropertyKeys)
  }

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

  def withLabel(defn: Label): TokenRegistry = {
    labels.insert(defn) match {
      case Right((Some(newLabels), _)) => copy(labels = newLabels)
      case _ => self
    }
  }

  def withRelType(defn: RelType): TokenRegistry = {
    relTypes.insert(defn) match {
      case Right((Some(newTypes), _)) => copy(relTypes = newTypes)
      case _ => self
    }
  }

  def withPropertyKey(defn: PropertyKey): TokenRegistry = {
    propertyKeys.insert(defn) match {
      case Right((Some(newKeys), _)) => copy(propertyKeys = newKeys)
      case _ => self
    }
  }
}
