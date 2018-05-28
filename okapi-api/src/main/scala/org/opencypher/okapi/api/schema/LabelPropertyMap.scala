/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.okapi.api.schema

import cats.instances.all._
import cats.syntax.semigroup._
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.api.types.CypherType.joinMonoid
import ujson.Js.Value
import upickle.Js
import upickle.default._

object PropertyKeys {
  type PropertyKeys = Map[String, CypherType]

  def empty = Map.empty[String, CypherType]

  def apply(tuple: (String, CypherType)*) = {
    tuple.toMap
  }
}

object LabelPropertyMap {

  val empty: LabelPropertyMap = LabelPropertyMap(Map.empty)

  implicit def rw: ReadWriter[LabelPropertyMap] = readwriter[Js.Value].bimap[LabelPropertyMap](
    labelPropertyMap => {
      val jsonEntries = labelPropertyMap.map.map {
        case (labelCombo, propKeys) => Js.Obj("labels" -> writeJs(labelCombo), "properties" -> writeJs(propKeys))
      }
      jsonEntries
    },
    (json: Value) => {
      val content = json.arr
        .map(value => readJs[Set[String]](value.obj("labels")) -> readJs[PropertyKeys](value.obj("properties")))
      LabelPropertyMap(content.toMap)
    }
  )
}

/**
  * Maps (a set of) labels to typed property keys.
  */
final case class LabelPropertyMap(map: Map[Set[String], PropertyKeys]) {

  /**
    * Registers the given property keys to the specified labels.
    *
    * @note This will override any previous binding for the label combination.
    * @param labels     set of labels
    * @param properties property keys for the given set of labels
    * @return updated LabelPropertyMap
    */
  def register(labels: Set[String], properties: PropertyKeys): LabelPropertyMap =
    copy(map.updated(labels, properties))

  /**
    * Returns the property keys that are associated with the given set of labels.
    *
    * @param labels set of labels
    * @return associated property keys
    */
  def properties(labels: Set[String]): PropertyKeys =
    map.getOrElse(labels, PropertyKeys.empty)

  /**
    * Merges this LabelPropertyMap with the given map. Property keys for label sets that exist in both maps are being
    * merged, diverging types are being joined.
    *
    * @param other LabelPropertyMap to merge
    * @return merged LabelPropertyMap
    */
  def ++(other: LabelPropertyMap): LabelPropertyMap = copy(map |+| other.map)

  /**
    * Returns the label property map with the given label combination `combo` removed.
    *
    * @param combo label combination to remove
    * @return updated label property map
    */
  def -(combo: Set[String]): LabelPropertyMap = {
    copy(map - combo)
  }

  /**
    * Returns a LabelPropertyMap that contains all label combinations which include one or more of the specified labels.
    *
    * @param knownLabels labels for which the properties should be extracted
    * @return extracted label property map
    */
  def filterForLabels(knownLabels: Set[String]): LabelPropertyMap =
    LabelPropertyMap(map.filterKeys(_.exists(knownLabels.contains)))

  /**
    * Returns all registered combinations of labels
    *
    * @return all registered label combinations.
    */
  def labelCombinations: Set[Set[String]] = map.keySet

  // utility signatures

  def register(labels: String*)(keys: (String, CypherType)*): LabelPropertyMap =
    register(labels.toSet, keys.toMap)

  def register(label: String, properties: PropertyKeys): LabelPropertyMap =
    register(Set(label), properties)

  def properties(label: String*): PropertyKeys =
    properties(Set(label: _*))

  def filterForLabels(labels: String*): LabelPropertyMap =
    filterForLabels(labels.toSet)

}
