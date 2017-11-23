/*
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
package org.opencypher.caps.api.schema

import org.opencypher.caps.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.caps.api.types.CypherType

object PropertyKeys {
  type PropertyKeys = Map[String, CypherType]

  def empty = Map.empty[String, CypherType]

}

object LabelPropertyMap {

  val empty: LabelPropertyMap = LabelPropertyMap(Map.empty)
}

/**
  * Maps (a set of) labels to typed property keys.
  */
final case class LabelPropertyMap(map: Map[Set[String], PropertyKeys]) {

  /*
    * TODO: Doc this one
    * @param labels
    * @param properties
    * @return
    */
  def register(labels: Set[String], properties: PropertyKeys): LabelPropertyMap = {
    copy(map.updated(labels, properties))
  }

  /* // TODO: doc this one
    *
    * @param labels
    * @return
    */
  def properties(labels: Set[String]): PropertyKeys =
    map.getOrElse(labels, PropertyKeys.empty)

  def ++(other: LabelPropertyMap): LabelPropertyMap =
    LabelPropertyMap(map ++ other.map)

  def forLabels(labels: Set[String]): LabelPropertyMap = {
    val existing = LabelPropertyMap(map.filterKeys(_.exists(labels.contains)))
    if (existing.map.contains(labels)) existing else LabelPropertyMap(Map(labels -> PropertyKeys.empty))
  }

  // utility signatures

  def register(labels: String*)(keys: (String, CypherType)*): LabelPropertyMap =
    register(labels.toSet, keys.toMap)

  def register(label: String, properties: PropertyKeys): LabelPropertyMap =
    register(Set(label), properties)

  def properties(label: String*): PropertyKeys =
    properties(Set(label: _*))

  def forLabels(labels: String*): LabelPropertyMap =
    forLabels(labels.toSet)

  def labelCombinations: Set[Set[String]] = map.keySet

}
