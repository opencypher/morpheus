/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.okapi.api.io.conversion

import org.opencypher.okapi.api.types.{CypherType, DefiniteCypherType}
import org.opencypher.okapi.impl.exception.IllegalArgumentException

/**
  * Represents a map from node/relationship property keys to keys in the source data.
  */
trait EntityMapping {

  // TODO: CTEntity
  def cypherType: CypherType with DefiniteCypherType

  def sourceIdKey: String

  def propertyMapping: Map[String, String]

  def idKeys: Seq[String]

  def optionalLabelKeys: Seq[String] = Seq.empty

  def relTypeKeys: Seq[String] = Seq.empty

  def allSourceKeys: Seq[String] = idKeys ++ optionalLabelKeys ++ relTypeKeys ++ propertyMapping.values.toSeq.sorted

  protected def preventOverwritingProperty(propertyKey: String): Unit =
    if (propertyMapping.contains(propertyKey))
      throw IllegalArgumentException("unique property key definitions",
        s"given key $propertyKey overwrites existing mapping")

  protected def validate(): Unit = {
    val sourceKeys = allSourceKeys
    if (allSourceKeys.size != sourceKeys.toSet.size) {
      val duplicateColumns = sourceKeys.groupBy(identity).filter { case (_, items) => items.size > 1 }
      throw IllegalArgumentException(
        "One-to-one mapping from entity elements to source keys",
        s"Duplicate columns: $duplicateColumns")
    }
  }

}
