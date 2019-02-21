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

import org.opencypher.okapi.api.graph.{Entity, IdKey, Pattern}
import org.opencypher.okapi.api.types.CTRelationship
import org.opencypher.okapi.impl.exception.IllegalArgumentException

object EntityMapping {
  def empty(pattern: Pattern) = EntityMapping(pattern, Map.empty, Map.empty)
}

// TODO document
// TODO add builder in Node/RelPattern style
case class EntityMapping(
  pattern: Pattern,
  properties: Map[Entity, Map[String, String]],
  idKeys: Map[Entity, Map[IdKey, String]]
) {

  validate()

  def allSourceKeys: Seq[String] =
    (
      idKeys.values.flatten.map(_._2).toSeq ++
      properties.values.flatten.map(_._2)
    ).sorted

  protected def validate(): Unit = {
    val sourceKeys = allSourceKeys
    if (allSourceKeys.size != sourceKeys.toSet.size) {
      val duplicateColumns = sourceKeys.groupBy(identity).filter { case (_, items) => items.size > 1 }
      throw IllegalArgumentException(
        "One-to-one mapping from entity elements to source keys",
        s"Duplicate columns: $duplicateColumns")
    }

    pattern.entities.foreach {
      case e@Entity(_, CTRelationship(types, _)) if types.size != 1 =>
        throw IllegalArgumentException(
          s"A single implied type for entity $e",
          types
        )
    }

  }
}

trait EntityMappingBuilder {

  type BuilderType <: EntityMappingBuilder

  def propertyMapping: Map[String, String]

  protected def updatePropertyMapping(propertyMapping: Map[String, String]): BuilderType

  def build: EntityMapping

  protected def validate(): Unit

  def withPropertyKey(tuple: (String, String)): BuilderType =
    withPropertyKey(tuple._1, tuple._2)

  def withPropertyKey(property: String): BuilderType =
    withPropertyKey(property, property)

  def withPropertyKey(propertyKey: String, sourcePropertyKey: String): BuilderType = {
    preventOverwritingProperty(propertyKey)
    updatePropertyMapping(propertyMapping.updated(propertyKey, sourcePropertyKey))
  }

  def withPropertyKeys(properties: String*): BuilderType = {
    if (properties.size != properties.toSet.size)
      throw IllegalArgumentException("unique propertyKey definitions",
        s"given key $properties overwrites existing mapping")

    withPropertyKeyMappings(properties.map(p => p -> p):_ *)
  }

  def withPropertyKeyMappings(tuples: (String, String)*): BuilderType = {
    val updatedMapping = tuples.foldLeft(propertyMapping) {
      case (oldMapping, (propertyKey, source)) =>
        preventOverwritingProperty(propertyKey)
        oldMapping.updated(propertyKey, source)
    }

    updatePropertyMapping(updatedMapping)
  }

  protected def preventOverwritingProperty(propertyKey: String): Unit =
    if (propertyMapping.contains(propertyKey))
      throw IllegalArgumentException("unique property key definitions",
        s"given key $propertyKey overwrites existing mapping")

}
